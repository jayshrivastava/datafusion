// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Input distribution requirements for physical execution plans.

use std::sync::Arc;

use datafusion_common::utils::compare_rows;
use datafusion_common::{Result, internal_err};
use datafusion_physical_expr::{
    Distribution, EquivalenceProperties, Partitioning, PartitioningSatisfaction,
    RangePartitioning,
};

use crate::execution_plan::{ExecutionPlan, ExecutionPlanProperties, InvariantLevel};

/// Distribution requirements for an [`ExecutionPlan`]'s inputs.
///
/// [`InputDistributionRequirements`] describes what distribution an operator
/// requires from each child.
///
/// - [`Self::new`] describes independent per-child requirements.
/// - [`Self::co_partitioned`] additionally requires child partitions with the
///   same index to cover compatible key ranges.
/// - [`Self::try_with_child_partitioning`] additionally requires a concrete
///   layout, including a partition count and any range bounds.
///
/// For a single-input aggregate:
///
/// ```text
/// AggregateExec
///   child 0 requirement: KeyPartitioned(group_exprs)
/// ```
///
/// each input partition can aggregate its own key domain independently.
///
/// For a partitioned join:
///
/// ```text
/// HashJoinExec
///   child 0 requirement: KeyPartitioned(left_keys)
///   child 1 requirement: KeyPartitioned(right_keys)
///
///   partition 0: join(left partition 0, right partition 0)
///   partition 1: join(left partition 1, right partition 1)
///   partition 2: join(left partition 2, right partition 2)
/// ```
///
/// each child must satisfy its own key requirement. In addition, matching
/// partition indexes must be safe to process together.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct InputDistributionRequirements {
    /// Per-child distribution requirements, indexed by child position.
    children: Vec<ChildDistributionRequirement>,
    /// Child indexes that must also have compatible partition layouts.
    co_partitioned: Option<Vec<usize>>,
}

/// Options for checking child distribution satisfaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChildSatisfactionOptions {
    allow_subset: bool,
}

impl ChildSatisfactionOptions {
    /// Create default satisfaction options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allow a child partitioning whose key expressions are a subset of the
    /// required key expressions to satisfy the requirement.
    pub fn with_allow_subset(mut self, allow_subset: bool) -> Self {
        self.allow_subset = allow_subset;
        self
    }

    /// Whether subset satisfaction is enabled.
    pub fn allow_subset(&self) -> bool {
        self.allow_subset
    }
}

impl InputDistributionRequirements {
    /// Create independent per-child requirements.
    pub fn new(per_child: Vec<Distribution>) -> Self {
        let children = per_child
            .into_iter()
            .map(|distribution| ChildDistributionRequirement {
                distribution,
                partitioning: None,
            })
            .collect();

        Self {
            children,
            co_partitioned: None,
        }
    }

    /// Create a requirement that all children are co-partitioned.
    ///
    /// Each child must satisfy its own [`Distribution`]. Matching partition
    /// indexes are processed together:
    ///
    /// ```text
    /// left:  Range(left.a ASC,  split_points=[10, 20])
    /// right: Range(right.x ASC, split_points=[10, 20])
    ///
    /// partition 0 from both sides contains keys before 10
    /// partition 1 from both sides contains keys in [10, 20)
    /// partition 2 from both sides contains keys at/after 20
    /// ```
    ///
    /// If the split points differ, partition `i` from one side no longer covers
    /// the same key range as partition `i` from the other side.
    pub fn co_partitioned(per_child: Vec<Distribution>) -> Self {
        debug_assert!(
            per_child.len() >= 2,
            "co-partitioned distribution requirements need at least two children"
        );
        let co_partitioned = (0..per_child.len()).collect();
        let mut result = Self::new(per_child);
        result.co_partitioned = Some(co_partitioned);
        result
    }

    /// Return the per-child distribution requirements.
    pub fn per_child_distributions(
        &self,
    ) -> impl ExactSizeIterator<Item = &Distribution> + '_ {
        self.children
            .iter()
            .map(ChildDistributionRequirement::distribution)
    }

    /// Whether these requirements include a co-partitioning relationship between children.
    pub fn is_co_partitioned(&self) -> bool {
        self.co_partitioned.is_some()
    }

    /// Return the distribution requirement for a child.
    pub fn child_distribution(&self, child_idx: usize) -> Option<&Distribution> {
        self.children
            .get(child_idx)
            .map(ChildDistributionRequirement::distribution)
    }

    /// Require a concrete partitioning for one child, in addition to its
    /// [`Distribution`] requirement.
    ///
    /// Expressions refer to the child's schema. The partition count is fixed,
    /// regardless of the optimizer's preferred parallelism. For a range layout,
    /// the routing keys and split points must match, and the child's bounds must
    /// fit within the required bounds; narrower child bounds are allowed.
    /// [`Partitioning::UnknownPartitioning`] constrains only the partition count.
    /// A one-partition layout also exposes the distribution as
    /// [`Distribution::SinglePartition`].
    ///
    /// This does not authorize discarding rows. If the input does not prove that
    /// all its rows fit within required bounds, distribution enforcement fails
    /// rather than filtering the input or inserting an unsafe repartition.
    pub fn try_with_child_partitioning(
        mut self,
        child_idx: usize,
        partitioning: Partitioning,
    ) -> Result<Self> {
        let Some(child) = self.children.get_mut(child_idx) else {
            return internal_err!(
                "missing distribution requirement for child {child_idx}"
            );
        };
        if partitioning.partition_count() == 0 {
            return internal_err!(
                "required partitioning must have at least one partition"
            );
        }
        child.partitioning = Some(partitioning);
        Ok(self)
    }

    /// Return the optional concrete partitioning requirement for a child.
    pub fn child_partitioning(&self, child_idx: usize) -> Option<&Partitioning> {
        self.children
            .get(child_idx)
            .and_then(|child| child.partitioning.as_ref())
    }

    /// Return the per-child distribution requirements.
    ///
    /// WARNING: This intentionally drops concrete partitioning requirements and
    /// any grouped relationship.
    pub fn into_per_child(self) -> Vec<Distribution> {
        self.children
            .into_iter()
            .map(|child| {
                if child.requires_single_partition() {
                    Distribution::SinglePartition
                } else {
                    child.distribution
                }
            })
            .collect()
    }

    /// Returns how a child satisfies its distribution requirement.
    ///
    /// This preserves the requirement set's satisfaction policy.
    pub fn child_satisfaction(
        &self,
        child_idx: usize,
        child: &dyn ExecutionPlan,
        options: ChildSatisfactionOptions,
    ) -> Result<PartitioningSatisfaction> {
        let Some(requirement) = self.children.get(child_idx) else {
            return internal_err!(
                "missing distribution requirement for child {child_idx}"
            );
        };

        if let Some(partitioning) = &requirement.partitioning
            && !satisfies_partitioning(
                child.output_partitioning(),
                partitioning,
                child.equivalence_properties(),
            )?
        {
            return Ok(PartitioningSatisfaction::NotSatisfied);
        }

        Ok(child.output_partitioning().satisfaction(
            &requirement.distribution,
            child.equivalence_properties(),
            options.allow_subset(),
        ))
    }

    /// Whether the child's known bounds allow a row-preserving repartition to
    /// its concrete requirement. This does not check whether its current layout
    /// already satisfies the requirement.
    ///
    /// # Public Only for Internal Use:
    ///
    /// This is not a public API and is for internal use only; see [API policy] for details.
    ///
    /// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
    #[doc(hidden)]
    pub fn can_repartition_child(
        &self,
        child_idx: usize,
        child: &dyn ExecutionPlan,
    ) -> Result<bool> {
        match self.child_partitioning(child_idx) {
            Some(required) => satisfies_bounds(
                child.output_partitioning(),
                required,
                child.equivalence_properties(),
            ),
            None => Ok(true),
        }
    }

    /// Return child indexes whose co-partitioning requirements are
    /// unsatisfied by the provided candidate children.
    ///
    /// Independent per-child requirements are intentionally ignored here, use
    /// [`Self::child_satisfaction`] for those checks. An empty result means all
    /// co-partitioning requirements are satisfied.
    ///
    /// # Public Only for Internal Use:
    ///
    /// This is not a public API and is for internal use only; see [API policy] for details.
    ///
    /// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
    #[doc(hidden)]
    pub fn unsatisfied_co_partitioned_children(
        &self,
        plan_name: &str,
        children: &[&dyn ExecutionPlan],
    ) -> Result<Vec<usize>> {
        self.validate_shape(plan_name, children.len())?;

        let Some(co_partitioned) = &self.co_partitioned else {
            return Ok(vec![]);
        };
        if self.co_partitioning_satisfied(co_partitioned, children)? {
            return Ok(vec![]);
        }

        Ok(co_partitioned.clone())
    }

    /// Validate the requirements against a plan's children.
    #[inline]
    pub(crate) fn check_invariants(
        &self,
        plan_name: &str,
        children: Vec<&Arc<dyn ExecutionPlan>>,
        check: InvariantLevel,
    ) -> Result<()> {
        let children_len = children.len();
        if self.children.len() != children_len {
            return self.validate_shape(plan_name, children_len);
        }

        if matches!(check, InvariantLevel::Executable) {
            for (idx, child) in children.iter().enumerate() {
                if let Some(required) = self.child_partitioning(idx)
                    && !self
                        .child_satisfaction(
                            idx,
                            child.as_ref(),
                            ChildSatisfactionOptions::new().with_allow_subset(true),
                        )?
                        .is_satisfied()
                {
                    return internal_err!(
                        "{plan_name} requires child {idx} partitioning {required}, got {}",
                        child.output_partitioning()
                    );
                }
            }
        }

        if let Some(co_partitioned) = &self.co_partitioned {
            self.validate_shape(plan_name, children_len)?;
            if matches!(check, InvariantLevel::Executable) {
                return self.check_co_partitioning_invariant(
                    plan_name,
                    co_partitioned,
                    children,
                );
            }
        }

        Ok(())
    }

    fn check_co_partitioning_invariant(
        &self,
        plan_name: &str,
        co_partitioned: &[usize],
        children: Vec<&Arc<dyn ExecutionPlan>>,
    ) -> Result<()> {
        let children = children
            .into_iter()
            .map(|child| child.as_ref())
            .collect::<Vec<_>>();
        if !self.co_partitioning_satisfied(co_partitioned, &children)? {
            return internal_err!(
                "{} requires children {:?} to be co-partitioned",
                plan_name,
                co_partitioned
            );
        }

        Ok(())
    }

    fn validate_shape(&self, plan_name: &str, children_len: usize) -> Result<()> {
        if self.children.len() != children_len {
            return internal_err!(
                "{plan_name}::input_distribution_requirements returned incorrect child count: {} != {}",
                self.children.len(),
                children_len
            );
        }

        if let Some(co_partitioned) = &self.co_partitioned {
            if co_partitioned.len() < 2 {
                return internal_err!(
                    "{plan_name} has invalid co-partitioning requirement: at least two children are required"
                );
            }
            let mut seen = vec![false; self.children.len()];
            for &child in co_partitioned {
                validate_child_index(plan_name, child, self.children.len(), &mut seen)?;
                if matches!(
                    self.children[child].distribution(),
                    Distribution::UnspecifiedDistribution
                ) {
                    return internal_err!(
                        "{plan_name} has invalid co-partitioning requirement: child {child} has unspecified distribution"
                    );
                }
            }
        }

        Ok(())
    }

    fn co_partitioning_satisfied(
        &self,
        co_partitioned: &[usize],
        children: &[&dyn ExecutionPlan],
    ) -> Result<bool> {
        let first_idx = co_partitioned[0];
        let first = children[first_idx];
        let first_partitioning = first.output_partitioning();

        if !self
            .child_satisfaction(first_idx, first, ChildSatisfactionOptions::new())?
            .is_satisfied()
        {
            return Ok(false);
        }

        for &child_idx in co_partitioned.iter().skip(1) {
            let child = children[child_idx];
            if !self
                .child_satisfaction(child_idx, child, ChildSatisfactionOptions::new())?
                .is_satisfied()
                || !compatible_co_partitioning_layout(
                    first_partitioning,
                    child.output_partitioning(),
                )
            {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

/// A distribution requirement for a single child.
#[derive(Debug, Clone)]
struct ChildDistributionRequirement {
    distribution: Distribution,
    partitioning: Option<Partitioning>,
}

impl ChildDistributionRequirement {
    fn requires_single_partition(&self) -> bool {
        self.partitioning
            .as_ref()
            .is_some_and(|p| p.partition_count() == 1)
    }

    fn distribution(&self) -> &Distribution {
        // Preserve a concrete one-partition constraint for rules that only
        // inspect Distribution, such as sort parallelization. One partition
        // already satisfies every key distribution.
        if self.requires_single_partition() {
            &Distribution::SinglePartition
        } else {
            &self.distribution
        }
    }
}

fn satisfies_partitioning(
    source: &Partitioning,
    required: &Partitioning,
    eq_properties: &EquivalenceProperties,
) -> Result<bool> {
    if source.partition_count() != required.partition_count()
        || !satisfies_bounds(source, required, eq_properties)?
    {
        return Ok(false);
    }

    // With one partition there is only one possible destination for every row.
    if source.partition_count() == 1 {
        return Ok(true);
    }

    Ok(match (source, required) {
        (_, Partitioning::UnknownPartitioning(_)) => true,
        (Partitioning::RoundRobinBatch(_), Partitioning::RoundRobinBatch(_)) => true,
        (Partitioning::Hash(_, _), Partitioning::Hash(exprs, _)) => source
            .satisfaction(
                &Distribution::KeyPartitioned(exprs.clone()),
                eq_properties,
                false,
            )
            .is_satisfied(),
        (Partitioning::Range(source), Partitioning::Range(required)) => {
            source.split_points() == required.split_points()
                && equivalent_range_ordering(source, required, eq_properties)
        }
        _ => false,
    })
}

fn equivalent_range_ordering(
    source: &RangePartitioning,
    required: &RangePartitioning,
    eq_properties: &EquivalenceProperties,
) -> bool {
    let eq_group = eq_properties.eq_group();
    source.ordering().len() == required.ordering().len()
        && source
            .ordering()
            .iter()
            .zip(required.ordering())
            .all(|(a, b)| {
                a.options == b.options
                    && (a.expr.eq(&b.expr)
                        || eq_group
                            .normalize_expr(Arc::clone(&a.expr))
                            .eq(&eq_group.normalize_expr(Arc::clone(&b.expr))))
            })
}

/// Bounds describe a domain in lexicographic sort order, including descending
/// keys and null placement. A missing source bound cannot prove a finite limit.
fn satisfies_bounds(
    source: &Partitioning,
    required: &Partitioning,
    eq_properties: &EquivalenceProperties,
) -> Result<bool> {
    let Partitioning::Range(required) = required else {
        return Ok(true);
    };
    if required.start_bound().is_none() && required.end_bound().is_none() {
        return Ok(true);
    }
    let Partitioning::Range(source) = source else {
        return Ok(false);
    };
    if !equivalent_range_ordering(source, required, eq_properties) {
        return Ok(false);
    }
    let sort_options = required
        .ordering()
        .iter()
        .map(|expr| expr.options)
        .collect::<Vec<_>>();
    for (lower, upper) in [
        (required.start_bound(), source.start_bound()),
        (source.end_bound(), required.end_bound()),
    ] {
        if let (Some(lower), Some(upper)) = (lower, upper)
            && lower != upper
            && compare_rows(lower.values(), upper.values(), &sort_options)?.is_gt()
        {
            return Ok(false);
        }
    }
    Ok(
        (required.start_bound().is_none() || source.start_bound().is_some())
            && (required.end_bound().is_none() || source.end_bound().is_some()),
    )
}

fn validate_child_index(
    plan_name: &str,
    child_idx: usize,
    child_count: usize,
    seen: &mut [bool],
) -> Result<()> {
    if child_idx >= child_count {
        return internal_err!(
            "{plan_name} has invalid distribution requirement: child index {child_idx} out of bounds"
        );
    }
    if seen[child_idx] {
        return internal_err!(
            "{plan_name} has invalid distribution requirement: child {child_idx} appears more than once"
        );
    }
    seen[child_idx] = true;
    Ok(())
}

fn compatible_co_partitioning_layout(
    first_partitioning: &Partitioning,
    other_partitioning: &Partitioning,
) -> bool {
    if first_partitioning.partition_count() == 1
        && other_partitioning.partition_count() == 1
    {
        return true;
    }

    if first_partitioning.partition_count() != other_partitioning.partition_count() {
        return false;
    }

    match (first_partitioning, other_partitioning) {
        (Partitioning::Hash(_, _), Partitioning::Hash(_, _)) => true,
        (Partitioning::Range(left), Partitioning::Range(right)) => {
            left.split_points() == right.split_points()
                && left.ordering().len() == right.ordering().len()
                && left
                    .ordering()
                    .iter()
                    .zip(right.ordering())
                    .all(|(left, right)| left.options == right.options)
        }
        _ => false,
    }
}

# Hash Join Dynamic Filter Expanded Matrix

## Background

For `HashJoinExec mode=Partitioned`, we use `CASE` expressions to achieve partition-aware dynamic filtering.
```
CASE hash(expr) % num_partitions
  WHEN 0 THEN filter_expr_for_partition_0
  WHEN 1 THEN filter_expr_for_partition_1
  WHEN 2 THEN filter_expr_for_partition_2
   ...
  ELSE false
END
```

Note that typically each `filter_expr_for_partition` takes the form of a range expression + a set membership expression
ex.
```
(some_min <= expr AND expr <= some_max)  AND expr in (some_set_of_keys)
```

Per partition `CASE`ing was added in https://github.com/apache/datafusion/pull/18451, but there are no benchmarks.

The tradeoff being made is - the `CASE` expression is more expensive (hashing + case iteration) but
it gives us more selective pruning. Is this trade off worth it?

## Goal

Determine the most performant representation for dynamic filters in partitioned hash joins. This benchmark compares 4
alternatives:

1. `case` - the current partition-aware expression:
```text
CASE hash(expr) % num_partitions
  WHEN 0 THEN filter_expr_for_partition_0
  WHEN 1 THEN filter_expr_for_partition_1
  ...
  ELSE false
END
```
2. `partitioned_or` - `OR` the filter expression for each partition:
```text
filter_expr_for_partition_0 OR filter_expr_for_partition_1 OR filter_expr_for_partition_2 ...
```
3. `global` - construct one non-partition-aware expression representing all partitions. Typically:
```
(some_min <= expr AND expr <= some_max)  AND expr in (some_set_of_keys)
```
For larger build-side sets, the membership component may be a hash-table lookup rather than an `IN` list.
4. `global_bounds_case_membership` - split the expression into global bounds and partition-aware membership:
```text
(global_min <= expr AND expr <= global_max)
AND
CASE hash(expr) % num_partitions
  WHEN 0 THEN expr IN keys_for_partition_0
  WHEN 1 THEN expr IN keys_for_partition_1
  ...
  ELSE false
END
```
The idea is to let the cheap bounds expression evaluate first before having evaluate the expensive case expression.

## Benchmark Specs

Benchmark: `target/release-nonlto/dfbench hj`
Data: `benchmarks/data/tpch_sf1`
Iterations: 5

Other notes:
- Metrics: Captured via `target/release-nonlto/datafusion-cli` with `EXPLAIN ANALYZE VERBOSE`
- We forced partitioned hash joins with no join reordering to isolate the partitioned hash joins we're interested in.
```
hash_join_single_partition_threshold=0
hash_join_single_partition_threshold_rows=0
datafusion.optimizer.join_reordering = false
```

## Benchmark Matrix

3 session configs * 4 queries * 3 target_partitions * 5 dynamic filter expression types = 180 cases
### Modes

| Mode | Parquet settings | Purpose |
| --- | --- | --- |
| `default_metadata` | `pushdown_filters=false`, `pruning=true`, `enable_page_index=true`, `bloom_filter_on_read=true` | Current datafusion defaults. Parquet pruning: yes, row-filtering: no |
| `row_filter_only` | `pushdown_filters=true`, `pruning=false`, `enable_page_index=false`, `bloom_filter_on_read=false` | Parquet pruning: no, row-filtering: yes |
| `full` | `pushdown_filters=true`, `pruning=true`, `enable_page_index=true`, `bloom_filter_on_read=true` | Parquet pruning: yes, row-filtering: yes |


### Queries

- Ignored - `Q23` (selectivity=n/a, clustering=n/a): [PR #4](https://github.com/jayshrivastava/datafusion/pull/4) found this was not very relevant because its probe-side filter uses a computed string expression that cannot be used for parquet pruning.
- `Q24 date 1 day` (selectivity=high, clustering=low): Dynamic filters should prune a lot of rows, but the resulting `l_orderkey` values are not clustered enough for useful row-group pruning.
- `Q25 date 92 days` (selectivity=medium, clustering=low): Dynamic filters should prune fewer rows than Q24, and metadata pruning should still be weak because the surviving keys are spread across `l_orderkey`.
- `Q26 key 1K range` (selectivity=high, clustering=high): Dynamic filters should prune a lot of rows, and the narrow clustered `l_orderkey` range should enable strong row-group/page pruning.
- `Q27 key 500K range` (selectivity=medium, clustering=high): Dynamic filters should allow more rows through than Q26, while still testing whether a wider clustered key range benefits from row-group/page pruning.


### Partition Counts

- `4`
- default target partitions on this host: `16`
- `64`

### Cases

| Case | Dynamic filter | Style |
| --- | --- | --- |
| A | off | `n/a` |
| B | on | `case` |
| C | on | `partitioned_or` |
| D | on | `global` |
| E | on | `global_bounds_case_membership` |

## Results

### Wall Time

Values are milliseconds, averaged across warm iterations 2-5.
Scores count row wins across each query table: each of the 9 rows contributes one win to the fastest expression type (A-E).

| Query | Partitions | Mode | A off | B case | C partitioned_or | D global | E bounds+case |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| Q24 | 4 | `default_metadata` | 50.541 | 52.163 | 56.779 | 52.668 | 52.080 |
| Q24 | 4 | `row_filter_only` | 62.220 | 103.531 | 89.642 | 75.894 | 102.274 |
| Q24 | 4 | `full` | 58.624 | 103.759 | 92.325 | 75.920 | 103.246 |
| Q24 | default (16) | `default_metadata` | 28.483 | 29.824 | 33.319 | 32.895 | 30.065 |
| Q24 | default (16) | `row_filter_only` | 30.387 | 68.881 | 92.312 | 27.541 | 58.592 |
| Q24 | default (16) | `full` | 38.638 | 62.046 | 96.083 | 30.391 | 61.374 |
| Q24 | 64 | `default_metadata` | 35.846 | 51.511 | 232.355 | 42.075 | 47.708 |
| Q24 | 64 | `row_filter_only` | 45.729 | 90.845 | 242.004 | 38.810 | 84.901 |
| Q24 | 64 | `full` | 38.306 | 105.232 | 382.046 | 40.099 | 97.558 |

Score (row wins):
`A off`: 6
`B case`: 0
`C partitioned_or`: 0
`D global`: 3
`E bounds+case`: 0

Score without A off (row wins):
`B case`: 1
`C partitioned_or`: 0
`D global`: 7
`E bounds+case`: 1

| Query | Partitions | Mode | A off | B case | C partitioned_or | D global | E bounds+case |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| Q25 | 4 | `default_metadata` | 54.740 | 55.748 | 59.245 | 55.857 | 55.573 |
| Q25 | 4 | `row_filter_only` | 66.969 | 126.795 | 129.224 | 123.826 | 120.726 |
| Q25 | 4 | `full` | 67.562 | 121.812 | 134.823 | 123.899 | 122.313 |
| Q25 | default (16) | `default_metadata` | 28.037 | 36.444 | 32.658 | 31.362 | 31.893 |
| Q25 | default (16) | `row_filter_only` | 32.295 | 77.946 | 114.891 | 106.055 | 63.583 |
| Q25 | default (16) | `full` | 45.261 | 66.877 | 122.726 | 100.535 | 71.092 |
| Q25 | 64 | `default_metadata` | 38.538 | 39.552 | 60.244 | 48.357 | 39.563 |
| Q25 | 64 | `row_filter_only` | 39.059 | 116.667 | 432.206 | 365.344 | 89.540 |
| Q25 | 64 | `full` | 44.417 | 102.948 | 439.042 | 353.950 | 101.340 |

Score (row wins):
`A off`: 9
`B case`: 0
`C partitioned_or`: 0
`D global`: 0
`E bounds+case`: 0

Score without A off (row wins):
`B case`: 3
`C partitioned_or`: 0
`D global`: 1
`E bounds+case`: 5

| Query | Partitions | Mode | A off | B case | C partitioned_or | D global | E bounds+case |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| Q26 | 4 | `default_metadata` | 36.693 | 36.806 | 7.317 | 6.786 | 6.667 |
| Q26 | 4 | `row_filter_only` | 52.096 | 81.856 | 34.943 | 31.893 | 33.056 |
| Q26 | 4 | `full` | 36.198 | 72.510 | 7.554 | 6.888 | 6.965 |
| Q26 | default (16) | `default_metadata` | 23.497 | 23.016 | 20.021 | 8.360 | 8.719 |
| Q26 | default (16) | `row_filter_only` | 23.424 | 52.294 | 32.095 | 19.001 | 18.285 |
| Q26 | default (16) | `full` | 19.832 | 45.611 | 23.606 | 8.690 | 8.766 |
| Q26 | 64 | `default_metadata` | 31.852 | 33.170 | 67.611 | 18.242 | 18.900 |
| Q26 | 64 | `row_filter_only` | 31.237 | 83.950 | 82.842 | 25.919 | 25.736 |
| Q26 | 64 | `full` | 28.761 | 92.670 | 68.243 | 17.389 | 17.460 |

Score (row wins):
`A off`: 0
`B case`: 0
`C partitioned_or`: 0
`D global`: 6
`E bounds+case`: 3

Score without A off (row wins):
`B case`: 0
`C partitioned_or`: 0
`D global`: 6
`E bounds+case`: 3

| Query | Partitions | Mode | A off | B case | C partitioned_or | D global | E bounds+case |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| Q27 | 4 | `default_metadata` | 52.523 | 46.899 | 24.520 | 23.624 | 23.914 |
| Q27 | 4 | `row_filter_only` | 53.927 | 106.274 | 73.510 | 67.825 | 69.298 |
| Q27 | 4 | `full` | 47.331 | 97.739 | 60.068 | 52.757 | 53.190 |
| Q27 | default (16) | `default_metadata` | 24.165 | 29.877 | 20.274 | 17.839 | 18.720 |
| Q27 | default (16) | `row_filter_only` | 28.419 | 65.502 | 87.211 | 79.177 | 58.562 |
| Q27 | default (16) | `full` | 25.920 | 61.518 | 83.972 | 73.317 | 48.940 |
| Q27 | 64 | `default_metadata` | 39.479 | 39.494 | 37.892 | 22.719 | 23.350 |
| Q27 | 64 | `row_filter_only` | 38.924 | 86.831 | 122.313 | 81.717 | 42.856 |
| Q27 | 64 | `full` | 39.519 | 88.041 | 114.784 | 83.150 | 38.942 |

Score (row wins):
`A off`: 5
`B case`: 0
`C partitioned_or`: 0
`D global`: 3
`E bounds+case`: 1

Score without A off (row wins):
`B case`: 0
`C partitioned_or`: 0
`D global`: 5
`E bounds+case`: 4

## Short Analysis

- `global` is the most consistently strong dynamic-filter shape in this run. It is the best dynamic style for Q24 in all modes, best for Q26, and close on Q27 default/metadata mode. Its advantage is that it creates one ordinary global predicate, so parquet pruning and row filtering do not have to route through a partition CASE and do not have to evaluate a large OR tree.
- `global_bounds_case_membership` is best on wider ranges where global bounds are useful but preserving partition-routed membership still avoids too many false positives. It is the best dynamic style for Q25 default/metadata and Q27 row-filter/full mode, but it still pays CASE routing for membership.
- Dynamic filtering is not automatically a win. On date-based Q25, the dynamic-off baseline is faster than every dynamic-filter style in the full/default-partition run because row-group pruning does not improve and row filtering adds CPU. The strongest wins are on clustered key-range queries Q26/Q27, where non-CASE dynamic predicates unlock row-group and page pruning.
- `case` is usually worse when `pushdown_filters=true`; the row-filter-only table shows the cost directly. `CaseExpr` evaluates the partition expression once, then iterates `WHEN` branches over remaining rows and evaluates only matching `THEN` predicates. It is not all predicates for all rows, but it is still CPU-heavy as partition count rises and it hides useful predicates from parquet pruning.
- `partitioned_or` is the weakest overall. It removes CASE routing, but it expands to an OR of per-partition predicates. At 64 partitions that large expression is expensive, and in several cases it is slower than both `global` and `global_bounds_case_membership`.
- Row-group pruning matters a lot only for the clustered key-range queries. Q26 full/default with `global` scans roughly one lineitem row group (`row_groups_pruned_statistics=53 total -> 1 matched`, `bytes_scanned=113.5 K`), while `case` keeps all row groups alive. For the date-based Q24/Q25 queries, dynamic values are spread across order keys, so row-group pruning is much less helpful.
- Removing CASE can lose partition-local selectivity: global and OR-style filters can admit rows matching another partition's predicate. The results here suggest that for clustered range-friendly keys, the pruning and CPU wins dominate that loss. For wider/non-clustered filters, the hybrid `global_bounds_case_membership` can be a better compromise.

## Appendix

### Queries

#### Q24

```sql
SELECT count(*)
FROM (
  SELECT o_orderkey AS k
  FROM orders
  WHERE o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-07-02'
) o
JOIN (
  SELECT l_orderkey AS k
  FROM lineitem
) l ON o.k = l.k
```

#### Q25

```sql
SELECT count(*)
FROM (
  SELECT o_orderkey AS k
  FROM orders
  WHERE o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-10-01'
) o
JOIN (
  SELECT l_orderkey AS k
  FROM lineitem
) l ON o.k = l.k
```

#### Q26

```sql
SELECT count(*)
FROM (
  SELECT o_orderkey AS k
  FROM orders
  WHERE o_orderkey BETWEEN 1000000 AND 1001000
) o
JOIN (
  SELECT l_orderkey AS k
  FROM lineitem
) l ON o.k = l.k
```

#### Q27

```sql
SELECT count(*)
FROM (
  SELECT o_orderkey AS k
  FROM orders
  WHERE o_orderkey BETWEEN 1000000 AND 1500000
) o
JOIN (
  SELECT l_orderkey AS k
  FROM lineitem
) l ON o.k = l.k
```

### Metrics

#### `default_metadata` mode

| Query | Case | Warm ms | lineitem output_rows | bytes_scanned | row_groups_pruned_statistics | page_index_rows_pruned | row_pushdown_eval_time | statistics_eval_time |
| --- | --- | ---: | ---: | ---: | --- | --- | ---: | ---: |
| Q24 | A `n/a` | 28.483 | 6.00 M | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 32ns | 32ns |
| Q24 | B `case` | 29.824 | 6.00 M | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 32ns | 32ns |
| Q24 | C `partitioned_or` | 33.319 | 5.98 M | 10.38 M | 53 total → 53 matched | 6.00 M total → 5.98 M matched | 48ns | 20.93ms |
| Q24 | D `global` | 32.895 | 5.98 M | 10.38 M | 53 total → 53 matched | 6.00 M total → 5.98 M matched | 48ns | 16.57ms |
| Q24 | E `global_bounds_case_membership` | 30.065 | 5.98 M | 10.38 M | 53 total → 53 matched | 6.00 M total → 5.98 M matched | 48ns | 7.67ms |
| Q25 | A `n/a` | 28.037 | 6.00 M | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 32ns | 32ns |
| Q25 | B `case` | 36.444 | 6.00 M | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 32ns | 32ns |
| Q25 | C `partitioned_or` | 32.658 | 6.00 M | 10.41 M | 53 total → 53 matched | 6.00 M total → 6.00 M matched | 48ns | 5.89ms |
| Q25 | D `global` | 31.362 | 6.00 M | 10.41 M | 53 total → 53 matched | 6.00 M total → 6.00 M matched | 48ns | 2.35ms |
| Q25 | E `global_bounds_case_membership` | 31.893 | 6.00 M | 10.41 M | 53 total → 53 matched | 6.00 M total → 6.00 M matched | 48ns | 1.81ms |
| Q26 | A `n/a` | 23.497 | 6.00 M | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 32ns | 32ns |
| Q26 | B `case` | 23.016 | 6.00 M | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 32ns | 32ns |
| Q26 | C `partitioned_or` | 20.021 | 20.10 K | 57.81 K | 53 total → 1 matched | 113.0 K total → 20.10 K matched | 48ns | 3.95ms |
| Q26 | D `global` | 8.360 | 20.10 K | 57.81 K | 53 total → 1 matched | 113.0 K total → 20.10 K matched | 48ns | 626.80µs |
| Q26 | E `global_bounds_case_membership` | 8.719 | 20.10 K | 57.81 K | 53 total → 1 matched | 113.0 K total → 20.10 K matched | 48ns | 462.70µs |
| Q27 | A `n/a` | 24.165 | 6.00 M | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 32ns | 32ns |
| Q27 | B `case` | 29.877 | 6.00 M | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 32ns | 32ns |
| Q27 | C `partitioned_or` | 20.274 | 524.7 K | 941.1 K | 53 total → 6 matched | 678.6 K total → 524.7 K matched | 48ns | 1.31ms |
| Q27 | D `global` | 17.839 | 524.7 K | 941.1 K | 53 total → 6 matched | 678.6 K total → 524.7 K matched | 48ns | 442.28µs |
| Q27 | E `global_bounds_case_membership` | 18.720 | 524.7 K | 941.1 K | 53 total → 6 matched | 678.6 K total → 524.7 K matched | 48ns | 373.70µs |

#### `row_filter_only` mode

| Query | Case | Warm ms | lineitem output_rows | pushdown_rows_pruned | pushdown_rows_matched | row_pushdown_eval_time | HashJoin input_rows |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q24 | A `n/a` | 30.387 | 6.00 M | 0 | 0 | 32ns | 6.00 M |
| Q24 | B `case` | 68.881 | 2.36 K | 6.00 M | 2.36 K | 502.53ms | 2.36 K |
| Q24 | C `partitioned_or` | 92.312 | 2.36 K | 6.00 M | 2.36 K | 726.89ms | 2.36 K |
| Q24 | D `global` | 27.541 | 2.36 K | 6.00 M | 2.36 K | 46.61ms | 2.36 K |
| Q24 | E `global_bounds_case_membership` | 58.592 | 2.36 K | 6.00 M | 2.36 K | 454.89ms | 2.36 K |
| Q25 | A `n/a` | 32.295 | 6.00 M | 0 | 0 | 32ns | 6.00 M |
| Q25 | B `case` | 77.946 | 229.7 K | 5.77 M | 229.7 K | 535.58ms | 229.7 K |
| Q25 | C `partitioned_or` | 114.891 | 229.7 K | 5.77 M | 229.7 K | 1.22s | 229.7 K |
| Q25 | D `global` | 106.055 | 229.7 K | 5.77 M | 229.7 K | 1.02s | 229.7 K |
| Q25 | E `global_bounds_case_membership` | 63.583 | 229.7 K | 5.77 M | 229.7 K | 507.51ms | 229.7 K |
| Q26 | A `n/a` | 23.424 | 6.00 M | 0 | 0 | 32ns | 6.00 M |
| Q26 | B `case` | 52.294 | 1.06 K | 6.00 M | 1.06 K | 429.22ms | 1.06 K |
| Q26 | C `partitioned_or` | 32.095 | 1.06 K | 6.00 M | 1.06 K | 103.62ms | 1.06 K |
| Q26 | D `global` | 19.001 | 1.06 K | 6.00 M | 1.06 K | 6.86ms | 1.06 K |
| Q26 | E `global_bounds_case_membership` | 18.285 | 1.06 K | 6.00 M | 1.06 K | 7.82ms | 1.06 K |
| Q27 | A `n/a` | 28.419 | 6.00 M | 0 | 0 | 32ns | 6.00 M |
| Q27 | B `case` | 65.502 | 499.5 K | 5.50 M | 499.5 K | 443.40ms | 499.5 K |
| Q27 | C `partitioned_or` | 87.211 | 499.5 K | 5.50 M | 499.5 K | 165.34ms | 499.5 K |
| Q27 | D `global` | 79.177 | 499.5 K | 5.50 M | 499.5 K | 86.99ms | 499.5 K |
| Q27 | E `global_bounds_case_membership` | 58.562 | 499.5 K | 5.50 M | 499.5 K | 54.01ms | 499.5 K |

#### `full` mode

| Query | Case | Warm ms | lineitem output_rows | bytes_scanned | row_groups_pruned_statistics | page_index_rows_pruned | pushdown_rows_pruned | row_pushdown_eval_time | statistics_eval_time |
| --- | --- | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: |
| Q24 | A `n/a` | 38.638 | 6.00 M | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 0 | 32ns | 32ns |
| Q24 | B `case` | 62.046 | 2.36 K | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 6.00 M | 444.95ms | 32ns |
| Q24 | C `partitioned_or` | 96.083 | 2.36 K | 10.41 M | 53 total → 53 matched | 6.00 M total → 5.98 M matched | 5.98 M | 724.60ms | 15.31ms |
| Q24 | D `global` | 30.391 | 2.36 K | 10.41 M | 53 total → 53 matched | 6.00 M total → 5.98 M matched | 5.98 M | 47.82ms | 12.72ms |
| Q24 | E `global_bounds_case_membership` | 61.374 | 2.36 K | 10.41 M | 53 total → 53 matched | 6.00 M total → 5.98 M matched | 5.98 M | 610.02ms | 10.83ms |
| Q25 | A `n/a` | 45.261 | 6.00 M | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 0 | 32ns | 32ns |
| Q25 | B `case` | 66.877 | 229.7 K | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 5.77 M | 523.91ms | 32ns |
| Q25 | C `partitioned_or` | 122.726 | 229.7 K | 10.41 M | 53 total → 53 matched | 6.00 M total → 6.00 M matched | 5.77 M | 1.16s | 5.46ms |
| Q25 | D `global` | 100.535 | 229.7 K | 10.41 M | 53 total → 53 matched | 6.00 M total → 6.00 M matched | 5.77 M | 1.23s | 5.70ms |
| Q25 | E `global_bounds_case_membership` | 71.092 | 229.7 K | 10.41 M | 53 total → 53 matched | 6.00 M total → 6.00 M matched | 5.77 M | 502.03ms | 1.87ms |
| Q26 | A `n/a` | 19.832 | 6.00 M | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 0 | 32ns | 32ns |
| Q26 | B `case` | 45.611 | 1.06 K | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 6.00 M | 455.73ms | 32ns |
| Q26 | C `partitioned_or` | 23.606 | 1.06 K | 113.5 K | 53 total → 1 matched | 113.0 K total → 20.10 K matched | 19.03 K | 647.90µs | 3.89ms |
| Q26 | D `global` | 8.690 | 1.06 K | 113.5 K | 53 total → 1 matched | 113.0 K total → 20.10 K matched | 19.03 K | 80.38µs | 464.37µs |
| Q26 | E `global_bounds_case_membership` | 8.766 | 1.06 K | 113.5 K | 53 total → 1 matched | 113.0 K total → 20.10 K matched | 19.03 K | 316.56µs | 460.82µs |
| Q27 | A `n/a` | 25.920 | 6.00 M | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 0 | 32ns | 32ns |
| Q27 | B `case` | 61.518 | 499.5 K | 10.41 M | 53 total → 53 matched | 0 total → 0 matched | 5.50 M | 482.68ms | 32ns |
| Q27 | C `partitioned_or` | 83.972 | 499.5 K | 1.01 M | 53 total → 6 matched | 678.6 K total → 524.7 K matched | 25.19 K | 88.21ms | 1.29ms |
| Q27 | D `global` | 73.317 | 499.5 K | 1.01 M | 53 total → 6 matched | 678.6 K total → 524.7 K matched | 25.19 K | 73.88ms | 462.35µs |
| Q27 | E `global_bounds_case_membership` | 48.940 | 499.5 K | 1.01 M | 53 total → 6 matched | 678.6 K total → 524.7 K matched | 25.19 K | 44.34ms | 423.28µs |

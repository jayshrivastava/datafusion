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

The `CASE` has one tradeoff:
Pro:
- more selective pruning
Cons:
- the `CASE` expression is more expensive (hashing + case iteration)
- parquet row group pruning does not support `CASE` expressions

## Goal

Determine the most performant representation for dynamic filters in partitioned hash joins. 

This benchmark compares 4
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
note that typically each `filter_expr_for_partition` takes the form of a range expression + a set membership expression like `(some_min <= expr AND expr <= some_max)  AND expr in (some_set_of_keys)

2. `partitioned_or` - `OR` the filter expression for each partition:
```text
filter_expr_for_partition_0 OR filter_expr_for_partition_1 OR filter_expr_for_partition_2 ...
```
note that typically each `filter_expr_for_partition` takes the form of a range expression + a set membership expression like `(some_min <= expr AND expr <= some_max)  AND expr in (some_set_of_keys)

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
- idea: let the cheap bounds expression evaluate first before having evaluate the expensive case expression
- idea: the range expression can be pushed down and used by row group pruning
## Benchmark Specs

Benchmark: `target/release-nonlto/dfbench hj`
Data: `benchmarks/data/tpch_sf10`
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

#### Q24
| Partitions | Mode | A off | B case | C partitioned_or | D global | E bounds+case |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 4 | `default_metadata` | 465.722 | 460.473 | 477.449 | 468.485 | 470.486 |
| 4 | `row_filter_only` | 526.440 | 995.163 | 1015.830 | 975.521 | 982.981 |
| 4 | `full` | 528.997 | 1000.302 | 1020.449 | 974.146 | 994.457 |
| default (16) | `default_metadata` | 188.199 | 196.423 | 203.589 | 193.877 | 195.594 |
| default (16) | `row_filter_only` | 220.484 | 461.659 | 907.010 | 809.177 | 419.129 |
| default (16) | `full` | 208.501 | 453.950 | 880.840 | 835.290 | 426.193 |
| 64 | `default_metadata` | 204.922 | 225.884 | 343.552 | 264.708 | 247.542 |
| 64 | `row_filter_only` | 227.413 | 745.341 | 2809.371 | 181.595 | 664.311 |
| 64 | `full` | 226.891 | 742.657 | 2824.108 | 218.150 | 692.601 |

Score (row wins):
`A off`: 6
`B case`: 1
`C partitioned_or`: 0
`D global`: 2
`E bounds+case`: 0

#### Q25
| Mode | A off | B case | C partitioned_or | D global | E bounds+case |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 4 | `default_metadata` | 504.448 | 496.652 | 508.760 | 498.044 | 497.850 |
| 4 | `row_filter_only` | 587.939 | 1142.925 | 1224.557 | 1165.142 | 1131.838 |
| 4 | `full` | 588.277 | 1147.174 | 1257.571 | 1241.397 | 1126.717 |
| default (16) | `default_metadata` | 191.018 | 198.033 | 205.095 | 203.688 | 207.899 |
| default (16) | `row_filter_only` | 225.449 | 521.935 | 968.712 | 898.191 | 493.396 |
| default (16) | `full` | 232.169 | 546.574 | 971.855 | 911.401 | 474.788 |
| 64 | `default_metadata` | 211.896 | 224.901 | 287.008 | 212.029 | 219.145 |
| 64 | `row_filter_only` | 237.690 | 834.370 | 3261.935 | 2570.214 | 754.216 |
| 64 | `full` | 229.238 | 861.605 | 3358.274 | 2641.969 | 768.966 |

Score (# of row wins):
`A off`: 8
`B case`: 1
`C partitioned_or`: 0
`D global`: 0
`E bounds+case`: 0

#### Q26
| Partitions | Mode | A off | B case | C partitioned_or | D global | E bounds+case |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 4 | `default_metadata` | 303.127 | 302.613 | 7.667 | 7.588 | 7.066 |
| 4 | `row_filter_only` | 397.303 | 747.175 | 308.117 | 257.679 | 254.426 |
| 4 | `full` | 306.680 | 645.800 | 7.624 | 7.820 | 7.639 |
| default (16) | `default_metadata` | 134.375 | 136.663 | 18.758 | 8.704 | 8.402 |
| default (16) | `row_filter_only` | 165.517 | 384.938 | 166.648 | 94.257 | 97.480 |
| default (16) | `full` | 134.065 | 329.371 | 21.677 | 8.562 | 8.657 |
| 64 | `default_metadata` | 141.490 | 142.441 | 64.229 | 18.099 | 18.675 |
| 64 | `row_filter_only` | 176.555 | 659.980 | 366.343 | 103.526 | 93.050 |
| 64 | `full` | 153.503 | 614.553 | 63.605 | 17.810 | 17.817 |

Score (# row wins):
`A off`: 0
`B case`: 0
`C partitioned_or`: 1
`D global`: 4
`E bounds+case`: 4

#### Q27
| Partitions | Mode | A off | B case | C partitioned_or | D global | E bounds+case |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 4 | `default_metadata` | 364.171 | 359.462 | 26.715 | 25.689 | 24.755 |
| 4 | `row_filter_only` | 446.116 | 762.978 | 338.652 | 299.793 | 302.268 |
| 4 | `full` | 358.447 | 668.456 | 55.262 | 53.235 | 58.311 |
| default (16) | `default_metadata` | 145.684 | 163.675 | 26.044 | 24.933 | 25.352 |
| default (16) | `row_filter_only` | 179.292 | 429.653 | 241.047 | 182.233 | 145.784 |
| default (16) | `full` | 150.104 | 360.386 | 113.766 | 104.913 | 66.768 |
| 64 | `default_metadata` | 178.401 | 175.959 | 46.274 | 31.120 | 32.144 |
| 64 | `row_filter_only` | 194.232 | 665.976 | 470.282 | 331.513 | 138.800 |
| 64 | `full` | 168.286 | 631.037 | 381.025 | 287.345 | 98.817 |

Score (row wins):
`A off`: 0
`B case`: 0
`C partitioned_or`: 0
`D global`: 4
`E bounds+case`: 5

## Analysis

Firstly, when is turning dynamic filtering off good?

All wins for `A off` are when
(a) pruning is disabled (`row_filter_only`) or
(b) when clustering is low (q24 and q25), meaning row group pruning is not effective

This are backed by metrics in the appendix below. For q24 and q25, we see that no row groups are pruned
```
row_groups_pruned_statistics = 524 total -> 524 matched
```
For q26 and q27, we prune far more rows:
```
Q26 row_groups_pruned_statistics = 524 total -> 1 matched
Q27 row_groups_pruned_statistics = 524 total -> 6 matched
```

Let's focus on cases where dyanmic filtering is useful. These are scores when `pruning=true`:
```
Score (row wins):
  A off: 9
  B case: 2
  C partitioned_or: 1
  D global: 7
  E bounds+case: 5

Score without A off (row wins):
  B case: 4
  C partitioned_or: 1
  D global: 10
  E bounds+case: 9
```

If you look at the default datafusion config case only (`pruning=true,row-filter=false`), these are the scores:

```
 Score (row wins):
  A off: 4
  B case: 2
  C partitioned_or: 0
  D global: 3
  E bounds+case: 3

Score without A off (row wins):
  B case: 4
  C partitioned_or: 0
  D global: 5
  E bounds+case: 3
```

`global` comes out as a winner, but `case` and `bounds+case` are not that far behind. By how
much does `global` win?

```
Average wall time across default-config rows for Q24/Q25 only:

  B case:            300.394 ms ± 126.990 ms
  C partitioned_or:  337.575 ms ± 117.383 ms
  D global:          306.805 ms ± 126.480 ms
  E bounds+case:     306.419 ms ± 127.637 ms

Average wall time across default-config rows for Q26/Q27 only:

  B case:            213.469 ms ± 101.715 ms
  C partitioned_or:   31.615 ms ± 21.003 ms
  D global:           19.355 ms ± 10.047 ms
  E bounds+case:      19.399 ms ± 10.091 ms
```

Clearly, `case` is worse. Should we use `global` or `bounds+case`? If you look at the metrics below,
cases `D` and `E` effectively prune the same number of row groups. Since `case` is not supported by
parquet pruning, `E` is likely only effective because of the `bounds` portion, not the `case`.

### Conclusion

`global` and `bounds+case` are effectively in terms of performance, but if you consider that parquet pruning is the most
important type of filtering, the `case` part of `bounds+case` is likely less useful. Furthermore, `global`
takes the edge if you consider simplicity.

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
| Q24 | A `n/a` | 188.199 | 59.99 M | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 32ns | 32ns |
| Q24 | B `case` | 196.423 | 59.99 M | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 32ns | 32ns |
| Q24 | C `partitioned_or` | 203.589 | 59.97 M | 104.1 M | 524 total → 524 matched | 59.99 M total → 59.97 M matched | 48ns | 6.71ms |
| Q24 | D `global` | 193.877 | 59.97 M | 104.1 M | 524 total → 524 matched | 59.99 M total → 59.97 M matched | 48ns | 2.17ms |
| Q24 | E `global_bounds_case_membership` | 195.594 | 59.97 M | 104.1 M | 524 total → 524 matched | 59.99 M total → 59.97 M matched | 48ns | 2.26ms |
| Q25 | A `n/a` | 191.018 | 59.99 M | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 32ns | 32ns |
| Q25 | B `case` | 198.033 | 59.99 M | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 32ns | 32ns |
| Q25 | C `partitioned_or` | 205.095 | 59.99 M | 104.1 M | 524 total → 524 matched | 59.99 M total → 59.99 M matched | 48ns | 5.14ms |
| Q25 | D `global` | 203.688 | 59.99 M | 104.1 M | 524 total → 524 matched | 59.99 M total → 59.99 M matched | 48ns | 2.18ms |
| Q25 | E `global_bounds_case_membership` | 207.899 | 59.99 M | 104.1 M | 524 total → 524 matched | 59.99 M total → 59.99 M matched | 48ns | 1.80ms |
| Q26 | A `n/a` | 134.375 | 59.99 M | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 32ns | 32ns |
| Q26 | B `case` | 136.663 | 59.99 M | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 32ns | 32ns |
| Q26 | C `partitioned_or` | 18.758 | 20.10 K | 58.30 K | 524 total → 1 matched | 114.2 K total → 20.10 K matched | 48ns | 2.79ms |
| Q26 | D `global` | 8.704 | 20.10 K | 58.30 K | 524 total → 1 matched | 114.2 K total → 20.10 K matched | 48ns | 915.45µs |
| Q26 | E `global_bounds_case_membership` | 8.402 | 20.10 K | 58.30 K | 524 total → 1 matched | 114.2 K total → 20.10 K matched | 48ns | 650.10µs |
| Q27 | A `n/a` | 145.684 | 59.99 M | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 32ns | 32ns |
| Q27 | B `case` | 163.675 | 59.99 M | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 32ns | 32ns |
| Q27 | C `partitioned_or` | 26.044 | 509.9 K | 921.5 K | 524 total → 6 matched | 686.3 K total → 509.9 K matched | 48ns | 1.47ms |
| Q27 | D `global` | 24.933 | 509.9 K | 921.5 K | 524 total → 6 matched | 686.3 K total → 509.9 K matched | 48ns | 552.18µs |
| Q27 | E `global_bounds_case_membership` | 25.352 | 509.9 K | 921.5 K | 524 total → 6 matched | 686.3 K total → 509.9 K matched | 48ns | 522.28µs |

#### `row_filter_only` mode

| Query | Case | Warm ms | lineitem output_rows | pushdown_rows_pruned | pushdown_rows_matched | row_pushdown_eval_time | HashJoin input_rows |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q24 | A `n/a` | 220.484 | 59.99 M | 0 | 0 | 32ns | 59.99 M |
| Q24 | B `case` | 461.659 | 24.20 K | 59.96 M | 24.20 K | 4.55s | 24.20 K |
| Q24 | C `partitioned_or` | 907.010 | 24.20 K | 59.96 M | 24.20 K | 11.67s | 24.20 K |
| Q24 | D `global` | 809.177 | 24.20 K | 59.96 M | 24.20 K | 10.62s | 24.20 K |
| Q24 | E `global_bounds_case_membership` | 419.129 | 24.20 K | 59.96 M | 24.20 K | 4.62s | 24.20 K |
| Q25 | A `n/a` | 225.449 | 59.99 M | 0 | 0 | 32ns | 59.99 M |
| Q25 | B `case` | 521.935 | 2.29 M | 57.69 M | 2.29 M | 4.96s | 2.29 M |
| Q25 | C `partitioned_or` | 968.712 | 2.29 M | 57.69 M | 2.29 M | 13.07s | 2.29 M |
| Q25 | D `global` | 898.191 | 2.29 M | 57.69 M | 2.29 M | 10.99s | 2.29 M |
| Q25 | E `global_bounds_case_membership` | 493.396 | 2.29 M | 57.69 M | 2.29 M | 5.33s | 2.29 M |
| Q26 | A `n/a` | 165.517 | 59.99 M | 0 | 0 | 32ns | 59.99 M |
| Q26 | B `case` | 384.938 | 1.06 K | 59.98 M | 1.06 K | 3.74s | 1.06 K |
| Q26 | C `partitioned_or` | 166.648 | 1.06 K | 59.98 M | 1.06 K | 969.94ms | 1.06 K |
| Q26 | D `global` | 94.257 | 1.06 K | 59.98 M | 1.06 K | 78.90ms | 1.06 K |
| Q26 | E `global_bounds_case_membership` | 97.480 | 1.06 K | 59.98 M | 1.06 K | 76.31ms | 1.06 K |
| Q27 | A `n/a` | 179.292 | 59.99 M | 0 | 0 | 32ns | 59.99 M |
| Q27 | B `case` | 429.653 | 499.5 K | 59.49 M | 499.5 K | 3.74s | 499.5 K |
| Q27 | C `partitioned_or` | 241.047 | 499.5 K | 59.49 M | 499.5 K | 984.44ms | 499.5 K |
| Q27 | D `global` | 182.233 | 499.5 K | 59.49 M | 499.5 K | 163.24ms | 499.5 K |
| Q27 | E `global_bounds_case_membership` | 145.784 | 499.5 K | 59.49 M | 499.5 K | 120.07ms | 499.5 K |

#### `full` mode

| Query | Case | Warm ms | lineitem output_rows | bytes_scanned | row_groups_pruned_statistics | page_index_rows_pruned | pushdown_rows_pruned | row_pushdown_eval_time | statistics_eval_time |
| --- | --- | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: |
| Q24 | A `n/a` | 208.501 | 59.99 M | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 0 | 32ns | 32ns |
| Q24 | B `case` | 453.950 | 24.20 K | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 59.96 M | 4.58s | 32ns |
| Q24 | C `partitioned_or` | 880.840 | 24.20 K | 104.1 M | 524 total → 524 matched | 59.99 M total → 59.97 M matched | 59.94 M | 11.26s | 6.34ms |
| Q24 | D `global` | 835.290 | 24.20 K | 104.1 M | 524 total → 524 matched | 59.99 M total → 59.97 M matched | 59.94 M | 10.54s | 2.58ms |
| Q24 | E `global_bounds_case_membership` | 426.193 | 24.20 K | 104.1 M | 524 total → 524 matched | 59.99 M total → 59.97 M matched | 59.94 M | 4.69s | 2.83ms |
| Q25 | A `n/a` | 232.169 | 59.99 M | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 0 | 32ns | 32ns |
| Q25 | B `case` | 546.574 | 2.29 M | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 57.69 M | 5.36s | 32ns |
| Q25 | C `partitioned_or` | 971.855 | 2.29 M | 104.1 M | 524 total → 524 matched | 59.99 M total → 59.99 M matched | 57.69 M | 12.08s | 6.64ms |
| Q25 | D `global` | 911.401 | 2.29 M | 104.1 M | 524 total → 524 matched | 59.99 M total → 59.99 M matched | 57.69 M | 10.76s | 2.42ms |
| Q25 | E `global_bounds_case_membership` | 474.788 | 2.29 M | 104.1 M | 524 total → 524 matched | 59.99 M total → 59.99 M matched | 57.69 M | 4.54s | 2.41ms |
| Q26 | A `n/a` | 134.065 | 59.99 M | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 0 | 32ns | 32ns |
| Q26 | B `case` | 329.371 | 1.06 K | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 59.98 M | 3.91s | 32ns |
| Q26 | C `partitioned_or` | 21.677 | 1.06 K | 116.4 K | 524 total → 1 matched | 114.2 K total → 20.10 K matched | 19.03 K | 682.22µs | 2.81ms |
| Q26 | D `global` | 8.562 | 1.06 K | 116.4 K | 524 total → 1 matched | 114.2 K total → 20.10 K matched | 19.03 K | 86.11µs | 978.88µs |
| Q26 | E `global_bounds_case_membership` | 8.657 | 1.06 K | 116.4 K | 524 total → 1 matched | 114.2 K total → 20.10 K matched | 19.03 K | 482.67µs | 598.21µs |
| Q27 | A `n/a` | 150.104 | 59.99 M | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 0 | 32ns | 32ns |
| Q27 | B `case` | 360.386 | 499.5 K | 104.1 M | 524 total → 524 matched | 0 total → 0 matched | 59.49 M | 3.73s | 32ns |
| Q27 | C `partitioned_or` | 113.766 | 499.5 K | 986.7 K | 524 total → 6 matched | 686.3 K total → 509.9 K matched | 10.39 K | 90.88ms | 1.62ms |
| Q27 | D `global` | 104.913 | 499.5 K | 986.7 K | 524 total → 6 matched | 686.3 K total → 509.9 K matched | 10.39 K | 75.88ms | 369.69µs |
| Q27 | E `global_bounds_case_membership` | 66.768 | 499.5 K | 986.7 K | 524 total → 6 matched | 686.3 K total → 509.9 K matched | 10.39 K | 41.74ms | 436.91µs |

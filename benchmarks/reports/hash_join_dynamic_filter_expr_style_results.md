# Partitioned Hash Join Dynamic Filter Expression Style Results

Generated: 2026-07-17T14:32:14Z

Repository:

- Git SHA: `95de38534`
- System: `Linux 6.8.0-1051-aws aarch64`
- Host available parallelism: `16`

## Background

For `HashJoinExec mode=Partitioned`, the current partition-aware dynamic
filter lowers each build partition's predicate into a `CASE` expression:

```sql
CASE hash_repartition(expr) % num_partitions
  WHEN 0 THEN filter_expr_for_partition_0
  WHEN 1 THEN filter_expr_for_partition_1
  WHEN 2 THEN filter_expr_for_partition_2
  ...
  ELSE false
END
```

The benchmark toggle compares that with a non-partition-aware expression:

```sql
filter_expr_for_partition_0
OR filter_expr_for_partition_1
OR filter_expr_for_partition_2
OR ...
```

The `global_or` expression is applied to every probe partition. It can do more
predicate work per row, but it exposes ordinary boolean predicates to Parquet
pruning instead of hiding them behind a partition-routing `CASE`.

## Setup

- Dataset: TPC-H SF1 parquet in `benchmarks/data/tpch_sf1`.
- Benchmark: `dfbench hj`.
- Profile: `target/release-nonlto`.
- Iterations per case: `5`.
- Default target partitions: DataFusion default on this host, `16`.
- Comparison target partitions: explicit `--partitions 4`.
- Dynamic filter expression style toggle:
  `datafusion.optimizer.hash_join_dynamic_filter_partitioned_expr_style`
  with values `case` and `global_or`.

The binaries were rebuilt before the run:

```bash
cargo build --profile release-nonlto --bin dfbench --bin datafusion-cli
```

Every measured query forces partitioned hash join by setting:

```sql
SET datafusion.optimizer.hash_join_single_partition_threshold = 0;
SET datafusion.optimizer.hash_join_single_partition_threshold_rows = 0;
```

The final benchmark matrix has five cases per query/partition-count pair:

| Case | Dynamic filters | Expr style | `datafusion.execution.parquet.pruning` | `datafusion.execution.parquet.enable_page_index` | `datafusion.execution.parquet.bloom_filter_on_read` | `datafusion.execution.parquet.pushdown_filters` |
| --- | --- | --- | --- | --- | --- | --- |
| A: baseline row filter | off | n/a | false | false | false | true |
| B: case row filter | on | `case` | false | false | false | true |
| C: global_or row filter | on | `global_or` | false | false | false | true |
| D: case full | on | `case` | true | true | true | true |
| E: global_or full | on | `global_or` | true | true | true | true |

The row-filter cases are the fairest direct comparison between `case` and
`global_or`, because Parquet row-group statistics pruning does not currently
understand the partition-routing `CASE` expression. The full cases show the
end-to-end behavior when all Parquet pruning/filtering paths are available.
Page-index pruning is page-level pruning, not row-group pruning; in
`EXPLAIN ANALYZE VERBOSE` it appears in the page-row metrics. Bloom filtering
is applied at the row-group read path and is reported separately from
statistics pruning.

## Queries

Q23 is the earlier skewed high-fanout string-key benchmark:

```sql
SELECT count(*)
FROM (
  SELECT 'high_fanout_string_join_key_' || CAST((s_suppkey % 415) + 1 AS VARCHAR) as k
  FROM supplier
  WHERE s_suppkey <= 32340
) s
JOIN (
  SELECT 'high_fanout_string_join_key_1' as k
  FROM lineitem
  WHERE l_orderkey % 265 = 0
) l ON s.k = l.k
```

Q24 was added to make hash join dynamic filtering useful for Parquet pruning.
It joins a selective `orders.o_orderkey` build side to the stored, clustered
`lineitem.l_orderkey` probe column:

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

## Q23 Default Partitions

Default target partitions: `16`.

### Timing

| Case | Result rows | Times (ms) | Avg (ms) | Avg excl. first (ms) | Min (ms) | Max (ms) |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| A: baseline row filter | 1 | 30.169, 22.293, 20.566, 21.318, 20.569 | 22.983 | 21.187 | 20.566 | 30.169 |
| B: case row filter | 1 | 33.575, 25.021, 21.587, 23.594, 22.766 | 25.309 | 23.242 | 21.587 | 33.575 |
| C: global_or row filter | 1 | 30.611, 21.551, 23.258, 24.022, 22.892 | 24.467 | 22.931 | 21.551 | 30.611 |
| D: case full | 1 | 30.668, 24.807, 22.867, 22.894, 23.194 | 24.886 | 23.440 | 22.867 | 30.668 |
| E: global_or full | 1 | 33.980, 31.523, 26.032, 23.555, 23.795 | 27.777 | 26.226 | 23.555 | 33.980 |

### Lineitem Scan Metrics

| Case | Dynamic expr shape | Scan output rows | Row groups by stats | Dynamic row-group pruned | Bloom row groups | Page rows | Bytes scanned | Row-filter rows pruned | Row-filter rows matched | Row-filter eval time | Stats eval time |
| --- | --- | ---: | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| A: baseline row filter | none | 22.80 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 5.98 M | 22.80 K | 30.48ms | 32ns |
| B: case row filter | case | 22.80 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 5.98 M | 22.80 K | 38.93ms | 32ns |
| C: global_or row filter | global_or | 22.80 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 5.98 M | 22.80 K | 30.59ms | 32ns |
| D: case full | case | 22.80 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 5.98 M | 22.80 K | 33.48ms | 32ns |
| E: global_or full | global_or | 22.80 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 5.98 M | 22.80 K | 36.43ms | 32ns |

### Notes

- Q23 does not demonstrate useful Parquet metadata pruning. All cases read all
  53 `lineitem` row groups.
- The probe filter is a synthetic string expression, so this mostly measures
  row-filter expression overhead and normal run-to-run noise.

## Q23 Four Partitions

Explicit target partitions: `4`.

### Timing

| Case | Result rows | Times (ms) | Avg (ms) | Avg excl. first (ms) | Min (ms) | Max (ms) |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| A: baseline row filter | 1 | 37.687, 33.881, 33.931, 33.100, 34.021 | 34.524 | 33.733 | 33.100 | 37.687 |
| B: case row filter | 1 | 39.713, 39.028, 34.033, 34.480, 34.028 | 36.256 | 35.392 | 34.028 | 39.713 |
| C: global_or row filter | 1 | 38.423, 34.972, 34.238, 34.170, 33.823 | 35.125 | 34.301 | 33.823 | 38.423 |
| D: case full | 1 | 38.574, 35.223, 35.244, 33.540, 33.756 | 35.267 | 34.441 | 33.540 | 38.574 |
| E: global_or full | 1 | 38.698, 34.649, 34.451, 34.501, 33.729 | 35.206 | 34.332 | 33.729 | 38.698 |

### Lineitem Scan Metrics

| Case | Dynamic expr shape | Scan output rows | Row groups by stats | Dynamic row-group pruned | Bloom row groups | Page rows | Bytes scanned | Row-filter rows pruned | Row-filter rows matched | Row-filter eval time | Stats eval time |
| --- | --- | ---: | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| A: baseline row filter | none | 22.80 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 5.98 M | 22.80 K | 26.96ms | 8ns |
| B: case row filter | case | 22.80 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 5.98 M | 22.80 K | 28.10ms | 8ns |
| C: global_or row filter | global_or | 22.80 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 5.98 M | 22.80 K | 29.98ms | 8ns |
| D: case full | case | 22.80 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 5.98 M | 22.80 K | 27.29ms | 8ns |
| E: global_or full | global_or | 22.80 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 5.98 M | 22.80 K | 27.47ms | 8ns |

### Notes

- Reducing to 4 partitions does not change the pruning story for Q23.
- The warm averages are clustered together, with `global_or` slightly cheaper
  than `case` in row-filter-only mode.

## Q24 Default Partitions

Default target partitions: `16`.

### Timing

| Case | Result rows | Times (ms) | Avg (ms) | Avg excl. first (ms) | Min (ms) | Max (ms) |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| A: baseline row filter | 1 | 32.792, 26.438, 25.075, 22.698, 23.025 | 26.006 | 24.309 | 22.698 | 32.792 |
| B: case row filter | 1 | 74.926, 62.483, 55.047, 55.203, 53.468 | 60.226 | 56.550 | 53.468 | 74.926 |
| C: global_or row filter | 1 | 37.469, 30.012, 33.039, 29.571, 29.125 | 31.843 | 30.437 | 29.125 | 37.469 |
| D: case full | 1 | 67.893, 42.581, 44.626, 45.776, 44.357 | 49.047 | 44.335 | 42.581 | 67.893 |
| E: global_or full | 1 | 25.224, 20.401, 21.462, 21.629, 21.225 | 21.988 | 21.179 | 20.401 | 25.224 |

### Lineitem Scan Metrics

| Case | Dynamic expr shape | Scan output rows | Row groups by stats | Dynamic row-group pruned | Bloom row groups | Page rows | Bytes scanned | Row-filter rows pruned | Row-filter rows matched | Row-filter eval time | Stats eval time |
| --- | --- | ---: | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| A: baseline row filter | none | 6.00 M | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 0 | 0 | 32ns | 32ns |
| B: case row filter | case | 1.06 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 6.00 M | 1.06 K | 445.73ms | 32ns |
| C: global_or row filter | global_or | 1.06 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 6.00 M | 1.06 K | 93.31ms | 32ns |
| D: case full | case | 1.06 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 6.00 M | 1.06 K | 488.78ms | 32ns |
| E: global_or full | global_or | 1.06 K | 53 total -> 1 matched | 0 | 1 total -> 1 matched | 113.0 K total -> 20.10 K matched | 113.5 K | 19.03 K | 1.06 K | 671.00us | 4.93ms |

### Notes

- In the fair row-filter-only comparison, `global_or` is much cheaper than
  `case`: warm average 30.437 ms versus 56.550 ms.
- In the full path, `global_or` exposes the order-key predicate to Parquet
  pruning: `lineitem` drops from 53 matched row groups to 1 matched row group.
- `case` keeps all 53 row groups and spends far more time evaluating the row
  filter.

## Q24 Four Partitions

Explicit target partitions: `4`.

### Timing

| Case | Result rows | Times (ms) | Avg (ms) | Avg excl. first (ms) | Min (ms) | Max (ms) |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| A: baseline row filter | 1 | 50.338, 46.842, 54.425, 46.502, 45.221 | 48.665 | 48.247 | 45.221 | 54.425 |
| B: case row filter | 1 | 84.734, 81.786, 81.049, 80.947, 80.453 | 81.794 | 81.059 | 80.453 | 84.734 |
| C: global_or row filter | 1 | 37.944, 35.361, 34.896, 35.511, 34.975 | 35.737 | 35.186 | 34.896 | 37.944 |
| D: case full | 1 | 76.265, 72.275, 73.004, 90.017, 73.227 | 76.957 | 77.130 | 72.275 | 90.017 |
| E: global_or full | 1 | 9.223, 7.581, 8.189, 7.280, 7.097 | 7.874 | 7.537 | 7.097 | 9.223 |

### Lineitem Scan Metrics

| Case | Dynamic expr shape | Scan output rows | Row groups by stats | Dynamic row-group pruned | Bloom row groups | Page rows | Bytes scanned | Row-filter rows pruned | Row-filter rows matched | Row-filter eval time | Stats eval time |
| --- | --- | ---: | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| A: baseline row filter | none | 6.00 M | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 0 | 0 | 8ns | 8ns |
| B: case row filter | case | 1.06 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 6.00 M | 1.06 K | 196.13ms | 8ns |
| C: global_or row filter | global_or | 1.06 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 6.00 M | 1.06 K | 21.16ms | 8ns |
| D: case full | case | 1.06 K | 53 total -> 53 matched | 0 | 53 total -> 53 matched | 0 total -> 0 matched | 10.41 M | 6.00 M | 1.06 K | 200.14ms | 8ns |
| E: global_or full | global_or | 1.06 K | 53 total -> 1 matched | 0 | 1 total -> 1 matched | 113.0 K total -> 20.10 K matched | 113.5 K | 19.03 K | 1.06 K | 207.62us | 602.28us |

### Notes

- With 4 partitions, the row-filter-only gap is larger: `global_or` has a
  warm average of 35.186 ms versus 81.059 ms for `case`.
- The full `global_or` path is the fastest run in the matrix because row-group
  and page-level pruning reduce the `lineitem` scan to one matched row group and
  113.5 K bytes scanned.
- The full `case` path still cannot use row-group statistics or page-index
  pruning for the partition-routing predicate.

## Summary

| Section | A baseline row filter warm avg (ms) | B case row filter warm avg (ms) | C global_or row filter warm avg (ms) | D case full warm avg (ms) | E global_or full warm avg (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q23 default partitions | 21.187 | 23.242 | 22.931 | 23.440 | 26.226 |
| Q23 4 partitions | 33.733 | 35.392 | 34.301 | 34.441 | 34.332 |
| Q24 default partitions | 24.309 | 56.550 | 30.437 | 44.335 | 21.179 |
| Q24 4 partitions | 48.247 | 81.059 | 35.186 | 77.130 | 7.537 |

Main takeaways:

- Q23 is not useful for evaluating Parquet metadata pruning from dynamic
  filters. It mostly measures row-filter overhead.
- Q24 is a better benchmark because the probe side uses the stored clustered
  `lineitem.l_orderkey` column.
- In row-filter-only mode, `global_or` is consistently faster than the
  partition-routing `CASE` expression for Q24.
- In full mode, `global_or` can be dramatically faster because Parquet pruning
  can understand the lowered predicate shape, while the current `CASE` shape
  keeps all row groups alive.
- `row_groups_pruned_dynamic_filter` stayed `0` in these runs. The useful
  row-group elimination for `global_or` appears in
  `row_groups_pruned_statistics`, because the dynamic filter predicate is
  materialized before the scan's statistics-pruning pass.

## Artifacts

Raw benchmark JSON and explain output are in:

```text
benchmarks/results/hash_join_dynamic_filter_expr_style_matrix/
```

Final-run artifact names begin with one of:

- `Q23_default_`
- `Q23_p4_`
- `Q24_default_`
- `Q24_p4_`

Older scratch artifacts from earlier matrix iterations are also present in the
same ignored results directory.

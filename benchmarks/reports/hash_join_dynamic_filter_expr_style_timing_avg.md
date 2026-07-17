# Partitioned Hash Join Dynamic Filter Timing Averages

Generated: 2026-07-17T14:32:14Z

Values are average runtime across 5 iterations.

## Case Session Properties

All cases forced partitioned hash join:

```sql
SET datafusion.optimizer.hash_join_single_partition_threshold = 0;
SET datafusion.optimizer.hash_join_single_partition_threshold_rows = 0;
```

| Case | Session properties |
| --- | --- |
| A: baseline row filter | - `datafusion.optimizer.enable_dynamic_filter_pushdown=false`<br>- `datafusion.execution.parquet.pruning=false`<br>- `datafusion.execution.parquet.enable_page_index=false`<br>- `datafusion.execution.parquet.bloom_filter_on_read=false`<br>- `datafusion.execution.parquet.pushdown_filters=true` |
| B: case row filter | - `datafusion.optimizer.enable_dynamic_filter_pushdown=true`<br>- `datafusion.optimizer.hash_join_dynamic_filter_partitioned_expr_style=case`<br>- `datafusion.execution.parquet.pruning=false`<br>- `datafusion.execution.parquet.enable_page_index=false`<br>- `datafusion.execution.parquet.bloom_filter_on_read=false`<br>- `datafusion.execution.parquet.pushdown_filters=true` |
| C: global_or row filter | - `datafusion.optimizer.enable_dynamic_filter_pushdown=true`<br>- `datafusion.optimizer.hash_join_dynamic_filter_partitioned_expr_style=global_or`<br>- `datafusion.execution.parquet.pruning=false`<br>- `datafusion.execution.parquet.enable_page_index=false`<br>- `datafusion.execution.parquet.bloom_filter_on_read=false`<br>- `datafusion.execution.parquet.pushdown_filters=true` |
| D: case full | - `datafusion.optimizer.enable_dynamic_filter_pushdown=true`<br>- `datafusion.optimizer.hash_join_dynamic_filter_partitioned_expr_style=case`<br>- `datafusion.execution.parquet.pruning=true`<br>- `datafusion.execution.parquet.enable_page_index=true`<br>- `datafusion.execution.parquet.bloom_filter_on_read=true`<br>- `datafusion.execution.parquet.pushdown_filters=true` |
| E: global_or full | - `datafusion.optimizer.enable_dynamic_filter_pushdown=true`<br>- `datafusion.optimizer.hash_join_dynamic_filter_partitioned_expr_style=global_or`<br>- `datafusion.execution.parquet.pruning=true`<br>- `datafusion.execution.parquet.enable_page_index=true`<br>- `datafusion.execution.parquet.bloom_filter_on_read=true`<br>- `datafusion.execution.parquet.pushdown_filters=true` |

## Q23 Default Partitions

| Case | Avg (ms) |
| --- | ---: |
| A: baseline row filter | 22.983 |
| B: case row filter | 25.309 |
| C: global_or row filter | 24.467 |
| D: case full | 24.886 |
| E: global_or full | 27.777 |

## Q23 Four Partitions

| Case | Avg (ms) |
| --- | ---: |
| A: baseline row filter | 34.524 |
| B: case row filter | 36.256 |
| C: global_or row filter | 35.125 |
| D: case full | 35.267 |
| E: global_or full | 35.206 |

## Q24 Default Partitions

| Case | Avg (ms) |
| --- | ---: |
| A: baseline row filter | 26.006 |
| B: case row filter | 60.226 |
| C: global_or row filter | 31.843 |
| D: case full | 49.047 |
| E: global_or full | 21.988 |

## Q24 Four Partitions

| Case | Avg (ms) |
| --- | ---: |
| A: baseline row filter | 48.665 |
| B: case row filter | 81.794 |
| C: global_or row filter | 35.737 |
| D: case full | 76.957 |
| E: global_or full | 7.874 |

## Interpretation

- Q23 is not a strong dynamic-filter benchmark. The probe predicate uses a
  synthetic string expression, so Parquet metadata pruning does not eliminate
  row groups and the results mostly measure row-filter expression overhead.
- Q24 shows dynamic filtering can be effective when the probe-side filter lands
  on a stored, clustered Parquet column. `global_or` exposes the predicate to
  Parquet pruning and is much faster than the partition-routing `CASE` shape,
  especially in the full-pruning cases.

## Queries

### Q23

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

### Q24

This is a custom hash-join benchmark query, not official TPC-H Query 4.

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

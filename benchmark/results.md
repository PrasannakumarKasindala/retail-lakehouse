# Benchmark results

Config: 2000 customers, 8000 orders/day, 2 days. Single process, local delta-rs + dbt-duckdb.
Spark-cluster throughput is not claimed (not measured).

| Stage | Wall time (s) | Throughput |
|---|---|---|
| generate | 0.33 | 159,247 rows/s |
| bronze append (delta-rs) | 0.19 | 281,813 rows/s |
| silver refine (duckdb + delta-rs) | 0.24 | 214,233 rows/s |
| gold build (dbt-duckdb, wall time) | 5.26 | n/a (fixed dbt overhead) |
| reconcile | 0.08 | 392,810 rows/s |

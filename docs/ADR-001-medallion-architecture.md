# ADR-001: Medallion architecture with Delta bronze/silver and a dbt gold star schema

## Status
Accepted.

## Context
We need a batch lakehouse that turns messy daily retail extracts into a governed,
query-ready dimensional model, with data quality enforced at layer boundaries and
a way to prove the output is correct. Two shapes were considered: a single
transform step (raw straight to a warehouse) versus a layered medallion
(bronze/silver/gold).

We also need the project to be runnable and testable by a reviewer without a
Spark cluster, while still representing the production compute honestly.

## Decision
Adopt a three-layer medallion:

- Bronze: append-only Delta, the immutable raw history. Every daily extract is
  preserved with lineage columns (`_extract_date`, `_ingested_at`), so silver can
  always be rebuilt and nothing is lost.
- Silver: Delta, the current cleaned and conformed snapshot. Deduplicate to the
  latest row per natural key, normalize fields, and enforce referential integrity
  so the star schema builds cleanly.
- Gold: a dbt-built star schema (dimensions + a sales fact) in the warehouse.

Split the runtime by concern:

- Runnable locally and in CI: bronze/silver via delta-rs (the `deltalake` package,
  no JVM), gold via dbt-duckdb, data quality via Great Expectations, and a
  cross-layer reconciler. This whole path runs in seconds on one process.
- Shipped as production code: PySpark jobs for bronze/silver at cluster scale, an
  Airflow DAG for orchestration, and Terraform for the S3 + Glue + IAM footprint.
  These are structure-tested, not run in CI, and are not benchmarked.

Extracts follow a common batch shape: a daily FULL extract of the dimensions and
an INCREMENTAL extract of the facts.

## Consequences
- A reviewer can `pip install` and `rlh run` to get real Delta tables, a real dbt
  star schema, real GE validation, and a dollar-denominated reconciliation, with
  no cluster.
- delta-rs and Spark are not byte-identical engines. The Delta tables are real and
  interoperable, but the local path is single-process; the Spark jobs carry the
  same logic (dedup-by-key window, cleaning, referential trim) for scale.
- Keeping bronze append-only costs storage but buys full replayability and a clean
  audit trail, which is the point of the layer.
- delta-rs holds a native runtime behind each table handle that can abort the
  process on teardown if a handle is still alive at exit; the Delta I/O wrapper
  therefore never leaks handles (see the module docstring), and the test suite is
  verified to exit cleanly.

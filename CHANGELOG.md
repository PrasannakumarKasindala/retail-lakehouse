# Changelog

All notable changes to this project are documented here. The format follows
Keep a Changelog, and the project adheres to Semantic Versioning.

## [0.1.0] - 2026-04-26

### Added
- Batch medallion lakehouse: bronze and silver as Delta tables (delta-rs), gold
  as a dbt-duckdb star schema.
- SCD2 customer dimension via a dbt snapshot (timestamp strategy) with
  point-in-time surrogate-key resolution in the sales fact.
- Great Expectations suites as the silver data-quality gate, shipped as JSON and
  runnable in-process.
- Cross-layer reconciliation with dollar-denominated drift and a release-gating
  exit code.
- Production code: PySpark bronze/silver jobs, an Airflow DAG, a docker-compose
  Airflow stack, and Terraform for S3 + Glue + IAM (structure-tested, not run in
  CI).
- `rlh` CLI (`generate`, `run`, `validate`, `reconcile`, `inspect`,
  `write-suites`), benchmark harness, and CI (lint, test matrix, smoke).

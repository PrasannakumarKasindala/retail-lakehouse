"""Airflow DAG: the daily retail-lakehouse batch.

One run per logical day. The extract for that day is assumed to have landed in
the raw zone (in dev, ``rlh generate`` produces it; in production a source-system
export or an upstream sensor does). The task chain mirrors the medallion:

    ingest_bronze -> refine_silver -> validate_ge -> build_gold(dbt) -> reconcile

The Great Expectations gate (validate_ge) and the reconciliation both raise on
failure, so a bad batch stops the DAG run rather than publishing a wrong gold
layer. Lake root and dbt project dir come from Airflow Variables / env.

Validated structurally by tests/test_airflow_dag_structure.py. Not run in CI.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

LAKE_ROOT = os.environ.get("RLH_LAKE_ROOT", "/opt/lake")
DBT_PROJECT_DIR = os.environ.get("RLH_DBT_PROJECT_DIR", "/opt/retail-lakehouse/transform")


def _cfg():
    from rlh.config import LakehouseConfig

    return LakehouseConfig.create(LAKE_ROOT, DBT_PROJECT_DIR)


def _ingest(ds=None, **_):
    from rlh import bronze

    bronze.ingest(_cfg(), ds)


def _refine(**_):
    from rlh import silver

    silver.refine(_cfg())


def _validate(**_):
    from rlh.expectations import validate_silver

    report = validate_silver(_cfg())
    if not report.ok:
        failed = {r.table: r.failed for r in report.results if not r.success}
        raise ValueError(f"silver data-quality failed: {failed}")


def _reconcile(**_):
    from rlh.reconcile import reconcile

    report = reconcile(_cfg())
    if not report.ok:
        raise ValueError(f"gold out of sync with silver: {report.breaks}")


with DAG(
    dag_id="retail_lakehouse_daily",
    description="Daily medallion build: bronze/silver Delta -> dbt gold star schema",
    schedule="@daily",
    start_date=datetime(2026, 3, 2),
    catchup=False,
    tags=["lakehouse", "medallion", "dbt", "delta"],
) as dag:
    ingest_bronze = PythonOperator(task_id="ingest_bronze", python_callable=_ingest)
    refine_silver = PythonOperator(task_id="refine_silver", python_callable=_refine)
    validate_ge = PythonOperator(task_id="validate_ge", python_callable=_validate)
    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"RLH_DUCKDB={LAKE_ROOT}/gold.duckdb "
            f"dbt build --profiles-dir . "
            f"--vars '{{serving_dir: {LAKE_ROOT}/serving}}'"
        ),
    )
    reconcile_layers = PythonOperator(task_id="reconcile", python_callable=_reconcile)

    ingest_bronze >> refine_silver >> validate_ge >> build_gold >> reconcile_layers

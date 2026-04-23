"""Structure test for the Airflow DAG (parsed, not executed).

airflow is not installed in CI, so this asserts the DAG file parses and wires the
medallion task chain in the right order, without importing airflow.
"""
import ast
from pathlib import Path

DAG_FILE = Path(__file__).resolve().parents[1] / "dags" / "retail_lakehouse_dag.py"


def test_dag_parses():
    ast.parse(DAG_FILE.read_text(encoding="utf-8"))


def test_task_ids_present():
    s = DAG_FILE.read_text(encoding="utf-8")
    for task_id in ("ingest_bronze", "refine_silver", "validate_ge",
                    "build_gold", "reconcile"):
        assert f'task_id="{task_id}"' in s


def test_dependency_chain_in_order():
    s = DAG_FILE.read_text(encoding="utf-8")
    chain = ("ingest_bronze >> refine_silver >> validate_ge "
             ">> build_gold >> reconcile_layers")
    assert chain in s


def test_dbt_build_is_invoked():
    s = DAG_FILE.read_text(encoding="utf-8")
    assert "dbt build" in s

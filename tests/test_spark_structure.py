"""Structure tests for the production Spark jobs (parsed, not executed).

pyspark is not installed in CI, so these assert the jobs parse and encode the
intended Delta write pattern and silver cleaning logic, without importing Spark.
"""
import ast
from pathlib import Path

SPARK = Path(__file__).resolve().parents[1] / "spark"


def _src(name):
    return (SPARK / name).read_text(encoding="utf-8")


def test_jobs_parse_and_define_entrypoints():
    for name, funcs in (("bronze_ingest.py", {"build_spark", "ingest", "main"}),
                        ("silver_refine.py", {"build_spark", "refine", "main"})):
        tree = ast.parse(_src(name))
        defined = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
        assert funcs <= defined, (name, funcs - defined)


def test_bronze_appends_delta_with_lineage():
    s = _src("bronze_ingest.py")
    assert "SparkSession" in s
    assert 'format("delta")' in s and 'mode("append")' in s
    assert "_extract_date" in s and "_ingested_at" in s


def test_silver_dedups_cleans_and_trims():
    s = _src("silver_refine.py")
    assert "Window" in s and "row_number" in s          # latest-per-key
    assert 'mode("overwrite")' in s                      # rebuild current snapshot
    assert "upper" in s and "trim" in s and "coalesce" in s  # cleaning
    assert "left_semi" in s                              # referential trim

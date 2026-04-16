"""Great Expectations validation of the silver layer.

Suites are declared as plain data (SUITES) so they are both runnable here and
serializable to great_expectations/expectations/*.json for teams that run GE in
their own pipeline. Validation runs against the serving snapshots and fails the
pipeline (non-zero exit upstream) if any expectation is not met.
"""

from __future__ import annotations

import json
import os
import warnings
from dataclasses import dataclass, field
from pathlib import Path

os.environ.setdefault("TQDM_DISABLE", "1")
warnings.filterwarnings("ignore")

from .config import LakehouseConfig  # noqa: E402
from .logging_setup import get_logger  # noqa: E402

log = get_logger()

SUITES: dict[str, list[dict]] = {
    "customers": [
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "customer_id"}},
        {"type": "expect_column_values_to_be_unique", "kwargs": {"column": "customer_id"}},
        {"type": "expect_column_values_to_be_in_set",
         "kwargs": {"column": "segment", "value_set": ["SMB", "MID", "ENT"]}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "city"}},
    ],
    "products": [
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "product_id"}},
        {"type": "expect_column_values_to_be_unique", "kwargs": {"column": "product_id"}},
        {"type": "expect_column_values_to_be_in_set",
         "kwargs": {"column": "category", "value_set": ["HW", "SW", "ACC"]}},
    ],
    "orders": [
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "order_id"}},
        {"type": "expect_column_values_to_be_unique", "kwargs": {"column": "order_id"}},
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "customer_id"}},
        {"type": "expect_column_values_to_be_in_set",
         "kwargs": {"column": "status",
                    "value_set": ["NEW", "PAID", "SHIPPED", "CANCELLED", "REFUNDED"]}},
    ],
    "order_items": [
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "order_item_id"}},
        {"type": "expect_column_values_to_be_unique", "kwargs": {"column": "order_item_id"}},
        {"type": "expect_column_values_to_be_between",
         "kwargs": {"column": "quantity", "min_value": 1}},
    ],
}


@dataclass
class SuiteResult:
    table: str
    success: bool
    failed: list = field(default_factory=list)   # (expectation_type, column)


@dataclass
class ValidationReport:
    results: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(r.success for r in self.results)


def _build_suite(table: str):
    import great_expectations as gx
    import great_expectations.expectations as gxe

    factory = {
        "expect_column_values_to_not_be_null": gxe.ExpectColumnValuesToNotBeNull,
        "expect_column_values_to_be_unique": gxe.ExpectColumnValuesToBeUnique,
        "expect_column_values_to_be_in_set": gxe.ExpectColumnValuesToBeInSet,
        "expect_column_values_to_be_between": gxe.ExpectColumnValuesToBeBetween,
    }
    suite = gx.ExpectationSuite(f"silver_{table}")
    for spec in SUITES[table]:
        suite.add_expectation(factory[spec["type"]](**spec["kwargs"]))
    return suite


def validate_silver(cfg: LakehouseConfig) -> ValidationReport:
    import pandas as pd

    import great_expectations as gx

    ctx = gx.get_context(mode="ephemeral")
    source = ctx.data_sources.add_pandas("silver")
    report = ValidationReport()
    for table in SUITES:
        df = pd.read_parquet(cfg.serving_path(table))
        # decimals -> float so numeric expectations compare cleanly
        for col in df.columns:
            if df[col].dtype == object and len(df) and hasattr(df[col].iloc[0], "as_tuple"):
                df[col] = df[col].astype(float)
        asset = source.add_dataframe_asset(f"{table}_asset")
        bd = asset.add_batch_definition_whole_dataframe(f"{table}_bd")
        batch = bd.get_batch(batch_parameters={"dataframe": df})
        result = batch.validate(_build_suite(table))
        failed = [(r.expectation_config.type,
                   r.expectation_config.kwargs.get("column"))
                  for r in result.results if not r.success]
        report.results.append(SuiteResult(table, result.success, failed))
    log.info("expectations.validate",
             extra={"ok": report.ok,
                    "failures": {r.table: r.failed for r in report.results if not r.success}})
    return report


def write_suites(out_dir: str) -> None:
    """Serialize the suites to JSON for the shipped great_expectations/ folder."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for table, specs in SUITES.items():
        payload = {"suite_name": f"silver_{table}", "expectations": specs}
        (out / f"silver_{table}.json").write_text(
            json.dumps(payload, indent=2), encoding="utf-8")

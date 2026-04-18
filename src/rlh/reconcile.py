"""Cross-layer reconciliation: does the gold star schema faithfully represent
the silver layer it was built from?

Checks the invariants that a correct medallion build must hold:

- revenue parity: sum(fact_sales.amount) == sum(quantity * unit_price) in silver
- lineage counts: one fact row per silver order line; one current dim row per
  silver customer
- referential integrity: no fact row with an unresolved (null) customer key
- SCD2 integrity: exactly one current version per customer

Any break is reported, with revenue disagreement expressed in dollars, and the
CLI exits non-zero so it can gate a release.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

import duckdb

from . import delta_io
from .config import LakehouseConfig
from .logging_setup import get_logger

log = get_logger()


@dataclass
class ReconReport:
    fact_revenue: float = 0.0
    silver_revenue: float = 0.0
    revenue_drift: float = 0.0
    fact_rows: int = 0
    silver_items: int = 0
    dim_current: int = 0
    silver_customers: int = 0
    unresolved_customer_sk: int = 0
    multi_current_versions: int = 0
    breaks: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.breaks


def reconcile(cfg: LakehouseConfig) -> ReconReport:
    con = duckdb.connect(str(cfg.duckdb_path))
    con.register("silver_order_items", delta_io.read_arrow(cfg.silver_dir("order_items")))
    con.register("silver_customers", delta_io.read_arrow(cfg.silver_dir("customers")))

    def scalar(sql: str):
        return con.execute(sql).fetchone()[0]

    r = ReconReport()
    r.fact_revenue = float(scalar("select coalesce(sum(amount), 0) from fact_sales"))
    r.silver_revenue = float(scalar(
        "select coalesce(sum(quantity * unit_price), 0) from silver_order_items"))
    r.revenue_drift = abs(round(Decimal(str(r.fact_revenue))
                                - Decimal(str(r.silver_revenue)), 2))
    r.fact_rows = scalar("select count(*) from fact_sales")
    r.silver_items = scalar("select count(*) from silver_order_items")
    r.dim_current = scalar("select count(*) from dim_customer where is_current")
    r.silver_customers = scalar("select count(*) from silver_customers")
    r.unresolved_customer_sk = scalar(
        "select count(*) from fact_sales where customer_sk is null")
    r.multi_current_versions = scalar(
        "select count(*) from (select customer_id from dim_customer "
        "where is_current group by customer_id having count(*) > 1)")
    con.close()

    if r.revenue_drift >= Decimal("0.005"):
        r.breaks.append(f"revenue drift ${r.revenue_drift}")
    if r.fact_rows != r.silver_items:
        r.breaks.append(f"fact rows {r.fact_rows} != silver order lines {r.silver_items}")
    if r.dim_current != r.silver_customers:
        r.breaks.append(
            f"current dim rows {r.dim_current} != silver customers {r.silver_customers}")
    if r.unresolved_customer_sk:
        r.breaks.append(f"{r.unresolved_customer_sk} fact rows with unresolved customer_sk")
    if r.multi_current_versions:
        r.breaks.append(f"{r.multi_current_versions} customers with >1 current version")

    r.revenue_drift = float(r.revenue_drift)
    log.info("reconcile.done", extra={"ok": r.ok, "breaks": r.breaks,
                                     "revenue_drift": r.revenue_drift})
    return r

"""Silver: turn the accumulated bronze history into the current, clean snapshot.

For each table, keep the latest row per natural key, normalize fields, and
enforce referential integrity so the star schema builds cleanly:

- customers: latest extract per customer_id; segment upper/trimmed; null city -> UNKNOWN
- products:  latest per product_id
- orders:    latest per order_id; drop null-status and orphan-customer orders
- order_items: latest per line; drop non-positive quantity and lines whose order
  or product was dropped

Silver is written as a Delta table (the curated lakehouse copy) and exported as a
single parquet per table (the serving snapshot the dbt warehouse reads).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import duckdb
import pyarrow.parquet as pq

from . import delta_io
from .config import TABLES, LakehouseConfig
from .logging_setup import get_logger

log = get_logger()


@dataclass
class RefineStats:
    kept: dict = field(default_factory=dict)
    dropped: dict = field(default_factory=dict)


def refine(cfg: LakehouseConfig) -> RefineStats:
    con = duckdb.connect()
    for table in TABLES:
        con.register(f"bronze_{table}", delta_io.read_arrow(cfg.bronze_dir(table)))

    customers = con.execute("""
        with ranked as (
            select *, row_number() over (
                partition by customer_id order by _ingested_at desc, updated_at desc
            ) as rn from bronze_customers)
        select customer_id, name, upper(trim(segment)) as segment,
               coalesce(city, 'UNKNOWN') as city, updated_at
        from ranked where rn = 1
    """).to_arrow_table()
    con.register("silver_customers", customers)

    products = con.execute("""
        with ranked as (
            select *, row_number() over (
                partition by product_id order by _ingested_at desc) as rn
            from bronze_products)
        select product_id, name, category, unit_price from ranked where rn = 1
    """).to_arrow_table()
    con.register("silver_products", products)

    orders = con.execute("""
        with ranked as (
            select *, row_number() over (
                partition by order_id order by _ingested_at desc) as rn
            from bronze_orders)
        select order_id, customer_id, order_ts, status
        from ranked
        where rn = 1 and status is not null
          and customer_id in (select customer_id from silver_customers)
    """).to_arrow_table()
    con.register("silver_orders", orders)

    order_items = con.execute("""
        with ranked as (
            select *, row_number() over (
                partition by order_item_id order by _ingested_at desc) as rn
            from bronze_order_items)
        select order_item_id, order_id, product_id, quantity, unit_price
        from ranked
        where rn = 1 and quantity > 0
          and order_id in (select order_id from silver_orders)
          and product_id in (select product_id from silver_products)
    """).to_arrow_table()

    outputs = {"customers": customers, "products": products,
               "orders": orders, "order_items": order_items}
    stats = RefineStats()
    cfg.serving_dir.mkdir(parents=True, exist_ok=True)
    for table, tbl in outputs.items():
        delta_io.write_table(cfg.silver_dir(table), tbl, mode="overwrite")
        pq.write_table(tbl, cfg.serving_path(table))
        bronze_n = con.execute(f"select count(*) from bronze_{table}").fetchone()[0]
        stats.kept[table] = tbl.num_rows
        stats.dropped[table] = bronze_n - tbl.num_rows
    con.close()
    log.info("silver.refine", extra={"kept": stats.kept, "dropped": stats.dropped})
    return stats

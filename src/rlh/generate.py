"""Generate synthetic retail source extracts, one landing folder per day.

Models a common batch shape: a daily FULL extract of the dimensions (customers,
products) and an INCREMENTAL extract of the facts (that day's new orders and
order lines). Across days a fraction of customers change (segment / city, with a
bumped ``updated_at``), which is what the SCD2 snapshot downstream turns into
version history. The raw extracts carry realistic mess (duplicate rows, null
cities, mixed-case segments, a few orphan orders and non-positive quantities) so
the silver layer and the Great Expectations suites have something to catch.
"""

from __future__ import annotations

from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from .config import LakehouseConfig
from .logging_setup import get_logger

log = get_logger()

SEGMENTS = ["SMB", "MID", "ENT"]
SEG_MESSY = {"SMB": [" smb", "Smb ", "SMB"], "MID": ["mid", " MID ", "Mid"],
             "ENT": ["ent ", "ENT", " Ent"]}
CITIES = ["NYC", "LA", "SF", "SEA", "CHI", "AUS", "BOS"]
CATEGORIES = ["HW", "SW", "ACC"]
STATUSES = ["NEW", "PAID", "SHIPPED", "CANCELLED", "REFUNDED"]


def _lcg(seed: int):
    x = seed & 0x7FFFFFFF
    while True:
        x = (1103515245 * x + 12345) & 0x7FFFFFFF
        yield x


def _dec(cents: int) -> Decimal:
    return (Decimal(cents) / Decimal(100)).quantize(Decimal("0.01"))


class _World:
    """Holds mutable master data so day-over-day changes are coherent."""

    def __init__(self, customers: int, products: int, seed: int):
        self.rng = _lcg(seed)
        r = self.rng
        self.customers = {
            i: {"customer_id": i, "name": f"cust_{i}",
                "segment": SEGMENTS[next(r) % 3], "city": CITIES[next(r) % len(CITIES)],
                "updated_at": None}
            for i in range(1, customers + 1)
        }
        self.next_customer = customers + 1
        self.products = [
            {"product_id": 1000 + i, "name": f"prod_{i}",
             "category": CATEGORIES[next(r) % 3], "unit_price": _dec(100 + next(r) % 49900)}
            for i in range(products)
        ]
        self.next_order = 1
        self.next_item = 1

    def evolve(self, day: str, change_rate: float, new_customers: int):
        r = self.rng
        ids = list(self.customers)
        for cid in ids:
            if next(r) % 1000 < change_rate * 1000:
                cust = self.customers[cid]
                if next(r) % 2:
                    cust["segment"] = SEGMENTS[next(r) % 3]
                else:
                    cust["city"] = CITIES[next(r) % len(CITIES)]
                cust["updated_at"] = f"{day} 00:00:00"
        for _ in range(new_customers):
            cid = self.next_customer
            self.next_customer += 1
            self.customers[cid] = {
                "customer_id": cid, "name": f"cust_{cid}",
                "segment": SEGMENTS[next(r) % 3], "city": CITIES[next(r) % len(CITIES)],
                "updated_at": f"{day} 00:00:00"}


def _customers_extract(world, day, r):
    rows = []
    for cust in world.customers.values():
        seg = cust["segment"]
        messy_seg = SEG_MESSY[seg][next(r) % 3]                  # mixed case / spaces
        city = None if next(r) % 100 < 5 else cust["city"]       # ~5% null city
        updated = cust["updated_at"] or f"{day} 00:00:00"
        rows.append({**cust, "segment": messy_seg, "city": city, "updated_at": updated})
        if next(r) % 100 < 2:                                    # ~2% duplicate row
            rows.append({**cust, "segment": messy_seg, "city": city, "updated_at": updated})
    return pa.table({
        "customer_id": [x["customer_id"] for x in rows],
        "name": [x["name"] for x in rows],
        "segment": [x["segment"] for x in rows],
        "city": [x["city"] for x in rows],
        "updated_at": [x["updated_at"] for x in rows],
    })


def _products_extract(world):
    p = world.products
    return pa.table({
        "product_id": [x["product_id"] for x in p],
        "name": [x["name"] for x in p],
        "category": [x["category"] for x in p],
        "unit_price": pa.array([x["unit_price"] for x in p], pa.decimal128(12, 2)),
    })


def _orders_extract(world, day, r, n_orders, max_customer):
    orders, items = [], []
    for _ in range(n_orders):
        oid = world.next_order
        world.next_order += 1
        if next(r) % 100 < 1:                                    # ~1% orphan order
            cid = max_customer + 500 + next(r) % 100
        else:
            cid = 1 + next(r) % max_customer
        hh = next(r) % 24
        status = None if next(r) % 100 < 1 else STATUSES[next(r) % len(STATUSES)]
        orders.append({"order_id": oid, "customer_id": cid,
                       "order_ts": f"{day} {hh:02d}:00:00", "status": status})
        for _ in range(1 + next(r) % 3):
            prod = world.products[next(r) % len(world.products)]
            qty = 0 if next(r) % 100 < 1 else 1 + next(r) % 5    # ~1% non-positive qty
            items.append({"order_item_id": world.next_item, "order_id": oid,
                          "product_id": prod["product_id"], "quantity": qty,
                          "unit_price": prod["unit_price"]})
            world.next_item += 1
    orders_t = pa.table({
        "order_id": [o["order_id"] for o in orders],
        "customer_id": [o["customer_id"] for o in orders],
        "order_ts": [o["order_ts"] for o in orders],
        "status": [o["status"] for o in orders],
    })
    items_t = pa.table({
        "order_item_id": [i["order_item_id"] for i in items],
        "order_id": [i["order_id"] for i in items],
        "product_id": [i["product_id"] for i in items],
        "quantity": [i["quantity"] for i in items],
        "unit_price": pa.array([i["unit_price"] for i in items], pa.decimal128(12, 2)),
    })
    return orders_t, items_t


def generate(cfg: LakehouseConfig, days: int = 2, customers: int = 500,
             products: int = 40, orders_per_day: int = 1500,
             change_rate: float = 0.10, new_per_day: int = 20,
             seed: int = 7) -> list[str]:
    world = _World(customers, products, seed)
    r = world.rng
    dates = [f"2026-03-{2 + d:02d}" for d in range(days)]
    for i, day in enumerate(dates):
        if i > 0:
            world.evolve(day, change_rate, new_per_day)
        raw = cfg.raw_dir(day)
        raw.mkdir(parents=True, exist_ok=True)
        pq.write_table(_customers_extract(world, day, r), raw / "customers.parquet")
        pq.write_table(_products_extract(world), raw / "products.parquet")
        orders_t, items_t = _orders_extract(world, day, r, orders_per_day,
                                            world.next_customer - 1)
        pq.write_table(orders_t, raw / "orders.parquet")
        pq.write_table(items_t, raw / "order_items.parquet")
        log.info("generate.extract", extra={"date": day, "orders": orders_t.num_rows,
                                            "customers": len(world.customers)})
    return dates

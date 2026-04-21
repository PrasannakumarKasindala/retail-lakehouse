from rlh import bronze, delta_io, silver
from rlh.config import LakehouseConfig
from rlh.generate import generate


def _silver(cfg, table):
    return delta_io.read_arrow(cfg.silver_dir(table)).to_pylist()


def test_cleaning_and_referential_integrity(tmp_path):
    cfg = LakehouseConfig.create(str(tmp_path))
    cfg.ensure()
    dates = generate(cfg, days=2, customers=80, products=10,
                     orders_per_day=200, change_rate=0.2, new_per_day=5, seed=9)
    for d in dates:
        bronze.ingest(cfg, d)
    silver.refine(cfg)

    customers = _silver(cfg, "customers")
    ids = [c["customer_id"] for c in customers]
    assert len(ids) == len(set(ids))                       # dedup by natural key
    assert all(c["segment"] in {"SMB", "MID", "ENT"} for c in customers)  # normalized
    assert all(c["city"] is not None for c in customers)   # null city filled

    valid_customers = set(ids)
    orders = _silver(cfg, "orders")
    assert all(o["status"] is not None for o in orders)
    assert all(o["customer_id"] in valid_customers for o in orders)  # orphans dropped

    valid_orders = {o["order_id"] for o in orders}
    valid_products = {p["product_id"] for p in _silver(cfg, "products")}
    items = _silver(cfg, "order_items")
    assert all(i["quantity"] > 0 for i in items)           # bad quantity dropped
    assert all(i["order_id"] in valid_orders for i in items)
    assert all(i["product_id"] in valid_products for i in items)

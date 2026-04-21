import pyarrow.parquet as pq

from rlh.config import TABLES, LakehouseConfig
from rlh.generate import generate


def _read(cfg, date, table):
    return pq.read_table(cfg.raw_dir(date) / f"{table}.parquet")


def test_determinism(tmp_path):
    a = LakehouseConfig.create(str(tmp_path / "a"))
    a.ensure()
    b = LakehouseConfig.create(str(tmp_path / "b"))
    b.ensure()
    da = generate(a, days=2, customers=30, orders_per_day=50, seed=5)
    db = generate(b, days=2, customers=30, orders_per_day=50, seed=5)
    assert da == db
    for t in TABLES:
        assert _read(a, da[0], t).to_pydict() == _read(b, db[0], t).to_pydict()


def test_messiness_present(tmp_path):
    cfg = LakehouseConfig.create(str(tmp_path))
    cfg.ensure()
    dates = generate(cfg, days=1, customers=200, orders_per_day=400, seed=1)
    cust = _read(cfg, dates[0], "customers").to_pydict()
    # mixed-case / whitespace segments exist (something to normalize)
    assert any(s != s.strip().upper() for s in cust["segment"])
    # some null cities exist
    assert any(c is None for c in cust["city"])


def test_unchanged_customer_keeps_updated_at(tmp_path):
    cfg = LakehouseConfig.create(str(tmp_path))
    cfg.ensure()
    dates = generate(cfg, days=2, customers=100, orders_per_day=10,
                     change_rate=0.0, new_per_day=0, seed=3)
    d1 = {r["customer_id"]: r["updated_at"]
          for r in _read(cfg, dates[0], "customers").to_pylist()}
    d2 = {r["customer_id"]: r["updated_at"]
          for r in _read(cfg, dates[1], "customers").to_pylist()}
    # with no changes, updated_at must not advance (no spurious SCD2 versions)
    assert all(d2[k] == d1[k] for k in d1)

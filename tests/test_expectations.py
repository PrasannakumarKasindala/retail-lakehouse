import pyarrow as pa
import pyarrow.parquet as pq

from rlh import bronze, silver
from rlh.config import LakehouseConfig
from rlh.expectations import validate_silver
from rlh.generate import generate


def _clean_lake(tmp_path):
    cfg = LakehouseConfig.create(str(tmp_path))
    cfg.ensure()
    for d in generate(cfg, days=1, customers=60, products=8,
                      orders_per_day=120, seed=4):
        bronze.ingest(cfg, d)
    silver.refine(cfg)
    return cfg


def test_clean_silver_passes(tmp_path):
    cfg = _clean_lake(tmp_path)
    assert validate_silver(cfg).ok


def test_injected_bad_segment_is_caught(tmp_path):
    cfg = _clean_lake(tmp_path)
    tbl = pq.read_table(cfg.serving_path("customers"))
    row = {k: v[0] for k, v in tbl.slice(0, 1).to_pydict().items()}
    row["segment"] = "BOGUS"
    bad = pa.Table.from_pylist(tbl.to_pylist() + [row], schema=tbl.schema)
    pq.write_table(bad, cfg.serving_path("customers"))

    report = validate_silver(cfg)
    assert not report.ok
    cust = next(r for r in report.results if r.table == "customers")
    assert any(exp == "expect_column_values_to_be_in_set" for exp, _ in cust.failed)

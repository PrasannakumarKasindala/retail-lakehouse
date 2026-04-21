import gc

import pytest

from rlh import bronze, silver, warehouse
from rlh.config import LakehouseConfig
from rlh.expectations import validate_silver
from rlh.generate import generate


@pytest.fixture(scope="session")
def built_lake(tmp_path_factory):
    """Build a small lakehouse once (both days) and share it across tests."""
    root = tmp_path_factory.mktemp("lake")
    cfg = LakehouseConfig.create(str(root))
    cfg.ensure()
    dates = generate(cfg, days=2, customers=40, products=8,
                     orders_per_day=60, change_rate=0.25, new_per_day=3, seed=11)
    for d in dates:
        bronze.ingest(cfg, d)
        silver.refine(cfg)
        assert validate_silver(cfg).ok
        result = warehouse.build_gold(cfg)
        assert result.ok, result.tail
    yield cfg
    gc.collect()


@pytest.fixture(autouse=True)
def _collect():
    # Release any delta-rs handles promptly so the interpreter exits cleanly.
    yield
    gc.collect()

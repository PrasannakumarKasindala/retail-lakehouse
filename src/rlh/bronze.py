"""Bronze: land each raw extract into an append-only Delta table, unchanged
except for lineage columns. Bronze is the immutable audit trail; every daily
extract is preserved, so silver can always be rebuilt and history is never lost.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from . import delta_io
from .config import TABLES, LakehouseConfig
from .logging_setup import get_logger

log = get_logger()


def ingest(cfg: LakehouseConfig, extract_date: str) -> dict[str, int]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    counts: dict[str, int] = {}
    for table in TABLES:
        raw = pq.read_table(cfg.raw_dir(extract_date) / f"{table}.parquet")
        n = raw.num_rows
        raw = raw.append_column("_extract_date", pa.array([extract_date] * n))
        raw = raw.append_column("_ingested_at", pa.array([ingested_at] * n))
        delta_io.write_table(cfg.bronze_dir(table), raw, mode="append")
        counts[table] = n
    log.info("bronze.ingest", extra={"date": extract_date, **counts})
    return counts

"""Thin wrapper over delta-rs (the deltalake package) for reading and writing
the bronze and silver Delta tables as Arrow."""

from __future__ import annotations

import gc
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


def write_table(path, table: pa.Table, mode: str = "append") -> None:
    """Append or overwrite a Delta table. Returns nothing to avoid leaking a handle."""
    write_deltalake(str(path), table, mode=mode)


def read_arrow(path) -> pa.Table:
    dt = DeltaTable(str(path))
    tbl = dt.to_pyarrow_table()
    return tbl


def version(path) -> int:
    dt = DeltaTable(str(path))
    v = dt.version()
    return v


def history(path) -> list:
    dt = DeltaTable(str(path))
    h = list(dt.history())
    return h


def exists(path) -> bool:
    p = Path(path)
    if not (p / "_delta_log").exists():
        return False
    try:
        dt = DeltaTable(str(path))
        return True
    except Exception:
        return False

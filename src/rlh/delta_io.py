"""Thin, leak-safe wrapper over delta-rs (the deltalake package).

delta-rs keeps a native runtime alive behind each DeltaTable handle. If a handle
is still reachable at interpreter shutdown the runtime can abort the process on
teardown, which would turn a green test run red. Every function here therefore
opens a handle in local scope, extracts plain Arrow / Python data, and releases
the handle (del + gc.collect) before returning, so nothing delta-rs owns
survives the call.
"""

from __future__ import annotations

import gc
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


def write_table(path, table: pa.Table, mode: str = "append") -> None:
    """Append or overwrite a Delta table. Returns nothing to avoid leaking a handle."""
    write_deltalake(str(path), table, mode=mode)
    gc.collect()


def read_arrow(path) -> pa.Table:
    dt = DeltaTable(str(path))
    tbl = dt.to_pyarrow_table()
    del dt
    gc.collect()
    return tbl


def version(path) -> int:
    dt = DeltaTable(str(path))
    v = dt.version()
    del dt
    gc.collect()
    return v


def history(path) -> list:
    dt = DeltaTable(str(path))
    h = list(dt.history())
    del dt
    gc.collect()
    return h


def exists(path) -> bool:
    p = Path(path)
    if not (p / "_delta_log").exists():
        return False
    try:
        dt = DeltaTable(str(path))
        del dt
        gc.collect()
        return True
    except Exception:
        return False

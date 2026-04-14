"""Run the dbt gold build (snapshot + marts + tests) via dbt-duckdb.

dbt is invoked as a subprocess so this works with a normal dbt install and the
project's own profiles. Each daily run re-runs the snapshot (capturing the
current serving state as a new SCD2 version if a customer changed) and rebuilds
the marts, then runs dbt tests as the gold-layer quality gate.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass

from .config import LakehouseConfig
from .logging_setup import get_logger

log = get_logger()


@dataclass
class DbtResult:
    ok: bool
    returncode: int
    tail: str


def build_gold(cfg: LakehouseConfig, command: str = "build") -> DbtResult:
    env = dict(os.environ)
    env["RLH_DUCKDB"] = str(cfg.duckdb_path.resolve())
    args = [
        "dbt", command,
        "--project-dir", str(cfg.dbt_project_dir),
        "--profiles-dir", str(cfg.dbt_project_dir),
        "--vars", f"{{serving_dir: {cfg.serving_dir.resolve()}}}",
    ]
    proc = subprocess.run(args, env=env, capture_output=True, text=True)
    out = (proc.stdout or "") + (proc.stderr or "")
    tail = "\n".join(line for line in out.splitlines()
                     if any(k in line for k in ("PASS=", "ERROR", "FAIL", "Completed")))
    ok = proc.returncode == 0
    log.info("warehouse.build_gold", extra={"ok": ok, "returncode": proc.returncode})
    return DbtResult(ok=ok, returncode=proc.returncode, tail=tail[-2000:])

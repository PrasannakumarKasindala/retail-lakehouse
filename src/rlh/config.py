"""Paths and configuration for the lakehouse layers.

One root directory holds every layer so a run is self-contained and easy to
inspect or delete:

    <root>/raw/<extract_date>/<table>.parquet   landing zone (as received)
    <root>/bronze/<table>/                       Delta, append-only raw history
    <root>/silver/<table>/                       Delta, current cleaned snapshot
    <root>/serving/<table>.parquet               current snapshot the warehouse reads
    <root>/gold.duckdb                            dbt-built star schema
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

TABLES = ("customers", "products", "orders", "order_items")


@dataclass
class LakehouseConfig:
    root: Path
    dbt_project_dir: Path

    @classmethod
    def create(cls, root: str, dbt_project_dir: str | None = None) -> LakehouseConfig:
        root_p = Path(root)
        # Resolution order: explicit arg, RLH_DBT_PROJECT_DIR env (set this for a
        # non-editable install, where transform/ is not next to the package), then
        # the repo layout used by an editable install and the tests.
        chosen = dbt_project_dir or os.environ.get("RLH_DBT_PROJECT_DIR")
        dbt_p = (Path(chosen) if chosen
                 else Path(__file__).resolve().parents[2] / "transform")
        return cls(root=root_p, dbt_project_dir=dbt_p)

    def raw_dir(self, extract_date: str) -> Path:
        return self.root / "raw" / extract_date

    def bronze_dir(self, table: str) -> Path:
        return self.root / "bronze" / table

    def silver_dir(self, table: str) -> Path:
        return self.root / "silver" / table

    @property
    def serving_dir(self) -> Path:
        return self.root / "serving"

    def serving_path(self, table: str) -> Path:
        return self.serving_dir / f"{table}.parquet"

    @property
    def duckdb_path(self) -> Path:
        return self.root / "gold.duckdb"

    def ensure(self) -> None:
        for p in (self.root, self.serving_dir):
            p.mkdir(parents=True, exist_ok=True)

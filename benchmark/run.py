"""Measure real per-stage throughput of the local pipeline.

Times each medallion stage on a single process (delta-rs + dbt-duckdb). These
are honest local numbers; the Spark jobs target a cluster and are not benchmarked
here (their throughput depends on cluster sizing and is not claimed).

    python benchmark/run.py                # print a table
    python benchmark/run.py --write        # also write benchmark/results.md
"""

from __future__ import annotations

import argparse
import tempfile
import time
from pathlib import Path

from rlh import bronze, delta_io, silver, warehouse
from rlh.config import TABLES, LakehouseConfig
from rlh.generate import generate
from rlh.reconcile import reconcile


def _rows(cfg, layer_fn):
    return sum(delta_io.read_arrow(layer_fn(t)).num_rows for t in TABLES)


def run(customers: int, orders_per_day: int, days: int) -> list[tuple]:
    root = tempfile.mkdtemp(prefix="rlh_bench_")
    cfg = LakehouseConfig.create(root)
    cfg.ensure()

    t0 = time.perf_counter()
    dates = generate(cfg, days=days, customers=customers, orders_per_day=orders_per_day)
    t_gen = time.perf_counter() - t0

    t0 = time.perf_counter()
    for d in dates:
        bronze.ingest(cfg, d)
    t_bronze = time.perf_counter() - t0
    bronze_rows = _rows(cfg, cfg.bronze_dir)

    t0 = time.perf_counter()
    silver.refine(cfg)
    t_silver = time.perf_counter() - t0

    t0 = time.perf_counter()
    warehouse.build_gold(cfg)
    t_gold = time.perf_counter() - t0

    t0 = time.perf_counter()
    rep = reconcile(cfg)
    t_recon = time.perf_counter() - t0

    return [
        ("generate", t_gen, bronze_rows / t_gen),
        ("bronze append (delta-rs)", t_bronze, bronze_rows / t_bronze),
        ("silver refine (duckdb + delta-rs)", t_silver, bronze_rows / t_silver),
        ("gold build (dbt-duckdb, wall time)", t_gold, None),
        ("reconcile", t_recon, rep.fact_rows / t_recon),
    ]


def render(rows, cfg_desc) -> str:
    lines = ["# Benchmark results", "",
             f"Config: {cfg_desc}. Single process, local delta-rs + dbt-duckdb.",
             "Spark-cluster throughput is not claimed (not measured).", "",
             "| Stage | Wall time (s) | Throughput |",
             "|---|---|---|"]
    for name, secs, rate in rows:
        rate_s = f"{rate:,.0f} rows/s" if rate else "n/a (fixed dbt overhead)"
        lines.append(f"| {name} | {secs:.2f} | {rate_s} |")
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--customers", type=int, default=2000)
    p.add_argument("--orders-per-day", type=int, default=8000)
    p.add_argument("--days", type=int, default=2)
    p.add_argument("--write", action="store_true")
    args = p.parse_args(argv)
    rows = run(args.customers, args.orders_per_day, args.days)
    desc = (f"{args.customers} customers, {args.orders_per_day} orders/day, "
            f"{args.days} days")
    out = render(rows, desc)
    print(out)
    if args.write:
        Path(__file__).with_name("results.md").write_text(out, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

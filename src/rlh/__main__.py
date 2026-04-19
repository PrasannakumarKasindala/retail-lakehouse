"""``rlh`` CLI: build and check the retail lakehouse.

Exit codes: 0 success, 1 data-quality or reconciliation failure (release gate),
2 bad config, 3 IO / engine error.
"""

from __future__ import annotations

import argparse
import sys

from . import __version__
from .config import TABLES, LakehouseConfig
from .logging_setup import get_logger

log = get_logger()


def _cmd_generate(args) -> int:
    from .generate import generate
    cfg = LakehouseConfig.create(args.root)
    cfg.ensure()
    dates = generate(cfg, days=args.days, customers=args.customers,
                     orders_per_day=args.orders_per_day)
    print(f"generated {len(dates)} daily extracts under {cfg.root}/raw: "
          f"{', '.join(dates)}")
    return 0


def _run_days(cfg, dates) -> int:
    from . import bronze, silver, warehouse
    from .expectations import validate_silver
    from .report import render_validation
    for d in dates:
        bronze.ingest(cfg, d)
        silver.refine(cfg)
        val = validate_silver(cfg)
        if not val.ok:
            print(render_validation(val))
            return 1
        dbt = warehouse.build_gold(cfg)
        if not dbt.ok:
            print(f"dbt build failed (exit {dbt.returncode}):\n{dbt.tail}")
            return 1
    return 0


def _cmd_run(args) -> int:
    from .generate import generate
    from .reconcile import reconcile
    from .report import render_recon
    cfg = LakehouseConfig.create(args.root)
    cfg.ensure()
    dates = generate(cfg, days=args.days, customers=args.customers,
                     orders_per_day=args.orders_per_day)
    rc = _run_days(cfg, dates)
    if rc != 0:
        return rc
    report = reconcile(cfg)
    print(render_recon(report))
    return 0 if report.ok else 1


def _cmd_reconcile(args) -> int:
    from .reconcile import reconcile
    from .report import render_recon
    cfg = LakehouseConfig.create(args.root)
    report = reconcile(cfg)
    print(render_recon(report))
    return 0 if report.ok else 1


def _cmd_validate(args) -> int:
    from .expectations import validate_silver
    from .report import render_validation
    cfg = LakehouseConfig.create(args.root)
    val = validate_silver(cfg)
    print(render_validation(val))
    return 0 if val.ok else 1


def _cmd_inspect(args) -> int:
    import duckdb

    from . import delta_io
    cfg = LakehouseConfig.create(args.root)
    print(f"lakehouse root: {cfg.root}")
    for table in TABLES:
        if delta_io.exists(cfg.bronze_dir(table)):
            bv = delta_io.version(cfg.bronze_dir(table))
            br = delta_io.read_arrow(cfg.bronze_dir(table)).num_rows
            sr = (delta_io.read_arrow(cfg.silver_dir(table)).num_rows
                  if delta_io.exists(cfg.silver_dir(table)) else 0)
            print(f"  {table:12s} bronze v{bv} rows={br:,}  silver rows={sr:,}")
    if cfg.duckdb_path.exists():
        con = duckdb.connect(str(cfg.duckdb_path))
        for t in ("dim_customer", "dim_product", "dim_date", "fact_sales"):
            try:
                n = con.execute(f"select count(*) from {t}").fetchone()[0]
                print(f"  gold.{t:12s} rows={n:,}")
            except Exception:
                pass
        con.close()
    return 0


def _cmd_write_suites(args) -> int:
    from .expectations import write_suites
    write_suites(args.out)
    print(f"wrote GE suites to {args.out}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="rlh",
        description="Build a batch medallion retail lakehouse (bronze/silver Delta, "
                    "dbt gold star schema with SCD2) and reconcile the layers.")
    p.add_argument("--version", action="version", version=f"rlh {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="generate synthetic daily source extracts")
    g.add_argument("--root", required=True)
    g.add_argument("--days", type=int, default=2)
    g.add_argument("--customers", type=int, default=500)
    g.add_argument("--orders-per-day", type=int, default=1500)
    g.set_defaults(func=_cmd_generate)

    r = sub.add_parser("run", help="run the full pipeline and reconcile")
    r.add_argument("--root", required=True)
    r.add_argument("--days", type=int, default=2)
    r.add_argument("--customers", type=int, default=500)
    r.add_argument("--orders-per-day", type=int, default=1500)
    r.set_defaults(func=_cmd_run)

    v = sub.add_parser("validate", help="run Great Expectations suites on silver")
    v.add_argument("--root", required=True)
    v.set_defaults(func=_cmd_validate)

    rc = sub.add_parser("reconcile", help="reconcile gold against silver")
    rc.add_argument("--root", required=True)
    rc.set_defaults(func=_cmd_reconcile)

    ins = sub.add_parser("inspect", help="show layer versions and row counts")
    ins.add_argument("--root", required=True)
    ins.set_defaults(func=_cmd_inspect)

    ws = sub.add_parser("write-suites", help="serialize GE suites to JSON")
    ws.add_argument("--out", default="great_expectations/expectations")
    ws.set_defaults(func=_cmd_write_suites)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (ValueError, KeyError) as e:
        log.error("config.invalid", extra={"detail": str(e)})
        print(f"error: {e}", file=sys.stderr)
        return 2
    except FileNotFoundError as e:
        log.error("input.not_found", extra={"detail": str(e)})
        print(f"error: {e}", file=sys.stderr)
        return 3
    except Exception as e:
        log.error("engine.error", extra={"detail": str(e)})
        print(f"error: {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())

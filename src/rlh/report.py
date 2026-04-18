"""Render validation and reconciliation results as text."""

from __future__ import annotations

from .expectations import ValidationReport
from .reconcile import ReconReport


def render_validation(report: ValidationReport) -> str:
    lines = ["=" * 64, "silver data-quality (Great Expectations)", "=" * 64]
    for r in report.results:
        mark = "PASS" if r.success else "FAIL"
        lines.append(f"  {mark}  silver_{r.table}")
        for exp_type, col in r.failed:
            lines.append(f"        - {exp_type} on {col}")
    lines.append("-" * 64)
    lines.append("verdict: " + ("all suites passed" if report.ok
                                else "DATA-QUALITY FAILURE"))
    return "\n".join(lines)


def render_recon(r: ReconReport) -> str:
    lines = ["=" * 64,
             f"cross-layer reconciliation :: {'PARITY' if r.ok else 'DRIFT DETECTED'}",
             "=" * 64,
             f"fact revenue    : ${r.fact_revenue:,.2f}",
             f"silver revenue  : ${r.silver_revenue:,.2f}",
             f"fact rows       : {r.fact_rows:,}   "
             f"(silver order lines {r.silver_items:,})",
             f"current dim rows: {r.dim_current:,}   "
             f"(silver customers {r.silver_customers:,})"]
    if not r.ok:
        if r.revenue_drift >= 0.005:
            lines.append(f"revenue drift   : ${r.revenue_drift:,.2f}")
        for b in r.breaks:
            lines.append(f"  break: {b}")
    lines.append("-" * 64)
    lines.append("verdict: " + ("gold faithfully represents silver" if r.ok
                                else "GOLD OUT OF SYNC WITH SILVER"))
    return "\n".join(lines)

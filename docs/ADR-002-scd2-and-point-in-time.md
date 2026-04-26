# ADR-002: SCD2 via dbt snapshot (timestamp strategy) and point-in-time fact resolution

## Status
Accepted.

## Context
The customer dimension changes over time (segment, city). Reporting needs to
attribute each sale to the customer as they were at the time of the sale, not as
they are now. That requires slowly-changing-dimension Type 2 (SCD2): keep every
version of a customer with a validity window, and resolve each fact to the version
that was valid at the event time.

Two questions had to be answered: how to build the version history, and how to
resolve the surrogate key in the fact.

## Decision
Build SCD2 with a dbt snapshot rather than hand-rolled MERGE logic. dbt snapshots
are declarative, tested by the community, and produce the standard SCD2 columns
(`dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`). The version hash `dbt_scd_id`
becomes the dimension surrogate key, so every historical version has a stable,
unique key a fact can point at.

Use the timestamp strategy keyed on the source `updated_at`, not the check
strategy. This matters for backfills and historical loads: the timestamp strategy
sets `valid_from` to the business change time (`updated_at`), which is what a
point-in-time join needs. The check strategy would set `valid_from` to the
snapshot run time, so a historical order could never fall inside any window.

Resolve the fact surrogate key with a point-in-time join: each order line joins to
the customer version where `order_ts >= valid_from and order_ts < coalesce(valid_to,
timestamp '9999-12-31')`.

## Consequences
- Correct historical attribution: a sale made while a customer was SMB stays SMB in
  the fact even after they become ENT. This is verified end to end (an order dated
  before a change resolves to the older version; one dated after resolves to the
  newer).
- The timestamp strategy trusts `updated_at` to reflect real changes. If the source
  bumps `updated_at` without a real change, the snapshot opens a spurious version.
  The generator was fixed to advance `updated_at` only on a real attribute change
  (see the "hardest problem" note in the README); a source that cannot guarantee
  this would need the check strategy plus a business-time column, trading away
  clean historical `valid_from` values.
- The `coalesce(valid_to, ...)` in the join is mandatory: the current version has a
  null `valid_to`, and without the coalesce every order for a current customer
  fails the upper-bound predicate and gets a null surrogate key. This was the main
  bug found during the build.
- Reconciliation asserts SCD2 integrity (exactly one current version per customer)
  and that no fact row has an unresolved customer key, so a regression in either
  the snapshot or the join fails the run.

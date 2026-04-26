# Contributing

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Checks (run before every commit)

```bash
ruff check .
pytest --cov=rlh --cov-report=term-missing
```

Both must pass. The test suite builds a small lakehouse end to end (real Delta
tables, a real dbt star schema, Great Expectations, reconciliation), so it needs
the dev extras installed. It is verified to exit cleanly (no delta-rs teardown
abort); keep it that way by never returning or holding a `DeltaTable` from
`rlh.delta_io` (extract Arrow / Python data and release the handle).

## Conventions

- Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `perf:`, `build:`,
  `ci:`, `chore:`).
- Ruff enforces style (line length 95, import order, pyupgrade, bugbear).
- No em dashes in prose, code, or docs.
- The Spark jobs and Airflow DAG are validated by structure tests, not executed
  in CI. If you change them, update the corresponding structure test.

## Non-editable installs

`transform/` (the dbt project) lives outside the Python package. For a
non-editable install set `RLH_DBT_PROJECT_DIR` to its path (the Docker image does
this). Editable installs and the tests resolve it automatically.

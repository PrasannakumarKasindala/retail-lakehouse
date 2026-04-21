"""Exercise the CLI end to end (covers __main__ and report renderers)."""
import pytest

from rlh.__main__ import main


@pytest.fixture(scope="module")
def cli_root(tmp_path_factory):
    root = str(tmp_path_factory.mktemp("cli"))
    rc = main(["run", "--root", root, "--days", "2",
               "--customers", "30", "--orders-per-day", "40"])
    assert rc == 0
    return root


def test_run_parity(cli_root, capsys):
    # re-reconcile the already-built lake
    rc = main(["reconcile", "--root", cli_root])
    out = capsys.readouterr().out
    assert rc == 0
    assert "PARITY" in out
    assert "fact revenue" in out


def test_validate(cli_root, capsys):
    rc = main(["validate", "--root", cli_root])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Great Expectations" in out


def test_inspect(cli_root, capsys):
    rc = main(["inspect", "--root", cli_root])
    out = capsys.readouterr().out
    assert rc == 0
    assert "bronze" in out and "gold." in out


def test_generate_only(tmp_path, capsys):
    rc = main(["generate", "--root", str(tmp_path), "--days", "1",
               "--customers", "10", "--orders-per-day", "10"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "generated" in out


def test_write_suites(tmp_path, capsys):
    rc = main(["write-suites", "--out", str(tmp_path / "ge")])
    assert rc == 0
    assert (tmp_path / "ge" / "silver_customers.json").exists()


def test_reconcile_missing_lake_is_engine_error(tmp_path):
    # no gold.duckdb / no silver -> engine error path, exit 3
    rc = main(["reconcile", "--root", str(tmp_path / "empty")])
    assert rc == 3

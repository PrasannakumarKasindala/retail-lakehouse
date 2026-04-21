import duckdb

from rlh.reconcile import reconcile


def test_parity_on_clean_build(built_lake):
    report = reconcile(built_lake)
    assert report.ok, report.breaks
    assert report.revenue_drift < 0.005
    assert report.fact_rows == report.silver_items
    assert report.dim_current == report.silver_customers
    assert report.unresolved_customer_sk == 0
    assert report.multi_current_versions == 0


def test_scd2_versions_only_on_real_change(built_lake):
    con = duckdb.connect(str(built_lake.duckdb_path))
    total = con.execute("select count(*) from dim_customer").fetchone()[0]
    current = con.execute(
        "select count(*) from dim_customer where is_current").fetchone()[0]
    changed = con.execute(
        "select count(*) from (select customer_id from dim_customer "
        "group by customer_id having count(*) > 1)").fetchone()[0]
    con.close()
    assert total > current            # some history exists
    assert changed >= 1               # at least one real change captured
    assert total == current + changed  # only changed customers have extra versions


def test_point_in_time_resolution(built_lake):
    con = duckdb.connect(str(built_lake.duckdb_path))
    # every fact row resolved a customer version
    unresolved = con.execute(
        "select count(*) from fact_sales where customer_sk is null").fetchone()[0]
    # and each resolved version was actually valid on the order date
    violations = con.execute("""
        select count(*) from fact_sales f
        join dim_customer dc on f.customer_sk = dc.customer_sk
        where f.date_day <  cast(dc.valid_from as date)
           or f.date_day >= cast(coalesce(dc.valid_to, timestamp '9999-12-31') as date)
    """).fetchone()[0]
    con.close()
    assert unresolved == 0
    assert violations == 0


def test_injected_gold_break_is_detected(built_lake):
    con = duckdb.connect(str(built_lake.duckdb_path))
    con.execute("create table _fact_backup as select * from fact_sales")
    con.execute("delete from fact_sales where rowid in "
                "(select rowid from fact_sales limit 3)")
    con.close()
    try:
        report = reconcile(built_lake)
        assert not report.ok
        assert report.fact_rows != report.silver_items
    finally:
        con = duckdb.connect(str(built_lake.duckdb_path))
        con.execute("delete from fact_sales")
        con.execute("insert into fact_sales select * from _fact_backup")
        con.execute("drop table _fact_backup")
        con.close()

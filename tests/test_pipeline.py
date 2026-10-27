import duckdb
import pytest

from conftest import DEFECTS, GOOD_TRIPS
from lakehouse.pipeline import month_range, run


def _q(settings, sql):
    s = settings.silver.as_posix()
    return duckdb.sql(sql.replace("$silver", s)).fetchall()


def test_month_range_crosses_year():
    assert month_range("2023-11", "2024-02") == [(2023, 11), (2023, 12), (2024, 1), (2024, 2)]


def test_month_range_rejects_reversed():
    with pytest.raises(ValueError):
        month_range("2024-03", "2024-01")


def test_planted_defects_are_quarantined(lakehouse):
    settings, _, _ = lakehouse
    got = dict(_q(settings, "SELECT reject_reason, count(*) FROM read_parquet('$silver/quarantine_trips/*/*/*.parquet') GROUP BY 1"))
    assert got == DEFECTS


def test_silver_keeps_exactly_the_clean_rows(lakehouse):
    settings, _, _ = lakehouse
    n = _q(settings, "SELECT count(*) FROM read_parquet('$silver/trips/*/*/*.parquet')")[0][0]
    # every planted defect is an extra row on top of the GOOD_TRIPS clean rows
    assert n == GOOD_TRIPS


def test_silver_trip_ids_unique(lakehouse):
    settings, _, _ = lakehouse
    total, distinct = _q(settings, "SELECT count(*), count(DISTINCT trip_id) FROM read_parquet('$silver/trips/*/*/*.parquet')")[0]
    assert total == distinct


def test_gold_reconciles_with_silver(lakehouse):
    settings, con, _ = lakehouse
    gold_trips, gold_rev = con.execute("SELECT sum(trips), sum(revenue) FROM gold.fct_trips_hourly").fetchone()
    silver_trips, silver_rev = _q(settings, "SELECT count(*), sum(total_amount) FROM read_parquet('$silver/trips/*/*/*.parquet')")[0]
    assert gold_trips == silver_trips
    assert gold_rev == silver_rev


def test_gold_tables_exist(lakehouse):
    _, con, _ = lakehouse
    tables = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='gold'").fetchall()}
    assert {"dim_zone", "dim_date", "fct_trips_hourly", "fct_weather_hourly",
            "mart_daily_kpis", "mart_weather_impact", "mart_zone_monthly"} <= tables


def test_daily_kpis_cover_every_day_with_weather(lakehouse):
    _, con, _ = lakehouse
    days, with_weather = con.execute("SELECT count(*), count(avg_temp_c) FROM gold.mart_daily_kpis").fetchone()
    assert days == with_weather > 0


def test_weather_impact_has_dry_baseline(lakehouse):
    _, con, _ = lakehouse
    lift = con.execute("""SELECT max(abs(demand_lift_vs_dry_pct)) FROM gold.mart_weather_impact
                          WHERE weather_condition = 'dry'""").fetchone()[0]
    assert lift == 0


def test_all_critical_dq_checks_passed(lakehouse):
    _, con, run_id = lakehouse
    failed = con.execute("SELECT check_name FROM meta.dq_results WHERE run_id=? AND severity='critical' AND NOT passed",
                         [run_id]).fetchall()
    assert failed == []


def test_rerun_is_idempotent(lakehouse, tmp_path):
    settings, con, _ = lakehouse
    before = con.execute("SELECT sum(trips), sum(revenue) FROM gold.fct_trips_hourly").fetchone()
    con.close()
    run(settings, [(2024, 1)], stages=("silver", "gold"))
    with duckdb.connect(str(settings.warehouse), read_only=True) as c:
        after = c.execute("SELECT sum(trips), sum(revenue) FROM gold.fct_trips_hourly").fetchone()
    assert before == after

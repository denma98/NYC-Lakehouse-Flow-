"""GOLD: star schema + business marts, materialised in the DuckDB warehouse and
exported as Parquet so any tool (Power BI, Spark, pandas) can consume them."""
from __future__ import annotations

import logging

from .sql_utils import copy_to_parquet, render

log = logging.getLogger(__name__)

GOLD_SQL = ["gold/01_dimensions.sql", "gold/02_facts.sql", "gold/03_marts.sql"]


def register_silver_views(con, settings) -> None:
    s = settings.silver.as_posix()
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW silver_trips AS
        SELECT * FROM read_parquet('{s}/trips/*/*/*.parquet', hive_partitioning = true);
        CREATE OR REPLACE TEMP VIEW silver_quarantine AS
        SELECT * FROM read_parquet('{s}/quarantine_trips/*/*/*.parquet', hive_partitioning = true);
        CREATE OR REPLACE TEMP VIEW silver_weather AS
        SELECT * EXCLUDE (year, month)
        FROM read_parquet('{s}/weather_hourly/*/*/*.parquet', hive_partitioning = true);
        CREATE OR REPLACE TEMP VIEW silver_zones AS
        SELECT * FROM read_parquet('{s}/zones/zones.parquet');
    """)


def build(con, settings) -> list[str]:
    register_silver_views(con, settings)
    for f in GOLD_SQL:
        log.info("[gold] running %s", f)
        con.execute(render(f, silver=settings.silver))

    tables = [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'gold' ORDER BY 1"
    ).fetchall()]
    for t in tables:
        copy_to_parquet(con, f"SELECT * FROM gold.{t}", settings.gold / f"{t}.parquet")
    log.info("[gold] built %d tables: %s", len(tables), ", ".join(tables))
    return tables

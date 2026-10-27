"""SILVER: cleaned, typed, de-duplicated and validated data.

Processing is incremental and idempotent at the month-partition level: each run
overwrites only the partitions it touches, so re-running a month never creates
duplicates. Rejected rows land in silver/quarantine with a reject_reason.
"""
from __future__ import annotations

import logging
from datetime import date

from .sql_utils import copy_to_parquet, render

log = logging.getLogger(__name__)


def _next_month(year: int, month: int) -> date:
    return date(year + month // 12, month % 12 + 1, 1)


def _partition(year: int, month: int) -> str:
    return f"year={year}/month={month:02d}"


def build_trips(con, settings, year: int, month: int) -> dict:
    src = settings.bronze / "tlc_yellow" / _partition(year, month) / "trips.parquet"
    con.execute(render(
        "silver/trips.sql",
        src=src,
        period_start=f"{year}-{month:02d}-01",
        period_end=_next_month(year, month).isoformat(),
    ))
    part = _partition(year, month)
    copy_to_parquet(
        con,
        "SELECT * EXCLUDE (reject_reason) FROM trips_staged WHERE reject_reason IS NULL ORDER BY pickup_at",
        settings.silver / "trips" / part / "trips.parquet",
    )
    copy_to_parquet(
        con,
        "SELECT * FROM trips_staged WHERE reject_reason IS NOT NULL",
        settings.silver / "quarantine_trips" / part / "rejected.parquet",
    )
    stats = dict(zip(
        ("total", "accepted", "rejected"),
        con.execute("""
            SELECT count(*), count(*) FILTER (WHERE reject_reason IS NULL),
                   count(*) FILTER (WHERE reject_reason IS NOT NULL)
            FROM trips_staged
        """).fetchone(),
    ))
    log.info("[silver] trips %s-%02d: %s", year, month, stats)
    return stats


def build_weather(con, settings, year: int, month: int) -> int:
    part = _partition(year, month)
    con.execute(render("silver/weather.sql", src=settings.bronze / "weather" / part / "weather.json"))
    copy_to_parquet(con, "SELECT * FROM weather_staged ORDER BY observed_hour",
                    settings.silver / "weather_hourly" / part / "weather.parquet")
    n = con.execute("SELECT count(*) FROM weather_staged").fetchone()[0]
    log.info("[silver] weather %s-%02d: %d hours", year, month, n)
    return n


def build_zones(con, settings) -> int:
    con.execute(render("silver/zones.sql", src=settings.bronze / "zones" / "taxi_zone_lookup.csv"))
    copy_to_parquet(con, "SELECT * FROM zones_staged ORDER BY zone_id", settings.silver / "zones" / "zones.parquet")
    return con.execute("SELECT count(*) FROM zones_staged").fetchone()[0]

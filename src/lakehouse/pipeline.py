"""Orchestrates Bronze -> Silver -> Gold with quality gates between layers."""
from __future__ import annotations

import logging
from datetime import date

from . import bronze, gold, silver
from .config import Settings
from .metadata import connect, tracked_run
from .quality import GOLD_CHECKS, SILVER_CHECKS, run_checks

log = logging.getLogger(__name__)


def month_range(start: str, end: str) -> list[tuple[int, int]]:
    """'2024-01', '2024-03' -> [(2024, 1), (2024, 2), (2024, 3)]"""
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    if (y, m) > (ey, em):
        raise ValueError(f"start {start} is after end {end}")
    out = []
    while (y, m) <= (ey, em):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def validate_months(months: list[tuple[int, int]]) -> None:
    today = date.today()
    for y, m in months:
        if (y, m) >= (today.year, today.month):
            raise ValueError(f"{y}-{m:02d} is not complete yet; TLC publishes data ~2 months late")


def run(settings: Settings, months, stages=("bronze", "silver", "gold"), force=False) -> str:
    con = connect(settings)
    try:
        with tracked_run(con, f"run stages={','.join(stages)} months={months[0]}..{months[-1]}") as run_id:
            if "bronze" in stages:
                bronze.ingest_zones(con, settings, run_id, force)
                for y, m in months:
                    bronze.ingest_trips(con, settings, y, m, run_id, force)
                    bronze.ingest_weather(con, settings, y, m, run_id, force)

            if "silver" in stages:
                silver.build_zones(con, settings)
                for y, m in months:
                    silver.build_trips(con, settings, y, m)
                    silver.build_weather(con, settings, y, m)
                gold.register_silver_views(con, settings)
                run_checks(con, SILVER_CHECKS, run_id)  # quality gate before Gold

            if "gold" in stages:
                gold.build(con, settings)
                run_checks(con, GOLD_CHECKS, run_id)
            log.info("Run %s finished successfully", run_id)
            return run_id
    finally:
        con.close()

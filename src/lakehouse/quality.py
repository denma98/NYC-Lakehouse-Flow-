"""Declarative data-quality checks run between layers.

Each check is a SQL query returning one numeric value plus a predicate.
Results are persisted to meta.dq_results; any failing *critical* check stops
the pipeline before bad data is promoted to the next layer (a "quality gate").
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

from .metadata import now

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Check:
    layer: str
    name: str
    severity: str  # "critical" | "warn"
    sql: str
    expected: str
    passes: Callable[[float], bool]


class DataQualityError(RuntimeError):
    pass


SILVER_CHECKS = [
    Check("silver", "trips_not_empty", "critical",
          "SELECT count(*) FROM silver_trips", "> 0", lambda v: v > 0),
    Check("silver", "trip_id_unique", "critical",
          "SELECT count(*) - count(DISTINCT trip_id) FROM silver_trips", "= 0", lambda v: v == 0),
    Check("silver", "pickup_ts_not_null", "critical",
          "SELECT count(*) FROM silver_trips WHERE pickup_at IS NULL", "= 0", lambda v: v == 0),
    Check("silver", "no_negative_amounts", "critical",
          "SELECT count(*) FROM silver_trips WHERE total_amount < 0 OR fare_amount < 0", "= 0", lambda v: v == 0),
    Check("silver", "reject_rate_pct", "warn",
          """SELECT 100.0 * (SELECT count(*) FROM silver_quarantine)
                   / ((SELECT count(*) FROM silver_quarantine) + (SELECT count(*) FROM silver_trips))""",
          "<= 10%", lambda v: v <= 10),
    Check("silver", "zone_referential_integrity", "warn",
          """SELECT count(*) FROM silver_trips t
             WHERE NOT EXISTS (SELECT 1 FROM silver_zones z WHERE z.zone_id = t.pickup_zone_id)""",
          "= 0 orphans", lambda v: v == 0),
    Check("silver", "weather_hour_completeness_pct", "critical",
          """SELECT 100.0 * count(*) / (24 * (date_diff('day', min(observed_hour)::DATE, max(observed_hour)::DATE) + 1))
             FROM silver_weather""",
          ">= 99%", lambda v: v >= 99),
]

GOLD_CHECKS = [
    Check("gold", "trip_count_reconciles", "critical",
          "SELECT (SELECT sum(trips) FROM gold.fct_trips_hourly) - (SELECT count(*) FROM silver_trips)",
          "= 0", lambda v: v == 0),
    Check("gold", "revenue_reconciles_abs_diff", "critical",
          "SELECT abs((SELECT sum(revenue) FROM gold.fct_trips_hourly) - (SELECT sum(total_amount) FROM silver_trips))",
          "< 0.01", lambda v: v < 0.01),
    Check("gold", "daily_kpis_no_date_gaps", "warn",
          "SELECT (SELECT count(*) FROM gold.dim_date) - (SELECT count(*) FROM gold.mart_daily_kpis)",
          "= 0", lambda v: v == 0),
    Check("gold", "weather_joined_all_days", "warn",
          "SELECT count(*) FROM gold.mart_daily_kpis WHERE avg_temp_c IS NULL",
          "= 0", lambda v: v == 0),
]


def run_checks(con, checks: list[Check], run_id: str) -> list[dict]:
    results = []
    for c in checks:
        value = con.execute(c.sql).fetchone()[0]
        value = float(value) if value is not None else float("nan")
        ok = bool(c.passes(value))
        results.append({"check": c.name, "severity": c.severity, "passed": ok, "observed": value})
        con.execute(
            "INSERT INTO meta.dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [run_id, c.layer, c.name, c.severity, ok, f"{value:.4g}", c.expected, now()],
        )
        level = logging.INFO if ok else (logging.ERROR if c.severity == "critical" else logging.WARNING)
        log.log(level, "[dq:%s] %-32s %s observed=%.4g expected %s",
                c.layer, c.name, "PASS" if ok else "FAIL", value, c.expected)

    critical = [r["check"] for r in results if not r["passed"] and r["severity"] == "critical"]
    if critical:
        raise DataQualityError(f"Critical data-quality checks failed: {', '.join(critical)}")
    return results

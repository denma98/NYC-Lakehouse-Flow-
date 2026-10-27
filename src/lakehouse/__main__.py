"""Command-line entry point.

    lakehouse run    --start 2024-01 --end 2024-03     # full Bronze -> Silver -> Gold
    lakehouse run    --start 2024-01 --end 2024-03 --stages silver gold
    lakehouse status                                    # recent runs + latest DQ results
"""
from __future__ import annotations

import argparse
import logging
import sys

from .config import Settings
from .metadata import connect
from .pipeline import month_range, run, validate_months


def _status(settings: Settings) -> None:
    if not settings.warehouse.exists():
        print("No warehouse yet - run the pipeline first.")
        return
    con = connect(settings, read_only=True)
    print("\nRecent pipeline runs")
    print(con.sql("SELECT run_id, status, started_at, finished_at - started_at AS duration, command "
                  "FROM meta.pipeline_runs ORDER BY started_at DESC LIMIT 5"))
    print("Bronze ingestion manifest")
    print(con.sql("SELECT source, partition_key, round(bytes / 1e6, 1) AS mb, left(sha256, 12) AS sha256, ingested_at "
                  "FROM meta.ingestion_log ORDER BY source, partition_key"))
    print("Latest data-quality results")
    print(con.sql("""SELECT layer, check_name, severity, passed, observed, expected FROM meta.dq_results
                     WHERE run_id = (SELECT run_id FROM meta.dq_results ORDER BY checked_at DESC LIMIT 1)"""))
    con.close()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="lakehouse", description="NYC Mobility medallion lakehouse")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run the pipeline for a range of months")
    r.add_argument("--start", required=True, help="first month, YYYY-MM")
    r.add_argument("--end", help="last month, YYYY-MM (default: same as --start)")
    r.add_argument("--stages", nargs="+", default=["bronze", "silver", "gold"],
                   choices=["bronze", "silver", "gold"])
    r.add_argument("--force", action="store_true", help="re-download even if already ingested")
    sub.add_parser("status", help="show runs, ingestion manifest and DQ results")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    settings = Settings.from_env()

    if args.cmd == "status":
        _status(settings)
        return 0

    months = month_range(args.start, args.end or args.start)
    validate_months(months)
    try:
        run(settings, months, tuple(args.stages), args.force)
    except Exception as exc:
        logging.error("Pipeline failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

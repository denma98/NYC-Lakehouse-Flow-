"""BRONZE: land raw source data exactly as received (immutable, append-only).

Each source is written to a Hive-style partition path and recorded in
meta.ingestion_log with a SHA-256 checksum, which makes ingestion idempotent:
re-running a month is a no-op unless --force is passed.
"""
from __future__ import annotations

import calendar
import hashlib
import json
import logging
import time
from pathlib import Path

import requests

from . import config
from .metadata import already_ingested, log_ingestion

log = logging.getLogger(__name__)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _download(url: str, dest: Path, params: dict | None = None, retries: int = 3) -> None:
    """Stream to a temp file, then atomically rename, so a crash never leaves a
    half-written file in bronze. Retries with exponential backoff."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, params=params, stream=True, timeout=120) as r:
                r.raise_for_status()
                with tmp.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
            tmp.replace(dest)
            return
        except requests.RequestException as exc:
            if attempt == retries:
                raise
            wait = 2 ** attempt
            log.warning("Download failed (%s), retry %d/%d in %ds", exc, attempt, retries, wait)
            time.sleep(wait)


def _land(con, settings, source, partition_key, dest, url, run_id, force, params=None) -> Path:
    if not force and already_ingested(con, source, partition_key) and dest.exists():
        log.info("[bronze] %s %s already ingested - skipping", source, partition_key)
        return dest
    log.info("[bronze] downloading %s %s", source, partition_key)
    _download(url, dest, params)
    log_ingestion(con, source, partition_key, dest, _sha256(dest), dest.stat().st_size, run_id)
    return dest


def ingest_trips(con, settings, year: int, month: int, run_id: str, force=False) -> Path:
    dest = settings.bronze / "tlc_yellow" / f"year={year}" / f"month={month:02d}" / "trips.parquet"
    url = config.TLC_TRIPS_URL.format(year=year, month=month)
    return _land(con, settings, "tlc_yellow", f"{year}-{month:02d}", dest, url, run_id, force)


def ingest_weather(con, settings, year: int, month: int, run_id: str, force=False) -> Path:
    last_day = calendar.monthrange(year, month)[1]
    dest = settings.bronze / "weather" / f"year={year}" / f"month={month:02d}" / "weather.json"
    params = {
        "latitude": config.NYC_LAT,
        "longitude": config.NYC_LON,
        "start_date": f"{year}-{month:02d}-01",
        "end_date": f"{year}-{month:02d}-{last_day}",
        "hourly": ",".join(config.WEATHER_VARS),
        "timezone": "America/New_York",
    }
    path = _land(con, settings, "weather", f"{year}-{month:02d}", dest, config.WEATHER_URL, run_id, force, params)
    json.loads(path.read_text())  # fail fast on a malformed payload
    return path


def ingest_zones(con, settings, run_id: str, force=False) -> Path:
    dest = settings.bronze / "zones" / "taxi_zone_lookup.csv"
    return _land(con, settings, "zones", "static", dest, config.TLC_ZONES_URL, run_id, force)

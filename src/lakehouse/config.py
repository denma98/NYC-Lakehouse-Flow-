"""Central configuration. Every path is derived from one data root so tests and
CI can point the whole lakehouse at a temp directory via LAKEHOUSE_DATA_DIR."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = PROJECT_ROOT / "sql"

# Public, free, no-API-key data sources
TLC_TRIPS_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
TLC_ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"
NYC_LAT, NYC_LON = 40.7128, -74.0060
WEATHER_VARS = ["temperature_2m", "precipitation", "rain", "snowfall", "wind_speed_10m"]


@dataclass(frozen=True)
class Settings:
    data_dir: Path

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(Path(os.getenv("LAKEHOUSE_DATA_DIR", PROJECT_ROOT / "data")).resolve())

    @property
    def bronze(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def silver(self) -> Path:
        return self.data_dir / "silver"

    @property
    def gold(self) -> Path:
        return self.data_dir / "gold"

    @property
    def warehouse(self) -> Path:
        return self.data_dir / "warehouse.duckdb"

    def ensure_dirs(self) -> None:
        for p in (self.bronze, self.silver, self.gold):
            p.mkdir(parents=True, exist_ok=True)

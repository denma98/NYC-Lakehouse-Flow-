"""Builds a tiny synthetic Bronze layer (Jan 2024) with known defects planted in
it, so the full pipeline can be tested offline in seconds."""
import json
from datetime import datetime, timedelta

import duckdb
import pytest

from lakehouse.config import Settings
from lakehouse.pipeline import run

GOOD_TRIPS = 2000
# Planted defects -> expected reject_reason
DEFECTS = {
    "negative_amount": 1,
    "invalid_distance": 1,
    "out_of_period": 1,
    "non_positive_duration": 1,
    "duplicate": 3,
}


def _write_trips(path):
    path.parent.mkdir(parents=True)
    con = duckdb.connect()
    con.execute(f"""
        CREATE TABLE t AS
        SELECT
            (i % 2 + 1)::BIGINT                                         AS VendorID,
            TIMESTAMP '2024-01-01' + INTERVAL (i * 22) MINUTE            AS tpep_pickup_datetime,
            TIMESTAMP '2024-01-01' + INTERVAL (i * 22 + 5 + i % 30) MINUTE AS tpep_dropoff_datetime,
            (i % 4 + 1)::DOUBLE                                          AS passenger_count,
            round(0.5 + (i % 30) / 10.0, 2)                              AS trip_distance,
            1.0                                                          AS RatecodeID,
            'N'                                                          AS store_and_fwd_flag,
            (i % 263 + 1)::INTEGER                                       AS PULocationID,
            ((i * 7) % 263 + 1)::INTEGER                                 AS DOLocationID,
            (i % 3 + 1)::BIGINT                                          AS payment_type,
            round(5 + (i % 40), 2)                                       AS fare_amount,
            1.0 AS extra, 0.5 AS mta_tax,
            round((i % 5), 2)                                            AS tip_amount,
            0.0 AS tolls_amount, 1.0 AS improvement_surcharge,
            round(5 + (i % 40) + 2.5 + (i % 5), 2)                       AS total_amount,
            2.5                                                          AS congestion_surcharge
        FROM range({GOOD_TRIPS}) r(i);

        -- planted defects
        INSERT INTO t SELECT * REPLACE (-10.0 AS fare_amount, -5.0 AS total_amount) FROM t WHERE PULocationID = 5 LIMIT 1;
        INSERT INTO t SELECT * REPLACE (0.0 AS trip_distance, 777.0 AS total_amount) FROM t WHERE PULocationID = 6 LIMIT 1;
        INSERT INTO t SELECT * REPLACE (TIMESTAMP '2023-12-31 23:00' AS tpep_pickup_datetime,
                                        TIMESTAMP '2023-12-31 23:20' AS tpep_dropoff_datetime) FROM t LIMIT 1;
        INSERT INTO t SELECT * REPLACE (tpep_pickup_datetime AS tpep_dropoff_datetime, 888.0 AS total_amount)
                      FROM t WHERE PULocationID = 7 LIMIT 1;
        INSERT INTO t SELECT * FROM t WHERE PULocationID IN (10, 11, 12) AND total_amount > 0 AND trip_distance > 0
                      QUALIFY row_number() OVER (PARTITION BY PULocationID) = 1;
    """)
    con.execute(f"COPY t TO '{path.as_posix()}' (FORMAT parquet)")
    con.close()


def _write_weather(path):
    path.parent.mkdir(parents=True)
    start = datetime(2024, 1, 1)
    hours = [start + timedelta(hours=h) for h in range(31 * 24)]
    payload = {"hourly": {
        "time": [h.strftime("%Y-%m-%dT%H:%M") for h in hours],
        "temperature_2m": [round(2 + (i % 24) / 4, 1) for i in range(len(hours))],
        "precipitation": [3.0 if i % 50 == 0 else (0.4 if i % 13 == 0 else 0.0) for i in range(len(hours))],
        "rain": [0.0] * len(hours),
        "snowfall": [0.7 if i % 97 == 0 else 0.0 for i in range(len(hours))],
        "wind_speed_10m": [12.0] * len(hours),
    }}
    path.write_text(json.dumps(payload))


def _write_zones(path):
    path.parent.mkdir(parents=True)
    rows = ["LocationID,Borough,Zone,service_zone"]
    rows += [f'{i},{"Queens" if i in (132, 138) else "Manhattan"},"Zone {i}",Yellow Zone' for i in range(1, 266)]
    path.write_text("\n".join(rows))


@pytest.fixture(scope="session")
def lakehouse(tmp_path_factory):
    settings = Settings(tmp_path_factory.mktemp("lake"))
    _write_trips(settings.bronze / "tlc_yellow" / "year=2024" / "month=01" / "trips.parquet")
    _write_weather(settings.bronze / "weather" / "year=2024" / "month=01" / "weather.json")
    _write_zones(settings.bronze / "zones" / "taxi_zone_lookup.csv")
    run_id = run(settings, [(2024, 1)], stages=("silver", "gold"))
    con = duckdb.connect(str(settings.warehouse), read_only=True)
    yield settings, con, run_id
    con.close()

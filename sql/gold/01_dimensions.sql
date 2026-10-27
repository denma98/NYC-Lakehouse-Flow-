-- GOLD: conformed dimensions for the star schema.
CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.dim_zone AS
SELECT * FROM read_parquet('$silver/zones/zones.parquet');

CREATE OR REPLACE TABLE gold.dim_date AS
WITH bounds AS (
    SELECT min(pickup_date) AS d0, max(pickup_date) AS d1 FROM silver_trips
)
SELECT
    CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
    CAST(d AS DATE)                        AS date,
    year(d)                                AS year,
    month(d)                               AS month,
    strftime(d, '%Y-%m')                   AS year_month,
    day(d)                                 AS day,
    dayname(d)                             AS day_name,
    isodow(d) IN (6, 7)                    AS is_weekend
FROM bounds, range(d0, d1 + INTERVAL 1 DAY, INTERVAL 1 DAY) AS t(d);

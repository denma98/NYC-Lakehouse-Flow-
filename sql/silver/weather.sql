-- SILVER: flatten the Open-Meteo columnar JSON (parallel arrays) into one row per hour
-- and derive a business-friendly weather_condition bucket.
CREATE OR REPLACE TEMP TABLE weather_staged AS
WITH raw AS (
    SELECT hourly FROM read_json_auto('$src')
),
flat AS (
    SELECT
        CAST(unnest(hourly.time) AS TIMESTAMP)        AS observed_hour,
        CAST(unnest(hourly.temperature_2m) AS DOUBLE) AS temperature_c,
        CAST(unnest(hourly.precipitation) AS DOUBLE)  AS precipitation_mm,
        CAST(unnest(hourly.rain) AS DOUBLE)           AS rain_mm,
        CAST(unnest(hourly.snowfall) AS DOUBLE)       AS snowfall_cm,
        CAST(unnest(hourly.wind_speed_10m) AS DOUBLE) AS wind_speed_kmh
    FROM raw
)
SELECT
    observed_hour,
    temperature_c,
    precipitation_mm,
    rain_mm,
    snowfall_cm,
    wind_speed_kmh,
    CASE
        WHEN snowfall_cm > 0          THEN 'snow'
        WHEN precipitation_mm >= 2.5  THEN 'heavy_rain'
        WHEN precipitation_mm > 0     THEN 'light_rain'
        ELSE 'dry'
    END AS weather_condition
FROM flat
WHERE observed_hour IS NOT NULL
-- DST fall-back produces a repeated local hour; keep one reading per hour
QUALIFY row_number() OVER (PARTITION BY observed_hour ORDER BY observed_hour) = 1;

-- SILVER: conform, validate, de-duplicate and enrich one month of yellow-taxi trips.
-- Every row gets a reject_reason; NULL means clean. Rejected rows are not dropped,
-- they are routed to a quarantine table so data loss is visible and auditable.
CREATE OR REPLACE TEMP TABLE trips_staged AS
WITH typed AS (
    SELECT
        CAST(VendorID AS INTEGER)              AS vendor_id,
        CAST(tpep_pickup_datetime AS TIMESTAMP)  AS pickup_at,
        CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_at,
        CAST(passenger_count AS INTEGER)       AS passenger_count,
        CAST(trip_distance AS DOUBLE)          AS trip_distance_mi,
        CAST(RatecodeID AS INTEGER)            AS rate_code_id,
        CAST(PULocationID AS INTEGER)          AS pickup_zone_id,
        CAST(DOLocationID AS INTEGER)          AS dropoff_zone_id,
        CAST(payment_type AS INTEGER)          AS payment_type_id,
        CAST(fare_amount AS DECIMAL(12, 2))    AS fare_amount,
        CAST(tip_amount AS DECIMAL(12, 2))     AS tip_amount,
        CAST(tolls_amount AS DECIMAL(12, 2))   AS tolls_amount,
        CAST(total_amount AS DECIMAL(12, 2))   AS total_amount,
        CAST(COALESCE(congestion_surcharge, 0) AS DECIMAL(12, 2)) AS congestion_surcharge
    FROM read_parquet('$src')
),
keyed AS (
    SELECT
        md5(concat_ws('|', vendor_id, pickup_at, dropoff_at, pickup_zone_id,
                      dropoff_zone_id, trip_distance_mi, total_amount)) AS trip_id,
        *,
        date_diff('second', pickup_at, dropoff_at) / 60.0 AS trip_duration_min
    FROM typed
),
flagged AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY trip_id ORDER BY pickup_at) AS _dup_rank
    FROM keyed
)
SELECT
    trip_id,
    vendor_id,
    pickup_at,
    dropoff_at,
    CAST(pickup_at AS DATE)                    AS pickup_date,
    date_trunc('hour', pickup_at)              AS pickup_hour,
    passenger_count,
    trip_distance_mi,
    round(trip_duration_min, 2)                AS trip_duration_min,
    CASE WHEN trip_duration_min > 0
         THEN round(trip_distance_mi / (trip_duration_min / 60.0), 2) END AS avg_speed_mph,
    rate_code_id,
    pickup_zone_id,
    dropoff_zone_id,
    pickup_zone_id IN (1, 132, 138) OR dropoff_zone_id IN (1, 132, 138) AS is_airport_trip,
    payment_type_id,
    CASE payment_type_id
        WHEN 0 THEN 'flex_fare' WHEN 1 THEN 'credit_card' WHEN 2 THEN 'cash'
        WHEN 3 THEN 'no_charge' WHEN 4 THEN 'dispute' WHEN 6 THEN 'voided'
        ELSE 'unknown' END                     AS payment_type,
    fare_amount,
    tip_amount,
    tolls_amount,
    congestion_surcharge,
    total_amount,
    CASE WHEN payment_type_id = 1 AND fare_amount > 0
         THEN round(tip_amount / fare_amount * 100, 2) END AS tip_pct,
    CASE
        WHEN _dup_rank > 1                                         THEN 'duplicate'
        WHEN pickup_at IS NULL OR dropoff_at IS NULL               THEN 'missing_timestamp'
        WHEN pickup_at <  TIMESTAMP '$period_start'
          OR pickup_at >= TIMESTAMP '$period_end'                  THEN 'out_of_period'
        WHEN trip_duration_min <= 0                                THEN 'non_positive_duration'
        WHEN trip_duration_min > 360                               THEN 'duration_over_6h'
        WHEN trip_distance_mi <= 0 OR trip_distance_mi > 200       THEN 'invalid_distance'
        WHEN fare_amount < 0 OR total_amount < 0                   THEN 'negative_amount'
        WHEN total_amount > 1000                                   THEN 'implausible_amount'
        WHEN pickup_zone_id NOT BETWEEN 1 AND 265
          OR dropoff_zone_id NOT BETWEEN 1 AND 265                 THEN 'unknown_zone'
        WHEN trip_distance_mi / nullif(trip_duration_min / 60.0, 0) > 100 THEN 'impossible_speed'
    END AS reject_reason
FROM flagged;

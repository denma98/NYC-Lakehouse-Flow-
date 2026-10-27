-- GOLD: hourly fact table. Grain = (pickup_hour, pickup_zone_id).
-- Aggregating ~3M trips/month to this grain makes BI queries sub-second.
CREATE OR REPLACE TABLE gold.fct_trips_hourly AS
SELECT
    t.pickup_hour,
    CAST(strftime(t.pickup_date, '%Y%m%d') AS INTEGER) AS date_key,
    t.pickup_zone_id,
    count(*)                                           AS trips,
    sum(coalesce(t.passenger_count, 0))                AS passengers,
    sum(t.total_amount)                                AS revenue,
    sum(t.fare_amount)                                 AS fare_revenue,
    sum(t.tip_amount)                                  AS tip_revenue,
    sum(t.trip_distance_mi)                            AS distance_mi,
    sum(t.trip_duration_min)                           AS duration_min,
    count(*) FILTER (WHERE t.payment_type = 'credit_card') AS credit_card_trips,
    count(*) FILTER (WHERE t.is_airport_trip)          AS airport_trips
FROM silver_trips t
GROUP BY ALL;

CREATE OR REPLACE TABLE gold.fct_weather_hourly AS
SELECT * FROM silver_weather;

-- GOLD: business-facing data marts answering concrete questions.

-- Q1: How is the business doing day by day, and how does weather relate to it?
CREATE OR REPLACE TABLE gold.mart_daily_kpis AS
WITH daily AS (
    SELECT
        d.date,
        d.day_name,
        d.is_weekend,
        sum(f.trips)                                         AS trips,
        round(sum(f.revenue), 2)                             AS revenue,
        round(sum(f.revenue) / sum(f.trips), 2)              AS avg_revenue_per_trip,
        round(sum(f.duration_min) / sum(f.trips), 1)         AS avg_trip_min,
        round(sum(f.distance_mi) / (sum(f.duration_min) / 60), 1) AS avg_speed_mph,
        round(100.0 * sum(f.credit_card_trips) / sum(f.trips), 1) AS credit_card_pct,
        round(100.0 * sum(f.airport_trips) / sum(f.trips), 1)     AS airport_trip_pct
    FROM gold.fct_trips_hourly f
    JOIN gold.dim_date d USING (date_key)
    GROUP BY ALL
),
weather AS (
    SELECT
        CAST(observed_hour AS DATE)        AS date,
        round(avg(temperature_c), 1)       AS avg_temp_c,
        round(sum(precipitation_mm), 1)    AS precipitation_mm,
        round(sum(snowfall_cm), 1)         AS snowfall_cm,
        mode(weather_condition)            AS dominant_condition
    FROM gold.fct_weather_hourly
    GROUP BY 1
)
SELECT
    daily.*,
    weather.* EXCLUDE (date),
    round(avg(trips) OVER (ORDER BY daily.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) AS trips_7d_avg,
    round(100.0 * (revenue / lag(revenue) OVER (ORDER BY daily.date) - 1), 1)                AS revenue_dod_pct
FROM daily
LEFT JOIN weather USING (date)
ORDER BY daily.date;

-- Q2: How much does rain/snow change demand, for the same hour and day type?
CREATE OR REPLACE TABLE gold.mart_weather_impact AS
WITH city_hourly AS (
    SELECT pickup_hour, sum(trips) AS trips, sum(revenue) AS revenue, sum(duration_min) AS duration_min
    FROM gold.fct_trips_hourly
    GROUP BY 1
),
joined AS (
    SELECT
        w.weather_condition,
        hour(c.pickup_hour)                                 AS hour_of_day,
        CASE WHEN isodow(c.pickup_hour) IN (6, 7) THEN 'weekend' ELSE 'weekday' END AS day_type,
        c.trips, c.revenue, c.duration_min
    FROM city_hourly c
    JOIN gold.fct_weather_hourly w ON w.observed_hour = c.pickup_hour
),
agg AS (
    SELECT
        weather_condition, hour_of_day, day_type,
        count(*)                                 AS hours_observed,
        round(avg(trips), 1)                     AS avg_trips_per_hour,
        round(sum(revenue) / sum(trips), 2)      AS avg_revenue_per_trip,
        round(sum(duration_min) / sum(trips), 1) AS avg_trip_min
    FROM joined
    GROUP BY ALL
)
SELECT
    a.*,
    round(100.0 * (a.avg_trips_per_hour / dry.avg_trips_per_hour - 1), 1) AS demand_lift_vs_dry_pct
FROM agg a
LEFT JOIN agg dry
       ON dry.weather_condition = 'dry'
      AND dry.hour_of_day = a.hour_of_day
      AND dry.day_type = a.day_type
ORDER BY a.day_type, a.hour_of_day, a.weather_condition;

-- Q3: Which pickup zones drive revenue, and how are they trending month over month?
CREATE OR REPLACE TABLE gold.mart_zone_monthly AS
WITH m AS (
    SELECT
        d.year_month,
        f.pickup_zone_id AS zone_id,
        sum(f.trips)     AS trips,
        round(sum(f.revenue), 2) AS revenue
    FROM gold.fct_trips_hourly f
    JOIN gold.dim_date d USING (date_key)
    GROUP BY ALL
)
SELECT
    m.year_month,
    z.borough,
    z.zone_name,
    m.trips,
    m.revenue,
    rank() OVER (PARTITION BY m.year_month ORDER BY m.revenue DESC) AS revenue_rank,
    round(100.0 * m.revenue / sum(m.revenue) OVER (PARTITION BY m.year_month), 2) AS revenue_share_pct,
    round(100.0 * (m.revenue / lag(m.revenue) OVER (PARTITION BY m.zone_id ORDER BY m.year_month) - 1), 1) AS revenue_mom_pct
FROM m
LEFT JOIN gold.dim_zone z USING (zone_id)
ORDER BY m.year_month, revenue_rank;

"""Streamlit dashboard on top of the Gold layer.  Run:  streamlit run dashboard/app.py"""
import duckdb
import streamlit as st

from lakehouse.config import Settings

settings = Settings.from_env()
st.set_page_config(page_title="NYC Mobility Lakehouse", layout="wide")
st.title("NYC Taxi Demand & Weather")
st.caption("Gold layer of a Bronze -> Silver -> Gold medallion lakehouse (DuckDB + Parquet)")

if not settings.warehouse.exists():
    st.error("Warehouse not found. Run `lakehouse run --start 2024-01 --end 2024-03` first.")
    st.stop()


@st.cache_data
def q(sql: str):
    with duckdb.connect(str(settings.warehouse), read_only=True) as con:
        return con.sql(sql).df()


daily = q("SELECT * FROM gold.mart_daily_kpis ORDER BY date")
months = sorted(q("SELECT DISTINCT year_month FROM gold.mart_zone_monthly")["year_month"])

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Trips", f"{daily.trips.sum():,.0f}")
c2.metric("Revenue", f"${daily.revenue.sum() / 1e6:,.1f}M")
c3.metric("Avg $/trip", f"${daily.revenue.sum() / daily.trips.sum():.2f}")
c4.metric("Avg speed", f"{daily.avg_speed_mph.mean():.1f} mph")
c5.metric("Card payments", f"{daily.credit_card_pct.mean():.1f}%")

st.subheader("Daily trips vs 7-day average")
st.line_chart(daily.set_index("date")[["trips", "trips_7d_avg"]])

left, right = st.columns(2)
with left:
    st.subheader("Demand change vs dry weather (weekday)")
    impact = q("""
        SELECT weather_condition,
               round(sum(avg_trips_per_hour * hours_observed) / sum(hours_observed), 0) AS avg_trips_per_hour,
               round(avg(demand_lift_vs_dry_pct), 1) AS avg_lift_vs_dry_pct,
               sum(hours_observed) AS hours
        FROM gold.mart_weather_impact
        WHERE day_type = 'weekday'
        GROUP BY 1 ORDER BY 1
    """)
    st.bar_chart(impact.set_index("weather_condition")["avg_lift_vs_dry_pct"])
    st.dataframe(impact, hide_index=True, width="stretch")
with right:
    st.subheader("Top pickup zones by revenue")
    month = st.selectbox("Month", months, index=len(months) - 1)
    zones = q(f"""
        SELECT revenue_rank AS rank, zone_name, borough, trips, revenue, revenue_share_pct, revenue_mom_pct
        FROM gold.mart_zone_monthly WHERE year_month = '{month}' AND revenue_rank <= 15
    """)
    st.dataframe(zones, hide_index=True, width="stretch")

st.subheader("Hourly demand profile by weather")
profile = q("""
    SELECT hour_of_day, weather_condition, avg_trips_per_hour
    FROM gold.mart_weather_impact WHERE day_type = 'weekday'
""").pivot(index="hour_of_day", columns="weather_condition", values="avg_trips_per_hour")
st.line_chart(profile)

with st.expander("Pipeline observability: runs, data quality and quarantine"):
    st.dataframe(q("SELECT run_id, status, started_at, finished_at, command FROM meta.pipeline_runs "
                   "ORDER BY started_at DESC LIMIT 10"), hide_index=True, width="stretch")
    st.dataframe(q("""SELECT layer, check_name, severity, passed, observed, expected, checked_at
                      FROM meta.dq_results ORDER BY checked_at DESC LIMIT 30"""),
                 hide_index=True, width="stretch")
    s = settings.silver.as_posix()
    st.dataframe(q(f"""SELECT reject_reason, count(*) AS rows
                       FROM read_parquet('{s}/quarantine_trips/*/*/*.parquet') GROUP BY 1 ORDER BY 2 DESC"""),
                 hide_index=True, width="stretch")

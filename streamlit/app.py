import streamlit as st
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

# Load credentials from .env file
# Must be called before any os.getenv() calls
load_dotenv()

# --- Page config ---
# Must be the first Streamlit call — throws an error otherwise
st.set_page_config(
    page_title="Shipment Tracking Dashboard",
    page_icon="🚚",
    layout="wide"  # use full browser width
)

# --- Database connection ---
# @st.cache_resource caches the connection across reruns
# Without this, a new PostgreSQL connection opens on every page refresh
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "logistics"),
        user=os.getenv("POSTGRES_USER", "victor"),
        password=os.getenv("POSTGRES_PASSWORD", "password"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=5432
    )

# @st.cache_data(ttl=60) caches query results for 60 seconds
# After 60 seconds the cache expires and the next call hits PostgreSQL fresh
# This prevents hammering the database on every filter change
@st.cache_data(ttl=60)
def query(sql):
    conn = get_connection()
    return pd.read_sql(sql, conn)  # returns a pandas DataFrame directly

# --- Load all three mart tables ---
delivery_times = query("SELECT * FROM dbt_dev.mart_delivery_times")
delay_rates    = query("SELECT * FROM dbt_dev.mart_delay_rates")
# Exclude delivered and failed — they are endpoints not stages
# LEAD() on the last event returns NULL so they distort the bottleneck chart
bottlenecks    = query("""
    SELECT * FROM dbt_dev.mart_bottlenecks
    WHERE stage NOT IN ('delivered', 'failed')
""")

# --- Header ---
st.title("🚚 Shipment Tracking Dashboard")
st.caption("Real-time logistics analytics — powered by Kafka, PostgreSQL, and dbt")
st.divider()

# --- Region filter ---
# Get unique non-null regions from delivery_times, sorted alphabetically
regions = sorted(delivery_times["region"].dropna().unique().tolist())

# Dropdown with "All" as the default option
selected_region = st.selectbox("Filter by region", ["All"] + regions)

# Apply filter to all three DataFrames if a specific region is selected
# pandas boolean indexing: df[df["column"] == value] returns matching rows only
if selected_region != "All":
    delivery_times = delivery_times[delivery_times["region"] == selected_region]
    delay_rates    = delay_rates[delay_rates["region"] == selected_region]
    bottlenecks    = bottlenecks[bottlenecks["region"] == selected_region]

st.divider()

# --- KPI metrics ---
# Calculate summary numbers from the filtered DataFrames
total_shipments = int(delay_rates["total_shipments"].sum())
total_delivered = int(delay_rates["delivered_shipments"].sum())
total_failed    = int(delay_rates["failed_shipments"].sum())
avg_delivery    = round(delivery_times["avg_delivery_minutes"].mean(), 2) if not delivery_times.empty else 0

# st.columns(4) creates four equal-width columns side by side
# st.metric renders a big number with a label — the KPI card look
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total shipments", total_shipments)
col2.metric("Delivered", total_delivered)
col3.metric("Failed", total_failed)
col4.metric("Avg delivery time", f"{avg_delivery} min")

st.divider()

# --- Charts: delivery times and failure rates side by side ---
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Avg delivery time by carrier")
    if not delivery_times.empty:
        # Group by carrier, average delivery minutes across all regions and days
        # .reset_index() turns the grouped result back into a normal DataFrame
        chart_data = delivery_times.groupby("carrier")["avg_delivery_minutes"].mean().reset_index()
        chart_data.columns = ["carrier", "avg_delivery_minutes"]
        # set_index("carrier") makes carrier the x-axis label
        st.bar_chart(chart_data.set_index("carrier"))
    else:
        st.info("No delivery data available")

with col_right:
    st.subheader("Failure rate by carrier (%)")
    if not delay_rates.empty:
        chart_data = delay_rates.groupby("carrier")["failure_rate_pct"].mean().reset_index()
        chart_data.columns = ["carrier", "failure_rate_pct"]
        st.bar_chart(chart_data.set_index("carrier"))
    else:
        st.info("No delay rate data available")

st.divider()

# --- Bottlenecks chart ---
st.subheader("Avg time per stage (minutes)")
if not bottlenecks.empty:
    chart_data = bottlenecks.groupby("stage")["avg_stage_duration_minutes"].mean().reset_index()
    chart_data.columns = ["stage", "avg_stage_duration_minutes"]

    # Sort stages in pipeline order rather than alphabetically
    # enumerate() produces (0, "order_created"), (1, "picked_up") etc.
    # The dict comprehension flips it to {"order_created": 0, "picked_up": 1}
    # .map() applies that dict to the stage column creating a numeric sort key
    stage_order = ["order_created", "picked_up", "in_transit", "out_for_delivery"]
    chart_data["order"] = chart_data["stage"].map(
        {s: i for i, s in enumerate(stage_order)}
    )
    chart_data = chart_data.sort_values("order").drop(columns="order")
    st.bar_chart(chart_data.set_index("stage"))
else:
    st.info("No bottleneck data available")

st.divider()

# --- Raw data expander ---
# st.expander creates a collapsible section — collapsed by default
# st.tabs creates tabbed sections inside it
with st.expander("View raw mart data"):
    tab1, tab2, tab3 = st.tabs(["Delivery times", "Delay rates", "Bottlenecks"])
    with tab1:
        st.dataframe(delivery_times, use_container_width=True)
    with tab2:
        st.dataframe(delay_rates, use_container_width=True)
    with tab3:
        st.dataframe(bottlenecks, use_container_width=True)
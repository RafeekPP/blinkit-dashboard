"""
Blinkit Operations Dashboard

A comprehensive dashboard for Blinkit order and delivery analytics,
powered by Snowflake data from BLINKIT_DW.RAW.
"""

from datetime import date, timedelta

import altair as alt
import pandas as pd
import streamlit as st
st.set_page_config(
    page_title="Blinkit Operations Dashboard",
    page_icon="🚚",
    layout="wide",
)

CHART_HEIGHT = 320


# =============================================================================
# Snowflake Connection (auto-detect local vs Streamlit-in-Snowflake)
# =============================================================================

IS_LOCAL = True
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
    IS_LOCAL = False
except Exception:
    session = None


def run_query(sql: str) -> pd.DataFrame:
    """Run a SQL query using the appropriate connection method."""
    if IS_LOCAL:
        conn = st.connection("snowflake")
        return conn.query(sql)
    else:
        return session.sql(sql).to_pandas()


# =============================================================================
# Data Loading
# =============================================================================

@st.cache_data(ttl=600, show_spinner="Loading orders...")
def load_orders() -> pd.DataFrame:
    df = run_query("""
        SELECT
            ORDER_ID, CUSTOMER_ID, ORDER_DATE, PROMISED_DELIVERY_TIME,
            ACTUAL_DELIVERY_TIME, DELIVERY_STATUS, ORDER_TOTAL,
            PAYMENT_METHOD, STORE_ID
        FROM BLINKIT_DW.RAW.BLINKIT_ORDERS
    """)
    df.columns = df.columns.str.lower()
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df


@st.cache_data(ttl=600, show_spinner="Loading order items...")
def load_order_items() -> pd.DataFrame:
    df = run_query("""
        SELECT ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, TOTAL_PRICE
        FROM BLINKIT_DW.RAW.BLINKIT_ORDER_ITEMS
    """)
    df.columns = df.columns.str.lower()
    return df


@st.cache_data(ttl=600, show_spinner="Loading delivery performance...")
def load_delivery_performance() -> pd.DataFrame:
    df = run_query("""
        SELECT
            ORDER_ID, DELIVERY_PARTNER_ID, PROMISED_TIME, ACTUAL_TIME,
            DELIVERY_TIME_MINUTES, DISTANCE_KM, DELIVERY_STATUS,
            REASONS_IF_DELAYED
        FROM BLINKIT_DW.RAW.BLINKIT_DELIVERY_PERFORMANCE
    """)
    df.columns = df.columns.str.lower()
    df["promised_time"] = pd.to_datetime(df["promised_time"])
    df["actual_time"] = pd.to_datetime(df["actual_time"])
    return df


# =============================================================================
# Filter Helpers
# =============================================================================

def filter_by_date_range(df: pd.DataFrame, date_col: str, start: date, end: date) -> pd.DataFrame:
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    return df[(df[date_col].dt.date >= start) & (df[date_col].dt.date <= end)]


# =============================================================================
# Load Data
# =============================================================================

orders = load_orders()
items = load_order_items()
delivery = load_delivery_performance()

# =============================================================================
# Sidebar Filters
# =============================================================================

with st.sidebar:
    st.markdown("## Filters")

    # Date range
    min_date = orders["order_date"].min().date()
    max_date = orders["order_date"].max().date()
    date_range = st.date_input(
        "Order date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = min_date, max_date

    # Delivery status
    all_statuses = sorted(orders["delivery_status"].dropna().unique().tolist())
    selected_statuses = st.multiselect(
        "Delivery status",
        options=all_statuses,
        default=all_statuses,
    )

    # Payment method
    all_payments = sorted(orders["payment_method"].dropna().unique().tolist())
    selected_payments = st.multiselect(
        "Payment method",
        options=all_payments,
        default=all_payments,
    )

    st.divider()
    if st.button("Reset filters", type="secondary", use_container_width=True):
        st.session_state.clear()
        st.experimental_rerun()

# =============================================================================
# Apply Filters
# =============================================================================

filtered_orders = filter_by_date_range(orders, "order_date", start_date, end_date)
filtered_orders = filtered_orders[filtered_orders["delivery_status"].isin(selected_statuses)]
filtered_orders = filtered_orders[filtered_orders["payment_method"].isin(selected_payments)]

filtered_order_ids = set(filtered_orders["order_id"])
filtered_items = items[items["order_id"].isin(filtered_order_ids)]
filtered_delivery = delivery[delivery["order_id"].isin(filtered_order_ids)]

# =============================================================================
# Page Header
# =============================================================================

st.markdown("# Blinkit Operations Dashboard")
st.caption(f"Showing **{len(filtered_orders):,}** orders from {start_date} to {end_date}")

# =============================================================================
# KPI Row
# =============================================================================

total_orders = len(filtered_orders)
total_revenue = filtered_orders["order_total"].sum()
avg_order_value = filtered_orders["order_total"].mean() if total_orders > 0 else 0
total_items_sold = int(filtered_items["quantity"].sum()) if not filtered_items.empty else 0

on_time_count = len(filtered_delivery[filtered_delivery["delivery_status"] == "On Time"])
on_time_pct = (on_time_count / len(filtered_delivery) * 100) if len(filtered_delivery) > 0 else 0
avg_distance = filtered_delivery["distance_km"].mean() if not filtered_delivery.empty else 0

kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)
kpi1.metric("Total Orders", f"{total_orders:,}")
kpi2.metric("Total Revenue", f"\u20B9{total_revenue:,.0f}")
kpi3.metric("Avg Order Value", f"\u20B9{avg_order_value:,.2f}")
kpi4.metric("Items Sold", f"{total_items_sold:,}")
kpi5.metric("On-Time Delivery", f"{on_time_pct:.1f}%")
kpi6.metric("Avg Distance", f"{avg_distance:.1f} km")

# =============================================================================
# Row 1: Revenue Trend + Delivery Status Distribution
# =============================================================================

col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("**Daily Revenue Trend**")

    daily_rev = (
        filtered_orders
        .groupby(filtered_orders["order_date"].dt.date)["order_total"]
        .sum()
        .reset_index()
    )
    daily_rev.columns = ["date", "revenue"]
    daily_rev["date"] = pd.to_datetime(daily_rev["date"])
    daily_rev = daily_rev.sort_values("date")

    if not daily_rev.empty:
        # 7-day rolling average
        daily_rev["7d_avg"] = daily_rev["revenue"].rolling(7, min_periods=1).mean()

        melted = daily_rev.melt(
            id_vars=["date"],
            value_vars=["revenue", "7d_avg"],
            var_name="series",
            value_name="value",
        )
        melted["series"] = melted["series"].map({"revenue": "Daily", "7d_avg": "7-day Avg"})

        chart = (
            alt.Chart(melted)
            .mark_line()
            .encode(
                x=alt.X("date:T", title=None),
                y=alt.Y("value:Q", title="Revenue (\u20B9)", scale=alt.Scale(zero=False)),
                color=alt.Color("series:N", title=None, legend=alt.Legend(orient="bottom")),
                strokeDash=alt.condition(
                    alt.datum.series == "7-day Avg",
                    alt.value([5, 5]),
                    alt.value([0]),
                ),
                tooltip=[
                    alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
                    alt.Tooltip("series:N", title="Series"),
                    alt.Tooltip("value:Q", title="Revenue", format="\u20B9,.0f"),
                ],
            )
            .properties(height=CHART_HEIGHT)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No data for selected filters.")

with col2:
    st.markdown("**Delivery Status Breakdown**")

    if not filtered_delivery.empty:
        status_counts = (
            filtered_delivery
            .groupby("delivery_status")
            .size()
            .reset_index(name="count")
        )
        color_map = {
            "On Time": "#2ecc71",
            "Early": "#3498db",
            "Slightly Delayed": "#f39c12",
            "Delayed": "#e74c3c",
        }
        chart = (
            alt.Chart(status_counts)
            .mark_arc(innerRadius=50)
            .encode(
                theta=alt.Theta("count:Q"),
                color=alt.Color(
                    "delivery_status:N",
                    title=None,
                    scale=alt.Scale(
                        domain=list(color_map.keys()),
                        range=list(color_map.values()),
                    ),
                    legend=alt.Legend(orient="bottom"),
                ),
                tooltip=[
                    alt.Tooltip("delivery_status:N", title="Status"),
                    alt.Tooltip("count:Q", title="Orders"),
                ],
            )
            .properties(height=CHART_HEIGHT)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No delivery data for selected filters.")

# =============================================================================
# Row 2: Orders by Payment Method + Order Volume Trend
# =============================================================================

col3, col4 = st.columns(2)

with col3:
    st.markdown("**Orders by Payment Method**")

    if not filtered_orders.empty:
        payment_df = (
            filtered_orders
            .groupby("payment_method")
            .agg(orders=("order_id", "count"), revenue=("order_total", "sum"))
            .reset_index()
            .sort_values("orders", ascending=False)
        )
        chart = (
            alt.Chart(payment_df)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("payment_method:N", title=None, sort="-y"),
                y=alt.Y("orders:Q", title="Number of Orders"),
                color=alt.Color("payment_method:N", title=None, legend=None),
                tooltip=[
                    alt.Tooltip("payment_method:N", title="Method"),
                    alt.Tooltip("orders:Q", title="Orders", format=","),
                    alt.Tooltip("revenue:Q", title="Revenue", format="\u20B9,.0f"),
                ],
            )
            .properties(height=CHART_HEIGHT)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No data for selected filters.")

with col4:
    st.markdown("**Daily Order Volume**")

    if not filtered_orders.empty:
        daily_orders = (
            filtered_orders
            .groupby(filtered_orders["order_date"].dt.date)["order_id"]
            .count()
            .reset_index()
        )
        daily_orders.columns = ["date", "orders"]
        daily_orders["date"] = pd.to_datetime(daily_orders["date"])
        daily_orders = daily_orders.sort_values("date")

        chart = (
            alt.Chart(daily_orders)
            .mark_area(opacity=0.5, line=True, color="#3498db")
            .encode(
                x=alt.X("date:T", title=None),
                y=alt.Y("orders:Q", title="Orders", scale=alt.Scale(zero=False)),
                tooltip=[
                    alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
                    alt.Tooltip("orders:Q", title="Orders"),
                ],
            )
            .properties(height=CHART_HEIGHT)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No data for selected filters.")

# =============================================================================
# Row 3: Delivery Distance Distribution + Top Delay Reasons
# =============================================================================

col5, col6 = st.columns(2)

with col5:
    st.markdown("**Delivery Distance Distribution**")

    if not filtered_delivery.empty:
        chart = (
            alt.Chart(filtered_delivery)
            .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3, color="#9b59b6")
            .encode(
                x=alt.X("distance_km:Q", bin=alt.Bin(maxbins=20), title="Distance (km)"),
                y=alt.Y("count():Q", title="Number of Deliveries"),
                tooltip=[
                    alt.Tooltip("distance_km:Q", bin=alt.Bin(maxbins=20), title="Distance Range"),
                    alt.Tooltip("count():Q", title="Count"),
                ],
            )
            .properties(height=CHART_HEIGHT)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No delivery data for selected filters.")

with col6:
    st.markdown("**Top Delay Reasons**")

    delayed = filtered_delivery[filtered_delivery["reasons_if_delayed"].notna()]
    if not delayed.empty:
        reasons_df = (
            delayed
            .groupby("reasons_if_delayed")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=True)
        )
        chart = (
            alt.Chart(reasons_df)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, color="#e67e22")
            .encode(
                x=alt.X("count:Q", title="Number of Delays"),
                y=alt.Y("reasons_if_delayed:N", title=None, sort="-x"),
                tooltip=[
                    alt.Tooltip("reasons_if_delayed:N", title="Reason"),
                    alt.Tooltip("count:Q", title="Count"),
                ],
            )
            .properties(height=CHART_HEIGHT)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No delayed deliveries in selected filters.")

# =============================================================================
# Row 4: Top Products + Detailed Data Tables
# =============================================================================

st.markdown("**Top 15 Products by Quantity Sold**")

if not filtered_items.empty:
    top_products = (
        filtered_items
        .groupby("product_id")
        .agg(qty_sold=("quantity", "sum"), revenue=("total_price", "sum"), orders=("order_id", "nunique"))
        .reset_index()
        .sort_values("qty_sold", ascending=False)
        .head(15)
    )
    top_products["product_id"] = top_products["product_id"].astype(str)

    chart = (
        alt.Chart(top_products)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, color="#1abc9c")
        .encode(
            x=alt.X("product_id:N", title="Product ID", sort="-y"),
            y=alt.Y("qty_sold:Q", title="Quantity Sold"),
            tooltip=[
                alt.Tooltip("product_id:N", title="Product"),
                alt.Tooltip("qty_sold:Q", title="Qty Sold", format=","),
                alt.Tooltip("revenue:Q", title="Revenue", format="\u20B9,.0f"),
                alt.Tooltip("orders:Q", title="Orders", format=","),
            ],
        )
        .properties(height=CHART_HEIGHT)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("No item data for selected filters.")

# =============================================================================
# Row 5: Raw Data Explorer
# =============================================================================

with st.expander("Explore Raw Data"):
    tab1, tab2, tab3 = st.tabs(["Orders", "Order Items", "Delivery Performance"])

    with tab1:
        st.dataframe(
            filtered_orders.sort_values("order_date", ascending=False),
            use_container_width=True,
            height=400,
        )

    with tab2:
        st.dataframe(
            filtered_items,
            use_container_width=True,
            height=400,
        )

    with tab3:
        st.dataframe(
            filtered_delivery.sort_values("promised_time", ascending=False),
            use_container_width=True,
            height=400,
        )

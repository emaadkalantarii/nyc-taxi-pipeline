import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text

@st.cache_resource
def get_engine():
    if "neon" in st.secrets:
        raw_url = st.secrets["neon"]["url"]
        clean_url = raw_url.replace("postgresql://", "postgresql+pg8000://").replace("postgres://", "postgresql+pg8000://")
        clean_url = clean_url.split("?")[0]
        return create_engine(
            clean_url,
            connect_args={"ssl_context": True}
        )
    else:
        return create_engine("postgresql+pg8000://airflow:airflow@localhost:5432/nyc_taxi")


@st.cache_data(ttl=300)
def query(sql):
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


st.set_page_config(
    page_title="NYC Taxi Analytics",
    page_icon="🚕",
    layout="wide"
)

st.sidebar.title("🚕 NYC Taxi Pipeline")
st.sidebar.markdown("**Data Engineering Portfolio Project**")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Hourly Patterns", "Revenue Trends", "Location Analysis", "Payment Insights"]
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Pipeline Stack**")
st.sidebar.markdown("- PySpark (ETL)")
st.sidebar.markdown("- Apache Airflow (Orchestration)")
st.sidebar.markdown("- PostgreSQL (Warehouse)")
st.sidebar.markdown("- dbt (SQL Models)")
st.sidebar.markdown("- Docker (Infrastructure)")
st.sidebar.markdown("- GitHub Actions (CI/CD)")


if page == "Overview":
    st.title("🚕 NYC Yellow Taxi Analytics — Q1 2024")
    st.markdown("End-to-end data pipeline processing **8.4 million taxi trips** across New York City.")

    col1, col2, col3, col4 = st.columns(4)

    daily = query("SELECT * FROM analytics.mart_revenue_trends WHERE trip_date >= '2024-01-01' ORDER BY trip_date")

    with col1:
        total_trips = daily["total_trips"].sum()
        st.metric("Total Trips", f"{total_trips:,.0f}")

    with col2:
        total_revenue = daily["daily_revenue"].sum()
        st.metric("Total Revenue", f"${total_revenue:,.0f}")

    with col3:
        avg_fare = query("SELECT ROUND(AVG(avg_fare)::numeric, 2) as avg_fare FROM public.hourly_stats")
        st.metric("Avg Fare", f"${avg_fare['avg_fare'].values[0]}")

    with col4:
        peak = query("SELECT pickup_hour, total_trips FROM analytics.mart_peak_hours ORDER BY total_trips DESC LIMIT 1")
        st.metric("Busiest Hour", f"{peak['pickup_hour'].values[0]}:00")

    st.markdown("---")

    st.subheader("Daily Trip Volume & Revenue")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily["trip_date"],
        y=daily["total_trips"],
        name="Daily Trips",
        line=dict(color="#1f77b4", width=2)
    ))
    fig.add_trace(go.Scatter(
        x=daily["trip_date"],
        y=daily["revenue_7day_avg"],
        name="7-Day Avg Revenue",
        line=dict(color="#ff7f0e", width=2, dash="dash"),
        yaxis="y2"
    ))
    fig.update_layout(
        yaxis=dict(title="Daily Trips"),
        yaxis2=dict(title="Revenue ($)", overlaying="y", side="right"),
        hovermode="x unified",
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Pipeline Architecture")
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.info("**Extract**\nNYC TLC Parquet\n→ Bronze Layer\n9.5M rows")
    col2.info("**Transform**\nPySpark Cleaning\n→ Silver Layer\n8.4M rows")
    col3.info("**Aggregate**\nBusiness Logic\n→ Gold Layer\n4 tables")
    col4.info("**Model**\ndbt SQL\n→ Analytics\n4 mart tables")
    col5.info("**Serve**\nPostgreSQL\n→ Dashboard\nLive queries")


elif page == "Hourly Patterns":
    st.title("⏰ Hourly Demand Patterns")

    hourly = query("SELECT * FROM analytics.mart_peak_hours ORDER BY pickup_hour")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Trips by Hour of Day")
        weekday = hourly[hourly["is_weekend"] == False]
        weekend = hourly[hourly["is_weekend"] == True]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=weekday["pickup_hour"],
            y=weekday["total_trips"],
            name="Weekday",
            marker_color="#1f77b4"
        ))
        fig.add_trace(go.Bar(
            x=weekend["pickup_hour"],
            y=weekend["total_trips"],
            name="Weekend",
            marker_color="#ff7f0e"
        ))
        fig.update_layout(
            barmode="group",
            xaxis_title="Hour of Day",
            yaxis_title="Total Trips",
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Average Fare by Hour")
        fig2 = px.line(
            hourly[hourly["is_weekend"] == False],
            x="pickup_hour",
            y="avg_fare",
            title="Weekday Average Fare by Hour",
            labels={"pickup_hour": "Hour", "avg_fare": "Avg Fare ($)"},
            color_discrete_sequence=["#2ca02c"]
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Demand Level Classification")
    demand_counts = hourly.groupby("demand_level")["total_trips"].sum().reset_index()
    fig3 = px.pie(
        demand_counts,
        values="total_trips",
        names="demand_level",
        color_discrete_map={
            "Peak": "#d62728",
            "Moderate": "#ff7f0e",
            "Off-Peak": "#2ca02c"
        }
    )
    st.plotly_chart(fig3, use_container_width=True)


elif page == "Revenue Trends":
    st.title("💰 Revenue Trends")

    trends = query("""
        SELECT * FROM analytics.mart_revenue_trends
        WHERE trip_date >= '2024-01-01'
        ORDER BY trip_date
    """)

    st.subheader("Daily Revenue with 7-Day Rolling Average")
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=trends["trip_date"],
        y=trends["daily_revenue"],
        name="Daily Revenue",
        marker_color="#1f77b4",
        opacity=0.6
    ))
    fig.add_trace(go.Scatter(
        x=trends["trip_date"],
        y=trends["revenue_7day_avg"],
        name="7-Day Rolling Avg",
        line=dict(color="#d62728", width=2)
    ))
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Revenue ($)",
        height=450,
        hovermode="x unified"
    )
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Day-over-Day Revenue Change (%)")
        trends_clean = trends.dropna(subset=["revenue_day_over_day_pct"])
        colors = ["#d62728" if x < 0 else "#2ca02c"
                  for x in trends_clean["revenue_day_over_day_pct"]]
        fig2 = go.Figure(go.Bar(
            x=trends_clean["trip_date"],
            y=trends_clean["revenue_day_over_day_pct"],
            marker_color=colors
        ))
        fig2.update_layout(
            xaxis_title="Date",
            yaxis_title="Change (%)",
            height=350
        )
        st.plotly_chart(fig2, use_container_width=True)

    with col2:
        st.subheader("Active Pickup Zones per Day")
        fig3 = px.line(
            trends,
            x="trip_date",
            y="active_pickup_zones",
            labels={"trip_date": "Date", "active_pickup_zones": "Active Zones"},
            color_discrete_sequence=["#9467bd"]
        )
        fig3.update_layout(height=350)
        st.plotly_chart(fig3, use_container_width=True)


elif page == "Location Analysis":
    st.title("📍 Location Performance")

    locations = query("SELECT * FROM analytics.mart_location_performance ORDER BY total_revenue DESC")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 20 Pickup Zones by Revenue")
        top20 = locations.head(20)
        fig = px.bar(
            top20,
            x="total_revenue",
            y=top20["pu_location_id"].astype(str),
            orientation="h",
            labels={"total_revenue": "Total Revenue ($)", "y": "Location ID"},
            color="total_revenue",
            color_continuous_scale="Blues"
        )
        fig.update_layout(height=500, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Location Tier Distribution")
        tier_counts = locations["location_tier"].value_counts().reset_index()
        tier_counts.columns = ["tier", "count"]
        fig2 = px.pie(
            tier_counts,
            values="count",
            names="tier",
            color_discrete_map={
                "High Value": "#d62728",
                "Medium Value": "#ff7f0e",
                "Low Value": "#1f77b4",
                "Minimal Value": "#7f7f7f"
            }
        )
        fig2.update_layout(height=500)
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Revenue vs Pickups by Location")
    fig3 = px.scatter(
        locations,
        x="total_pickups",
        y="total_revenue",
        color="location_tier",
        size="avg_fare",
        hover_data=["pu_location_id", "revenue_per_trip"],
        labels={
            "total_pickups": "Total Pickups",
            "total_revenue": "Total Revenue ($)",
            "location_tier": "Tier"
        }
    )
    fig3.update_layout(height=450)
    st.plotly_chart(fig3, use_container_width=True)


elif page == "Payment Insights":
    st.title("💳 Payment Method Insights")

    payments = query("SELECT * FROM analytics.mart_payment_insights ORDER BY pickup_month, total_trips DESC")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Trips by Payment Method")
        payment_totals = payments.groupby("payment_type_desc")["total_trips"].sum().reset_index()
        fig = px.pie(
            payment_totals,
            values="total_trips",
            names="payment_type_desc",
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Average Tip Rate by Payment Method")
        tip_rates = payments.groupby("payment_type_desc")["tip_rate_pct"].mean().reset_index()
        fig2 = px.bar(
            tip_rates.sort_values("tip_rate_pct", ascending=True),
            x="tip_rate_pct",
            y="payment_type_desc",
            orientation="h",
            labels={"tip_rate_pct": "Avg Tip Rate (%)", "payment_type_desc": "Payment Method"},
            color="tip_rate_pct",
            color_continuous_scale="Greens"
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Market Share by Payment Method per Month")
    fig3 = px.bar(
        payments,
        x="pickup_month",
        y="market_share_pct",
        color="payment_type_desc",
        barmode="stack",
        labels={
            "pickup_month": "Month",
            "market_share_pct": "Market Share (%)",
            "payment_type_desc": "Payment Method"
        },
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    fig3.update_layout(height=400)
    st.plotly_chart(fig3, use_container_width=True)

    st.subheader("Revenue by Payment Method per Month")
    fig4 = px.line(
        payments,
        x="pickup_month",
        y="total_revenue",
        color="payment_type_desc",
        markers=True,
        labels={
            "pickup_month": "Month",
            "total_revenue": "Total Revenue ($)",
            "payment_type_desc": "Payment Method"
        }
    )
    fig4.update_layout(height=400)
    st.plotly_chart(fig4, use_container_width=True)
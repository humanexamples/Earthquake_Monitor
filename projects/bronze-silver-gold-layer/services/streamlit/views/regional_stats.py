"""
pages/regional_stats.py  ·  Page 3 — Regional Stats
Bar and scatter charts comparing regions by event count,
average composite score, max magnitude, and population exposure.
Also shows the global daily summary trend line.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from db import query


def render():
    st.title("📊 Regional Stats")
    st.caption("Aggregated statistics per region from the Gold layer.")

    days = st.slider("Show last N days", 1, 90, 30)

    # ── Regional stats ────────────────────────────────────────────────────────
    regional = query("""
        SELECT
            region,
            SUM(event_count)                              AS total_events,
            ROUND(AVG(avg_magnitude)::numeric, 2)         AS avg_magnitude,
            ROUND(MAX(max_magnitude)::numeric, 2)         AS max_magnitude,
            ROUND(AVG(avg_composite_score)::numeric, 2)   AS avg_composite_score,
            ROUND(MAX(max_composite_score)::numeric, 2)   AS max_composite_score,
            ROUND(AVG(avg_tsunami_risk)::numeric, 2)      AS avg_tsunami_risk,
            ROUND(MAX(max_tsunami_risk)::numeric, 2)      AS max_tsunami_risk,
            ROUND(SUM(total_population_exposed)::numeric / 1e6, 2) AS pop_exposed_M
        FROM gold.regional_stats
        WHERE processing_date >= CURRENT_DATE - INTERVAL '%s days'
        GROUP BY region
        ORDER BY avg_composite_score DESC
        LIMIT 20
    """, [days])

    if regional.empty:
        st.warning("No regional data found. Run the gold_aggregation_pipeline DAG first.")
        return

    # ── Top regions bar chart ─────────────────────────────────────────────────
    st.subheader("Top 20 regions by average composite score")
    fig_bar = px.bar(
        regional,
        x="avg_composite_score",
        y="region",
        orientation="h",
        color="avg_composite_score",
        color_continuous_scale="RdYlGn_r",
        labels={"avg_composite_score": "Avg Composite Score", "region": "Region"},
    )
    fig_bar.update_layout(yaxis={"categoryorder": "total ascending"},
                          height=550, margin=dict(l=0, r=0, t=20, b=0))
    st.plotly_chart(fig_bar, use_container_width=True)

    # ── Scatter: magnitude vs composite score ─────────────────────────────────
    st.subheader("Magnitude vs composite score by region")
    fig_scatter = px.scatter(
        regional,
        x="max_magnitude",
        y="max_composite_score",
        size="total_events",
        color="avg_tsunami_risk",
        color_continuous_scale="OrRd",
        hover_name="region",
        labels={
            "max_magnitude":     "Max Magnitude",
            "max_composite_score": "Max Composite Score",
            "avg_tsunami_risk":  "Avg Tsunami Risk",
        },
    )
    fig_scatter.update_layout(height=450, margin=dict(l=0, r=0, t=20, b=0))
    st.plotly_chart(fig_scatter, use_container_width=True)

    # ── Global daily summary trend ────────────────────────────────────────────
    st.subheader("Global daily event count (last 30 days)")
    daily = query("""
        SELECT
            processing_date::text AS date,
            total_events,
            ROUND(max_magnitude_global::numeric, 1)        AS max_magnitude,
            ROUND(max_composite_score_global::numeric, 2)  AS max_composite_score
        FROM gold.daily_summary
        WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY processing_date
    """)

    if not daily.empty:
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Bar(
            x=daily["date"], y=daily["total_events"],
            name="Total events", marker_color="#0e7490"))
        fig_trend.add_trace(go.Scatter(
            x=daily["date"], y=daily["max_magnitude"],
            name="Max magnitude", yaxis="y2",
            line=dict(color="#f97316", width=2)))
        fig_trend.update_layout(
            yaxis=dict(title="Event count"),
            yaxis2=dict(title="Max magnitude", overlaying="y", side="right"),
            height=350, margin=dict(l=0, r=0, t=20, b=0),
            legend=dict(orientation="h"))
        st.plotly_chart(fig_trend, use_container_width=True)

    # ── Raw data table ────────────────────────────────────────────────────────
    with st.expander("Show raw regional data"):
        st.dataframe(regional.reset_index(drop=True), use_container_width=True)


render()

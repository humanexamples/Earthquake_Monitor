"""
pages/data_quality.py  ·  Page 5 — Data Quality Profiling
Shows row counts, duplicate counts, and null rates
for Bronze, Silver, and Gold layers side by side.
Data populated by the daily data_quality_report Airflow DAG.
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from db import query


def render():
    st.title("✅ Data Quality Profiling")
    st.caption("Daily profile of all three medallion layers. Populated by the data_quality_report Airflow DAG.")

    days = st.slider("Show last N days", 1, 30, 7)

    # ── Load quality profiles ─────────────────────────────────────────────────
    profiles = query("""
        SELECT layer, date::text AS date, row_count,
               distinct_ids, duplicate_count
        FROM gold.quality_profiles
        WHERE date >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY date DESC, layer
    """, [days])

    if profiles.empty:
        st.warning("No quality profiles found. The data_quality_report DAG runs daily at midnight.")
        return

    # ── Latest values per layer ────────────────────────────────────────────────
    st.subheader("Latest profile per layer")
    latest = profiles.groupby("layer").first().reset_index()

    cols = st.columns(3)
    layer_icons = {"bronze": "🥉", "silver": "🥈", "gold": "🥇"}
    layer_colors = {"bronze": "#b45309", "silver": "#64748b", "gold": "#ca8a04"}

    for i, row in latest.iterrows():
        with cols[i % 3]:
            icon  = layer_icons.get(row["layer"], "📦")
            color = layer_colors.get(row["layer"], "#0e7490")
            st.markdown(f"### {icon} {row['layer'].capitalize()} Layer")
            st.metric("Row count",        f"{int(row['row_count']):,}")
            st.metric("Distinct IDs",     f"{int(row['distinct_ids']):,}")
            dup_color = "🔴" if row["duplicate_count"] > 0 else "🟢"
            st.metric("Duplicates",       f"{dup_color} {int(row['duplicate_count'])}")
            st.caption(f"As of: {row['date']}")

    st.divider()

    # ── Row count trend over time ─────────────────────────────────────────────
    st.subheader("Row count trend over time")
    fig = go.Figure()
    for layer, color in layer_colors.items():
        df_layer = profiles[profiles["layer"] == layer].sort_values("date")
        if not df_layer.empty:
            fig.add_trace(go.Scatter(
                x=df_layer["date"],
                y=df_layer["row_count"],
                name=f"{layer_icons[layer]} {layer.capitalize()}",
                line=dict(color=color, width=2),
                mode="lines+markers",
            ))
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Row count",
        height=350,
        margin=dict(l=0, r=0, t=20, b=0),
        legend=dict(orientation="h"),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Duplicate count trend ─────────────────────────────────────────────────
    st.subheader("Duplicate count per layer")
    dup_pivot = profiles.pivot_table(
        index="date", columns="layer", values="duplicate_count", aggfunc="sum"
    ).fillna(0).reset_index()

    fig_dup = px.bar(
        dup_pivot.melt(id_vars="date", var_name="layer", value_name="duplicates"),
        x="date", y="duplicates", color="layer",
        color_discrete_map=layer_colors,
        barmode="group",
        labels={"date": "Date", "duplicates": "Duplicate count", "layer": "Layer"},
    )
    fig_dup.update_layout(height=300, margin=dict(l=0, r=0, t=20, b=0))
    st.plotly_chart(fig_dup, use_container_width=True)

    # ── Gold layer event stats from DWH ──────────────────────────────────────
    st.divider()
    st.subheader("Gold layer — event count and max score per day")
    gold_daily = query("""
        SELECT
            processing_date::text  AS date,
            COUNT(*)               AS event_count,
            ROUND(MAX(composite_score)::numeric, 2) AS max_composite_score,
            ROUND(AVG(composite_score)::numeric, 2) AS avg_composite_score
        FROM gold.earthquake_events
        WHERE processing_date >= CURRENT_DATE - INTERVAL '%s days'
        GROUP BY processing_date
        ORDER BY processing_date
    """, [days])

    if not gold_daily.empty:
        st.dataframe(gold_daily.reset_index(drop=True), use_container_width=True)

    # ── Raw profiles table ────────────────────────────────────────────────────
    with st.expander("Show raw quality profiles"):
        st.dataframe(profiles.reset_index(drop=True), use_container_width=True)


render()

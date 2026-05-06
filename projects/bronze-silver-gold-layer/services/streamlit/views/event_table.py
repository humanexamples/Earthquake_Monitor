"""
pages/event_table.py  ·  Page 2 — Event Table
Filterable, sortable table of all Gold layer events.
Filters: region, magnitude range, date range, composite score.
"""
import streamlit as st
import pandas as pd
from db import query


def render():
    st.title("📋 Event Table")
    st.caption("All scored earthquake events from the Gold layer. Filter and sort as needed.")

    # ── Sidebar filters ───────────────────────────────────────────────────────
    st.sidebar.subheader("Filters")

    # Load distinct regions for the dropdown
    regions_df = query("SELECT DISTINCT region FROM gold.earthquake_events ORDER BY region")
    regions    = ["All"] + (regions_df["region"].tolist() if not regions_df.empty else [])
    region     = st.sidebar.selectbox("Region", regions)

    mag_min, mag_max = st.sidebar.slider("Magnitude range", 0.0, 10.0, (0.0, 10.0), 0.1)
    score_min        = st.sidebar.slider("Min composite score", 0.0, 10.0, 0.0, 0.5)
    days             = st.sidebar.slider("Show last N days", 1, 365, 60)

    # ── Query ─────────────────────────────────────────────────────────────────
    region_filter = "AND region = %s" if region != "All" else ""
    params = [days, mag_min, mag_max, score_min]
    if region != "All":
        params.insert(1, region)

    df = query(f"""
        SELECT
            event_id,
            region,
            magnitude,
            depth,
            ROUND(composite_score::numeric, 2)        AS composite_score,
            ROUND(tsunami_risk::numeric, 2)           AS tsunami_risk,
            ROUND(building_vulnerability::numeric, 2) AS building_vuln,
            ROUND(population_exposure::numeric, 2)    AS population_exp,
            ROUND(infrastructure_risk::numeric, 2)    AS infra_risk,
            event_time::text   AS event_time,
            processing_date::text AS date
        FROM gold.earthquake_events
        WHERE processing_date >= CURRENT_DATE - INTERVAL '%s days'
          {region_filter}
          AND magnitude       BETWEEN %s AND %s
          AND composite_score >= %s
        ORDER BY composite_score DESC
    """, params)

    if df.empty:
        st.warning("No events found for the selected filters.")
        return

    # ── Summary metrics ───────────────────────────────────────────────────────
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total events",     len(df))
    m2.metric("Max magnitude",    f"{df['magnitude'].max():.1f}")
    m3.metric("Max composite",    f"{df['composite_score'].max():.2f}")
    m4.metric("Avg composite",    f"{df['composite_score'].mean():.2f}")

    # ── Table ─────────────────────────────────────────────────────────────────
    st.dataframe(
        df.reset_index(drop=True),
        use_container_width=True,
        column_config={
            "composite_score": st.column_config.ProgressColumn(
                "Composite Score", min_value=0, max_value=10, format="%.2f"),
            "tsunami_risk": st.column_config.ProgressColumn(
                "Tsunami Risk", min_value=0, max_value=10, format="%.2f"),
        },
    )

    # ── Download button ───────────────────────────────────────────────────────
    st.download_button(
        label="Download as CSV",
        data=df.to_csv(index=False),
        file_name="earthquake_events.csv",
        mime="text/csv",
    )


render()

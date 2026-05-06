"""
pages/world_map.py  ·  Page 1 — World Map
Displays all earthquake events as markers on a world map.
Marker color and size driven by composite_score.
Tooltip shows event details and all four impact scores.
"""
import streamlit as st
import pandas as pd
import pydeck as pdk
from db import query


def render():
    st.title("🗺️ World Map — Earthquake Events")
    st.caption("Each marker represents one seismic event. Size and color reflect the composite impact score.")

    # ── Filters ───────────────────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    with col1:
        days = st.slider("Show last N days", 1, 90, 30)
    with col2:
        min_mag = st.slider("Minimum magnitude", 0.0, 9.0, 2.0, 0.1)
    with col3:
        min_score = st.slider("Minimum composite score", 0.0, 10.0, 0.0, 0.5)

    # ── Query ─────────────────────────────────────────────────────────────────
    df = query("""
        SELECT
            event_id, region, lat, lon, magnitude, depth,
            event_time::text AS event_time,
            tsunami_risk, building_vulnerability,
            population_exposure, infrastructure_risk,
            composite_score, processing_date::text AS processing_date
        FROM gold.earthquake_events
        WHERE processing_date >= CURRENT_DATE - INTERVAL '%s days'
          AND magnitude        >= %s
          AND composite_score  >= %s
        ORDER BY composite_score DESC
    """, [days, min_mag, min_score])

    if df.empty:
        st.warning("No events found for the selected filters.")
        return

    st.metric("Events shown", len(df))

    # ── Color scale: green → yellow → red by composite_score ─────────────────
    def score_to_color(score):
        s = float(score) / 10.0
        r = int(min(255, 2 * s * 255))
        g = int(min(255, 2 * (1 - s) * 255))
        return [r, g, 50, 180]

    df["color"]  = df["composite_score"].apply(score_to_color)
    df["radius"] = (df["composite_score"] * 40000 + 20000).astype(int)

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["lon", "lat"],
        get_color="color",
        get_radius="radius",
        pickable=True,
        opacity=0.8,
        stroked=True,
        filled=True,
    )

    tooltip = {
        "html": """
            <b>{region}</b><br/>
            Magnitude: <b>{magnitude}</b> · Depth: {depth} km<br/>
            Composite score: <b>{composite_score}</b><br/>
            Tsunami risk: {tsunami_risk} &nbsp;|&nbsp;
            Building: {building_vulnerability}<br/>
            Population: {population_exposure} &nbsp;|&nbsp;
            Infrastructure: {infrastructure_risk}<br/>
            <small>{event_time}</small>
        """,
        "style": {"backgroundColor": "#1a1a2e", "color": "white", "fontSize": "12px"},
    }

    st.pydeck_chart(pdk.Deck(
        map_style="mapbox://styles/mapbox/dark-v10",
        initial_view_state=pdk.ViewState(latitude=20, longitude=0, zoom=1.5, pitch=0),
        layers=[layer],
        tooltip=tooltip,
    ))

    # ── Top events table ──────────────────────────────────────────────────────
    st.subheader("Top 10 highest-impact events")
    st.dataframe(
        df[["event_id", "region", "magnitude", "depth",
            "composite_score", "tsunami_risk", "processing_date"]]
        .head(10)
        .reset_index(drop=True),
        use_container_width=True,
    )


render()

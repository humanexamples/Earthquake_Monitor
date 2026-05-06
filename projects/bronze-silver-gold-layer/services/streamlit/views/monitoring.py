"""
pages/monitoring.py  ·  Page 4 — 7-Day Monitoring Window
Shows all events currently under 7-day monitoring (M>=6.0).
Displays aftershock counts, GDACS/NOAA alert levels,
and weather forecast summaries from monitoring.events.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from db import query


def render():
    st.title("🔴 Monitoring Window — M≥6.0 Events")
    st.caption("Active 7-day monitoring: GDACS tsunami alerts, NOAA warnings, USGS aftershocks, Open-Meteo forecast.")

    # ── Active monitored events ───────────────────────────────────────────────
    events = query("""
        SELECT DISTINCT ON (e.event_id)
            e.event_id,
            e.region,
            e.magnitude,
            e.depth,
            e.lat,
            e.lon,
            ROUND(e.composite_score::numeric, 2)   AS composite_score,
            ROUND(e.tsunami_risk::numeric, 2)       AS tsunami_risk,
            e.event_time::text                      AS event_time,
            e.processing_date::text                 AS date
        FROM gold.earthquake_events e
        WHERE e.magnitude  >= 6.0
          AND e.depth       < 150
          AND e.processing_date >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY e.event_id, e.composite_score DESC
    """)

    if events.empty:
        st.info("No M≥6.0 events in the last 7 days currently under monitoring.")
        return

    st.metric("Events under monitoring", len(events))

    # ── Event selector ────────────────────────────────────────────────────────
    selected_id = st.selectbox(
        "Select event to inspect",
        options=events["event_id"].tolist(),
        format_func=lambda eid: f"{eid} — {events.loc[events['event_id']==eid, 'region'].values[0]} "
                                f"(M{events.loc[events['event_id']==eid, 'magnitude'].values[0]})",
    )

    selected = events[events["event_id"] == selected_id].iloc[0]

    # ── Event summary ─────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Magnitude",       f"M{selected['magnitude']}")
    c2.metric("Depth",           f"{selected['depth']} km")
    c3.metric("Composite score", f"{selected['composite_score']}")
    c4.metric("Tsunami risk",    f"{selected['tsunami_risk']}")
    st.caption(f"Region: {selected['region']} · Event time: {selected['event_time']}")

    st.divider()

    # ── Monitoring poll results ───────────────────────────────────────────────
    polls = query("""
        SELECT source, alert_level, aftershock_count,
               polled_at::text AS polled_at, data_json
        FROM monitoring.events
        WHERE event_id = %s
        ORDER BY polled_at DESC
    """, [selected_id])

    if polls.empty:
        st.info("No monitoring poll results yet for this event. The monitoring DAG runs every 15 minutes.")
        return

    col_gdacs, col_noaa = st.columns(2)

    # GDACS
    with col_gdacs:
        st.subheader("GDACS Alerts")
        gdacs = polls[polls["source"] == "gdacs"]
        if gdacs.empty:
            st.write("No GDACS data yet.")
        else:
            latest = gdacs.iloc[0]
            level  = latest.get("alert_level") or "Unknown"
            color  = {"Red": "🔴", "Orange": "🟠", "Green": "🟢"}.get(level, "⚪")
            st.write(f"{color} Alert level: **{level}**")
            st.caption(f"Last polled: {latest['polled_at']}")

    # NOAA
    with col_noaa:
        st.subheader("NOAA Warnings")
        noaa = polls[polls["source"] == "noaa"]
        if noaa.empty:
            st.write("No NOAA data yet.")
        else:
            latest = noaa.iloc[0]
            st.write(f"Last polled: {latest['polled_at']}")

    # Aftershocks over time
    st.subheader("USGS Aftershock count over time")
    aftershocks = polls[polls["source"] == "usgs_aftershocks"].copy()
    if aftershocks.empty:
        st.write("No aftershock data yet.")
    else:
        aftershocks["aftershock_count"] = pd.to_numeric(
            aftershocks["aftershock_count"], errors="coerce").fillna(0)
        fig = px.line(
            aftershocks.sort_values("polled_at"),
            x="polled_at", y="aftershock_count",
            labels={"polled_at": "Poll time", "aftershock_count": "Aftershock count"},
            markers=True,
        )
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

    # Open-Meteo forecast
    st.subheader("Weather forecast (Open-Meteo)")
    meteo = polls[polls["source"] == "openmeteo_forecast"]
    if meteo.empty:
        st.write("No weather forecast data yet.")
    else:
        import json
        try:
            data  = json.loads(meteo.iloc[0]["data_json"])
            daily = data.get("daily", {})
            if daily:
                df_w = pd.DataFrame({
                    "date":      daily.get("time", []),
                    "temp_max":  daily.get("temperature_2m_max", []),
                    "precip":    daily.get("precipitation_sum", []),
                    "wind":      daily.get("windspeed_10m_max", []),
                })
                st.dataframe(df_w.reset_index(drop=True), use_container_width=True)
        except Exception:
            st.write("Forecast data could not be parsed.")

    # ── All monitored events summary table ───────────────────────────────────
    st.divider()
    st.subheader("All monitored events")
    st.dataframe(
        events[["event_id", "region", "magnitude", "depth",
                "composite_score", "tsunami_risk", "date"]]
        .reset_index(drop=True),
        use_container_width=True,
    )


render()

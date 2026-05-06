"""
app.py  ·  Earthquake Monitoring Pipeline — Streamlit Dashboard
Phase 3: Presentation layer — reads only from the Gold DWH.

Uses st.navigation() (Streamlit >= 1.29) so every page is reachable
both via the sidebar and by direct URL:
  /world_map  /event_table  /regional_stats  /monitoring  /data_quality
"""
import streamlit as st

st.set_page_config(
    page_title="Earthquake Monitor",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.sidebar.title("🌍 Earthquake Monitor")
st.sidebar.caption("Medallion Pipeline · Gold Layer")
st.sidebar.divider()
st.sidebar.caption("gold.earthquake_events")
st.sidebar.caption("gold.regional_stats")
st.sidebar.caption("gold.daily_summary")
st.sidebar.caption("monitoring.events")
st.sidebar.caption("gold.quality_profiles")

pg = st.navigation([
    st.Page("views/world_map.py",      title="World Map",         icon="🗺️"),
    st.Page("views/event_table.py",    title="Event Table",       icon="📋"),
    st.Page("views/regional_stats.py", title="Regional Stats",    icon="📊"),
    st.Page("views/monitoring.py",     title="Monitoring Window", icon="🔴"),
    st.Page("views/data_quality.py",   title="Data Quality",      icon="✅"),
])
pg.run()

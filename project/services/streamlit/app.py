import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Earthquake Monitor", page_icon="🌍", layout="wide")

# ── DB ────────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_conn():
    return psycopg2.connect(
        host=    os.getenv("POSTGRES_GOLD_HOST",     "postgres_gold"),
        port=    os.getenv("POSTGRES_GOLD_PORT",     "5432"),
        dbname=  os.getenv("POSTGRES_GOLD_DB",       "earthquake_gold"),
        user=    os.getenv("POSTGRES_GOLD_USER",     "gold_user"),
        password=os.getenv("POSTGRES_GOLD_PASSWORD", "gold_password"),
    )


@st.cache_data(ttl=30)
def load(sql: str) -> pd.DataFrame:
    try:
        return pd.read_sql(sql, get_conn())
    except Exception:
        get_conn.clear()
        return pd.read_sql(sql, get_conn())


PLOT_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="#0e1117",
    plot_bgcolor="#0e1117",
    margin={"r": 0, "t": 40, "l": 0, "b": 0},
    height=480,
)

GEO_LAYOUT = dict(
    showframe=False,
    showcoastlines=True,
    coastlinecolor="#444",
    showland=True,
    landcolor="#1a1a2e",
    showocean=True,
    oceancolor="#0d1b2a",
    showcountries=True,
    countrycolor="#333",
    bgcolor="#0e1117",
    projection_type="natural earth",
)


def make_globe(df: pd.DataFrame, size_col: str, color_col: str,
               title: str, scale: str, hover: dict) -> px.scatter_geo:
    """Build a full-earth scatter_geo that handles NaN sizes gracefully."""
    plot_df = df.dropna(subset=["coordinate_lat", "coordinate_lon"]).copy()
    median_val = plot_df[size_col].median()
    fill_val = median_val if pd.notna(median_val) else 3.0
    plot_df["_size"] = plot_df[size_col].fillna(fill_val).clip(lower=0.5)

    fig = px.scatter_geo(
        plot_df,
        lat="coordinate_lat", lon="coordinate_lon",
        size="_size", color=color_col,
        hover_name="location_country",
        hover_data={**hover, "_size": False, "coordinate_lat": False, "coordinate_lon": False},
        color_continuous_scale=scale,
        size_max=28,
        title=title,
    )
    fig.update_geos(**GEO_LAYOUT)
    fig.update_layout(**PLOT_LAYOUT)
    return fig


def style_table(df: pd.DataFrame, mag_col: str = "magnitude"):
    styler = df.style
    if mag_col in df.columns:
        styler = styler.background_gradient(
            cmap="YlOrRd",
            subset=[mag_col],
            vmin=0, vmax=10,
        )
    for col in ("infrastructure_hospitals_count", "infrastructure_police_count",
                "infrastructure_aerodrome_count"):
        if col in df.columns:
            styler = styler.background_gradient(cmap="Blues", subset=[col], vmin=0)
    for col in ("historical_earthquake_magnitudes_over4_median",
                "historical_earthquake_magnitudes_over4_count",
                "historical_earthquake_magnitudes_over4_max"):
        if col in df.columns:
            styler = styler.background_gradient(cmap="Oranges", subset=[col], vmin=0)
    return styler


def fmt_mag(series: pd.Series) -> str:
    v = series.dropna()
    return f"{v.max():.1f} M" if not v.empty else "—"


# ── Layout ────────────────────────────────────────────────────────────────────

st.title("🌍 Earthquake Monitor")

col_refresh, _ = st.columns([1, 9])
with col_refresh:
    if st.button("Refresh"):
        st.cache_data.clear()
        st.rerun()

tab1, tab2, tab3, tab4 = st.tabs([
    "🔴 Last 24 h – Top 10",
    "📋 All Earthquakes",
    "🌍 Country Overview",
    "📈 Historically Strongest",
])

# ── Tab 1: Top 10 last 24 h ───────────────────────────────────────────────────

with tab1:
    df = load("SELECT * FROM top10_earthquakes_24h ORDER BY magnitude DESC NULLS LAST")

    st.subheader("Top 10 Earthquakes in the Last 24 Hours")

    if df.empty:
        st.info("No data available for the last 24 hours.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Strongest Earthquake", fmt_mag(df["magnitude"]))
        c2.metric("Count", len(df))
        avg = df["magnitude"].mean()
        c3.metric("Avg Magnitude", f"{avg:.1f} M" if pd.notna(avg) else "—")

        fig = make_globe(
            df, size_col="magnitude", color_col="magnitude",
            title="Earthquake Map (last 24 h)", scale="Reds",
            hover={"magnitude": True, "location_state": True,
                   "location_settlement": True, "date": True, "time": True},
        )
        st.plotly_chart(fig, use_container_width=True)

        display_cols = ["unid", "magnitude", "location_country", "location_state",
                        "location_settlement", "date", "time", "coordinate_depth", "population"]
        st.dataframe(
            style_table(df[display_cols].reset_index(drop=True)),
            use_container_width=True,
        )

# ── Tab 2: All earthquakes ────────────────────────────────────────────────────

with tab2:
    df_all = load(
        "SELECT * FROM earthquake_events_gold ORDER BY date DESC NULLS LAST, time DESC NULLS LAST"
    )

    st.subheader("All Earthquakes")

    if df_all.empty:
        st.info("No data available.")
    else:
        f1, f2, f3 = st.columns([2, 2, 3])

        with f1:
            countries = ["All"] + sorted(df_all["location_country"].dropna().unique().tolist())
            country_sel = st.selectbox("Country", countries)

        with f2:
            mag_lo = float(df_all["magnitude"].min(skipna=True) or 0)
            mag_hi = float(df_all["magnitude"].max(skipna=True) or 10)
            if mag_lo == mag_hi:
                mag_hi = mag_lo + 0.1
            mag_range = st.slider("Magnitude", mag_lo, mag_hi, (mag_lo, mag_hi), 0.1)

        with f3:
            dates = pd.to_datetime(df_all["date"].dropna())
            if not dates.empty:
                d_min, d_max = dates.min().date(), dates.max().date()
                date_range = st.date_input("Date range", value=(d_min, d_max),
                                           min_value=d_min, max_value=d_max)
            else:
                date_range = ()

        filtered = df_all.copy()
        if country_sel != "All":
            filtered = filtered[filtered["location_country"] == country_sel]
        filtered = filtered[
            filtered["magnitude"].between(mag_range[0], mag_range[1], inclusive="both") |
            filtered["magnitude"].isna()
        ]
        if len(date_range) == 2:
            filtered["date"] = pd.to_datetime(filtered["date"])
            filtered = filtered[
                (filtered["date"].dt.date >= date_range[0]) &
                (filtered["date"].dt.date <= date_range[1])
            ]

        c1, c2, c3 = st.columns(3)
        c1.metric("Results", len(filtered))
        c2.metric("Strongest Magnitude", fmt_mag(filtered["magnitude"]))
        avg_f = filtered["magnitude"].mean()
        c3.metric("Avg Magnitude", f"{avg_f:.1f} M" if pd.notna(avg_f) else "—")

        if not filtered.empty:
            fig = make_globe(
                filtered, size_col="magnitude", color_col="magnitude",
                title="Earthquake Map", scale="YlOrRd",
                hover={"magnitude": True, "date": True},
            )
            st.plotly_chart(fig, use_container_width=True)

            display_cols = [
                "unid", "magnitude", "location_country", "location_state",
                "location_settlement", "date", "time", "coordinate_depth", "population",
                "historical_earthquake_magnitudes_over4_median",
                "historical_earthquake_magnitudes_over4_count",
                "historical_earthquake_magnitudes_over4_max",
                "infrastructure_hospitals_count",
                "infrastructure_police_count",
                "infrastructure_aerodrome_count",
            ]
            st.dataframe(
                style_table(filtered[display_cols].reset_index(drop=True)),
                use_container_width=True,
            )

# ── Tab 3: Country summary ────────────────────────────────────────────────────

with tab3:
    df_c = load("SELECT * FROM country_summary ORDER BY mag_max DESC NULLS LAST")

    st.subheader("Country Overview")

    if df_c.empty:
        st.info("No country data available.")
    else:
        c1, c2 = st.columns(2)

        with c1:
            fig_bar = px.bar(
                df_c.head(20),
                x="country", y="mag_max",
                color="mag_max",
                color_continuous_scale="Reds",
                title="Top 20 Countries by Max Magnitude",
                labels={"country": "Country", "mag_max": "Max Magnitude"},
                template="plotly_dark",
            )
            fig_bar.update_layout(
                xaxis_tickangle=-40, height=400,
                paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        with c2:
            fig_sc = px.scatter(
                df_c.dropna(subset=["mag_median", "mag_max"]),
                x="mag_median", y="mag_max",
                text="country",
                title="Median vs. Max Magnitude by Country",
                labels={"mag_median": "Median Magnitude", "mag_max": "Max Magnitude"},
                template="plotly_dark",
            )
            fig_sc.update_traces(textposition="top center")
            fig_sc.update_layout(
                height=400,
                paper_bgcolor="#0e1117", plot_bgcolor="#16213e",
            )
            st.plotly_chart(fig_sc, use_container_width=True)

        display = df_c.copy()
        for col in ("states", "settlements"):
            if col in display.columns:
                display[col] = display[col].apply(
                    lambda v: ", ".join(v) if isinstance(v, list) else (v or "")
                )

        styled = display[["country", "date_min", "date_max", "mag_median", "mag_max",
                           "states", "settlements"]].reset_index(drop=True)
        st.dataframe(
            style_table(styled, mag_col="mag_max"),
            use_container_width=True,
        )

# ── Tab 4: Top historical ─────────────────────────────────────────────────────

with tab4:
    df_h = load(
        "SELECT * FROM top_historical_earthquakes "
        "ORDER BY historical_earthquake_magnitudes_over4_max DESC NULLS LAST"
    )

    st.subheader("Historically Strongest Earthquakes by Max Historical Magnitude in the Region")

    if df_h.empty:
        st.info("No data available.")
    else:
        hist_col = "historical_earthquake_magnitudes_over4_max"
        c1, c2, c3 = st.columns(3)
        c1.metric("Strongest Historical Magnitude", fmt_mag(df_h[hist_col]))
        c2.metric("Count", len(df_h))
        avg_h = df_h[hist_col].mean()
        c3.metric("Avg Historical Magnitude", f"{avg_h:.1f} M" if pd.notna(avg_h) else "—")

        fig = make_globe(
            df_h, size_col=hist_col, color_col=hist_col,
            title="Map: Historically Strongest Earthquakes", scale="Plasma",
            hover={"magnitude": True, hist_col: True, "location_state": True, "date": True},
        )
        st.plotly_chart(fig, use_container_width=True)

        display_cols = ["unid", hist_col, "magnitude", "location_country",
                        "location_state", "location_settlement", "date", "time"]
        st.dataframe(
            style_table(df_h[display_cols].reset_index(drop=True), mag_col=hist_col),
            use_container_width=True,
        )

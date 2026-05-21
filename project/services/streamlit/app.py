import os

import matplotlib
import matplotlib.colors as mcolors
import pandas as pd
import plotly.express as px
import psycopg2
import pydeck as pdk
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


_CMAP_MAP = {"Reds": "Reds", "YlOrRd": "YlOrRd", "Plasma": "plasma"}

_COL_RENAME = {
    "location_country":                              "Country",
    "location_state":                                "State",
    "location_settlement":                           "Settlement",
    "coordinate_depth":                              "depth (km)",
    "historical_earthquake_magnitudes_over4_median": "hist. earthquake (> 4.0 M) - median",
    "historical_earthquake_magnitudes_over4_count":  "hist. earthquake (> 4.0 M) - count",
    "historical_earthquake_magnitudes_over4_max":    "hist. earthquake (> 4.0 M) - max magnitude",
    "historical_earthquake_datetimes_over4_min":     "hist. earthquake (> 4.0 M) - earliest date",
    "historical_earthquake_datetimes_over4_max":     "hist. earthquake (> 4.0 M) - latest date",
    "infrastructure_hospitals_count":                "hospitals count (within 100 km)",
    "infrastructure_police_count":                   "police stations count (within 100 km)",
    "infrastructure_aerodrome_count":                "aerodromes count (within 100 km)",
    "population":                                    "population (within 100 km)",
}

def make_pydeck_map(
    df: pd.DataFrame,
    size_col: str,
    color_col: str,
    scale: str = "YlOrRd",
    extra_hover: list | None = None,
    tooltip_html: str | None = None,
) -> None:
    plot_df = df.dropna(subset=["coordinate_lat", "coordinate_lon"]).copy()

    for col in ("location_country", "location_state", "location_settlement"):
        if col in plot_df.columns:
            plot_df[col] = plot_df[col].fillna("")
    if "time" in plot_df.columns:
        plot_df["time"] = plot_df["time"].apply(fmt_time)
    if "date" in plot_df.columns:
        plot_df["date"] = plot_df["date"].apply(
            lambda v: str(v)[:10] if pd.notna(v) else ""
        )

    # quadratic radius in metres — stronger quakes are clearly larger;
    # pydeck uses real-world metres so circles shrink naturally when zooming out
    median_val = plot_df[size_col].median()
    fill_val = median_val if pd.notna(median_val) else 3.0
    plot_df["_radius"] = plot_df[size_col].fillna(fill_val).clip(lower=0.5) ** 2 * 5_000

    cmap_fn = matplotlib.colormaps[_CMAP_MAP.get(scale, "YlOrRd")]
    vmin = float(plot_df[color_col].min(skipna=True) or 0)
    vmax = float(plot_df[color_col].max(skipna=True) or 9)
    if vmin == vmax:
        vmax = vmin + 1
    norm = mcolors.Normalize(vmin=vmin, vmax=vmax)

    def _color(v):
        if pd.isna(v):
            return [100, 100, 100, 160]
        r, g, b, _ = cmap_fn(norm(float(v)))
        return [int(r * 255), int(g * 255), int(b * 255), 200]

    plot_df["_color"] = plot_df[color_col].apply(_color)

    if tooltip_html is None:
        tooltip_html = (
            "<b>{location_country}</b><br/>"
            "State: {location_state}<br/>"
            "Settlement: {location_settlement}<br/>"
            "Magnitude: {magnitude}<br/>"
            "Date: {date}<br/>"
            "Time: {time}"
        )
        if extra_hover:
            for label, col in extra_hover:
                if col in plot_df.columns:
                    tooltip_html += f"<br/>{label}: {{{col}}}"

    st.pydeck_chart(
        pdk.Deck(
            layers=[
                pdk.Layer(
                    "ScatterplotLayer",
                    data=plot_df,
                    get_position=["coordinate_lon", "coordinate_lat"],
                    get_radius="_radius",
                    get_fill_color="_color",
                    pickable=True,
                    opacity=0.8,
                    radius_min_pixels=4,
                    radius_max_pixels=40,
                )
            ],
            initial_view_state=pdk.ViewState(
                latitude=20, longitude=10, zoom=1, pitch=0
            ),
            map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
            tooltip={
                "html": tooltip_html,
                "style": {
                    "backgroundColor": "#1a1a2e",
                    "color": "white",
                    "padding": "8px",
                    "borderRadius": "4px",
                },
            },
        ),
        use_container_width=True,
    )


def style_table(df: pd.DataFrame, mag_col: str = "magnitude"):
    styler = df.style
    if mag_col in df.columns:
        styler = styler.background_gradient(
            cmap="YlOrRd",
            subset=[mag_col],
            vmin=0, vmax=10,
        )
    for col in ("hospitals count (within 100 km)", "police stations count (within 100 km)",
                "aerodromes count (within 100 km)"):
        if col in df.columns:
            styler = styler.background_gradient(cmap="Blues", subset=[col], vmin=0)
    for col in ("hist. earthquake (> 4.0 M) - median", "hist. earthquake (> 4.0 M) - count",
                "hist. earthquake (> 4.0 M) - max"):
        if col in df.columns:
            styler = styler.background_gradient(cmap="Oranges", subset=[col], vmin=0)

    fmt = {}
    mag_display_cols = {
        "magnitude", "mag_median", "mag_max",
        "hist. earthquake (> 4.0 M) - median",
        "hist. earthquake (> 4.0 M) - max",
        "depth (km)",
    }
    for col in df.columns:
        if col in mag_display_cols:
            fmt[col] = "{:.1f}"
        elif col in ("population", "population (within 100 km)"):
            fmt[col] = lambda v: str(int(v)) if pd.notna(v) else ""
        elif col == "datetime":
            fmt[col] = lambda v: str(v) if pd.notna(v) else ""
    if fmt:
        styler = styler.format(fmt, na_rep="")

    return styler


def fmt_mag(series: pd.Series) -> str:
    v = series.dropna()
    return f"{v.max():.1f} M" if not v.empty else "—"


def fmt_time(t) -> str:
    if t is None:
        return ""
    try:
        return str(t)[:8]
    except Exception:
        return ""


def add_datetime_col(tbl: pd.DataFrame) -> pd.DataFrame:
    date_str = tbl["date"].apply(lambda v: str(v)[:10] if pd.notna(v) else "")
    time_str = tbl["time"].apply(fmt_time)
    tbl["datetime"] = (date_str + " " + time_str).str.strip()
    return tbl.drop(columns=["date", "time"])


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

        make_pydeck_map(
            df, size_col="magnitude", color_col="magnitude", scale="Reds",
            tooltip_html=(
                "<b>{location_country}</b><br/>"
                "UNID: {unid}<br/>"
                "State: {location_state}<br/>"
                "Settlement: {location_settlement}<br/>"
                "Magnitude: {magnitude}<br/>"
                "Depth (km): {coordinate_depth}<br/>"
                "Date & Time: {date} {time}"
            ),
        )

        display_cols = ["unid", "magnitude", "location_country", "location_state",
                        "location_settlement", "date", "time", "coordinate_depth", "population"]
        tbl = df[display_cols].copy().reset_index(drop=True)
        tbl = add_datetime_col(tbl)
        tbl = tbl.rename(columns=_COL_RENAME)
        st.dataframe(style_table(tbl), use_container_width=True)

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

        f4, _ = st.columns([2, 5])
        with f4:
            depth_lo = float(df_all["coordinate_depth"].min(skipna=True) or 0)
            depth_hi = float(df_all["coordinate_depth"].max(skipna=True) or 700)
            if depth_lo == depth_hi:
                depth_hi = depth_lo + 1
            depth_range = st.slider("Depth (km)", depth_lo, depth_hi, (depth_lo, depth_hi), 1.0)

        filtered = df_all.copy()
        if country_sel != "All":
            filtered = filtered[filtered["location_country"] == country_sel]
        filtered = filtered[
            filtered["magnitude"].between(mag_range[0], mag_range[1], inclusive="both") |
            filtered["magnitude"].isna()
        ]
        filtered = filtered[
            filtered["coordinate_depth"].between(depth_range[0], depth_range[1], inclusive="both") |
            filtered["coordinate_depth"].isna()
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
            make_pydeck_map(
                filtered, size_col="magnitude", color_col="magnitude", scale="YlOrRd",
                tooltip_html=(
                    "<b>{location_country}</b><br/>"
                    "UNID: {unid}<br/>"
                    "State: {location_state}<br/>"
                    "Settlement: {location_settlement}<br/>"
                    "Magnitude: {magnitude}<br/>"
                    "Depth (km): {coordinate_depth}<br/>"
                    "Date & Time: {date} {time}"
                ),
            )

            display_cols = [
                "unid", "magnitude", "location_country", "location_state",
                "location_settlement", "date", "time", "coordinate_depth", "population",
                "historical_earthquake_magnitudes_over4_median",
                "historical_earthquake_magnitudes_over4_count",
                "historical_earthquake_magnitudes_over4_max",
                "historical_earthquake_datetimes_over4_min",
                "historical_earthquake_datetimes_over4_max",
                "infrastructure_hospitals_count",
                "infrastructure_police_count",
                "infrastructure_aerodrome_count",
            ]
            tbl2 = filtered[display_cols].copy().reset_index(drop=True)
            tbl2 = add_datetime_col(tbl2)
            tbl2 = tbl2.rename(columns=_COL_RENAME)
            st.dataframe(style_table(tbl2), use_container_width=True)

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

        make_pydeck_map(
            df_h, size_col=hist_col, color_col=hist_col, scale="Plasma",
            tooltip_html=(
                "<b>{location_country}</b><br/>"
                "UNID: {unid}<br/>"
                "State: {location_state}<br/>"
                "Settlement: {location_settlement}<br/>"
                "Magnitude: {magnitude}<br/>"
                "Depth (km): {coordinate_depth}<br/>"
                "hist. max magnitude (> 4.0 M): {historical_earthquake_magnitudes_over4_max}<br/>"
                "Latest hist. earthquake (> 4.0 M): {date} {time}"
            ),
        )

        display_cols = ["unid", hist_col, "magnitude", "location_country",
                        "location_state", "location_settlement", "date", "time"]
        tbl4 = df_h[display_cols].copy().reset_index(drop=True)
        tbl4 = add_datetime_col(tbl4)
        tbl4 = tbl4.rename(columns={"datetime": "latest hist. earthquake (> 4.0 M)"})
        tbl4 = tbl4.rename(columns=_COL_RENAME)
        st.dataframe(style_table(tbl4, mag_col=hist_col), use_container_width=True)

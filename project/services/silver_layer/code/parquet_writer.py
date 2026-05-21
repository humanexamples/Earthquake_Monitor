import json
import os
from io import BytesIO

import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET          = os.getenv("S3_BUCKET", "eq-monitor")
SINGLE_EQ_KEY      = "silver/earthquake_events.parquet"
SINGLE_COUNTRY_KEY = "silver/countries.parquet"


def _to_float(value) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _split_time(time_str) -> tuple[str | None, str | None]:
    """Split ISO datetime string into date and time parts.
    '2026-05-17T10:16:27.0Z' → ('2026-05-17', '10:16:27.0')
    """
    if not time_str:
        return None, None
    try:
        parts = str(time_str).split("T")
        date_part = parts[0]
        time_part = parts[1].rstrip("Z") if len(parts) > 1 else None
        return date_part, time_part
    except Exception:
        return None, None


def _get_category(el: dict) -> str | None:
    tags    = el.get("tags", {})
    amenity = tags.get("amenity", "")
    aeroway = tags.get("aeroway", "")
    if amenity == "hospital":
        return "hospitals"
    elif amenity == "police":
        return "police"
    elif aeroway == "aerodrome":
        return "aerodrome"
    return None


def _read_single_file(s3) -> pd.DataFrame:
    """Read the single earthquake_events Parquet. Returns empty DataFrame if not found."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=SINGLE_EQ_KEY)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return pd.DataFrame()
        raise


def _write_single_file(s3, df: pd.DataFrame) -> None:
    buf = BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=SINGLE_EQ_KEY, Body=buf.getvalue())


# ── Earthquake events ─────────────────────────────────────────────────────────

def write_earthquake_event(s3, unid: str, bronze: dict) -> None:
    """Upsert one earthquake event into the single silver Parquet file.

    time is split into date and time columns.
    historical_earthquake_magnitudes: all unique mag values from the bronze
    historical_earthquakes.json (deduped by USGS ID).
    infrastructure_*_places: names per category from infrastructure.json.
    """
    raw      = bronze["earthquake_event"]
    props    = raw.get("data", {}).get("properties", {})
    coords   = raw.get("data", {}).get("geometry", {}).get("coordinates", [])
    location = raw.get("location", {})

    depth               = props.get("depth") or (coords[2] if len(coords) > 2 else None)
    date_val, time_val  = _split_time(props.get("time"))

    unique_eqs = {eq["_id"]: eq for eq in bronze["historical_earthquakes"] if eq.get("_id") is not None}
    historical_earthquake_magnitudes = [eq.get("mag") for eq in unique_eqs.values()]
    historical_earthquake_datetimes  = [
        pd.Timestamp(eq["time"], unit="ms").isoformat()
        if eq.get("time") is not None else None
        for eq in unique_eqs.values()
    ]

    infra = bronze.get("infrastructure", [])
    hospital_places  = [el.get("tags", {}).get("name", "unknown")
                        for el in infra if _get_category(el) == "hospitals"]
    police_places    = [el.get("tags", {}).get("name", "unknown")
                        for el in infra if _get_category(el) == "police"]
    aerodrome_places = [el.get("tags", {}).get("name", "unknown")
                        for el in infra if _get_category(el) == "aerodrome"]

    record = {
        "unid":                             unid,
        "coordinate_lat":                   _to_float(props.get("lat")),
        "coordinate_lon":                   _to_float(props.get("lon")),
        "coordinate_depth":                 _to_float(depth),
        "magnitude":                        _to_float(props.get("mag")),
        "date":                             date_val,
        "time":                             time_val,
        "population":                       _to_float(raw.get("population")),
        "location_country":                 location.get("country"),
        "location_state":                   location.get("state"),
        "location_settlement":              location.get("settlement"),
        "historical_earthquake_magnitudes": historical_earthquake_magnitudes,
        "historical_earthquake_datetimes":  historical_earthquake_datetimes,
        "infrastructure_hospital_places":   hospital_places,
        "infrastructure_police_places":     police_places,
        "infrastructure_aerodrome_places":  aerodrome_places,
    }

    try:
        df = _read_single_file(s3)
        if not df.empty:
            df = df[df["unid"] != unid]         # remove existing row (upsert)
        df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
        _write_single_file(s3, df)
    except Exception as e:
        print(f"Parquet write failed [earthquake_event {unid}]: {e}", flush=True)
        raise


# ── Country summary ───────────────────────────────────────────────────────────

def write_country_summary(s3, bronze: dict) -> None:
    """Upsert one earthquake event into the single country summary Parquet.

    Path: silver/countries/countries.parquet
    One row per country, array columns:
      country, date[], time[], mag[], lat[], lon[], depth[], state[], settlement[]

    time is split into date[] and time[] arrays.
    """
    raw      = bronze["earthquake_event"]
    props    = raw.get("data", {}).get("properties", {})
    coords   = raw.get("data", {}).get("geometry", {}).get("coordinates", [])
    location = raw.get("location", {})

    country = location.get("country")
    if not country:
        return

    depth                = props.get("depth") or (coords[2] if len(coords) > 2 else None)
    new_date, new_time   = _split_time(props.get("time"))
    new_mag              = _to_float(props.get("mag"))
    new_lat              = _to_float(props.get("lat"))
    new_lon              = _to_float(props.get("lon"))
    new_depth            = _to_float(depth)
    new_state            = location.get("state")
    new_settlement       = location.get("settlement")

    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=SINGLE_COUNTRY_KEY)
        df  = pd.read_parquet(BytesIO(obj["Body"].read()))
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchKey":
            raise
        df = pd.DataFrame(columns=[
            "country", "date", "time", "mag", "lat", "lon",
            "depth", "state", "settlement",
        ])

    mask = df["country"] == country
    if mask.any():
        idx = df.index[mask][0]
        df.at[idx, "date"]       = list(df.at[idx, "date"])       + [new_date]
        df.at[idx, "time"]       = list(df.at[idx, "time"])       + [new_time]
        df.at[idx, "mag"]        = list(df.at[idx, "mag"])        + [new_mag]
        df.at[idx, "lat"]        = list(df.at[idx, "lat"])        + [new_lat]
        df.at[idx, "lon"]        = list(df.at[idx, "lon"])        + [new_lon]
        df.at[idx, "depth"]      = list(df.at[idx, "depth"])      + [new_depth]
        df.at[idx, "state"]      = list(df.at[idx, "state"])      + [new_state]
        df.at[idx, "settlement"] = list(df.at[idx, "settlement"]) + [new_settlement]
    else:
        new_row = pd.DataFrame([{
            "country":    country,
            "date":       [new_date],
            "time":       [new_time],
            "mag":        [new_mag],
            "lat":        [new_lat],
            "lon":        [new_lon],
            "depth":      [new_depth],
            "state":      [new_state],
            "settlement": [new_settlement],
        }])
        df = pd.concat([df, new_row], ignore_index=True)

    buf = BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=SINGLE_COUNTRY_KEY, Body=buf.getvalue())

"""
rebuild_silver_from_bronze.py
Recreates silver/earthquake_events.parquet from scratch by re-processing
all existing bronze event files in MinIO.

Run inside the silver_layer container:
  docker exec silver_layer python /app/rebuild_silver_from_bronze.py
"""

import json
import os
from io import BytesIO

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from parquet_writer import (
    _to_float,
    _split_time,
    _get_category,
    _write_single_file,
    S3_BUCKET,
    SINGLE_EQ_KEY,
)

load_dotenv()


def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=         os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=    os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
    )


def list_all_events(s3) -> dict[str, str]:
    """Return {unid: prefix} for all events found under bronze/events/.

    The folder date reflects the ingestion date, NOT the earthquake date in the unid,
    so we read the actual path from MinIO directly instead of computing it.
    """
    events = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="bronze/events/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/earthquake_event.json"):
                # key: bronze/events/2026/05/13/20260421_0000078/earthquake_event.json
                parts  = key.rsplit("/", 2)   # [...prefix, unid, filename]
                unid   = parts[-2]
                prefix = key[: key.rfind("/")]  # everything before /earthquake_event.json
                events[unid] = prefix           # last write wins for duplicate unids
    return events


def read_json(s3, key: str):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except ClientError:
        return None


def build_record(unid: str, bronze: dict) -> dict:
    """Apply the same logic as parquet_writer.write_earthquake_event, but return a dict."""
    raw      = bronze["earthquake_event"]
    props    = raw.get("data", {}).get("properties", {})
    coords   = raw.get("data", {}).get("geometry", {}).get("coordinates", [])
    location = raw.get("location", {})

    depth              = props.get("depth") or (coords[2] if len(coords) > 2 else None)
    date_val, time_val = _split_time(props.get("time"))

    unique_eqs = {eq["id"]: eq for eq in bronze["historical_earthquakes"] if eq.get("id")}
    magnitudes = [eq.get("mag") for eq in unique_eqs.values()]

    infra            = bronze.get("infrastructure", [])
    hospital_places  = [el.get("tags", {}).get("name", "unknown")
                        for el in infra if _get_category(el) == "hospitals"]
    police_places    = [el.get("tags", {}).get("name", "unknown")
                        for el in infra if _get_category(el) == "police"]
    aerodrome_places = [el.get("tags", {}).get("name", "unknown")
                        for el in infra if _get_category(el) == "aerodrome"]

    return {
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
        "historical_earthquake_magnitudes": magnitudes,
        "infrastructure_hospital_places":   hospital_places,
        "infrastructure_police_places":     police_places,
        "infrastructure_aerodrome_places":  aerodrome_places,
    }


def main():
    s3 = make_s3_client()

    print("Scanning bronze/events/ for all unids ...", flush=True)
    events = list_all_events(s3)
    total  = len(events)
    print(f"  {total} unique events found.", flush=True)

    records = []
    skipped = 0

    for i, (unid, prefix) in enumerate(sorted(events.items()), start=1):
        eq_data = read_json(s3, f"{prefix}/earthquake_event.json")
        if eq_data is None:
            print(f"  [{i}/{total}] SKIP {unid}: earthquake_event.json missing", flush=True)
            skipped += 1
            continue

        hist  = read_json(s3, f"{prefix}/historical_earthquakes.json") or []
        infra = read_json(s3, f"{prefix}/infrastructure.json") or []

        bronze = {
            "earthquake_event":       eq_data,
            "historical_earthquakes": hist  if isinstance(hist,  list) else [],
            "infrastructure":         infra if isinstance(infra, list) else [],
        }

        try:
            records.append(build_record(unid, bronze))
        except Exception as e:
            print(f"  [{i}/{total}] ERROR {unid}: {e}", flush=True)
            skipped += 1

        if i % 50 == 0 or i == total:
            print(f"  {i}/{total} processed, {skipped} skipped", flush=True)

    if not records:
        print("No records built — aborting.", flush=True)
        return

    print(f"\nBuilding DataFrame ({len(records)} rows) ...", flush=True)
    df = pd.DataFrame(records)

    null_mag = df["magnitude"].isna().sum()
    print(f"  magnitude: {len(df) - null_mag} filled, {null_mag} NULL", flush=True)

    print(f"Writing to {SINGLE_EQ_KEY} ...", flush=True)
    _write_single_file(s3, df)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()

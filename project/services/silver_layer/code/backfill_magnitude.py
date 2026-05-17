"""
backfill_magnitude.py
Patches NULL magnitude values in silver/earthquake_events.parquet
by reading data.properties.mag from each event's bronze JSON.

Run inside the silver_layer container:
  docker exec silver_layer python /app/backfill_magnitude.py
"""

import json
from io import BytesIO

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

load_dotenv()

S3_BUCKET     = os.getenv("S3_BUCKET", "eq-monitor")
SINGLE_EQ_KEY = "silver/earthquake_events.parquet"


def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=         os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=    os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
    )


def bronze_key(unid: str) -> str:
    """Derive bronze path from unid like '20260421_0000078' → 'bronze/events/2026/04/21/...'"""
    date_part = unid.split("_")[0]          # e.g. 20260421
    yyyy, mm, dd = date_part[:4], date_part[4:6], date_part[6:8]
    return f"bronze/events/{yyyy}/{mm}/{dd}/{unid}/earthquake_event.json"


def fetch_mag(s3, unid: str) -> float | None:
    try:
        obj   = s3.get_object(Bucket=S3_BUCKET, Key=bronze_key(unid))
        data  = json.loads(obj["Body"].read())
        props = data.get("data", {}).get("properties", {})
        raw   = props.get("mag")
        return float(raw) if raw is not None else None
    except ClientError:
        return None
    except Exception as e:
        print(f"  Warning: could not read bronze for {unid}: {e}", flush=True)
        return None


def main():
    s3 = make_s3_client()

    print("Reading silver/earthquake_events.parquet ...", flush=True)
    obj = s3.get_object(Bucket=S3_BUCKET, Key=SINGLE_EQ_KEY)
    df  = pd.read_parquet(BytesIO(obj["Body"].read()))

    null_mask = df["magnitude"].isna()
    null_count = null_mask.sum()
    print(f"  {null_count} rows with NULL magnitude (of {len(df)} total)", flush=True)

    if null_count == 0:
        print("  Nothing to backfill.", flush=True)
        return

    fixed = 0
    for idx in df.index[null_mask]:
        unid = df.at[idx, "unid"]
        mag  = fetch_mag(s3, unid)
        if mag is not None:
            df.at[idx, "magnitude"] = mag
            fixed += 1
            if fixed % 50 == 0:
                print(f"  {fixed}/{null_count} patched ...", flush=True)

    print(f"  Patched {fixed} rows, {null_count - fixed} remain NULL (bronze file missing).",
          flush=True)

    print("  Writing updated parquet back to MinIO ...", flush=True)
    buf = BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=SINGLE_EQ_KEY, Body=buf.getvalue())
    print("  Done.", flush=True)


if __name__ == "__main__":
    main()

import os
from io import BytesIO

import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET          = os.getenv("S3_BUCKET",       "eq-monitor")
SINGLE_EQ_KEY      = "silver/earthquake_events.parquet"
SINGLE_COUNTRY_KEY = "silver/countries.parquet"


def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
    )


def read_silver_event(s3, unid: str) -> dict | None:
    """Read one event by unid from the single silver earthquake_events Parquet."""
    try:
        obj  = s3.get_object(Bucket=S3_BUCKET, Key=SINGLE_EQ_KEY)
        df   = pd.read_parquet(BytesIO(obj["Body"].read()))
        rows = df[df["unid"] == unid]
        if rows.empty:
            print(f"Event {unid} not found in silver Parquet", flush=True)
            return None
        return rows.iloc[0].to_dict()
    except Exception as e:
        print(f"S3 read failed [{SINGLE_EQ_KEY}]: {e}", flush=True)
        return None


def read_silver_countries(s3) -> list[dict]:
    """Read all rows from the single silver countries Parquet."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=SINGLE_COUNTRY_KEY)
        df  = pd.read_parquet(BytesIO(obj["Body"].read()))
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"S3 read failed [{SINGLE_COUNTRY_KEY}]: {e}", flush=True)
        return []

import json
import os
from datetime import datetime

import boto3
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET", "eq-monitor")


def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
    )


def _read_json(s3, key: str):
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"S3 read failed [{key}]: {e}", flush=True)
        return None


def read_bronze_files(s3, unid: str, received_at: str) -> dict | None:
    date_path = datetime.fromisoformat(received_at).strftime("%Y/%m/%d")
    prefix    = f"bronze/events/{date_path}/{unid}"

    earthquake_event = _read_json(s3, f"{prefix}/earthquake_event.json")
    if earthquake_event is None:
        return None

    return {
        "earthquake_event":       earthquake_event,
        "historical_earthquakes": _read_json(s3, f"{prefix}/historical_earthquakes.json") or [],
        "infrastructure":         _read_json(s3, f"{prefix}/infrastructure.json") or [],
        "location":               _read_json(s3, f"{prefix}/location.json") or {},
    }

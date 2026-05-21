import json
import os
from datetime import datetime

import boto3
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET             = os.getenv("S3_BUCKET", "eq-monitor")
SHARED_HISTORICAL_KEY = "bronze/shared/historical_earthquakes.json"
SHARED_INFRA_KEY      = "bronze/shared/infrastructure.json"


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

    hist_ids  = {str(i) for i in earthquake_event.get("historical_earthquake_ids", [])}
    infra_ids = {str(i) for i in earthquake_event.get("infrastructure_ids", [])}

    # Try shared files first (new format), fall back to per-event files (old format)
    shared_hist = _read_json(s3, SHARED_HISTORICAL_KEY) or {}
    historical_earthquakes = [v for k, v in shared_hist.items() if k in hist_ids]
    if not historical_earthquakes and hist_ids:
        historical_earthquakes = _read_json(s3, f"{prefix}/historical_earthquakes.json") or []

    shared_infra = _read_json(s3, SHARED_INFRA_KEY) or {}
    infrastructure = [v for k, v in shared_infra.items() if k in infra_ids]
    if not infrastructure and infra_ids:
        infrastructure = _read_json(s3, f"{prefix}/infrastructure.json") or []

    return {
        "earthquake_event":       earthquake_event,
        "historical_earthquakes": historical_earthquakes,
        "infrastructure":         infrastructure,
    }

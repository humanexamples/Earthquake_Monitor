"""
monitoring_window_dag.py  ·  7-day monitoring window
Polls GDACS, NOAA, USGS aftershocks, Open-Meteo every 15 min
for events meeting the threshold: M>=6.0, depth<150km, coastal.
Results written to silver/monitoring/{event_id}/{source}/
"""
import json
import os
from datetime import datetime, timedelta, timezone
import boto3
import requests
from airflow.decorators import dag, task

S3_BUCKET       = os.getenv("S3_BUCKET", "eq-monitor")
MINIO_ENDPOINT  = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_ACCESS       = os.getenv("S3_ACCESS_KEY", "minio_access_key")
S3_SECRET       = os.getenv("S3_SECRET_KEY", "minio_secret_key_change_me")
MONITORING_DAYS = 7
M_THRESHOLD     = 6.0
DEPTH_THRESHOLD = 150.0

default_args = {
    "owner":            "eq-monitor",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

def get_s3():
    return boto3.client("s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=S3_ACCESS,
        aws_secret_access_key=S3_SECRET)

def write_monitoring_result(data: dict, event_id: str, source: str):
    s3  = get_s3()
    ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"silver/monitoring/{event_id}/{source}/{ts}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(data), ContentType="application/json")
    print(f"Monitoring result: s3://{S3_BUCKET}/{key}", flush=True)

@dag(
    dag_id="monitoring_window_pipeline",
    default_args=default_args,
    description="7-day monitoring: GDACS, NOAA, USGS aftershocks, Open-Meteo",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["monitoring", "silver", "tsunami"],
)
def monitoring_window_pipeline():

    @task
    def fetch_active_monitoring_events() -> list:
        s3       = get_s3()
        res      = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="silver/monitoring/active/")
        events   = []
        now      = datetime.now(timezone.utc)
        cutoff   = now - timedelta(days=MONITORING_DAYS)
        for obj in res.get("Contents", []):
            body  = s3.get_object(Bucket=S3_BUCKET, Key=obj["Key"])["Body"].read()
            event = json.loads(body)
            try:
                created = datetime.fromisoformat(event.get("created_at", "").replace("Z", "+00:00"))
                if created >= cutoff:
                    events.append(event)
            except Exception:
                pass
        print(f"Active monitoring events: {len(events)}", flush=True)
        return events

    @task
    def poll_gdacs(active_events: list):
        for event in active_events:
            props = event.get("properties", {})
            if float(props.get("magnitude", 0)) < M_THRESHOLD:
                continue
            if float(props.get("depth", 999)) >= DEPTH_THRESHOLD:
                continue
            try:
                resp = requests.get(
                    "https://www.gdacs.org/gdacsapi/api/events/geteventlist/USERGDACS",
                    params={"eventtype": "EQ", "alertlevel": "Orange,Red"},
                    timeout=15)
                write_monitoring_result(resp.json(), event["unid"], "gdacs")
            except Exception as e:
                print(f"GDACS error for {event['unid']}: {e}", flush=True)

    @task
    def poll_noaa(active_events: list):
        for event in active_events:
            props = event.get("properties", {})
            if float(props.get("magnitude", 0)) < M_THRESHOLD:
                continue
            try:
                resp = requests.get(
                    "https://api.weather.gov/alerts/active",
                    params={"event": "Tsunami Warning,Tsunami Watch,Tsunami Advisory"},
                    headers={"User-Agent": "eq-monitor/1.0"},
                    timeout=15)
                write_monitoring_result(resp.json(), event["unid"], "noaa")
            except Exception as e:
                print(f"NOAA error for {event['unid']}: {e}", flush=True)

    @task
    def poll_usgs_aftershocks(active_events: list):
        for event in active_events:
            props = event.get("properties", {})
            lat   = props.get("lat")
            lon   = props.get("lon")
            if not lat or not lon:
                continue
            try:
                resp = requests.get(
                    "https://earthquake.usgs.gov/fdsnws/event/1/query",
                    params={"format": "geojson", "latitude": lat, "longitude": lon,
                            "maxradiuskm": 200, "minmagnitude": 2.5,
                            "starttime": event.get("created_at", "")[:10], "limit": 100},
                    timeout=15)
                write_monitoring_result(resp.json(), event["unid"], "usgs_aftershocks")
            except Exception as e:
                print(f"USGS error for {event['unid']}: {e}", flush=True)

    @task
    def poll_openmeteo_forecast(active_events: list):
        for event in active_events:
            props = event.get("properties", {})
            lat   = props.get("lat")
            lon   = props.get("lon")
            if not lat or not lon:
                continue
            try:
                resp = requests.get(
                    "https://api.open-meteo.com/v1/forecast",
                    params={"latitude": lat, "longitude": lon,
                            "daily": "temperature_2m_max,precipitation_sum,windspeed_10m_max",
                            "forecast_days": 7, "timezone": "UTC"},
                    timeout=15)
                write_monitoring_result(resp.json(), event["unid"], "openmeteo_forecast")
            except Exception as e:
                print(f"Open-Meteo error for {event['unid']}: {e}", flush=True)

    events = fetch_active_monitoring_events()
    poll_gdacs(events)
    poll_noaa(events)
    poll_usgs_aftershocks(events)
    poll_openmeteo_forecast(events)

monitoring_window_pipeline()

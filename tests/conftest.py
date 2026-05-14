"""
conftest.py — shared pytest fixtures for the earthquake pipeline integration tests.

All connection parameters are read from environment variables so the test container
picks them up automatically from env_file: .env.dev in docker-compose.yml.

Session scope: Kafka event is produced exactly once per test run; downstream tests
poll for its arrival in MongoDB, Bronze S3, and Silver S3 independently.
"""
import json
import os
import pytest
import boto3
from datetime import datetime, timezone
from kafka import KafkaProducer
from pymongo import MongoClient

# ── Connection parameters from env (set via env_file: .env.dev) ─────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MONGO_URI       = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGO_DB        = os.getenv("MONGODB_DB", "earthquake")
MONGO_COLL      = os.getenv("MONGODB_COLLECTION", "raw_events")
S3_ENDPOINT     = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_ACCESS       = os.getenv("S3_ACCESS_KEY", "minio_access_key")
S3_SECRET       = os.getenv("S3_SECRET_KEY", "minio_secret_key_change_me")
S3_BUCKET       = os.getenv("S3_BUCKET", "eq-monitor")
DWH_HOST        = os.getenv("DWH_HOST", "postgres-dwh")
DWH_PORT        = int(os.getenv("DWH_PORT", "5432"))
DWH_DB          = os.getenv("DWH_DB", "earthquake")
DWH_USER        = os.getenv("DWH_USER", "eq_user")
DWH_PASSWORD    = os.getenv("DWH_PASSWORD", "eq_password_change_me")
AIRFLOW_URL     = f"http://airflow-webserver:{os.getenv('AIRFLOW_WEBSERVER_PORT', '8080')}"
AIRFLOW_USER    = os.getenv("AIRFLOW_ADMIN_USERNAME", "admin")
AIRFLOW_PASS    = os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin_password_change_me")


@pytest.fixture(scope="session")
def test_id() -> str:
    """Unique event ID shared across the entire test session."""
    return f"INTTEST_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"


@pytest.fixture(scope="session")
def s3():
    """Reusable boto3 S3 client connected to MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS,
        aws_secret_access_key=S3_SECRET,
    )


@pytest.fixture(scope="session")
def mongo_raw():
    """Reusable PyMongo collection handle for raw_events."""
    return MongoClient(MONGO_URI)[MONGO_DB][MONGO_COLL]


@pytest.fixture(scope="session")
def produced_event(test_id: str) -> dict:
    """
    Produce exactly one synthetic M6.8 test event to Kafka.
    Session-scoped — runs once per pytest invocation, result reused by all tests.
    Uses 'magnitude' (not 'mag') so validators.py accepts it as a valid mock event.
    """
    event = {
        "action": "create",
        "data": {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [28.98, 41.02, 10.0]},
            "properties": {
                "unid":             test_id,
                "source_catalog":   "EMSC",
                "flynn_region":     "INTEGRATION TEST",
                "lat":              41.02,
                "lon":              28.98,
                "depth":            10.0,
                "magnitude":        6.8,
                "magnitude_type":   "mw",
                "time":             datetime.now(timezone.utc).isoformat(),
                "lastupdate":       datetime.now(timezone.utc).isoformat(),
            },
        },
    }
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    record = producer.send("raw-seismic-events", event).get(timeout=10)
    producer.close()
    return {"event": event, "topic": record.topic, "partition": record.partition, "offset": record.offset}

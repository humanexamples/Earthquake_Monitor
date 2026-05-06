"""
Phase 1 smoke test — runs INSIDE Docker on kafka-network.
Uses internal service names (kafka:9092, mongodb:27017, etc.)
"""
import json
import time
import boto3
from kafka import KafkaProducer
from pymongo import MongoClient
from datetime import datetime, timezone

TEST_ID = f"SMOKE_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

MOCK_EVENT = {
    "action": "create",
    "data": {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [28.98, 41.02, 10.0]},
        "properties": {
            "unid": TEST_ID,
            "source_catalog": "EMSC",
            "flynn_region": "SMOKE TEST",
            "lat": 41.02,
            "lon": 28.98,
            "depth": 10.0,
            "magnitude": 6.8,
            "magnitude_type": "mw",
            "time": datetime.now(timezone.utc).isoformat(),
            "lastupdate": datetime.now(timezone.utc).isoformat(),
        },
    },
}

KAFKA_BOOTSTRAP = "kafka:9092"
MONGO_URI = "mongodb://mongodb:27017"
S3_ENDPOINT = "http://minio:9000"
S3_ACCESS = "minio_access_key"
S3_SECRET = "minio_secret_key_change_me"
S3_BUCKET = "eq-monitor"


def test_kafka():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    future = producer.send("raw-seismic-events", MOCK_EVENT)
    record = future.get(timeout=10)
    assert record.topic == "raw-seismic-events"
    producer.close()
    print(f"  Kafka: event produced to {record.topic}[{record.partition}]@{record.offset}")


def test_mongodb():
    col = MongoClient(MONGO_URI)["earthquake"]["raw_events"]
    deadline = time.time() + 15
    while time.time() < deadline:
        if col.find_one({"_id": TEST_ID}):
            print(f"  MongoDB: event found — id={TEST_ID}")
            return
        time.sleep(0.5)
    raise AssertionError("Event not in MongoDB after 15s")


def test_bronze():
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS,
        aws_secret_access_key=S3_SECRET,
    )
    date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    key = f"bronze/events/{date_path}/{TEST_ID}.json"
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            s3.get_object(Bucket=S3_BUCKET, Key=key)
            print(f"  Bronze: file found at s3://{S3_BUCKET}/{key}")
            return
        except Exception:
            time.sleep(0.5)
    raise AssertionError(f"Event not in Bronze after 15s")


def test_silver():
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS,
        aws_secret_access_key=S3_SECRET,
    )
    date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    key = f"silver/events/{date_path}/{TEST_ID}_enriched.json"
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
            content = json.loads(resp["Body"].read())
            assert content.get("ge_validation_passed") is True
            print(f"  Silver: enriched file found at s3://{S3_BUCKET}/{key}")
            return
        except AssertionError:
            raise
        except Exception:
            time.sleep(1)
    raise AssertionError(
        "Enriched event not in Silver after 60s — check: docker logs flink-enrichment --tail=20"
    )


if __name__ == "__main__":
    print(f"\nPhase 1 smoke test (in-container) — event ID: {TEST_ID}\n")
    test_kafka()
    time.sleep(1)
    test_mongodb()
    test_bronze()
    test_silver()
    print("\nAll Phase 1 smoke tests passed.")

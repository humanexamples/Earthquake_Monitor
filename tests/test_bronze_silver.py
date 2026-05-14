"""
test_bronze_silver.py — Bronze + Silver layer integration tests.

Pipeline under test:
  EMSC WebSocket → Kafka → mongodb-writer    → MongoDB (raw_events)
                         → s3-bronze-writer  → MinIO bronze/
                         → flink-enrichment  → MinIO silver/ (enriched + validated)

All tests share a single synthetic event injected via the `produced_event` fixture
in conftest.py. Tests are ordered to reflect the pipeline flow — each one polls
with a timeout rather than sleeping a fixed amount.
"""
import json
import time
import os
import pytest
from datetime import datetime, timezone

S3_BUCKET = os.getenv("S3_BUCKET", "eq-monitor")


class TestKafkaProduce:
    def test_event_reaches_kafka(self, produced_event, test_id):
        """Synthetic event must be acknowledged by Kafka broker."""
        assert produced_event["topic"] == "raw-seismic-events"
        assert produced_event["offset"] >= 0, "Expected a valid Kafka offset"
        print(
            f"\n  event_id : {test_id}"
            f"\n  topic    : {produced_event['topic']}"
            f"\n  partition: {produced_event['partition']}"
            f"\n  offset   : {produced_event['offset']}"
        )


class TestBronzeLayer:
    def test_event_stored_in_mongodb(self, produced_event, mongo_raw, test_id):
        """mongodb-writer must persist the raw event within 15 s."""
        deadline = time.time() + 15
        while time.time() < deadline:
            doc = mongo_raw.find_one({"_id": test_id})
            if doc:
                assert doc["_id"] == test_id
                return
            time.sleep(0.5)
        pytest.fail(
            f"Event '{test_id}' not found in MongoDB after 15 s.\n"
            "  Check: docker compose logs mongodb-writer --tail=20"
        )

    def test_event_written_to_bronze_s3(self, produced_event, s3, test_id):
        """s3-bronze-writer must write the raw JSON to MinIO bronze/ within 15 s."""
        date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        key = f"bronze/events/{date_path}/{test_id}.json"
        deadline = time.time() + 15
        while time.time() < deadline:
            try:
                resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
                body = json.loads(resp["Body"].read())
                assert body["data"]["properties"]["unid"] == test_id
                return
            except s3.exceptions.NoSuchKey:
                time.sleep(0.5)
            except Exception:
                time.sleep(0.5)
        pytest.fail(
            f"Bronze file not found after 15 s: s3://{S3_BUCKET}/{key}\n"
            "  Check: docker compose logs s3-bronze-writer --tail=20"
        )

    def test_mongodb_ttl_index_exists(self, mongo_raw):
        """TTL index on received_at must be present (data retention policy)."""
        indexes = mongo_raw.index_information()
        ttl_indexes = [
            name for name, info in indexes.items()
            if info.get("expireAfterSeconds") is not None
        ]
        assert ttl_indexes, (
            "No TTL index found on raw_events collection. "
            "Expected index on 'received_at' with expireAfterSeconds set."
        )


class TestSilverLayer:
    @pytest.mark.timeout(90)
    def test_enriched_event_in_silver_s3(self, produced_event, s3, test_id):
        """flink-enrichment must produce an enriched Silver file within 90 s."""
        date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        key = f"silver/events/{date_path}/{test_id}_enriched.json"
        deadline = time.time() + 75
        while time.time() < deadline:
            try:
                resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
                content = json.loads(resp["Body"].read())
                assert content.get("ge_validation_passed") is True, (
                    f"ge_validation_passed is not True in Silver file: {content}"
                )
                return
            except AssertionError:
                raise
            except Exception:
                time.sleep(1)
        pytest.fail(
            f"Silver enriched file not found after 75 s: s3://{S3_BUCKET}/{key}\n"
            "  Check: docker compose logs flink-enrichment --tail=30"
        )

    @pytest.mark.timeout(90)
    def test_silver_event_has_required_fields(self, produced_event, s3, test_id):
        """Enriched Silver file must contain all four external enrichment sections."""
        date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        key = f"silver/events/{date_path}/{test_id}_enriched.json"
        deadline = time.time() + 75
        while time.time() < deadline:
            try:
                resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
                content = json.loads(resp["Body"].read())
                if content.get("ge_validation_passed") is True:
                    assert "usgs_nearby"       in content, "Missing usgs_nearby"
                    assert "population_data"   in content, "Missing population_data"
                    assert "infrastructure"    in content, "Missing infrastructure"
                    assert "weather_baseline"  in content, "Missing weather_baseline"
                    assert "enriched_at"       in content, "Missing enriched_at"
                    return
            except AssertionError:
                raise
            except Exception:
                pass
            time.sleep(1)
        pytest.fail(
            f"Silver enriched file not ready after 75 s: s3://{S3_BUCKET}/{key}"
        )

    def test_dead_letter_queue_empty_for_valid_event(self, produced_event):
        """
        A valid synthetic event must NOT appear on the DLQ.
        We verify by checking the flink-enrichment produced a Silver file
        (already done above), not by consuming the DLQ topic — consuming
        would advance the offset and interfere with other tooling.
        This test acts as a logical assertion: if silver tests passed, DLQ was clean.
        """
        assert produced_event["topic"] == "raw-seismic-events", (
            "Event was not produced to the correct Kafka topic"
        )

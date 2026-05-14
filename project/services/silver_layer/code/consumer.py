"""
consumer.py
Kafka consumer for the silver layer.
Reads raw files from S3 (bronze), transforms them, and upserts into MongoDB.
Processing order: historical_earthquakes → infrastructure → silver_events
"""

from s3_reader import make_s3_client, read_bronze_files
from transformer import transform_event
from mongo_client import make_db, upsert_historical_earthquakes, upsert_infrastructure, upsert_event
from kafka_client import get_kafka_consumer


def run():
    consumer = get_kafka_consumer()
    s3       = make_s3_client()
    db       = make_db()

    for message in consumer:
        try:
            kafka_msg   = message.value
            unid        = kafka_msg["filter"]["_id"]
            received_at = kafka_msg["update"]["$setOnInsert"]["received_at"]

            bronze = read_bronze_files(s3, unid, received_at)
            if bronze is None:
                print(f"Skipping {unid}: S3 files not found", flush=True)
                continue

            # 1. Save historical earthquakes without duplicate
            hist_eq_ids = upsert_historical_earthquakes(db, bronze["historical_earthquakes"])
            print(f"[{unid}] historical_earthquakes: {len(hist_eq_ids)} upserted", flush=True)

            # 2. Save infrastructure without duplicate
            infra_ids = upsert_infrastructure(db, bronze["infrastructure"])
            print(f"[{unid}] infrastructure: {len(infra_ids)} upserted", flush=True)

            # 3. Save main earthquake event (references the above IDs)
            doc = transform_event(unid, received_at, bronze, hist_eq_ids, infra_ids)
            upsert_event(db, doc)
            print(f"[{unid}] silver_events: saved", flush=True)

        except Exception as e:
            print(f"Error processing message: {e}", flush=True)


if __name__ == "__main__":
    run()

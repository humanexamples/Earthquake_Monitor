"""
consumer.py
Kafka consumer for the silver layer.
Reads raw files from S3 (bronze), transforms them, and upserts into MongoDB.
"""
import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer

from s3_reader import make_s3_client, read_bronze_files
from transformer import transform
from mongo_client import make_collection, upsert_event

load_dotenv()

KAFKA_TOPIC            = os.getenv("KAFKA_TOPIC", "raw-seismic-events")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID         = os.getenv("KAFKA_GROUP_ID", "silver-layer")


def run():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    s3         = make_s3_client()
    collection = make_collection()

    print(f"Silver layer listening on topic: {KAFKA_TOPIC}", flush=True)

    for message in consumer:
        try:
            kafka_msg   = message.value
            unid        = kafka_msg["filter"]["_id"]
            received_at = kafka_msg["update"]["$setOnInsert"]["received_at"]

            bronze = read_bronze_files(s3, unid, received_at)
            if bronze is None:
                print(f"Skipping {unid}: S3 files not found", flush=True)
                continue

            doc = transform(unid, received_at, bronze)
            upsert_event(collection, doc)
            print(f"Saved to MongoDB: {unid}", flush=True)

        except Exception as e:
            print(f"Error processing {message.value}: {e}", flush=True)


if __name__ == "__main__":
    run()

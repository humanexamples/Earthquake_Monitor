"""
consumer.py
Kafka consumer for the silver layer.

Per earthquake event: writes Parquet files to MinIO, then sends
a Kafka message to the gold layer topic.
  silver/earthquake_events/{unid}.parquet
  silver/countries/{country}.parquet
"""

from s3_reader import make_s3_client, read_bronze_files
from parquet_writer import write_earthquake_event, write_country_summary
from kafka_client import get_kafka_consumer, get_kafka_producer, send_silver_event


def run():
    consumer = get_kafka_consumer()
    producer = get_kafka_producer()
    s3       = make_s3_client()

    for message in consumer:
        try:
            kafka_msg   = message.value
            unid        = kafka_msg["filter"]["_id"]
            received_at = kafka_msg["update"]["$setOnInsert"]["received_at"]

            bronze = read_bronze_files(s3, unid, received_at)
            if bronze is None:
                print(f"Skipping {unid}: S3 files not found", flush=True)
                continue

            write_earthquake_event(s3, unid, bronze)
            print(f"[{unid}] earthquake_event: saved to MinIO", flush=True)

            write_country_summary(s3, bronze)
            print(f"[{unid}] country_summary: updated", flush=True)

            send_silver_event(producer, unid)
            print(f"[{unid}] silver event sent to gold layer", flush=True)

        except Exception as e:
            print(f"Error processing message: {e}", flush=True)


if __name__ == "__main__":
    run()

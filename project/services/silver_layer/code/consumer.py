from mongo_reader import make_mongo_db, read_bronze_event
from s3_reader import make_s3_client
from parquet_writer import write_earthquake_event, write_country_summary
from kafka_client import get_kafka_consumer, get_kafka_producer, send_silver_event


def run():
    consumer = get_kafka_consumer()
    producer = get_kafka_producer()
    db       = make_mongo_db()
    s3       = make_s3_client()

    for message in consumer:
        try:
            unid = message.value["filter"]["_id"]

            bronze = read_bronze_event(db, unid)
            if bronze is None:
                print(f"Skipping {unid}: not found in MongoDB", flush=True)
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

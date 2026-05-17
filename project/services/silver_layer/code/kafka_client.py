import os
import json
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC",        "raw-seismic-events")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID",     "silver-layer")
SILVER_TOPIC            = os.getenv("SILVER_KAFKA_TOPIC", "silver-processed-events")


def get_kafka_consumer():
    print(f"Silver layer listening on topic: {KAFKA_TOPIC}", flush=True)
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )


def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def send_silver_event(producer, unid: str) -> None:
    producer.send(SILVER_TOPIC, {"unid": unid})
    producer.flush()
import os
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS",  "kafka:9092")
SILVER_TOPIC            = os.getenv("SILVER_KAFKA_TOPIC",       "silver-processed-events")
KAFKA_GROUP_ID          = os.getenv("GOLD_KAFKA_GROUP_ID",      "gold-layer")


def get_kafka_consumer():
    print(f"Gold layer listening on topic: {SILVER_TOPIC}", flush=True)
    return KafkaConsumer(
        SILVER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

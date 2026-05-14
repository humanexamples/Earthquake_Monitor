import os
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_TOPIC            = os.getenv("KAFKA_TOPIC", "raw-seismic-events")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID         = os.getenv("KAFKA_GROUP_ID", "silver-layer")

def get_kafka_consumer() :
    print(f"Silver layer listening on topic: {KAFKA_TOPIC}", flush=True)
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
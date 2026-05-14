import datetime
import json
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-seismic-events")

def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def send_event(producer: KafkaProducer, unid: str) -> None:
    kafka_msg = {
        "filter": {"_id": unid},
        "update": {
            "$setOnInsert": {
                "_id":              unid,
                "received_at":      datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
        },
    }
    producer.send(KAFKA_TOPIC, kafka_msg)

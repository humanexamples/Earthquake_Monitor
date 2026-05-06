"""
emsc_listener.py
Persistent WebSocket listener for EMSC seismic events.
Forwards every event to the Kafka topic raw-seismic-events.
Reconnects automatically on any connection failure.
"""
import asyncio
import json
import os
import websockets
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

EMSC_WS_URL             = os.getenv("EMSC_WS_URL")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "raw-seismic-events")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


async def listen():
    while True:
        try:
            async with websockets.connect(EMSC_WS_URL) as ws:
                print("Connected to EMSC WebSocket", flush=True)
                async for message in ws:
                    event = json.loads(message)
                    producer.send(KAFKA_TOPIC, event)
                    unid = event.get("data", {}).get("properties", {}).get("unid", "unknown")
                    print(f"Sent: {unid}", flush=True)
        except Exception as e:
            print(f"Connection lost: {e} — reconnecting in 5s...", flush=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(listen())

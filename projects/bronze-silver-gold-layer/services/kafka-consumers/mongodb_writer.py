"""
mongodb_writer.py  ·  BRONZE layer consumer
Reads raw events from Kafka and writes them to MongoDB.
MongoDB is the operational raw store — short-term memory for the pipeline.
TTL: 30 days. Deduplication via upsert $setOnInsert.
"""
import json
import os
from datetime import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
MONGODB_URI   = os.getenv("MONGODB_URI")
MONGODB_DB    = os.getenv("MONGODB_DB", "earthquake")
MONGODB_COLL  = os.getenv("MONGODB_COLLECTION", "raw_events")
TTL_DAYS      = int(os.getenv("MONGODB_TTL_DAYS", "30"))

consumer = KafkaConsumer(
    "raw-seismic-events",
    bootstrap_servers=KAFKA_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="mongodb-raw-writer",
)

client     = MongoClient(MONGODB_URI)
collection = client[MONGODB_DB][MONGODB_COLL]

# Create TTL index — documents auto-deleted after TTL_DAYS days
collection.create_index(
    "received_at",
    expireAfterSeconds=TTL_DAYS * 86400
)

print(f"MongoDB writer started — TTL: {TTL_DAYS} days", flush=True)

for message in consumer:
    event = message.value
    unid  = event.get("data", {}).get("properties", {}).get("unid", "unknown")

    collection.update_one(
        {"_id": unid},
        {
            "$setOnInsert": {
                **event,
                "_id":              unid,
                "received_at":      datetime.utcnow(),
                "worldpop_fetched": False,
                "overpass_fetched": False,
                "usgs_fetched":     False,
                "meteo_fetched":    False,
            }
        },
        upsert=True,
    )
    print(f"MongoDB: {unid}", flush=True)

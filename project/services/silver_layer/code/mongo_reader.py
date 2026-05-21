import os

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "earthquake_bronze")


def make_mongo_db():
    return MongoClient(MONGO_URI)[MONGO_DB]


def read_bronze_event(db, unid: str) -> dict | None:
    event = db.earthquake_events.find_one({"_id": unid})
    if event is None:
        return None

    hist_ids  = event.get("historical_earthquake_ids", [])
    infra_ids = event.get("infrastructure_ids", [])

    historical_earthquakes = list(db.historical_earthquakes.find({"_id": {"$in": hist_ids}}))
    infrastructure         = list(db.infrastructures.find({"_id": {"$in": infra_ids}}))

    return {
        "earthquake_event":       event,
        "historical_earthquakes": historical_earthquakes,
        "infrastructure":         infrastructure,
    }

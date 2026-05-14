import os

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGODB_DB  = os.getenv("MONGODB_DB", "earthquake")


def make_db():
    client = MongoClient(MONGODB_URI)
    return client[MONGODB_DB]


def upsert_historical_earthquakes(db, earthquakes: list) -> list:
    """Upsert each historical earthquake, return IDs that exist in MongoDB.
    Duplicates in the input are removed before processing (first occurrence wins).
    """
    collection = db["historical_earthquakes"]
    # deduplicate input: keep first occurrence of each id
    unique = {eq["id"]: eq for eq in reversed(earthquakes) if eq.get("id")}
    ids = []
    for eq_id, eq in unique.items():
        try:
            collection.update_one(
                {"_id": eq_id},
                {"$set": {**eq, "_id": eq_id}},
                upsert=True,
            )
            ids.append(eq_id)
        except PyMongoError as e:
            print(f"MongoDB upsert failed [historical_eq {eq_id}]: {e}", flush=True)
    return ids


def upsert_infrastructure(db, elements: list) -> list:
    """Upsert each OSM element, return IDs that exist in MongoDB.
    Duplicates in the input are removed before processing (first occurrence wins).
    """
    collection = db["infrastructure"]
    # deduplicate input: keep first occurrence of each id
    unique = {el["id"]: el for el in reversed(elements) if el.get("id") is not None}
    ids = []
    for el_id, el in unique.items():
        try:
            collection.update_one(
                {"_id": el_id},
                {"$set": {**el, "_id": el_id}},
                upsert=True,
            )
            ids.append(el_id)
        except PyMongoError as e:
            print(f"MongoDB upsert failed [infrastructure {el_id}]: {e}", flush=True)
    return ids


def upsert_event(db, doc: dict) -> None:
    """Upsert the main earthquake event into silver_events."""
    try:
        db["earthquake_events"].update_one(
            {"_id": doc["_id"]},
            {"$set": doc},
            upsert=True,
        )
    except PyMongoError as e:
        print(f"MongoDB upsert failed [event {doc['_id']}]: {e}", flush=True)
        raise

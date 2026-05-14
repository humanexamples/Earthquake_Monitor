import os

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

load_dotenv()

MONGODB_URI        = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGODB_DB         = os.getenv("MONGODB_DB", "earthquake")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "silver_events")


def make_collection():
    client = MongoClient(MONGODB_URI)
    return client[MONGODB_DB][MONGODB_COLLECTION]


def upsert_event(collection, doc: dict) -> None:
    try:
        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": doc},
            upsert=True,
        )
    except PyMongoError as e:
        print(f"MongoDB upsert failed [{doc['_id']}]: {e}", flush=True)
        raise

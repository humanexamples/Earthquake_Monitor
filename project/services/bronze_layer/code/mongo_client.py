import os

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "earthquake_bronze")


def make_mongo_db():
    return MongoClient(MONGO_URI)[MONGO_DB]


def upsert_earthquake_event(db, unid: str, event: dict) -> None:
    fields = {k: v for k, v in event.items() if k != "_id"}
    db.earthquake_events.update_one(
        {"_id": unid},
        {"$set": fields},
        upsert=True,
    )


def upsert_historical_earthquakes(db, earthquakes: list) -> None:
    if not earthquakes:
        return
    ops = [
        UpdateOne(
            {"_id": eq["id"]},
            {"$setOnInsert": {k: v for k, v in eq.items() if k != "id"}},
            upsert=True,
        )
        for eq in earthquakes if eq.get("id")
    ]
    if ops:
        db.historical_earthquakes.bulk_write(ops, ordered=False)


def upsert_infrastructure(db, elements: list) -> None:
    if not elements:
        return
    ops = [
        UpdateOne(
            {"_id": el["id"]},
            {"$setOnInsert": {k: v for k, v in el.items() if k != "id"}},
            upsert=True,
        )
        for el in elements if el.get("id") is not None
    ]
    if ops:
        db.infrastructures.bulk_write(ops, ordered=False)

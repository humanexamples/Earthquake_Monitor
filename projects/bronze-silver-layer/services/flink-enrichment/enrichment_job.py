"""
enrichment_job.py  ·  SILVER layer producer
Reads raw events from Kafka, enriches with external APIs,
validates with Great Expectations, and writes to S3 Silver.
Invalid records → Kafka DLQ (raw-seismic-events-dlq).

Enrichment sources:
  - USGS Earthquake Catalog   (historical seismic context)
  - WorldPop API              (population density at epicenter)
  - Overpass API / OSM        (hospitals, airports within 150km)
  - Open-Meteo Archive API    (30-day weather baseline)

Deduplication and state tracking via MongoDB.
"""
import json
import os
from datetime import datetime
import boto3
import requests
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from dotenv import load_dotenv
from validators import validate

load_dotenv()

# ── Configuration from environment ─────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
MONGODB_URI   = os.getenv("MONGODB_URI")
MONGODB_DB    = os.getenv("MONGODB_DB", "earthquake")
MONGODB_COLL  = os.getenv("MONGODB_COLLECTION", "raw_events")
S3_ENDPOINT   = os.getenv("S3_ENDPOINT_URL")
S3_ACCESS     = os.getenv("S3_ACCESS_KEY")
S3_SECRET     = os.getenv("S3_SECRET_KEY")
S3_BUCKET     = os.getenv("S3_BUCKET", "eq-monitor")

# ── Clients ─────────────────────────────────────────────────────────────────
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS,
    aws_secret_access_key=S3_SECRET,
)

mongo_collection = MongoClient(MONGODB_URI)[MONGODB_DB][MONGODB_COLL]

consumer = KafkaConsumer(
    "raw-seismic-events",
    bootstrap_servers=KAFKA_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="flink-enrichment",
)

dlq_producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


# ── Enrichment API calls ────────────────────────────────────────────────────

def fetch_usgs_nearby(lat: float, lon: float) -> dict:
    """USGS: historical M4.0+ earthquakes within 500km over 10 years."""
    try:
        resp = requests.get(
            "https://earthquake.usgs.gov/fdsnws/event/1/query",
            params={
                "format":       "geojson",
                "latitude":     lat,
                "longitude":    lon,
                "maxradiuskm":  500,
                "minmagnitude": 4.0,
                "starttime":    "2014-01-01",
                "limit":        1000,
            },
            timeout=15,
        )
        features   = resp.json().get("features", [])
        magnitudes = [f["properties"]["mag"] for f in features if f["properties"].get("mag")]
        return {
            "count":         len(features),
            "max_magnitude": max(magnitudes) if magnitudes else None,
        }
    except Exception as e:
        print(f"USGS fetch failed: {e}", flush=True)
        return {}


def fetch_worldpop(lat: float, lon: float) -> dict:
    """WorldPop: population density within 50km of epicenter (2020)."""
    try:
        resp = requests.get(
            "https://api.worldpop.org/v1/services/stats",
            params={
                "dataset":           "wpgppop",
                "year":              2020,
                "geojson":           json.dumps({"type": "Point", "coordinates": [lon, lat]}),
                "runasdemographics": True,
            },
            timeout=10,
        )
        return resp.json().get("data", {})
    except Exception as e:
        print(f"WorldPop fetch failed: {e}", flush=True)
        return {}


def fetch_overpass(lat: float, lon: float) -> dict:
    """Overpass/OSM: hospitals and airports within 150km."""
    try:
        radius = 150000  # 150km in metres
        query  = f"""
        [out:json][timeout:25];
        (
          node["amenity"="hospital"](around:{radius},{lat},{lon});
          node["aeroway"="aerodrome"](around:{radius},{lat},{lon});
        );
        out count;
        """
        resp = requests.post(
            "https://overpass-api.de/api/interpreter",
            data={"data": query},
            timeout=25,
        )
        counts = resp.json().get("elements", [])
        return {"infrastructure_count_150km": len(counts)}
    except Exception as e:
        print(f"Overpass fetch failed: {e}", flush=True)
        return {}


def fetch_openmeteo(lat: float, lon: float) -> dict:
    """Open-Meteo: 30-day historical weather baseline."""
    try:
        from datetime import timedelta
        end   = datetime.utcnow().date()
        start = end - timedelta(days=30)
        resp  = requests.get(
            "https://archive-api.open-meteo.com/v1/archive",
            params={
                "latitude":   lat,
                "longitude":  lon,
                "start_date": str(start),
                "end_date":   str(end),
                "daily":      "temperature_2m_mean,precipitation_sum,windspeed_10m_max",
                "timezone":   "UTC",
            },
            timeout=15,
        )
        daily = resp.json().get("daily", {})
        temps = [t for t in daily.get("temperature_2m_mean", []) if t is not None]
        precip = [p for p in daily.get("precipitation_sum", []) if p is not None]
        winds = [w for w in daily.get("windspeed_10m_max", []) if w is not None]
        return {
            "temp_avg_30d":      round(sum(temps)  / len(temps),  2) if temps  else None,
            "precipitation_30d": round(sum(precip) / len(precip), 2) if precip else None,
            "wind_speed_avg":    round(sum(winds)  / len(winds),  2) if winds  else None,
        }
    except Exception as e:
        print(f"Open-Meteo fetch failed: {e}", flush=True)
        return {}


# ── Enrichment with MongoDB state tracking ──────────────────────────────────

def enrich_event(event: dict) -> dict:
    """
    Enrich a raw event with data from all four external APIs.
    Checks MongoDB state flags before each API call — if a step
    was already completed (e.g. after a crash + restart), it is skipped.
    """
    props    = event.get("data", {}).get("properties", {})
    unid     = props.get("unid")
    lat      = props.get("lat")
    lon      = props.get("lon")
    existing = mongo_collection.find_one({"_id": unid}) or {}

    enriched = dict(event)

    # USGS
    if not existing.get("usgs_fetched"):
        usgs = fetch_usgs_nearby(lat, lon)
        enriched["usgs_nearby"] = usgs
        mongo_collection.update_one({"_id": unid}, {"$set": {"usgs_fetched": True}})
        print(f"  usgs_fetched: {unid}", flush=True)
    else:
        print(f"  usgs skip (cached): {unid}", flush=True)

    # WorldPop
    if not existing.get("worldpop_fetched"):
        worldpop = fetch_worldpop(lat, lon)
        enriched["population_data"] = worldpop
        mongo_collection.update_one({"_id": unid}, {"$set": {"worldpop_fetched": True}})
        print(f"  worldpop_fetched: {unid}", flush=True)
    else:
        print(f"  worldpop skip (cached): {unid}", flush=True)

    # Overpass
    if not existing.get("overpass_fetched"):
        overpass = fetch_overpass(lat, lon)
        enriched["infrastructure"] = overpass
        mongo_collection.update_one({"_id": unid}, {"$set": {"overpass_fetched": True}})
        print(f"  overpass_fetched: {unid}", flush=True)
    else:
        print(f"  overpass skip (cached): {unid}", flush=True)

    # Open-Meteo
    if not existing.get("meteo_fetched"):
        meteo = fetch_openmeteo(lat, lon)
        enriched["weather_baseline"] = meteo
        mongo_collection.update_one({"_id": unid}, {"$set": {"meteo_fetched": True}})
        print(f"  meteo_fetched: {unid}", flush=True)
    else:
        print(f"  meteo skip (cached): {unid}", flush=True)

    enriched["enriched_at"]          = datetime.utcnow().isoformat()
    enriched["ge_validation_passed"] = True
    return enriched


def write_to_silver(enriched: dict, unid: str) -> str:
    """Write enriched event to S3 Silver layer."""
    date_path = datetime.utcnow().strftime("%Y/%m/%d")
    key       = f"silver/events/{date_path}/{unid}_enriched.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(enriched),
        ContentType="application/json",
    )
    return key


# ── Main consumer loop ──────────────────────────────────────────────────────

print("Flink enrichment job started", flush=True)

for message in consumer:
    event = message.value
    unid  = event.get("data", {}).get("properties", {}).get("unid", "unknown")

    print(f"Processing: {unid}", flush=True)

    # Validate raw event before enrichment
    passed, reason = validate(event)
    if not passed:
        print(f"  VALIDATION FAILED ({reason}) — routing to DLQ: {unid}", flush=True)
        dlq_producer.send("raw-seismic-events-dlq", {**event, "dlq_reason": reason})
        continue

    try:
        enriched = enrich_event(event)
        key      = write_to_silver(enriched, unid)
        print(f"  Silver: s3://{S3_BUCKET}/{key}", flush=True)
    except Exception as e:
        print(f"  ENRICHMENT ERROR ({e}) — routing to DLQ: {unid}", flush=True)
        dlq_producer.send("raw-seismic-events-dlq", {**event, "dlq_reason": str(e)})

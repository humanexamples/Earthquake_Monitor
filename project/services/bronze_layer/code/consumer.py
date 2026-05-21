import asyncio
import json

import websockets

from kafka_client import make_producer, send_event
from mongo_client import make_mongo_db, upsert_earthquake_event, upsert_historical_earthquakes, upsert_infrastructure
from fetchers import fetch_usgs_nearby, fetch_worldpop, fetch_overpass, fetch_location


EMSC_WS_URL   = "wss://www.seismicportal.eu/standing_order/websocket"
MAX_RADIUS_KM = 100.0

producer = make_producer()
db       = make_mongo_db()


async def listen():
    while True:
        try:
            async with websockets.connect(EMSC_WS_URL) as ws:
                print("Connected to EMSC WebSocket", flush=True)
                async for message in ws:
                    earthquake_event = json.loads(message)
                    properties = earthquake_event.get("data", {}).get("properties", {})
                    unid = properties.get("unid", "unknown")
                    lat  = properties.get("lat")
                    lon  = properties.get("lon")

                    historical_earthquakes = []
                    population             = None
                    infrastructure         = []
                    location               = {"country": None, "state": None, "settlement": None}

                    if lat is not None and lon is not None:
                        print("Fetch historical Earthquakes data", flush=True)
                        historical_earthquakes = fetch_usgs_nearby(lat=lat, lon=lon, radius_km=MAX_RADIUS_KM)
                        print("Fetch population data", flush=True)
                        population = fetch_worldpop(lat=lat, lon=lon, radius_km=MAX_RADIUS_KM)
                        print("Fetch infrastructure data", flush=True)
                        infrastructure = fetch_overpass(lat=lat, lon=lon, radius_km=MAX_RADIUS_KM)
                        print("Fetch location", flush=True)
                        location = fetch_location(lat=lat, lon=lon)

                    earthquake_event["historical_earthquake_ids"] = [eq["id"] for eq in historical_earthquakes]
                    earthquake_event["population"]                = population
                    earthquake_event["infrastructure_ids"]        = [el["id"] for el in infrastructure]
                    earthquake_event["location"]                  = location

                    upsert_earthquake_event(db, unid, earthquake_event)
                    upsert_historical_earthquakes(db, historical_earthquakes)
                    upsert_infrastructure(db, infrastructure)

                    send_event(producer, unid)
                    print(f"Sent: {unid}", flush=True)

        except Exception as e:
            print(f"Connection lost: {e} — reconnecting in 5s...", flush=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(listen())

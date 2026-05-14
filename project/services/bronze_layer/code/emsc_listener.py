"""
emsc_listener.py
Persistent WebSocket listener for EMSC seismic events.
Save raw events to S3 (bronze layer) and forward to Kafka (raw-seismic-events topic).
Reconnects automatically on any connection failure.
"""
import asyncio
import json

import websockets

from kafka_client import make_producer, send_event
from s3_client import make_client, upload_to_s3
from fetchers import fetch_usgs_nearby, fetch_worldpop, fetch_overpass, fetch_location


EMSC_WS_URL = "wss://www.seismicportal.eu/standing_order/websocket"

producer = make_producer()
s3       = make_client()

MAX_Radius_KM = 100.0

async def listen():
    while True:
        try:
            async with websockets.connect(EMSC_WS_URL) as ws:
                print("Connected to EMSC WebSocket", flush=True)
                async for message in ws:
                    earthquake_event = json.loads(message)
                    properties = earthquake_event.get("data", {}).get("properties", {})
                    unid  = properties.get("unid", "unknown")
                    lat = properties.get("lat", None)
                    lon = properties.get("lon", None)
                    historical_earthquakes = []
                    population = None
                    infrastructure = []
                    location = {'land': None, 'bundesland': None, 'ort': None}
                    if lat is not None and lon is not None:
                        print("Fetch historical Earthquakes data")
                        historical_earthquakes = fetch_usgs_nearby(lat=lat, lon=lon, radius_km=MAX_Radius_KM)
                        print("Fetch population data")
                        population = fetch_worldpop(lat=lat, lon=lon, radius_km=MAX_Radius_KM)
                        print("Fetch infrastructure data")
                        infrastructure = fetch_overpass(lat=lat, lon=lon, radius_km=MAX_Radius_KM)
                        print("fetch location")
                        location = fetch_location(lat=lat, lon=lon)

                    earthquake_event["historical_earthquake_ids"] = [eq["id"] for eq in historical_earthquakes]
                    earthquake_event["population"] = population
                    earthquake_event["infrastructure_ids"] = [eq["id"] for eq in infrastructure]

                    upload_to_s3(s3, unid, earthquake_event, historical_earthquakes, infrastructure, location)


                    send_event(producer, unid)
                    print(f"Sent: {unid}", flush=True)

        except Exception as e:
            print(f"Connection lost: {e} — reconnecting in 5s...", flush=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(listen())

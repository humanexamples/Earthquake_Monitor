"""
fetchers.py
External API calls for the enrichment pipeline.
"""
import http.client
import json
import math
import urllib.parse
import requests

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
WORLD_POP = "https://api.worldpop.org/v1"
OVERPASS_HOST     = "overpass-api.de"
OVERPASS_ENDPOINT = "/api/interpreter"
Nominatim_URL = "https://nominatim.openstreetmap.org/reverse"

EARTH_CIRCUMFERENCE_KM = 40_075
TIMEOUT_SEC = 240  # seconds to wait for an event to arrive


def _worldpop_params(lat: float, lon: float, radius_km: float) -> str:
    """Build the GeoJSON hexagon query string for the WorldPop Stats API."""
    lat_per_km = 360 / EARTH_CIRCUMFERENCE_KM
    lon_per_km = 360 / (EARTH_CIRCUMFERENCE_KM * math.cos(math.radians(lat)))

    coordinates = [[
        [
            lon + radius_km * math.sin(math.radians(60 * i)) * lon_per_km,
            lat + radius_km * math.cos(math.radians(60 * i)) * lat_per_km,
        ]
        for i in range(6)
    ]]

    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {"type": "Polygon", "coordinates": coordinates},
            }
        ],
    }
    return f"dataset=wpgppop&year=2020&geojson={json.dumps(geojson)}"

def fetch_worldpop(lat: float, lon: float, radius_km: float) -> str | None:
    """WorldPop: population density within radius_km of epicenter (2020)."""
    try:
        resp = requests.get(
            f"{WORLD_POP}/services/stats",
            params=_worldpop_params(lat, lon, radius_km),
            timeout=TIMEOUT_SEC,
        )
        if resp.status_code == 200:
            task_id = resp.json().get("taskid", "")
            # print(f"WorldPop task submitted, task ID: {task_id}")
            resp = requests.get(
                f"{WORLD_POP}/tasks/{task_id}",
                timeout=TIMEOUT_SEC,
            )
            return resp.json().get("data", {}).get("total_population", "")
    except Exception as e:
        print(f"WorldPop fetch failed: {e}", flush=True)
        return None
    
def fetch_usgs_nearby(lat: float, lon: float, radius_km: float) -> list:
    """USGS: historical M4.0+ earthquakes within radius_km since 2014."""
    try:
        resp = requests.get(
            USGS_URL,
            params={
                "format":       "geojson",
                "latitude":     lat,
                "longitude":    lon,
                "maxradiuskm":  radius_km,
                "minmagnitude": 4.0,
                "starttime":    "2014-01-01",
                "limit":        1000,
            },
            timeout=TIMEOUT_SEC,
        )
        features   = resp.json().get("features", [])

        return [
            {
                "id":    f.get("id"),
                "mag":   f["properties"].get("mag"),
                "time":  f["properties"].get("time"),
                "place": f["properties"].get("place"),
                "coordinates": 
                {
                    "lon": f["geometry"]["coordinates"][0],
                    "lat": f["geometry"]["coordinates"][1],
                    "depth_km": f["geometry"]["coordinates"][2],
                }
            }
            for f in features
        ]
    except Exception as e:
        print(f"USGS fetch failed: {e}", flush=True)
        return []
    
def fetch_overpass(lat: float, lon: float, radius_km: float) -> list:
    try:
        radius = int(radius_km * 1000)  # convert km to meters
        query = (
            "[out:json][timeout:60];\n"
            "(\n"
            f'  nwr["amenity"="hospital"](around:{radius},{lat},{lon});\n'
            f'  nwr["aeroway"="aerodrome"](around:{radius},{lat},{lon});\n'
            f'  nwr["amenity"="police"](around:{radius},{lat},{lon});\n'
            ");\n"
            "out tags;"
        )
        body = urllib.parse.urlencode({"data": query}).encode("utf-8")
        conn = http.client.HTTPConnection(OVERPASS_HOST, 80, timeout=60)
        conn.request("POST", OVERPASS_ENDPOINT, body=body, headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent":   "EarthquakeMonitor/1.0",
        })
        resp = conn.getresponse()
        if resp.status != 200:
            raise RuntimeError(f"Overpass API returned HTTP {resp.status}: {resp.reason}")
        return json.loads(resp.read().decode("utf-8")).get("elements", [])
    except Exception as e:
        print(f"Overpass fetch failed: {e}", flush=True)
        return []
    
def fetch_location(lat: float, lon: float):
    """Nominatim: human-readable location name for given lat/lon."""
    try:
        headers = {
            "User-Agent": "MeinPythonNotebook/1.0 (kontakt@beispiel.de)"  # Bitte anpassen!
        }
        params = {
            "lat": lat,
            "lon": lon,
            "format": "json",
            "accept-language": "en"
        }
        response = requests.get(Nominatim_URL, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        adresse = data.get("address", {})
        return {
            "country":    adresse.get("country"),
            "state":      adresse.get("state"),
            "settlement": adresse.get("city") or adresse.get("town") or adresse.get("village"),
        }
    except Exception as e:
        print(f"Nominatim fetch failed: {e}", flush=True)
        return {"country": None, "state": None, "settlement": None}
"""
transformer.py
Cleans and normalises raw bronze data into a silver-layer document.
"""


def _to_float(value) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _split_infrastructure(elements: list) -> dict:
    hospitals, airports, police_stations = [], [], []
    for el in elements:
        tags    = el.get("tags", {})
        entry   = {"id": el.get("id"), "name": tags.get("name", "unknown"), "type": el.get("type")}
        amenity = tags.get("amenity", "")
        aeroway = tags.get("aeroway", "")
        if amenity == "hospital":
            hospitals.append(entry)
        elif aeroway == "aerodrome":
            airports.append(entry)
        elif amenity == "police":
            police_stations.append(entry)
    return {"hospitals": hospitals, "airports": airports, "police_stations": police_stations}


def transform(unid: str, received_at: str, bronze: dict) -> dict:
    raw      = bronze["earthquake_event"]
    props    = raw.get("data", {}).get("properties", {})
    coords   = raw.get("data", {}).get("geometry", {}).get("coordinates", [])
    location = bronze["location"]

    depth_km = _to_float(props.get("depth")) or (
        _to_float(coords[2]) if len(coords) > 2 else None
    )

    return {
        "_id":         unid,
        "received_at": received_at,
        "source":      "EMSC",
        "event": {
            "unid":           unid,
            "action":         raw.get("action"),
            "time":           props.get("time"),
            "lat":            _to_float(props.get("lat")),
            "lon":            _to_float(props.get("lon")),
            "depth_km":       depth_km,
            "magnitude":      _to_float(props.get("mag")),
            "magnitude_type": props.get("magtype"),
            "region":         props.get("region"),
            "flynn_region":   props.get("flynn_region"),
            "event_type":     props.get("evtype"),
        },
        "location": {
            "country": location.get("land"),
            "state":   location.get("bundesland"),
            "city":    location.get("ort"),
        },
        "population_100km":              raw.get("population"),
        "historical_earthquake_count":   len(bronze["historical_earthquakes"]),
        "historical_earthquakes":        bronze["historical_earthquakes"],
        "infrastructure":                _split_infrastructure(bronze["infrastructure"]),
    }

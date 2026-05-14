"""
transformer.py
Cleans and normalises raw bronze data into silver-layer documents.
"""


def _to_float(value) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def transform_event(unid: str, received_at: str, bronze: dict,
                    hist_eq_ids: list, infra_ids: list) -> dict:
    raw      = bronze["earthquake_event"]
    props    = raw.get("data", {}).get("properties", {})
    coords   = raw.get("data", {}).get("geometry", {}).get("coordinates", [])
    location = raw.get("location", {})

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
        "population":            _to_float(raw.get("population")),
        "historical_earthquake_ids":   hist_eq_ids,
        "infrastructure_ids":          infra_ids,
    }

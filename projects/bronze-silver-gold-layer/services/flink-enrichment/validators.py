"""
validators.py  ·  SILVER layer — per-record validation
Great Expectations checks run on every enriched event
before it is written to the Silver layer.
Failed records → Kafka DLQ (never silently dropped).
"""
from typing import Tuple

VALID_TSUNAMI_LEVELS = {"Green", "Orange", "Red"}


def validate(event: dict) -> Tuple[bool, str]:
    """
    Validate an enriched event record.
    Returns (True, "") on pass.
    Returns (False, reason) on failure.
    """
    props = event.get("data", {}).get("properties", {})

    # Check 1: event ID must not be null
    unid = props.get("unid")
    if unid is None:
        return False, "unid is null"

    # Check 2: magnitude must be present and between 0 and 10
    # Real EMSC events use "mag"; synthetic/mock events use "magnitude"
    mag = props.get("magnitude") if props.get("magnitude") is not None else props.get("mag")
    if mag is None:
        return False, "magnitude is null"
    if not (0 <= float(mag) <= 10):
        return False, f"magnitude out of range: {mag}"

    # Check 3: depth must be present and positive
    depth = props.get("depth")
    if depth is None:
        return False, "depth is null"
    if float(depth) < 0:
        return False, f"depth is negative: {depth}"

    # Check 4: coordinates must be geographically valid
    lat = props.get("lat")
    lon = props.get("lon")
    if lat is None or lon is None:
        return False, "lat or lon is null"
    if not (-90 <= float(lat) <= 90):
        return False, f"lat out of range: {lat}"
    if not (-180 <= float(lon) <= 180):
        return False, f"lon out of range: {lon}"

    # Check 5: tsunami alert level must be in allowed set (if present)
    tsunami_level = event.get("tsunami_alert_level")
    if tsunami_level is not None and tsunami_level not in VALID_TSUNAMI_LEVELS:
        return False, f"invalid tsunami_alert_level: {tsunami_level}"

    return True, ""

import statistics
from datetime import date as date_type


def _to_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    try:
        return list(value)
    except Exception:
        return []


def _parse_date(val):
    if val is None:
        return None
    if isinstance(val, date_type):
        return val
    try:
        from datetime import datetime
        return datetime.strptime(str(val), "%Y-%m-%d").date()
    except Exception:
        return None


def process_country(row: dict) -> dict:
    dates    = [_parse_date(d) for d in _to_list(row.get("date")) if d is not None]
    dates    = [d for d in dates if d is not None]
    mags     = [float(m) for m in _to_list(row.get("mag")) if m is not None]
    states   = list({s for s in _to_list(row.get("state"))   if s is not None and str(s).strip()})
    settlements = list({s for s in _to_list(row.get("settlement")) if s is not None and str(s).strip()})

    return {
        "country":     row.get("country"),
        "date_min":    min(dates) if dates else None,
        "date_max":    max(dates) if dates else None,
        "mag_median":  round(statistics.median(mags), 4) if mags else None,
        "mag_max":     round(max(mags), 4) if mags else None,
        "states":      states,
        "settlements": settlements,
    }


def process_event(row: dict) -> dict:
    mags      = _to_list(row.get("historical_earthquake_magnitudes"))
    datetimes = _to_list(row.get("historical_earthquake_datetimes"))

    over4_pairs = [
        (float(m), dt)
        for m, dt in zip(mags, datetimes)
        if m is not None and float(m) > 4
    ]
    mags_over4      = [m  for m, _  in over4_pairs]
    datetimes_over4 = [dt for _,  dt in over4_pairs if dt is not None]

    hospitals  = _to_list(row.get("infrastructure_hospital_places"))
    police     = _to_list(row.get("infrastructure_police_places"))
    aerodrome  = _to_list(row.get("infrastructure_aerodrome_places"))

    return {
        "unid":               row.get("unid"),
        "magnitude":          row.get("magnitude"),
        "coordinate_lat":     row.get("coordinate_lat"),
        "coordinate_lon":     row.get("coordinate_lon"),
        "coordinate_depth":   row.get("coordinate_depth"),
        "date":               row.get("date"),
        "time":               row.get("time"),
        "population":         row.get("population"),
        "location_country":   row.get("location_country"),
        "location_state":     row.get("location_state"),
        "location_settlement":row.get("location_settlement"),
        "historical_earthquake_magnitudes_over4_median": (
            round(statistics.median(mags_over4), 4) if mags_over4 else None
        ),
        "historical_earthquake_magnitudes_over4_count": len(mags_over4),
        "historical_earthquake_magnitudes_over4_max": (
            round(max(mags_over4), 4) if mags_over4 else None
        ),
        "historical_earthquake_datetimes_over4_min": (
            min(datetimes_over4) if datetimes_over4 else None
        ),
        "historical_earthquake_datetimes_over4_max": (
            max(datetimes_over4) if datetimes_over4 else None
        ),
        "infrastructure_hospitals_count":  len(hospitals),
        "infrastructure_police_count":     len(police),
        "infrastructure_aerodrome_count":  len(aerodrome),
    }

import os

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

TABLE               = "earthquake_events_gold"
TABLE_TOP10         = "top10_earthquakes_24h"
TABLE_TOP_HISTORICAL = "top_historical_earthquakes"
TABLE_COUNTRIES     = "country_summary"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    unid                                            VARCHAR PRIMARY KEY,
    magnitude                                       FLOAT,
    coordinate_lat                                  FLOAT,
    coordinate_lon                                  FLOAT,
    coordinate_depth                                FLOAT,
    date                                            DATE,
    time                                            TIME,
    population                                      FLOAT,
    location_country                                VARCHAR,
    location_state                                  VARCHAR,
    location_settlement                             VARCHAR,
    historical_earthquake_magnitudes_over4_median   FLOAT,
    historical_earthquake_magnitudes_over4_count    INTEGER,
    historical_earthquake_magnitudes_over4_max      FLOAT,
    infrastructure_hospitals_count                  INTEGER,
    infrastructure_police_count                     INTEGER,
    infrastructure_aerodrome_count                  INTEGER
);
"""

CREATE_TOP10_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_TOP10} (
    unid                VARCHAR PRIMARY KEY,
    magnitude           FLOAT,
    coordinate_lat      FLOAT,
    coordinate_lon      FLOAT,
    coordinate_depth    FLOAT,
    date                DATE,
    time                TIME,
    population          FLOAT,
    location_country    VARCHAR,
    location_state      VARCHAR,
    location_settlement VARCHAR
);
"""

CREATE_TOP_HISTORICAL_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_TOP_HISTORICAL} (
    unid                                        VARCHAR PRIMARY KEY,
    magnitude                                   FLOAT,
    coordinate_lat                              FLOAT,
    coordinate_lon                              FLOAT,
    coordinate_depth                            FLOAT,
    date                                        DATE,
    time                                        TIME,
    population                                  FLOAT,
    location_country                            VARCHAR,
    location_state                              VARCHAR,
    location_settlement                         VARCHAR,
    historical_earthquake_magnitudes_over4_max  FLOAT
);
"""

UPSERT_SQL = f"""
INSERT INTO {TABLE} (
    unid, magnitude, coordinate_lat, coordinate_lon, coordinate_depth,
    date, time, population, location_country, location_state, location_settlement,
    historical_earthquake_magnitudes_over4_median,
    historical_earthquake_magnitudes_over4_count,
    historical_earthquake_magnitudes_over4_max,
    infrastructure_hospitals_count,
    infrastructure_police_count,
    infrastructure_aerodrome_count
) VALUES %s
ON CONFLICT (unid) DO UPDATE SET
    magnitude                                     = EXCLUDED.magnitude,
    coordinate_lat                                = EXCLUDED.coordinate_lat,
    coordinate_lon                                = EXCLUDED.coordinate_lon,
    coordinate_depth                              = EXCLUDED.coordinate_depth,
    date                                          = EXCLUDED.date,
    time                                          = EXCLUDED.time,
    population                                    = EXCLUDED.population,
    location_country                              = EXCLUDED.location_country,
    location_state                                = EXCLUDED.location_state,
    location_settlement                           = EXCLUDED.location_settlement,
    historical_earthquake_magnitudes_over4_median = EXCLUDED.historical_earthquake_magnitudes_over4_median,
    historical_earthquake_magnitudes_over4_count  = EXCLUDED.historical_earthquake_magnitudes_over4_count,
    historical_earthquake_magnitudes_over4_max    = EXCLUDED.historical_earthquake_magnitudes_over4_max,
    infrastructure_hospitals_count                = EXCLUDED.infrastructure_hospitals_count,
    infrastructure_police_count                   = EXCLUDED.infrastructure_police_count,
    infrastructure_aerodrome_count                = EXCLUDED.infrastructure_aerodrome_count;
"""

# WHERE statement: Filters to only events that occurred in the last 24 hours. date (type DATE) + time (type TIME) combines into a timestamp for comparison.
# ORDER statement: Strongest earthquakes first. Events with no magnitude value are pushed to the end.
TRUNCATE_TOP10_SQL        = f"SET lock_timeout = '5s'; TRUNCATE {TABLE_TOP10};"
INSERT_TOP10_SQL = f"""
INSERT INTO {TABLE_TOP10} (
    unid, magnitude, coordinate_lat, coordinate_lon, coordinate_depth,
    date, time, population, location_country, location_state, location_settlement
)
SELECT
    unid, magnitude, coordinate_lat, coordinate_lon, coordinate_depth,
    date, time, population, location_country, location_state, location_settlement
FROM {TABLE}
WHERE date + time >= NOW()::timestamp - INTERVAL '24 hours'
ORDER BY magnitude DESC NULLS LAST
LIMIT 10;
"""

CREATE_COUNTRIES_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_COUNTRIES} (
    country      VARCHAR PRIMARY KEY,
    date_min     DATE,
    date_max     DATE,
    mag_median   FLOAT,
    mag_max      FLOAT,
    states       TEXT[],
    settlements  TEXT[]
);
"""

TRUNCATE_TOP_HISTORICAL_SQL = f"SET lock_timeout = '5s'; TRUNCATE {TABLE_TOP_HISTORICAL};"
INSERT_TOP_HISTORICAL_SQL = f"""
INSERT INTO {TABLE_TOP_HISTORICAL} (
    unid, magnitude, coordinate_lat, coordinate_lon, coordinate_depth,
    date, time, population, location_country, location_state, location_settlement,
    historical_earthquake_magnitudes_over4_max
)
SELECT
    unid, magnitude, coordinate_lat, coordinate_lon, coordinate_depth,
    date, time, population, location_country, location_state, location_settlement,
    historical_earthquake_magnitudes_over4_max
FROM {TABLE}
WHERE historical_earthquake_magnitudes_over4_max IS NOT NULL
ORDER BY historical_earthquake_magnitudes_over4_max DESC NULLS LAST
LIMIT 10;
"""


UPSERT_COUNTRY_SQL = f"""
INSERT INTO {TABLE_COUNTRIES} (
    country, date_min, date_max, mag_median, mag_max, states, settlements
) VALUES %s
ON CONFLICT (country) DO UPDATE SET
    date_min    = EXCLUDED.date_min,
    date_max    = EXCLUDED.date_max,
    mag_median  = EXCLUDED.mag_median,
    mag_max     = EXCLUDED.mag_max,
    states      = EXCLUDED.states,
    settlements = EXCLUDED.settlements;
"""


def make_postgres_conn():
    return psycopg2.connect(
        host=    os.getenv("POSTGRES_GOLD_HOST",     "postgres_gold"),
        port=    os.getenv("POSTGRES_GOLD_PORT",     "5432"),
        dbname=  os.getenv("POSTGRES_GOLD_DB",       "earthquake_gold"),
        user=    os.getenv("POSTGRES_GOLD_USER",     "gold_user"),
        password=os.getenv("POSTGRES_GOLD_PASSWORD", "gold_password"),
    )


def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
        cur.execute(CREATE_TOP10_SQL)
        cur.execute(CREATE_TOP_HISTORICAL_SQL)
        cur.execute(CREATE_COUNTRIES_SQL)
    conn.commit()


def upsert_event(conn, record: dict) -> None:
    row = (
        record.get("unid"),
        record.get("magnitude"),
        record.get("coordinate_lat"),
        record.get("coordinate_lon"),
        record.get("coordinate_depth"),
        record.get("date") or None,
        record.get("time") or None,
        record.get("population"),
        record.get("location_country"),
        record.get("location_state"),
        record.get("location_settlement"),
        record.get("historical_earthquake_magnitudes_over4_median"),
        record.get("historical_earthquake_magnitudes_over4_count"),
        record.get("historical_earthquake_magnitudes_over4_max"),
        record.get("infrastructure_hospitals_count"),
        record.get("infrastructure_police_count"),
        record.get("infrastructure_aerodrome_count"),
    )
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, [row])
    conn.commit()


def refresh_top10(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(TRUNCATE_TOP10_SQL)
        conn.commit()
        cur.execute(INSERT_TOP10_SQL)
    conn.commit()


def refresh_top_historical(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(TRUNCATE_TOP_HISTORICAL_SQL)
        conn.commit()
        cur.execute(INSERT_TOP_HISTORICAL_SQL)
    conn.commit()


def upsert_countries(conn, records: list[dict]) -> None:
    if not records:
        return
    rows = [
        (
            r.get("country"),
            r.get("date_min") or None,
            r.get("date_max") or None,
            r.get("mag_median"),
            r.get("mag_max"),
            r.get("states") or [],
            r.get("settlements") or [],
        )
        for r in records
    ]
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_COUNTRY_SQL, rows)
    conn.commit()

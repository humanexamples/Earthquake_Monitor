# Earthquake Monitor — Pipeline & Dashboard Documentation

## Overview

The Earthquake Monitor is a real-time data pipeline that ingests live seismic events from a global seismic network, enriches them with contextual data, and makes them queryable through an interactive dashboard.

The pipeline follows the **Medallion Architecture**: data passes through three successive quality layers — Bronze (raw), Silver (cleaned), and Gold (analytics-ready) — before reaching the dashboard.

---

## Data Flow

```
EMSC WebSocket (live seismic feed)
        │
        ▼
┌──────────────────┐
│   Bronze Layer   │  Ingestion & enrichment → MinIO + Kafka
└──────────────────┘
        │ Kafka topic: raw-seismic-events
        ▼
┌──────────────────┐
│   Silver Layer   │  Transformation & Parquet storage → MinIO + Kafka
└──────────────────┘
        │ Kafka topic: silver-processed-events
        ▼
┌──────────────────┐
│   Gold Layer     │  Aggregation & analytics → PostgreSQL
└──────────────────┘
        │ SQL queries
        ▼
┌──────────────────┐
│   Streamlit      │  Interactive dashboard
└──────────────────┘
```

---

## Bronze Layer — Raw Ingestion

**What it does:**
The bronze layer is the entry point of the pipeline. It maintains a persistent WebSocket connection to the EMSC (European-Mediterranean Seismic Centre) live feed. Every time an earthquake event is received, it enriches the raw event data with information from four external APIs and stores everything in object storage.

**Data sources used per event:**
| Source | Data fetched |
|---|---|
| EMSC WebSocket | Raw seismic event (magnitude, coordinates, time) |
| USGS Earthquake API | Historical earthquakes M4.0+ within 100 km radius since 2014 |
| WorldPop API | Population density around the epicenter |
| Overpass API (OpenStreetMap) | Nearby infrastructure: hospitals, police stations, aerodromes |
| Nominatim API (OpenStreetMap) | Reverse geocoding: country, state, settlement name |

**Storage (MinIO):**
Each event is stored as three JSON files under a path that reflects the ingestion date:

```
bronze/events/{YYYY}/{MM}/{DD}/{unid}/
    earthquake_event.json       ← raw EMSC event + location metadata
    historical_earthquakes.json ← USGS historical data for the region
    infrastructure.json         ← Overpass amenity data
```

The `unid` (unique ID) identifies each earthquake event, e.g. `20260514_0000128`.

**Kafka:**
After storing in MinIO, the bronze layer publishes the event ID to the Kafka topic `raw-seismic-events` to signal downstream layers.

**Technologies:** Python · asyncio / websockets · boto3 (S3/MinIO) · kafka-python · requests

---

## Silver Layer — Transformation & Parquet Storage

**What it does:**
The silver layer consumes events from Kafka, reads the raw bronze JSON files from MinIO, cleans and normalises the data, and writes it to Parquet format — a columnar storage format optimised for analytics.

**Transformations applied:**
- Extracts and casts numeric fields (latitude, longitude, depth, magnitude) to `float`
- Splits the ISO timestamp into separate `date` and `time` columns
- Flattens nested infrastructure objects into named lists grouped by category (hospitals, police, aerodromes)
- Deduplicates historical earthquake entries by their USGS ID
- Aggregates all events per country into a separate country summary file

**Storage (MinIO):**
Two Parquet files are maintained with an upsert pattern (read → modify → write):

| File | Description |
|---|---|
| `silver/earthquake_events.parquet` | One row per earthquake event |
| `silver/countries.parquet` | One row per country; arrays of all event dates, magnitudes, locations |

**Kafka:**
After writing to Parquet, the silver layer publishes `{"unid": "<id>"}` to the topic `silver-processed-events`.

**Technologies:** Python · pandas · pyarrow · boto3 · kafka-python

---

## Gold Layer — Analytics & Data Warehouse

**What it does:**
The gold layer consumes events from the silver Kafka topic, reads the processed Parquet data, computes analytics-ready derived columns, and loads the results into a PostgreSQL data warehouse. Four tables are maintained.

**Derived columns computed per event:**
| Column | Description |
|---|---|
| `historical_earthquake_magnitudes_over4_median` | Median magnitude of historical M4+ earthquakes in the region |
| `historical_earthquake_magnitudes_over4_count` | Count of historical M4+ earthquakes in the region |
| `historical_earthquake_magnitudes_over4_max` | Strongest historical earthquake in the region |
| `infrastructure_hospitals_count` | Number of hospitals within radius |
| `infrastructure_police_count` | Number of police stations within radius |
| `infrastructure_aerodrome_count` | Number of aerodromes within radius |

**PostgreSQL tables:**

| Table | Description |
|---|---|
| `earthquake_events_gold` | Primary fact table — all events with all derived columns |
| `top10_earthquakes_24h` | Snapshot: top 10 earthquakes by magnitude in the last 24 hours |
| `top_historical_earthquakes` | Snapshot: top 10 locations with the strongest historical seismic activity |
| `country_summary` | Per-country aggregation: date range, median/max magnitude, affected states and settlements |

The two snapshot tables are refreshed after every new event using a TRUNCATE + INSERT pattern.

**Technologies:** Python · psycopg2 · pandas · PostgreSQL 16 · kafka-python

---

## Infrastructure Services

All services run as Docker containers on a shared `kafka-network` bridge network.

| Service | Technology | Role |
|---|---|---|
| Zookeeper | Confluent CP 7.5.3 | Kafka cluster coordinator |
| Kafka | Confluent CP 7.5.3 | Event streaming bus between layers |
| MinIO | MinIO (latest) | S3-compatible object storage for Bronze and Silver |
| PostgreSQL | PostgreSQL 16 | Relational data warehouse for Gold layer |
| MongoDB | MongoDB 7.0 | Document store (provisioned, not active in current pipeline) |
| Apache Spark | Bitnami Spark 3.5 | Distributed compute engine (provisioned for future analytics) |
| bronze_layer | Python 3.x | Seismic event ingestor |
| silver_layer | Python 3.x | Data transformer |
| gold_layer | Python 3.x | Analytics loader |
| streamlit | Python 3.x | Dashboard frontend |

---

## Streamlit Dashboard

**URL:** http://localhost:8501

The dashboard connects to the PostgreSQL gold database and presents the data across four tabs. All data is cached for 30 seconds and can be manually refreshed with the **Refresh** button.

---

### Tab 1 — Last 24 h: Top 10

Shows the ten strongest earthquakes that occurred in the last 24 hours.

- **Metrics bar:** Magnitude of the strongest event · Total event count · Average magnitude
- **Globe map:** Interactive scatter-geo map; marker size and colour (red scale) represent magnitude; hover shows location, date, and time
- **Table:** unid, magnitude, country, state, settlement, date, time, depth, population

---

### Tab 2 — All Earthquakes

Full event table with interactive filters.

- **Filters:** Country dropdown · Magnitude range slider · Date range picker
- **Metrics bar:** Filtered event count · Strongest magnitude · Average magnitude
- **Globe map:** Same scatter-geo map as Tab 1 with yellow-orange-red colour scale
- **Table:** All columns from Tab 1 plus the derived analytics columns (historical magnitude stats, infrastructure counts); cells are colour-coded with gradient highlighting (magnitude → red, infrastructure → blue, historical → orange)

---

### Tab 3 — Country Overview

Country-level aggregated statistics.

- **Bar chart:** Top 20 countries ranked by maximum earthquake magnitude
- **Scatter plot:** Median magnitude vs. maximum magnitude per country (country labels on each point)
- **Table:** country, date range (min/max), median magnitude, max magnitude, list of affected states, list of affected settlements

---

### Tab 4 — Historically Strongest

Shows locations with the strongest historical seismic background — regions where large earthquakes have occurred before.

- **Metrics bar:** Strongest historical magnitude recorded · Count of entries · Average historical magnitude
- **Globe map:** Marker size and colour (plasma scale) represent the historical maximum magnitude
- **Table:** unid, historical max magnitude, current event magnitude, country, state, settlement, date, time

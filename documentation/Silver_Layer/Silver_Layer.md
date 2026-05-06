# Silver Layer — Enriched & Validated Events

> Earthquake Monitoring Pipeline · Medallion Architecture · Layer 2 of 3

---

## 1. What the Silver Layer Is

The Silver layer is the **enriched, validated, quality-assured** version of every
raw seismic event. It sits between the raw Bronze archive and the business-ready Gold
aggregations. Nothing reaches Silver without passing both real-time enrichment and
per-record validation. Nothing reaches Gold without passing a dataset-level quality
check against Silver.

Silver answers the question: **what happened, and what does it mean in context?**

A Bronze event is a set of coordinates and a magnitude number. A Silver event is the
same event enriched with population density at the epicenter, infrastructure within
reach, historical seismic context for the region, weather conditions, and a validation
stamp confirming the record is complete and structurally sound.

---

## 2. Purpose and Responsibilities

| Responsibility | Description |
|---|---|
| Enrichment | Adds USGS, WorldPop, Overpass, Open-Meteo data to every raw event |
| Validation | Great Expectations checks block invalid records before Silver write |
| DLQ routing | Invalid records parked in Kafka DLQ — never silently dropped |
| State tracking | MongoDB tracks enrichment progress per event for crash resilience |
| Downstream input | PySpark reads Silver to compute impact scores for the Gold layer |
| Monitoring writes | 7-day monitoring poll results written under `silver/monitoring/` |

**What Silver does NOT do:**

- It does not aggregate across events
- It does not compute impact scores
- It is not queried directly by the dashboard
- It does not delete or overwrite Bronze

---

## 3. Where Silver Sits in the Pipeline

```
Bronze Layer  (raw events from EMSC)
      │
      │  Flink reads from Kafka
      │  (not from Bronze directly — Kafka is the real-time source)
      ▼
┌─────────────────────────────────────────────────────────┐
│  Apache Flink  (flink-enrichment container)              │
│                                                          │
│  1. Check MongoDB — already enriched? skip API calls     │
│  2. Call USGS — 10yr seismic history, 500km radius       │
│  3. Call WorldPop — population density at epicenter      │
│  4. Call Overpass — hospitals, airports within 150km     │
│  5. Call Open-Meteo — 30-day weather baseline            │
│  6. Validate with Great Expectations                     │
└──────────────────────────┬──────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              │ valid                   │ invalid
              ▼                         ▼
     ┌─────────────────┐      ┌──────────────────────┐
     │  MinIO / S3     │      │  Kafka DLQ            │
     │                 │      │  raw-seismic-          │
     │  SILVER LAYER   │      │  events-dlq            │
     │                 │      │  (parked for review)   │
     │  silver/        │      └──────────────────────┘
     │  events/        │
     │  YYYY/MM/DD/    │
     │  {unid}_        │
     │  enriched.json  │
     └────────┬────────┘
              │
    ┌─────────┴──────────────────────────────────┐
    │                                             │
    ▼                                             ▼
Airflow S3KeySensor                      7-day monitoring
detects new Silver files                 results also written
triggers Gold aggregation DAG            to silver/monitoring/
```

---

## 4. Enrichment APIs

Silver adds data from four external sources. Flink calls these APIs in sequence,
checking MongoDB before each call to avoid duplicate API hits for the same event.

### 4.1 USGS Earthquake Catalog

**URL:** `https://earthquake.usgs.gov/fdsnws/event/1/query`

Fetches all M4.0+ earthquakes within 500 km of the epicenter over the past 10 years.
Provides historical seismic context — how active is this region, what is the maximum
recorded magnitude nearby.

**Added fields:**
```json
"usgs_nearby": {
  "count": 34,
  "max_magnitude": 6.2
}
```

### 4.2 WorldPop Population Density

**URL:** `https://api.worldpop.org/v1/services/stats`

Returns the estimated population within a radius of the epicenter based on 2020
census data. Used downstream in the Gold layer to compute the population exposure
impact score.

**Added fields:**
```json
"population_data": {
  "total_population": 4100000
}
```

### 4.3 Overpass API (OpenStreetMap)

**URL:** `https://overpass-api.de/api/interpreter`

Queries OpenStreetMap for hospitals, fire stations, and airports within 150 km of
the epicenter. Used to assess infrastructure risk.

**Added fields:**
```json
"infrastructure": {
  "hospitals_150km": 42,
  "airports_150km": 3
}
```

### 4.4 Open-Meteo Archive

**URL:** `https://archive-api.open-meteo.com/v1/archive`

Returns a 30-day weather baseline (temperature, precipitation, wind speed) for the
region. Used as context for rescue and response conditions.

**Added fields:**
```json
"weather_baseline": {
  "temp_avg_30d": 4.2,
  "precipitation_30d": 62.1,
  "wind_speed_avg": 3.8
}
```

---

## 5. Data Structure

### 5.1 Storage path convention

```
s3://eq-monitor/silver/events/YYYY/MM/DD/{unid}_enriched.json
```

Example:
```
s3://eq-monitor/silver/events/2024/01/15/20240115_0000123_enriched.json
```

### 5.2 Full Silver record

```json
{
  "action": "create",
  "data": {
    "type": "Feature",
    "geometry": {
      "type": "Point",
      "coordinates": [28.979530, 41.015137, 10.0]
    },
    "properties": {
      "unid": "20240115_0000123",
      "source_catalog": "EMSC",
      "flynn_region": "ISTANBUL, TURKEY",
      "lat": 41.015137,
      "lon": 28.979530,
      "depth": 10.0,
      "magnitude": 6.8,
      "magnitude_type": "mw",
      "time": "2024-01-15T03:47:22.0Z",
      "lastupdate": "2024-01-15T03:47:55.0Z"
    }
  },
  "enriched_at": "2024-01-15T03:47:59.004Z",
  "population_data": {
    "total_population": 4100000
  },
  "usgs_nearby": {
    "count": 34,
    "max_magnitude": 6.2
  },
  "weather_baseline": {
    "temp_avg_30d": 4.2,
    "precipitation_30d": 62.1,
    "wind_speed_avg": 3.8
  },
  "ge_validation_passed": true
}
```

### 5.3 Fields added by enrichment

| Field | Source | Purpose |
|---|---|---|
| `enriched_at` | Flink | Timestamp of enrichment — measures pipeline latency |
| `population_data` | WorldPop | Population within 50km — feeds Gold population exposure score |
| `usgs_nearby.count` | USGS | Number of historical M4.0+ events nearby — feeds regional risk |
| `usgs_nearby.max_magnitude` | USGS | Largest historical event nearby — context for current magnitude |
| `weather_baseline` | Open-Meteo | 30-day weather context for response planning |
| `ge_validation_passed` | Great Expectations | True = record passed all quality checks |

---

## 6. Great Expectations Validation

Every enriched record is validated before being written to Silver. These checks run
inside the Flink enrichment container, synchronously per event.

| Check | Rule | On failure |
|---|---|---|
| Event ID | `unid` must not be null | Route to DLQ |
| Magnitude | Must be between 0 and 10 | Route to DLQ |
| Depth | Must be a positive number | Route to DLQ |
| Coordinates | Lat between -90/90, lon between -180/180 | Route to DLQ |
| Tsunami alert level | If present, must be `Green`, `Orange`, or `Red` | Route to DLQ |

A record that fails any check is **never written to Silver**. It is routed to the
Kafka Dead Letter Queue topic (`raw-seismic-events-dlq`) with a structured error
message. The Flink job continues processing the next event immediately — one bad
record never blocks the pipeline.

---

## 7. The Dead Letter Queue

The DLQ (`raw-seismic-events-dlq`) is a second Kafka topic that isolates failed
records without dropping them. A human can inspect the DLQ, fix the underlying issue
(a schema change in the EMSC feed, for example), and replay the corrected records
back through the enrichment pipeline.

**Without the DLQ**, the only options for a bad record are crashing the job or
dropping the record silently. Both are worse than parking it.

---

## 8. Deduplication and State Tracking via MongoDB

Flink does not process each event in isolation. Before calling any external API, it
checks the MongoDB operational store to determine whether this event has already been
fully or partially enriched.

**Why this matters:**

- Kafka can deliver the same message more than once if the consumer restarts between
  processing and committing the offset
- Without deduplication, a Flink restart would re-call WorldPop and USGS for every
  event in the last committed window — wasting API quota and producing duplicate
  Silver files

**How it works:**

MongoDB stores an enrichment state document for each event:

```json
{
  "_id": "20240115_0000123",
  "worldpop_fetched": true,
  "usgs_fetched": true,
  "overpass_fetched": false,
  "meteo_fetched": false,
  "received_at": "2024-01-15T03:47:56Z"
}
```

Before each API call, Flink checks the corresponding flag. If `worldpop_fetched` is
`true`, the WorldPop call is skipped and the cached result is reused. This makes
enrichment resumable from any intermediate state.

---

## 9. How Silver Is Written

### 9.1 `services/flink-enrichment/enrichment_job.py` (core logic)

```python
def write_to_silver(enriched: dict, unid: str) -> str:
    """Write the enriched event to S3 Silver and return the key."""
    date_path = datetime.utcnow().strftime("%Y/%m/%d")
    key       = f"silver/events/{date_path}/{unid}_enriched.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(enriched),
        ContentType="application/json"
    )
    return key

# ── main consumer loop ─────────────────────────────────────
for message in consumer:
    event = message.value
    unid  = event.get("data", {}).get("properties", {}).get("unid", "unknown")

    if not validate(event):
        print(f"Validation failed for {unid} — routing to DLQ")
        dlq_producer.send("raw-seismic-events-dlq", event)
        continue

    try:
        enriched = enrich_event(event)   # calls APIs, checks MongoDB state
        key      = write_to_silver(enriched, unid)
        print(f"Written to S3 Silver: {key}")
    except Exception as e:
        print(f"Enrichment failed for {unid}: {e} — routing to DLQ")
        dlq_producer.send("raw-seismic-events-dlq", event)
```

### 9.2 Docker Compose service

```yaml
flink-enrichment:
  build: ./services/flink-enrichment
  restart: always
  depends_on:
    kafka:
      condition: service_healthy
    mongodb:
      condition: service_started
    minio-init:
      condition: service_completed_successfully
    flink-jobmanager:
      condition: service_healthy
  env_file: .env.dev
```

---

## 10. Who Reads Silver

| Consumer | When | Why |
|---|---|---|
| **Airflow S3KeySensor** | Every 15 minutes | Detects new Silver files to trigger Gold aggregation |
| **PySpark `deequ_silver_check.py`** | Before every Gold write | Dataset-level quality gate |
| **PySpark `impact_scoring.py`** | Hourly | Reads Silver events, computes impact scores, writes Gold |
| **PySpark `regional_aggregation.py`** | Hourly | Reads Silver for regional stats |
| **PySpark `profile_layer.py`** | Daily | Profiles Silver for the quality report |
| **7-day monitoring DAG** | Every 15 min | Writes monitoring poll results to `silver/monitoring/` |

---

## 11. Deequ Silver Quality Checks

Before PySpark is allowed to write anything to Gold, `deequ_silver_check.py` runs
a dataset-level validation over the Silver files for the current processing window.
These checks are distinct from the per-record Great Expectations checks — they
validate the dataset as a whole.

| Check | What is verified | On failure |
|---|---|---|
| No null event IDs | Every record has a `unid` | DAG fails, Gold write blocked |
| Valid magnitudes | All magnitudes between 0 and 10 | DAG fails, Gold write blocked |
| Positive depths | All depths > 0 | DAG fails, Gold write blocked |
| Valid coordinates | Lat and lon within geographic bounds | DAG fails, Gold write blocked |
| No duplicates | Distinct event IDs = total record count | DAG fails, Gold write blocked |
| GE flags clean | All `ge_validation_passed` are `true` | Warning logged, not blocking |

If any blocking check fails, the Airflow DAG exits with code 1. The Gold layer is
not touched. The Silver files remain intact for manual inspection and retry.

---

## 12. 7-Day Monitoring in Silver

For events that meet the monitoring threshold (M≥6.0, depth <150 km, coastal
proximity), the Airflow monitoring DAG writes ongoing poll results into a separate
Silver path:

```
s3://eq-monitor/silver/monitoring/{event_id}/gdacs/{timestamp}.json
s3://eq-monitor/silver/monitoring/{event_id}/noaa/{timestamp}.json
s3://eq-monitor/silver/monitoring/{event_id}/usgs_aftershocks/{timestamp}.json
s3://eq-monitor/silver/monitoring/{event_id}/openmeteo_forecast/{timestamp}.json
```

These monitoring results are enrichment data in the Silver sense — they augment the
original event with evolving real-world context. They feed into the Gold aggregation
on the next hourly run, appending aftershock counts, tsunami alert status, and
forecast updates to the event's Gold record.

---

## 13. Patterns Applied to Silver

### Quality-gated layer promotion
A record can only enter Silver if it passes Great Expectations validation. A dataset
can only promote to Gold if it passes the Deequ Silver check. Two distinct quality
gates at two different granularities — record-level and dataset-level.

### Enrichment state resilience
MongoDB tracks which enrichment steps have completed per event. If Flink crashes
mid-enrichment, it resumes from the last completed step rather than restarting from
scratch. This prevents redundant API calls and ensures Silver records are always
fully enriched, never half-enriched.

### Dead letter isolation
Invalid records are never silently dropped and never block the main pipeline. They
are parked in the DLQ for inspection and can be replayed once the root cause is fixed.

### Idempotent writes
`s3.put_object` with the same Silver path key overwrites the file with identical
content on retry. Safe to replay.

---

## 14. Technologies Per Stage

| Stage | Storage | Enrichment engine | Validation |
|---|---|---|---|
| feature / develop / test | MinIO (`minio/minio:latest`) | Flink container (`flink:1.18-scala_2.12-java11`) | Great Expectations in Flink |
| release (staging) | Amazon S3 | Flink on ECS Fargate | Great Expectations in Flink |
| main (production) | Amazon S3 | Flink on ECS Fargate | Great Expectations in Flink |

Flink is not replaced by an AWS managed service in this pipeline. The same container
image runs locally and in production on ECS Fargate. Python and the enrichment script
are installed on top of the base Flink image via `apt-get` in the Dockerfile.

---

## 15. Verifying Silver

### Check Silver files are being written

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc ls local/eq-monitor/silver/events/ --recursive | head -20
```

### Read a specific Silver file

```bash
mc cat local/eq-monitor/silver/events/2024/01/15/20240115_0000123_enriched.json \
  | python3 -m json.tool
```

### Check the enrichment container logs

```bash
docker compose logs flink-enrichment --tail=20
# Expected output:
# Enriching event: 20240115_0000123
# worldpop_fetched for 20240115_0000123
# usgs_fetched for 20240115_0000123
# Written to S3 Silver: silver/events/2024/01/15/20240115_0000123_enriched.json
```

### Inspect the DLQ

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic raw-seismic-events-dlq \
  --from-beginning \
  --max-messages 5
```

### Check MongoDB enrichment state

```bash
docker compose exec mongodb mongosh earthquake
db.raw_events.findOne({ _id: "20240115_0000123" })
# worldpop_fetched, usgs_fetched should be true
```

---

*Earthquake Monitoring Pipeline · Silver Layer · Enriched & Validated Events*

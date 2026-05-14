# Gold Layer — Aggregated, Scored & Business-Ready

> Earthquake Monitoring Pipeline · Medallion Architecture · Layer 3 of 3

---

## 1. What the Gold Layer Is

The Gold layer is the **final, business-ready output** of the pipeline. It takes
every enriched Silver event and transforms it into impact scores, regional
aggregations, and daily summaries that the data warehouse and the Streamlit dashboard
can query directly.

Gold answers the question: **how serious is this event, where does it rank, and what
is the regional picture?**

Where Bronze stores raw truth and Silver stores enriched context, Gold stores
**derived insight** — computed, validated, and loaded. It is the only layer the
dashboard ever reads. It is the most valuable layer from a business perspective and
the one that is always rebuildable from Silver, which is itself rebuildable from Bronze.

---

## 2. Purpose and Responsibilities

| Responsibility | Description |
|---|---|
| Impact scoring | Computes four scores per event (tsunami risk, building vulnerability, population exposure, infrastructure risk) and a composite |
| Regional aggregation | Groups events by region, computes daily averages, maxima, and totals |
| Daily summary | One global row per day with total events, max scores, regions affected |
| Quality gate | Deequ Gold check runs before any DWH load — blocks corrupt data |
| DWH loading | PySpark writes Gold files to PostgreSQL (local) or Snowflake (production) |
| Data profiling | Quality profiles for all three layers stored in Gold for Streamlit |
| Monitoring aggregation | 7-day monitoring results merged into Gold on each hourly run |

**What Gold does NOT do:**

- It does not re-fetch external APIs
- It does not store raw or enriched events
- It is not the source of truth — Silver and Bronze are

---

## 3. Where Gold Sits in the Pipeline

```
Silver Layer  (enriched, validated events)
      │
      ▼
Airflow S3KeySensor  (detects new Silver files)
      │
      ▼
deequ_silver_check.py  (PySpark quality gate)
      │
 ┌────┴──────┐
 │ pass      │ fail → DAG failed · alert raised · Silver intact
 ▼
gold_aggregation_pipeline  (Airflow DAG, hourly)
      │
 ┌────┴──────────────────────┐
 │                            │  run in parallel
 ▼                            ▼
impact_scoring.py      regional_aggregation.py
      │                            │
      └────────────┬───────────────┘
                   ▼
          deequ_gold_check.py  (dataset quality gate)
                   │
              ┌────┴──────┐
              │ pass      │ fail → DAG failed · alert raised
              ▼
          load_to_dwh.py
                   │
      ┌────────────┴────────────────────────┐
      │                                      │
      ▼                                      ▼
 ┌──────────────────────┐        ┌──────────────────────────┐
 │  MinIO / S3          │        │  PostgreSQL DWH           │
 │                      │        │  (local)                  │
 │  GOLD LAYER          │        │  Snowflake (production)   │
 │                      │        │                           │
 │  gold/               │        │  gold.earthquake_events   │
 │  impact_scores/      │        │  gold.regional_stats      │
 │  regional_stats/     │        │  gold.daily_summary       │
 │  daily_summary/      │        │  gold.quality_profiles    │
 │  quality_profiles/   │        │  monitoring.events        │
 └──────────────────────┘        └────────────┬─────────────┘
                                               │
                                               ▼
                                          Streamlit
                                          (Phase 3)
```

---

## 4. Impact Scoring

Every Silver event is scored on four dimensions. All scores are normalised to a 0–10
scale. The composite score is a weighted average.

### 4.1 Scoring formulas

| Score | Formula | Weight in composite |
|---|---|---|
| Tsunami risk | `min(10, max(0, (magnitude - 4) * 2.5 * (1 - depth/300)))` | 30% |
| Building vulnerability | MMI: `min(10, max(0, 1.5 * magnitude - 0.0133 * depth + 1.3))` | 25% |
| Population exposure | `min(10, max(0, population_50km / 1_000_000))` | 25% |
| Infrastructure risk | `min(10, max(0, magnitude * 0.8 + (1 - depth/300) * 2))` | 20% |
| **Composite score** | `tsunami*0.30 + building*0.25 + population*0.25 + infra*0.20` | — |

### 4.2 Score interpretation

| Score range | Meaning |
|---|---|
| 0.0 – 2.5 | Low impact — minor event, sparse population, deep or inland |
| 2.5 – 5.0 | Moderate impact — noticeable event, some population exposure |
| 5.0 – 7.5 | High impact — significant event, dense population, shallow depth |
| 7.5 – 10.0 | Extreme impact — major event, coastal, densely populated |

### 4.3 Tsunami risk in detail

Tsunami risk scores highest for events that are:
- Magnitude 6.0 or above
- Depth below 70 km (shallow)
- Near a coastline

The pre-check threshold for activating the 7-day monitoring window (M≥6.0,
depth <150 km, coastal) is derived from this scoring logic. Events below the
threshold will score 0 on tsunami risk regardless of magnitude.

---

## 5. Data Structures

### 5.1 Gold impact scores path

```
s3://eq-monitor/gold/impact_scores/YYYY/MM/DD/
```

### 5.2 Gold impact score record

```json
{
  "event_id": "20240115_0000123",
  "lat": 41.015137,
  "lon": 28.979530,
  "magnitude": 6.8,
  "depth": 10.0,
  "event_time": "2024-01-15T03:47:22.0Z",
  "region": "ISTANBUL, TURKEY",
  "enriched_at": "2024-01-15T03:47:59.004Z",
  "processing_date": "2024-01-15",
  "scored_at": "2024-01-15T04:00:12.000Z",
  "population_50km": 4100000.0,
  "usgs_nearby_count": 34.0,
  "usgs_max_magnitude": 6.2,
  "mmi_estimate": 9.06,
  "tsunami_risk": 8.93,
  "building_vulnerability": 9.06,
  "population_exposure": 4.1,
  "infrastructure_risk": 7.44,
  "composite_score": 7.52,
  "ge_validation_passed": true
}
```

### 5.3 Regional stats path and record

```
s3://eq-monitor/gold/regional_stats/YYYY/MM/DD/
```

```json
{
  "region": "ISTANBUL, TURKEY",
  "processing_date": "2024-01-15",
  "event_count": 3,
  "unique_events": 3,
  "avg_magnitude": 5.1,
  "max_magnitude": 6.8,
  "avg_composite_score": 5.23,
  "max_composite_score": 7.52,
  "avg_tsunami_risk": 4.10,
  "max_tsunami_risk": 8.93,
  "avg_population_exposure": 3.20,
  "total_population_exposed": 4100000.0,
  "aggregated_at": "2024-01-15T04:00:45.000Z"
}
```

### 5.4 Daily summary path and record

```
s3://eq-monitor/gold/daily_summary/YYYY/MM/DD/
```

```json
{
  "processing_date": "2024-01-15",
  "region": "GLOBAL",
  "total_events": 47,
  "max_magnitude_global": 6.8,
  "max_composite_score_global": 7.52,
  "regions_affected": 12,
  "total_population_exposed_global": 18400000.0,
  "aggregated_at": "2024-01-15T04:01:02.000Z"
}
```

---

## 6. Deequ Gold Quality Checks

Before `load_to_dwh.py` is allowed to write anything to the data warehouse,
`deequ_gold_check.py` validates the Gold dataset for the current processing window.

| Check | What is verified | On failure |
|---|---|---|
| Score column ranges | All five score columns strictly between 0 and 10 | DAG fails, DWH load blocked |
| No duplicate event IDs | Distinct event IDs = total record count | DAG fails, DWH load blocked |
| Composite consistency | `composite_score` matches weighted components within ±0.01 | DAG fails, DWH load blocked |

If any check fails, the DWH is not touched. The Gold files remain in MinIO. The
Airflow DAG is marked failed and an alert is raised. The Silver files that produced
the bad Gold output are intact and can be investigated.

---

## 7. The Four Airflow DAGs that Write Gold

### `gold_aggregation_pipeline` (hourly)

The core Gold-producing DAG. Dependency graph:

```
[impact_scoring.py, regional_aggregation.py]  ← parallel
              │
              ▼
     deequ_gold_check.py  ← quality gate
              │
              ▼
        load_to_dwh.py  ← DWH load
```

### `silver_enrichment_pipeline` (every 15 min)

Upstream gate. Uses `S3KeySensor` to detect new Silver files, then runs the Deequ
Silver check. If Silver passes, the Gold aggregation DAG is unblocked for its next
hourly run.

### `monitoring_window_pipeline` (every 15 min)

For M≥6.0 events, writes ongoing monitoring poll results to
`silver/monitoring/{event_id}/{source}/`. These are picked up and merged into Gold
on the next hourly aggregation run, enriching the event's Gold record with aftershock
counts and tsunami alert status over the 7-day window.

### `data_quality_report` (daily)

Profiles all three medallion layers and writes the results to
`gold/quality_profiles/`. This data powers the Streamlit data profiling page.

---

## 8. The Data Warehouse Schema

Gold data is loaded into two schemas in PostgreSQL (local) or Snowflake (production).

### `gold.earthquake_events`

One row per event per processing date. This is the primary table the dashboard
queries for maps and event listings.

```sql
CREATE TABLE gold.earthquake_events (
    event_id               VARCHAR(64)      NOT NULL,
    lat                    DOUBLE PRECISION,
    lon                    DOUBLE PRECISION,
    magnitude              DOUBLE PRECISION,
    depth                  DOUBLE PRECISION,
    event_time             TIMESTAMPTZ,
    region                 VARCHAR(255),
    processing_date        DATE,
    scored_at              TIMESTAMPTZ,
    tsunami_risk           DOUBLE PRECISION  CHECK (tsunami_risk BETWEEN 0 AND 10),
    building_vulnerability DOUBLE PRECISION  CHECK (building_vulnerability BETWEEN 0 AND 10),
    population_exposure    DOUBLE PRECISION  CHECK (population_exposure BETWEEN 0 AND 10),
    infrastructure_risk    DOUBLE PRECISION  CHECK (infrastructure_risk BETWEEN 0 AND 10),
    composite_score        DOUBLE PRECISION  CHECK (composite_score BETWEEN 0 AND 10),
    population_50km        DOUBLE PRECISION,
    usgs_nearby_count      DOUBLE PRECISION,
    usgs_max_magnitude     DOUBLE PRECISION,
    ge_validation_passed   BOOLEAN,
    PRIMARY KEY (event_id, processing_date)
);
```

### `gold.regional_stats`

One row per region per processing date. Used for regional comparison charts.

### `gold.daily_summary`

One row per day. Used for trend charts and headline statistics.

### `gold.quality_profiles`

One row per layer per date. Powers the Streamlit data profiling page.

### `monitoring.events`

All monitoring poll results (GDACS, NOAA, USGS aftershocks, Open-Meteo). Linked to
events in `gold.earthquake_events` by `event_id`.

---

## 9. PySpark Jobs

### 9.1 `impact_scoring.py`

Reads Silver JSON, extracts nested fields via `F.col("data.properties.unid")`, applies
scoring formulas as PySpark column expressions, adds `processing_date` and `scored_at`
metadata, writes to `gold/impact_scores/`.

```python
# Scoring example — tsunami risk
df = df.withColumn(
    "tsunami_risk",
    F.least(
        F.lit(10.0),
        F.greatest(
            F.lit(0.0),
            (F.col("magnitude") - F.lit(4.0)) * F.lit(2.5) *
            (F.lit(1.0) - F.col("depth") / F.lit(300.0))
        )
    ).cast(DoubleType())
)
```

### 9.2 `regional_aggregation.py`

Groups Gold impact scores by `region`, computes aggregations with `groupBy().agg()`,
adds a global summary row, writes to `gold/regional_stats/` and `gold/daily_summary/`.

### 9.3 `load_to_dwh.py`

Reads from three Gold S3 paths and writes to PostgreSQL via PySpark JDBC:

```python
df.write \
  .mode("append") \
  .jdbc(url=JDBC_URL, table="gold.earthquake_events", properties=JDBC_PROPS)
```

### 9.4 `profile_layer.py`

Profiles any single layer on request. Computes row count, distinct ID count,
duplicate count, and null rate per column. Writes a JSON profile to
`gold/quality_profiles/`.

---

## 10. Patterns Applied to Gold

### Quality-gated layer promotion
Gold can only be written if the Deequ Silver check passed. The DWH can only be loaded
if the Deequ Gold check passed. Two sequential quality gates ensure that corrupt or
incomplete data never reaches the dashboard.

### Fan-out within a DAG
`impact_scoring.py` and `regional_aggregation.py` run as parallel Spark jobs within
the same DAG run. Both read Silver independently. Only when both complete does the
Deequ Gold check gate fire.

### Idempotent batch jobs
Every PySpark job writes with `mode("overwrite")` to its Gold S3 path. Retries after
transient failures overwrite the partial output cleanly. The DWH load uses
`mode("append")` but the DAG is scheduled once per processing window, preventing
double-loading.

### Separation of compute and storage
PySpark jobs are stateless — they read from MinIO and write to MinIO. The Spark
cluster can be restarted at any time without data loss. This mirrors the AWS Glue
pattern where compute and storage are fully decoupled.

### Progressive 7-day enrichment
Gold records for significant events are not static. Each hourly aggregation run
merges the latest monitoring results (aftershocks, tsunami alerts, weather) into
the Gold record, updating impact assessments as the situation evolves over 7 days.

---

## 11. Technologies Per Stage

| Stage | Gold storage | Batch engine | Data warehouse |
|---|---|---|---|
| feature / develop / test | MinIO (`minio/minio:latest`) | `apache/spark:4.0.2-python3` containers | PostgreSQL 15 (port 5439) |
| release (staging) | Amazon S3 | AWS Glue (2 DPU) | Snowflake (X-Small warehouse) |
| main (production) | Amazon S3 | AWS Glue (10 DPU) | Snowflake (Medium warehouse) |

The PySpark scripts are identical in all environments. The only differences are:
- `SPARK_MASTER` env variable — local Spark URL vs empty on AWS Glue
- JDBC connection string — PostgreSQL vs Snowflake connector
- S3 endpoint — MinIO locally vs AWS S3 endpoint in production

---

## 12. Verifying Gold

### Check Gold files in MinIO

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc ls local/eq-monitor/gold/ --recursive | head -20
```

### Read a Gold impact score file

```bash
mc cat local/eq-monitor/gold/impact_scores/2024/01/15/*.json \
  | python3 -m json.tool | head -40
```

### Query the data warehouse

```bash
docker compose exec postgres-dwh psql -U eq_user -d earthquake -c \
  "SELECT event_id, region, magnitude, composite_score
   FROM gold.earthquake_events
   ORDER BY composite_score DESC
   LIMIT 10;"
```

### Check regional stats

```bash
docker compose exec postgres-dwh psql -U eq_user -d earthquake -c \
  "SELECT region, event_count, max_magnitude, avg_composite_score
   FROM gold.regional_stats
   ORDER BY avg_composite_score DESC
   LIMIT 10;"
```

### Trigger a DAG manually

```bash
docker compose exec airflow-webserver \
  airflow dags trigger gold_aggregation_pipeline \
  --conf '{"date": "2024-01-15"}'
```

### Check Airflow task logs

In the Airflow UI (`localhost:8080`), navigate to:
`gold_aggregation_pipeline → compute_impact_scores → Logs`

Or from the CLI:

```bash
docker compose exec airflow-webserver \
  airflow tasks logs gold_aggregation_pipeline compute_impact_scores 2024-01-15
```

---

## 13. Gold in the Medallion Architecture

```
Bronze  ──  raw · immutable · permanent
               │
               │  Flink enriches
               ▼
Silver  ──  enriched · validated · resumable
               │
               │  PySpark scores + aggregates
               ▼
Gold    ──  scored · aggregated · quality-checked  ← this layer
               │
               │  load_to_dwh.py
               ▼
DWH     ──  query-optimized · Streamlit reads here
```

Gold is the layer that the whole pipeline exists to produce. Bronze preserves
the raw truth. Silver adds context. Gold turns context into insight.

---

*Earthquake Monitoring Pipeline · Gold Layer · Aggregated, Scored & Business-Ready*

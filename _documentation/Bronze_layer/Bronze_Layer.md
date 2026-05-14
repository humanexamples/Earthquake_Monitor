# Bronze Layer — Raw Canonical Archive

> Earthquake Monitoring Pipeline · Medallion Architecture · Layer 1 of 3

---

## 1. What the Bronze Layer Is

The Bronze layer is the **single source of truth** for the entire pipeline. It is the
permanent, immutable archive of every seismic event exactly as it was received from
the EMSC WebSocket — no transformations, no added fields, no deletions, ever.

Bronze answers exactly one question: **what did the source actually send, and when?**

Every other layer in the pipeline — Silver, Gold, the data warehouse — can be fully
rebuilt from Bronze at any time. If Flink produces corrupt Silver output, if a PySpark
job introduces wrong Gold aggregations, or if any downstream bug corrupts data, a
complete replay from Bronze restores everything to a clean state. This makes Bronze
not just a storage layer but a **resilience guarantee** for the entire pipeline.

---

## 2. Purpose and Responsibilities

| Responsibility | Description |
|---|---|
| Permanent archive | Every raw event stored forever, never modified, never deleted |
| Replay source | Full pipeline can be rebuilt from Bronze at any time |
| Audit trail | What was received, from whom, and when — immutable record |
| Decoupled ingestion | Written by an independent Kafka consumer, not the listener itself |
| Downstream trigger | Airflow S3KeySensor watches Bronze for new files to trigger Phase 2 |

**What Bronze does NOT do:**

- It does not validate, clean, or transform data
- It does not compute anything
- It is never queried directly by the dashboard
- It is not a backup — it is the primary archive

---

## 3. Where Bronze Sits in the Pipeline

```
EMSC WebSocket
      │
      ▼
emsc-listener  (always-on container)
      │
      ▼
Apache Kafka  (topic: raw-seismic-events)
      │
      ├──────────────────────────────────┐
      │  consumer group: s3-bronze-writer │
      │                                  │
      ▼                                  ▼
  MongoDB                          ┌─────────────┐
  (operational store)              │ MinIO / S3  │
                                   │             │
                                   │  BRONZE     │ ← this layer
                                   │  LAYER      │
                                   │             │
                                   │ bronze/     │
                                   │ events/     │
                                   │ YYYY/MM/DD/ │
                                   │ {unid}.json │
                                   └──────┬──────┘
                                          │
                    ┌─────────────────────┼──────────────────────┐
                    │                     │                       │
                    ▼                     ▼                       ▼
             Flink reads           Airflow S3KeySensor     PySpark bulk
             for replay            detects new files       historical load
             (if needed)           triggers Phase 2        (USGS 10yr catalog)
```

---

## 4. Data Structure

### 4.1 Storage path convention

```
s3://eq-monitor/bronze/events/YYYY/MM/DD/{unid}.json
```

Example:
```
s3://eq-monitor/bronze/events/2024/01/15/20240115_0000123.json
```

The path is date-partitioned by ingestion date (UTC). The filename is the event's
unique ID (`unid`) as assigned by EMSC. This makes every file directly addressable
and human-readable without any query engine.

### 4.2 File content — raw EMSC payload

The file contains the raw WebSocket message exactly as received. No fields are added,
removed, or modified. This is the only guarantee Bronze makes:

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
  }
}
```

### 4.3 Key fields

| Field | Type | Description |
|---|---|---|
| `action` | string | Always `"create"` for new events |
| `data.properties.unid` | string | Unique event ID — used as filename and primary key |
| `data.properties.lat` | float | Epicenter latitude |
| `data.properties.lon` | float | Epicenter longitude |
| `data.properties.depth` | float | Depth in kilometres |
| `data.properties.magnitude` | float | Magnitude value |
| `data.properties.magnitude_type` | string | Scale used (ml, mb, mw, etc.) |
| `data.properties.time` | ISO 8601 | Event detection time (UTC) |
| `data.properties.flynn_region` | string | Named geographic region |

---

## 5. How Bronze Is Written

A dedicated Kafka consumer (`s3-bronze-writer`) reads from the
`raw-seismic-events` topic using its own consumer group. It writes each event to
MinIO as a JSON file and commits the Kafka offset only after the write succeeds.

This means:
- If MinIO is temporarily unavailable, the consumer falls behind but no events are
  lost — they queue in Kafka until MinIO recovers
- If the consumer crashes mid-write, it restarts from the last committed offset and
  retries the write — this is safe because `boto3 put_object` is idempotent for the
  same key
- The listener never knows whether the Bronze write succeeded — it is fully decoupled

### 5.1 `services/kafka-consumers/s3_bronze_writer.py`

```python
import json
import os
from datetime import datetime
import boto3
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

consumer = KafkaConsumer(
    "raw-seismic-events",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="s3-bronze-writer"
)

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY")
)

BUCKET = os.getenv("S3_BUCKET", "eq-monitor")

for message in consumer:
    event     = message.value
    unid      = event.get("data", {}).get("properties", {}).get("unid", "unknown")
    date_path = datetime.utcnow().strftime("%Y/%m/%d")
    key       = f"bronze/events/{date_path}/{unid}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(event),
        ContentType="application/json"
    )
    print(f"S3 Bronze: {key}")
```

### 5.2 Docker Compose service

```yaml
s3-bronze-writer:
  build: ./services/kafka-consumers
  restart: always
  command: python s3_bronze_writer.py
  depends_on:
    kafka:
      condition: service_healthy
    minio-init:
      condition: service_completed_successfully
  env_file: .env.dev
```

> **Why `minio-init` dependency?** The `eq-monitor` bucket must exist before the
> first write. The `minio-init` service creates it on startup using the MinIO client.
> Without this dependency, the first write attempt would fail with `NoSuchBucket`.

---

## 6. Who Reads Bronze

| Consumer | When | Why |
|---|---|---|
| **Flink** (enrichment) | Reads from Kafka, uses Bronze as fallback replay source | If enrichment needs to be rerun, Bronze provides the raw input |
| **Airflow S3KeySensor** | Every 15 minutes | Detects new Bronze files to trigger the Silver enrichment DAG |
| **PySpark** (historical load) | Once at project start | Bulk-loads the USGS 10-year earthquake catalog from Bronze |
| **Airflow data quality DAG** | Daily | Profiles the Bronze layer for row counts and null rates |

**Streamlit never reads Bronze.** The dashboard only queries the Gold layer and the
data warehouse. Reading raw Bronze directly from a UI would couple presentation to
storage internals and break if the Bronze schema changes.

---

## 7. Patterns Applied to Bronze

### Immutability
Once written, a Bronze file is never overwritten or deleted. This is enforced by IAM
policy in production (S3 write-only, no delete permission on the `bronze/` prefix)
and by convention in local development. Immutability is what makes replay reliable —
if Bronze could be modified, replaying from it would not guarantee the same result.

### Idempotent writes
`boto3 put_object` with the same key overwrites the existing file with identical
content. If the consumer retries a write after a crash, the result is exactly the
same file. No duplicates, no corruption.

### Decoupled fan-out
Bronze is written by an independent Kafka consumer group, not by the listener. The
listener publishes once to Kafka and never touches storage. This means a Bronze write
failure never affects the listener, and the listener never slows down waiting for a
storage write to complete.

### Date partitioning
Storing files under `YYYY/MM/DD/` allows Spark and Athena to use partition pruning
when reading Bronze — a query for a specific date range does not scan the entire
bucket, only the relevant partitions. This becomes important when Bronze accumulates
months or years of data.

---

## 8. Bronze in the Medallion Architecture

```
Bronze  ──  raw · immutable · permanent · source of truth
   │
   │  Flink reads and enriches
   ▼
Silver  ──  enriched · validated · quality-assured
   │
   │  PySpark aggregates and scores
   ▼
Gold    ──  aggregated · scored · business-ready
   │
   │  Loaded into DWH
   ▼
DWH     ──  query-optimized · Streamlit reads here
```

Bronze is the foundation. If Silver is corrupted, rebuild from Bronze. If Gold is
wrong, rebuild from Silver (which was built from Bronze). The chain is only as strong
as its first link — which is why Bronze is the only layer that can never be deleted.

---

## 9. Technologies Per Stage

| Stage | Storage | Notes |
|---|---|---|
| feature / develop / test | MinIO (`minio/minio:latest`) | Local S3 equivalent, web console at `localhost:9001` |
| release (staging) | Amazon S3 (`eq-monitor-staging`) | `S3_ENDPOINT_URL` left empty, `boto3` uses AWS endpoint automatically |
| main (production) | Amazon S3 (`eq-monitor-prod`) | S3 lifecycle policies for cost management on old partitions |

The `s3_bronze_writer.py` code is identical in all environments. Only
`S3_ENDPOINT_URL` in `.env.*` changes — locally `http://minio:9000`, empty on AWS.

---

## 10. Verifying Bronze

### 10.1 Start the Containers

```bash
docker compose --env-file .env-dev up --build -d

# Check the container status
docker compose  ps --all --format "table {{.Name}}\t{{.Status}}"
```

### 10.1 Check Container Logs

```bash
docker compose logs emsc-listener --tail=20

# Expected something like this:
# emsc-listener  | Connected to EMSC WebSocket
# emsc-listener  | Sent: 20260429_0000348 ..
```

```bash
docker compose logs mongodb --tail=20
```

```bash
docker compose logs s3-bronze-writer --tail=20
# Expected something like this:
# s3-bronze-writer  | S3 Bronze writer started — bucket: eq-monitor
# s3-bronze-writer  | Bronze: s3://eq-monitor/bronze/events/2026/04/29/20260429_0000348.json ..
```

Download the MinIO client from [here](https://dl.min.io/client/mc/release/windows-amd64/mc.exe) and register the file in system environments.

Then run:

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
```

Now you can request all the files in MinIO:

```bash
mc ls local/eq-monitor/bronze/events/ --recursive | head -20

# Expected something like this:
# [2026-04-30 00:01:02 CEST]   465B STANDARD 2026/04/29/20260404_0000625.json
# [2026-04-29 23:56:02 CEST]   465B STANDARD 2026/04/29/20260429_0000348.json ..
```


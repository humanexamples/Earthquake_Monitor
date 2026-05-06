# Integration Test Guide — Earthquake Monitoring Pipeline

> Bronze + Silver + Gold · Docker Compose · pytest

---

## Overview

The integration tests verify the full data pipeline end-to-end inside Docker,
using the same network and service names as the production stack.
A single synthetic M 6.8 earthquake event is injected into Kafka at the start
of each test run and then tracked as it flows through every layer.

```
Kafka ──► MongoDB (raw_events)       TestBronzeLayer
     ──► MinIO  bronze/events/       TestBronzeLayer
     ──► Flink enrichment
              └──► MinIO silver/events/   TestSilverLayer
                   (ge_validation_passed)

Airflow webserver + scheduler        TestAirflow
Spark master + worker                TestSpark
PostgreSQL DWH (gold.* schema)       TestPostgresDWH
Streamlit dashboard                  TestStreamlit
```

**17 tests · ~17 s typical runtime · no host Python required**

---

## Prerequisites

| Requirement | How to check |
|---|---|
| Docker Desktop running | `docker info` |
| Full stack up and healthy | `docker compose --env-file .env.dev ps` |
| Test-runner image built | `docker images \| grep test-runner` |

Start the stack if it is not already running:

```bash
docker compose --env-file .env up --build -d
```

Wait until all containers show **Up (healthy)** or **Up** before running tests:

```bash
docker compose --env-file .env ps --format "table {{.Name}}\t{{.Status}}"
```

---

## Running the Tests

All test commands must be run from the project root directory:

```bash
cd bronze-silver-gold-layer
```

### Run the full test suite

```bash
docker compose --env-file .env --profile test run --rm test-runner
```

This is the standard command. It starts the `test-runner` container, mounts
`./tests/`, runs `pytest tests/ -v`, then removes the container on exit.

### Run a single test file

```bash
# Bronze + Silver layer only
docker compose --env-file .env --profile test run --rm test-runner \
  pytest tests/test_bronze_silver.py -v

# Gold infrastructure only
docker compose --env-file .env --profile test run --rm test-runner \
  pytest tests/test_gold_infra.py -v
```

### Run a single test class or test

```bash
# One class
docker compose --env-file .env --profile test run --rm test-runner \
  pytest tests/test_gold_infra.py::TestPostgresDWH -v

# One specific test
docker compose --env-file .env --profile test run --rm test-runner \
  pytest tests/test_gold_infra.py::TestSpark::test_worker_registered -v
```

### Stop on first failure

```bash
docker compose --env-file .env --profile test run --rm test-runner \
  pytest tests/ -v -x
```

### Show print output inside tests

```bash
docker compose --env-file .env --profile test run --rm test-runner \
  pytest tests/ -v -s
```

> **Note:** The `test-runner` service uses `profiles: ["test"]`, so it is
> never started by a plain `docker compose up`. It only runs on explicit
> `docker compose --profile test run` invocations.

---

## Reading the Test Output

### Successful run (all 17 pass)

```
============================= test session starts ==============================
collecting ... collected 17 items

tests/test_bronze_silver.py::TestKafkaProduce::test_event_reaches_kafka PASSED [  5%]
tests/test_bronze_silver.py::TestBronzeLayer::test_event_stored_in_mongodb PASSED [ 11%]
tests/test_bronze_silver.py::TestBronzeLayer::test_event_written_to_bronze_s3 PASSED [ 17%]
tests/test_bronze_silver.py::TestBronzeLayer::test_mongodb_ttl_index_exists PASSED [ 23%]
tests/test_bronze_silver.py::TestSilverLayer::test_enriched_event_in_silver_s3 PASSED [ 29%]
tests/test_bronze_silver.py::TestSilverLayer::test_silver_event_has_required_fields PASSED [ 35%]
tests/test_bronze_silver.py::TestSilverLayer::test_dead_letter_queue_empty_for_valid_event PASSED [ 41%]
tests/test_gold_infra.py::TestAirflow::test_webserver_healthy PASSED     [ 47%]
tests/test_gold_infra.py::TestAirflow::test_all_dags_loaded PASSED       [ 52%]
tests/test_gold_infra.py::TestAirflow::test_dags_are_not_paused PASSED   [ 58%]
tests/test_gold_infra.py::TestSpark::test_master_ui_reachable PASSED     [ 64%]
tests/test_gold_infra.py::TestSpark::test_worker_registered PASSED       [ 70%]
tests/test_gold_infra.py::TestPostgresDWH::test_all_schemas_exist PASSED [ 76%]
tests/test_gold_infra.py::TestPostgresDWH::test_all_tables_exist PASSED  [ 82%]
tests/test_gold_infra.py::TestPostgresDWH::test_gold_earthquake_events_columns PASSED [ 88%]
tests/test_gold_infra.py::TestPostgresDWH::test_eq_user_can_write PASSED [ 94%]
tests/test_gold_infra.py::TestStreamlit::test_dashboard_reachable PASSED [100%]

============================= 17 passed in 16.99s ==============================
```

The summary line at the bottom is the pass/fail verdict. Typical runtime is
**15–20 s** on a warm stack (Silver enrichment via Flink takes the most time,
up to 75 s if the external APIs are slow).

### Reading the status markers

| Marker | Meaning |
|---|---|
| `PASSED` | Test assertion succeeded |
| `FAILED` | Assertion failed — a `FAILURES` section follows |
| `ERROR` | The test itself threw an uncaught exception (not an assertion) |
| `SKIPPED` | Test was skipped (none currently configured) |
| `[  5%]` | Progress through the collected test set |

### What a failure looks like

```
=================================== FAILURES ===================================
________________ TestSilverLayer::test_enriched_event_in_silver_s3 _____________

tests/test_bronze_silver.py:102: in test_enriched_event_in_silver_s3
    pytest.fail(
E   AssertionError: Silver enriched file not found after 75 s:
E     s3://eq-monitor/silver/events/2026/05/06/INTTEST_20260506_093000_enriched.json
E     Check: docker compose logs flink-enrichment --tail=30

=========================== short test summary info ============================
FAILED tests/test_bronze_silver.py::TestSilverLayer::test_enriched_event_in_silver_s3
======================== 1 failed, 16 passed in 89.4s ==========================
```

The failure block shows:
- **Which test** failed (class + function name)
- **Where** in the file (line number)
- **What went wrong** (the assertion message)
- **What to check** (the log command printed inside the test)

---

## Test Reference

### test_bronze_silver.py

| Class | Test | What it checks | Timeout |
|---|---|---|---|
| `TestKafkaProduce` | `test_event_reaches_kafka` | Kafka broker acknowledges the synthetic event; topic and offset are valid | instant |
| `TestBronzeLayer` | `test_event_stored_in_mongodb` | `mongodb-writer` persists the raw event to `earthquake.raw_events` | 15 s |
| `TestBronzeLayer` | `test_event_written_to_bronze_s3` | `s3-bronze-writer` writes `bronze/events/YYYY/MM/DD/{id}.json` to MinIO | 15 s |
| `TestBronzeLayer` | `test_mongodb_ttl_index_exists` | A TTL index on `received_at` is present (30-day retention policy) | instant |
| `TestSilverLayer` | `test_enriched_event_in_silver_s3` | `flink-enrichment` writes `silver/events/YYYY/MM/DD/{id}_enriched.json` with `ge_validation_passed: true` | 75 s |
| `TestSilverLayer` | `test_silver_event_has_required_fields` | Enriched file contains `usgs_nearby`, `population_data`, `infrastructure`, `weather_baseline`, `enriched_at` | 75 s |
| `TestSilverLayer` | `test_dead_letter_queue_empty_for_valid_event` | Logical check: Silver file existed → event was not routed to DLQ | instant |

### test_gold_infra.py

| Class | Test | What it checks | Timeout |
|---|---|---|---|
| `TestAirflow` | `test_webserver_healthy` | `metadatabase.status == healthy` AND `scheduler.status == healthy` from `/health` | 90 s |
| `TestAirflow` | `test_all_dags_loaded` | All 4 DAGs appear in `GET /api/v1/dags` | instant |
| `TestAirflow` | `test_dags_are_not_paused` | All 4 DAGs are active; auto-unpauses any that are paused via `PATCH /api/v1/dags/{id}` | instant |
| `TestSpark` | `test_master_ui_reachable` | Spark master web UI returns HTTP 200 | 30 s |
| `TestSpark` | `test_worker_registered` | `aliveworkers >= 1` in Spark master JSON API | 45 s |
| `TestPostgresDWH` | `test_all_schemas_exist` | Both `gold` and `monitoring` schemas exist | instant |
| `TestPostgresDWH` | `test_all_tables_exist` | All 5 tables exist: `gold.earthquake_events`, `gold.regional_stats`, `gold.daily_summary`, `gold.quality_profiles`, `monitoring.events` | instant |
| `TestPostgresDWH` | `test_gold_earthquake_events_columns` | Key impact score columns present: `composite_score`, `tsunami_risk`, `population_exposure`, `building_vulnerability`, `infrastructure_risk` | instant |
| `TestPostgresDWH` | `test_eq_user_can_write` | `eq_user` has `INSERT` privilege on `gold.earthquake_events` | instant |
| `TestStreamlit` | `test_dashboard_reachable` | Streamlit returns HTTP 200 on port 8501 | 30 s |

---

## Diagnosing Failures

Each failing test prints the exact log command to run. Below is the full
reference for each service.

### Bronze layer failures

**`test_event_reaches_kafka` fails**
```bash
# Is Kafka healthy?
docker compose ps kafka
docker compose logs kafka --tail=20

# Can the container reach Kafka?
docker compose --profile test run --rm test-runner \
  python -c "from kafka import KafkaProducer; KafkaProducer(bootstrap_servers='kafka:9092'); print('OK')"
```

**`test_event_stored_in_mongodb` fails**
```bash
docker compose logs mongodb-writer --tail=30
# Look for: "ServerSelectionTimeoutError" → networking issue
# Look for: "duplicate key" → test ID collision (harmless, re-run)

# Check MongoDB directly
docker compose exec mongodb mongosh earthquake \
  --eval "db.raw_events.find().sort({received_at:-1}).limit(3).pretty()"
```

**`test_event_written_to_bronze_s3` fails**
```bash
docker compose logs s3-bronze-writer --tail=30
# Look for: "InvalidAccessKeyId" → MinIO credentials mismatch
# Look for: "EndpointConnectionError" → networking issue

# List bronze files directly
mc ls local/eq-monitor/bronze/events/ --recursive | tail -5
```

### Silver layer failures

**`test_enriched_event_in_silver_s3` or `test_silver_event_has_required_fields` fail**
```bash
docker compose logs flink-enrichment --tail=40
```

| Log pattern | Cause | Fix |
|---|---|---|
| `VALIDATION FAILED (magnitude is null)` | Real EMSC events use `mag` field; `validators.py` did not pick it up | Check `validators.py` handles both `mag` and `magnitude` |
| `ENRICHMENT ERROR (InvalidAccessKeyId)` | MinIO service account missing | Run `mc admin user add ...` |
| `ENRICHMENT ERROR (Name or service not known)` | Network isolation — flink-enrichment can't reach minio/mongodb | Add `kafka-network` to the missing service in `docker-compose.yml` |
| External API calls timing out | Open-Meteo / USGS / Overpass rate-limited | Transient; re-run after a minute |

```bash
# Confirm Silver file exists in MinIO
mc ls local/eq-monitor/silver/events/ --recursive | tail -5

# Inspect an enriched file
mc cat local/eq-monitor/silver/events/YYYY/MM/DD/<id>_enriched.json \
  | python3 -m json.tool | head -40
```

### Gold infrastructure failures

**`test_webserver_healthy` fails**
```bash
docker compose logs airflow-webserver --tail=30
# Look for: "password authentication failed" → DB password mismatch
# Look for: "Name or service not known" → postgres-airflow not on kafka-network
```

**`test_all_dags_loaded` fails**
```bash
docker compose logs airflow-scheduler --tail=30
# Look for Python syntax errors or import errors in DAG files

# List DAGs from inside the container
docker compose exec airflow-scheduler airflow dags list
```

**`test_dags_are_not_paused` fails**

This test auto-unpauses DAGs via the REST API. If it still fails, the Airflow
API is not reachable. Check that `.env.dev` contains:
```
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
```
Then recreate the Airflow containers:
```bash
docker compose --env-file .env up -d --no-deps airflow-webserver airflow-scheduler
```

**`test_worker_registered` fails**
```bash
docker compose logs spark-worker --tail=20
# Look for: connection refused → spark-master not reachable
docker compose restart spark-worker
```

**`test_all_tables_exist` or `test_all_schemas_exist` fail**
```bash
docker compose exec postgres-dwh psql -U eq_user -d earthquake -c "\dt gold.*"
docker compose exec postgres-dwh psql -U eq_user -d earthquake -c "\dt monitoring.*"

# If eq_user does not exist, re-run the DB setup:
docker compose exec postgres-dwh psql -U your_dwh_user -d earthquake -c \
  "CREATE USER eq_user WITH PASSWORD 'eq_password_change_me'; \
   GRANT ALL ON SCHEMA gold TO eq_user; \
   GRANT ALL ON ALL TABLES IN SCHEMA gold TO eq_user;"
```

**`test_dashboard_reachable` fails**
```bash
docker compose logs streamlit --tail=20
# Look for: "connection to server ... failed" → postgres-dwh not on kafka-network
```

---

## Common Failure Patterns

### Stack was just started — Silver tests time out

Flink enrichment calls four external APIs (USGS, WorldPop, Overpass, Open-Meteo).
On the first run they can take 20–60 s per event. Wait 2 minutes after bringing
the stack up before running tests.

### Tests pass on the first run but fail on re-runs

The `produced_event` fixture generates a new unique event ID every session
(`INTTEST_YYYYMMDD_HHMMSS`). Each run is independent. If re-runs fail, the
issue is a service regression, not test interference.

### `docker compose run` recreates containers before running

Docker Compose compares the current container state against `docker-compose.yml`.
If you recently edited the compose file (e.g., added `networks`), it recreates
affected containers. Wait for them to become healthy again — the
`test-runner` container waits on health checks automatically.

### A test was `ERROR` instead of `FAILED`

An `ERROR` means the test itself crashed before reaching an assertion — usually
a missing fixture, an import error, or a client connection that raised an
uncaught exception. Run with `-s` to see the full traceback:

```bash
docker compose --env-file .env.dev --profile test run --rm test-runner \
  pytest tests/ -v -s
```

---

## Test File Layout

```
bronze-silver-gold-layer/
├── tests/
│   ├── conftest.py                  # session fixtures: test_id, s3, mongo_raw, produced_event
│   ├── test_bronze_silver.py        # 7 tests  — Kafka → MongoDB → Bronze → Silver
│   ├── test_gold_infra.py           # 10 tests — Airflow, Spark, DWH, Streamlit
│   ├── smoke_test_phase1_docker.py  # standalone script (legacy, not run by pytest)
│   └── smoke_test_phase2_docker.py  # standalone script (legacy, not run by pytest)
└── services/
    └── test-runner/
        ├── Dockerfile               # python:3.11-slim + requirements
        └── requirements.txt         # pytest, kafka-python, pymongo, boto3, psycopg2-binary, requests
```

> `smoke_test_phase*.py` are standalone scripts that predate the pytest suite.
> They are not collected by pytest (filename does not start with `test_`) and
> can still be run manually with `python smoke_test_phase1_docker.py`.

---

## Configuration

The test container inherits all credentials from `env_file: .env.dev`.
No hardcoded values — every connection parameter comes from the environment.

| Variable | Used by | Default in `.env.dev` |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | conftest — produce event | `kafka:9092` |
| `MONGODB_URI` | conftest — mongo fixture | `mongodb://mongodb:27017` |
| `S3_ENDPOINT_URL` | conftest — S3 fixture | `http://minio:9000` |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | conftest — S3 fixture | `minio_access_key` / `minio_secret_key_change_me` |
| `S3_BUCKET` | test_bronze_silver | `eq-monitor` |
| `DWH_HOST`, `DWH_PORT`, `DWH_DB` | test_gold_infra | `postgres-dwh` / `5432` / `earthquake` |
| `DWH_USER` / `DWH_PASSWORD` | test_gold_infra | `eq_user` / `eq_password_change_me` |
| `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD` | test_gold_infra | `admin` / `admin_password_change_me` |

---

*Earthquake Monitoring Pipeline · Integration Tests · Bronze + Silver + Gold*

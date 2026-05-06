"""
test_gold_infra.py — Gold layer infrastructure integration tests.

Services under test:
  Airflow webserver + scheduler  (health + DAG loading)
  Spark master                   (UI reachability)
  PostgreSQL DWH                 (schema completeness)
  Streamlit dashboard            (HTTP reachability)

These tests verify infrastructure readiness, not data flow.
They do not depend on the `produced_event` fixture.
"""
import os
import time
import pytest
import requests
import psycopg2

# ── Connection parameters from env (set via env_file: .env.dev) ─────────────
AIRFLOW_URL  = f"http://airflow-webserver:{os.getenv('AIRFLOW_WEBSERVER_PORT', '8080')}"
AIRFLOW_USER = os.getenv("AIRFLOW_ADMIN_USERNAME", "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin_password_change_me")
SPARK_URL    = "http://spark-master:8080"
STREAMLIT_URL = "http://streamlit:8501"
MONITOR_URL   = "http://pipeline-monitor:8502"
DWH_HOST     = os.getenv("DWH_HOST", "postgres-dwh")
DWH_PORT     = int(os.getenv("DWH_PORT", "5432"))
DWH_DB       = os.getenv("DWH_DB", "earthquake")
DWH_USER     = os.getenv("DWH_USER", "eq_user")
DWH_PASSWORD = os.getenv("DWH_PASSWORD", "eq_password_change_me")

EXPECTED_DAGS = [
    "silver_enrichment_pipeline",
    "gold_aggregation_pipeline",
    "monitoring_window_pipeline",
    "data_quality_report",
]

EXPECTED_TABLES = [
    ("gold",       "earthquake_events"),
    ("gold",       "regional_stats"),
    ("gold",       "daily_summary"),
    ("gold",       "quality_profiles"),
    ("monitoring", "events"),
]


class TestAirflow:
    @pytest.mark.timeout(120)
    def test_webserver_healthy(self):
        """
        Airflow webserver must report metadatabase + scheduler as healthy.
        Airflow 2.9 health response has no top-level 'status' — check nested.
        """
        deadline = time.time() + 90
        while time.time() < deadline:
            try:
                r = requests.get(f"{AIRFLOW_URL}/health", timeout=5)
                if r.status_code == 200:
                    body = r.json()
                    db_ok    = body.get("metadatabase", {}).get("status") == "healthy"
                    sched_ok = body.get("scheduler",    {}).get("status") == "healthy"
                    if db_ok and sched_ok:
                        return
            except Exception:
                pass
            time.sleep(5)
        pytest.fail(
            "Airflow not healthy after 90 s.\n"
            "  Check: docker compose logs airflow-webserver --tail=20"
        )

    def test_all_dags_loaded(self):
        """All four pipeline DAGs must appear in the Airflow DAG registry."""
        r = requests.get(
            f"{AIRFLOW_URL}/api/v1/dags",
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=15,
        )
        assert r.status_code == 200, (
            f"Airflow DAG list API returned {r.status_code}.\n"
            f"  Ensure AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth is set."
        )
        loaded = {d["dag_id"] for d in r.json().get("dags", [])}
        missing = [dag for dag in EXPECTED_DAGS if dag not in loaded]
        assert not missing, (
            f"DAGs not loaded: {missing}\n"
            "  Check: docker compose logs airflow-scheduler --tail=30"
        )

    def test_dags_are_not_paused(self):
        """
        All four pipeline DAGs must be active (not paused).
        If any are paused, unpause them via the REST API first, then verify.
        Airflow creates DAGs paused by default; unpausing is idempotent.
        """
        r = requests.get(
            f"{AIRFLOW_URL}/api/v1/dags",
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=15,
        )
        assert r.status_code == 200
        dag_states = {d["dag_id"]: d.get("is_paused") for d in r.json().get("dags", [])}
        paused = [dag for dag in EXPECTED_DAGS if dag_states.get(dag) is True]
        for dag_id in paused:
            requests.patch(
                f"{AIRFLOW_URL}/api/v1/dags/{dag_id}",
                auth=(AIRFLOW_USER, AIRFLOW_PASS),
                json={"is_paused": False},
                timeout=10,
            )
        if paused:
            # re-fetch to confirm
            r2 = requests.get(
                f"{AIRFLOW_URL}/api/v1/dags",
                auth=(AIRFLOW_USER, AIRFLOW_PASS),
                timeout=15,
            )
            dag_states = {d["dag_id"]: d.get("is_paused") for d in r2.json().get("dags", [])}
        still_paused = [dag for dag in EXPECTED_DAGS if dag_states.get(dag) is True]
        assert not still_paused, f"DAGs still paused after unpause attempt: {still_paused}"


class TestSpark:
    @pytest.mark.timeout(60)
    def test_master_ui_reachable(self):
        """Spark master web UI must respond with HTTP 200."""
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                if requests.get(SPARK_URL, timeout=5).status_code == 200:
                    return
            except Exception:
                time.sleep(2)
        pytest.fail(
            "Spark master UI not reachable after 30 s.\n"
            "  Check: docker compose logs spark-master --tail=20"
        )

    @pytest.mark.timeout(60)
    def test_worker_registered(self):
        """At least one Spark worker must be registered with the master."""
        deadline = time.time() + 45
        while time.time() < deadline:
            try:
                r = requests.get(f"{SPARK_URL}/json/", timeout=5)
                if r.status_code == 200:
                    alive_workers = r.json().get("aliveworkers", 0)
                    if alive_workers >= 1:
                        return
            except Exception:
                pass
            time.sleep(3)
        pytest.fail(
            "No alive Spark workers registered after 45 s.\n"
            "  Check: docker compose logs spark-worker --tail=20"
        )


class TestPostgresDWH:
    @pytest.fixture(scope="class")
    def dwh_conn(self):
        conn = psycopg2.connect(
            host=DWH_HOST, port=DWH_PORT,
            dbname=DWH_DB, user=DWH_USER, password=DWH_PASSWORD,
        )
        yield conn
        conn.close()

    def test_all_schemas_exist(self, dwh_conn):
        """gold and monitoring schemas must exist."""
        cur = dwh_conn.cursor()
        cur.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name IN ('gold', 'monitoring')"
        )
        found = {row[0] for row in cur.fetchall()}
        assert "gold"       in found, "Schema 'gold' is missing"
        assert "monitoring" in found, "Schema 'monitoring' is missing"

    def test_all_tables_exist(self, dwh_conn):
        """All five DWH tables (gold.* + monitoring.events) must exist."""
        cur = dwh_conn.cursor()
        missing = []
        for schema, table in EXPECTED_TABLES:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s)",
                (schema, table),
            )
            if not cur.fetchone()[0]:
                missing.append(f"{schema}.{table}")
        assert not missing, f"Missing DWH tables: {missing}"

    def test_gold_earthquake_events_columns(self, dwh_conn):
        """gold.earthquake_events must have the expected impact score columns."""
        cur = dwh_conn.cursor()
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'gold' AND table_name = 'earthquake_events'"
        )
        cols = {row[0] for row in cur.fetchall()}
        required = {
            "event_id", "magnitude", "composite_score",
            "tsunami_risk", "population_exposure",
            "building_vulnerability", "infrastructure_risk",
        }
        missing = required - cols
        assert not missing, f"Missing columns in gold.earthquake_events: {missing}"

    def test_eq_user_can_write(self, dwh_conn):
        """eq_user must have INSERT privilege on gold tables."""
        cur = dwh_conn.cursor()
        cur.execute(
            "SELECT has_table_privilege(%s, 'gold.earthquake_events', 'INSERT')",
            (DWH_USER,),
        )
        assert cur.fetchone()[0], f"User '{DWH_USER}' lacks INSERT on gold.earthquake_events"


STREAMLIT_PAGES = [
    "world_map",
    "event_table",
    "regional_stats",
    "monitoring",
    "data_quality",
]


class TestStreamlit:
    @pytest.mark.timeout(60)
    def test_dashboard_reachable(self):
        """Streamlit root URL must respond with HTTP 200."""
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                if requests.get(STREAMLIT_URL, timeout=5).status_code == 200:
                    return
            except Exception:
                time.sleep(2)
        pytest.fail(
            "Streamlit dashboard not reachable after 30 s.\n"
            "  Check: docker compose logs streamlit --tail=20"
        )

    @pytest.mark.timeout(30)
    @pytest.mark.parametrize("page", STREAMLIT_PAGES)
    def test_page_url_reachable(self, page):
        """Every named page URL must return HTTP 200 (st.navigation routing)."""
        url = f"{STREAMLIT_URL}/{page}"
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                r = requests.get(url, timeout=5)
                if r.status_code == 200:
                    return
                if r.status_code == 404:
                    pytest.fail(
                        f"Page /{page} returned 404 — st.navigation() not wired up.\n"
                        "  Check: services/streamlit/app.py uses st.navigation()."
                    )
            except Exception:
                pass
            time.sleep(2)
        pytest.fail(f"Page /{page} not reachable after 20 s.")


class TestMonitorApp:
    @pytest.mark.timeout(60)
    def test_monitor_reachable(self):
        """Pipeline monitor (port 8502) health endpoint must return HTTP 200."""
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                r = requests.get(f"{MONITOR_URL}/_stcore/health", timeout=5)
                if r.status_code == 200:
                    return
            except Exception:
                pass
            time.sleep(2)
        pytest.fail(
            "Pipeline monitor not reachable after 30 s.\n"
            "  Check: docker compose logs pipeline-monitor --tail=20"
        )

"""
Phase 2 smoke test — runs INSIDE Docker on kafka-network.
Uses internal service names.
"""
import time
import requests
import psycopg2

AIRFLOW_URL = "http://airflow-webserver:8080"
AIRFLOW_USER = "admin"
AIRFLOW_PASS = "admin_password_change_me"
SPARK_URL = "http://spark-master:8080"
STREAMLIT_URL = "http://streamlit:8501"
DWH_HOST = "postgres-dwh"
DWH_PORT = 5432
DWH_DB = "earthquake"
DWH_USER = "eq_user"
DWH_PASSWORD = "eq_password_change_me"


def test_airflow():
    # Airflow 2.9 health response has no top-level "status"; check nested components
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            r = requests.get(f"{AIRFLOW_URL}/health", timeout=5)
            if r.status_code == 200:
                body = r.json()
                db_ok = body.get("metadatabase", {}).get("status") == "healthy"
                sched_ok = body.get("scheduler", {}).get("status") == "healthy"
                if db_ok and sched_ok:
                    print("  Airflow: healthy (metadatabase + scheduler)")
                    return
        except Exception as e:
            print(f"  Airflow not ready yet: {e}")
        time.sleep(5)
    raise AssertionError("Airflow not healthy after 90s")


def test_airflow_dags():
    r = requests.get(
        f"{AIRFLOW_URL}/api/v1/dags",
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        timeout=10,
    )
    assert r.status_code == 200, f"DAG list returned {r.status_code}"
    loaded = [d["dag_id"] for d in r.json().get("dags", [])]
    expected = [
        "silver_enrichment_pipeline",
        "gold_aggregation_pipeline",
        "monitoring_window_pipeline",
        "data_quality_report",
    ]
    missing = [d for d in expected if d not in loaded]
    assert not missing, f"DAGs not loaded: {missing}"
    print(f"  Airflow: all 4 DAGs loaded")


def test_spark():
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            if requests.get(SPARK_URL, timeout=5).status_code == 200:
                print(f"  Spark: master UI healthy")
                return
        except Exception:
            time.sleep(2)
    raise AssertionError("Spark master not responding after 30s")


def test_dwh_schema():
    conn = psycopg2.connect(
        host=DWH_HOST, port=DWH_PORT, dbname=DWH_DB, user=DWH_USER, password=DWH_PASSWORD
    )
    cur = conn.cursor()
    expected = [
        ("gold", "earthquake_events"),
        ("gold", "regional_stats"),
        ("gold", "daily_summary"),
        ("gold", "quality_profiles"),
        ("monitoring", "events"),
    ]
    missing = []
    for schema, table in expected:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name=%s)",
            (schema, table),
        )
        if not cur.fetchone()[0]:
            missing.append(f"{schema}.{table}")
    conn.close()
    assert not missing, f"Missing tables: {missing}"
    print("  PostgreSQL DWH: all 5 tables exist")


def test_streamlit():
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            if requests.get(STREAMLIT_URL, timeout=5).status_code == 200:
                print(f"  Streamlit: dashboard reachable")
                return
        except Exception:
            time.sleep(2)
    raise AssertionError("Streamlit not responding after 30s")


if __name__ == "__main__":
    print("\nPhase 2 smoke tests (in-container)\n")
    test_airflow()
    test_airflow_dags()
    test_spark()
    test_dwh_schema()
    test_streamlit()
    print("\nAll Phase 2 smoke tests passed.")

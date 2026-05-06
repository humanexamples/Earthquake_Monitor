"""
Phase 2 smoke test — Gold layer (Airflow, Spark, DWH, Streamlit)
Checks that all infrastructure services are healthy and reachable.
"""
import time
import requests
import psycopg2

AIRFLOW_URL = "http://localhost:8080"
AIRFLOW_USER = "admin"
AIRFLOW_PASS = "admin_password_change_me"
SPARK_URL = "http://localhost:8082"
STREAMLIT_URL = "http://localhost:8501"
DWH_HOST = "localhost"
DWH_PORT = 5439
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
                    print(f"  Airflow: healthy (metadatabase + scheduler)")
                    return
        except Exception as e:
            print(f"  Airflow not ready: {e} — retrying...")
        time.sleep(5)
    raise AssertionError("Airflow not healthy after 90s")


def test_airflow_dags():
    r = requests.get(
        f"{AIRFLOW_URL}/api/v1/dags",
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        timeout=10,
    )
    assert r.status_code == 200, f"Airflow DAG list returned {r.status_code}: {r.text[:200]}"
    loaded = [d["dag_id"] for d in r.json().get("dags", [])]
    expected = [
        "silver_enrichment_pipeline",
        "gold_aggregation_pipeline",
        "monitoring_window_pipeline",
        "data_quality_report",
    ]
    missing = [dag for dag in expected if dag not in loaded]
    assert not missing, f"DAGs not loaded: {missing}"
    print(f"  Airflow: all 4 DAGs loaded ({', '.join(expected)})")


def test_spark():
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            if requests.get(SPARK_URL, timeout=5).status_code == 200:
                print(f"  Spark: master UI healthy at {SPARK_URL}")
                return
        except Exception:
            time.sleep(2)
    raise AssertionError("Spark master UI not responding after 30s")


def test_dwh_schema():
    conn = psycopg2.connect(
        host=DWH_HOST, port=DWH_PORT, dbname=DWH_DB, user=DWH_USER, password=DWH_PASSWORD
    )
    cur = conn.cursor()
    expected_tables = [
        ("gold", "earthquake_events"),
        ("gold", "regional_stats"),
        ("gold", "daily_summary"),
        ("gold", "quality_profiles"),
        ("monitoring", "events"),
    ]
    missing = []
    for schema, table in expected_tables:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name=%s)",
            (schema, table),
        )
        if not cur.fetchone()[0]:
            missing.append(f"{schema}.{table}")
    conn.close()
    assert not missing, f"Missing DWH tables: {missing}"
    print(f"  PostgreSQL DWH: all 5 tables exist")


def test_streamlit():
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            if requests.get(STREAMLIT_URL, timeout=5).status_code == 200:
                print(f"  Streamlit: dashboard reachable at {STREAMLIT_URL}")
                return
        except Exception:
            time.sleep(2)
    raise AssertionError("Streamlit not responding after 30s")


if __name__ == "__main__":
    print("\nPhase 2 smoke tests\n")
    test_airflow()
    test_airflow_dags()
    test_spark()
    test_dwh_schema()
    test_streamlit()
    print("\nAll Phase 2 smoke tests passed.")

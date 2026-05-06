"""
app.py — Pipeline Health Monitor
Real-time status of every Bronze / Silver / Gold pipeline component.
Runs on port 8502 as a separate service. Auto-refreshes every 30 s.

Layout
  🥉 Bronze  │ Kafka  │ MongoDB  │ MinIO bronze/
  🥈 Silver  │ Flink  │ MinIO silver/
  🥇 Gold    │ Spark  │ MinIO gold/  │ PostgreSQL DWH
             └ Airflow DAG run table
"""
import concurrent.futures
import os
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2
import pymongo
import requests
import streamlit as st
from kafka import KafkaConsumer, TopicPartition
from streamlit_autorefresh import st_autorefresh

# ── Connection parameters from .env ─────────────────────────────────────────
KAFKA   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC   = os.getenv("KAFKA_TOPIC", "raw-seismic-events")
MONGO   = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
S3_URL  = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_KEY  = os.getenv("S3_ACCESS_KEY", "minio_access_key")
S3_SEC  = os.getenv("S3_SECRET_KEY", "minio_secret_key_change_me")
BUCKET  = os.getenv("S3_BUCKET", "eq-monitor")
PG_HOST = os.getenv("DWH_HOST", "postgres-dwh")
PG_PORT = int(os.getenv("DWH_PORT", "5432"))
PG_DB   = os.getenv("DWH_DB", "earthquake")
PG_USER = os.getenv("DWH_USER", "eq_user")
PG_PASS = os.getenv("DWH_PASSWORD", "eq_password_change_me")
AF_URL  = "http://airflow-webserver:8080"
AF_AUTH = (
    os.getenv("AIRFLOW_ADMIN_USERNAME", "admin"),
    os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin_password_change_me"),
)
FL_URL  = os.getenv("FLINK_JOBMANAGER_URL", "http://flink-jobmanager:8081")
SP_URL  = "http://spark-master:8080"

DAGS = [
    "silver_enrichment_pipeline",
    "gold_aggregation_pipeline",
    "monitoring_window_pipeline",
    "data_quality_report",
]


# ── Utilities ────────────────────────────────────────────────────────────────

def _ago(ts) -> str:
    """Human-readable elapsed time since ts (aware or naive datetime, or None)."""
    if ts is None:
        return "—"
    if hasattr(ts, "tzinfo") and ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    s = max(0, int((datetime.now(timezone.utc) - ts).total_seconds()))
    if s < 60:    return f"{s}s ago"
    if s < 3600:  return f"{s // 60}m {s % 60}s ago"
    if s < 86400: return f"{s // 3600}h {(s % 3600) // 60}m ago"
    return f"{s // 86400}d ago"


def _dot(ok: bool) -> str:
    return "🟢" if ok else "🔴"


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_URL,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SEC,
    )


# ── Component checks — each returns {"ok": bool, ...} ───────────────────────

def check_kafka() -> dict:
    try:
        c = KafkaConsumer(
            bootstrap_servers=KAFKA,
            request_timeout_ms=5_000,
            api_version_auto_timeout_ms=5_000,
        )
        parts = c.partitions_for_topic(TOPIC) or set()
        total = 0
        for p in parts:
            tp = TopicPartition(TOPIC, p)
            c.assign([tp])
            c.seek_to_end(tp)
            total += c.position(tp)
        c.close()
        return {"ok": True, "messages": total, "partitions": len(parts)}
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}


def check_mongodb() -> dict:
    try:
        cli  = pymongo.MongoClient(MONGO, serverSelectionTimeoutMS=4_000)
        col  = cli["earthquake"]["raw_events"]
        cnt  = col.estimated_document_count()
        doc  = col.find_one(sort=[("received_at", -1)], projection={"received_at": 1})
        last = doc.get("received_at") if doc else None
        cli.close()
        return {"ok": True, "count": cnt, "last_ts": last}
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}


def check_s3(prefix: str) -> dict:
    """
    Check MinIO prefix.
    - today_count : files written today (fast — single-day prefix)
    - last_ts     : most recent LastModified across today + 2 previous days
    """
    try:
        s3  = _s3_client()
        s3.head_bucket(Bucket=BUCKET)          # connectivity check
        now = datetime.now(timezone.utc)
        today_count, latest = 0, None
        for delta in range(3):                 # today, yesterday, day before
            day  = (now - timedelta(days=delta)).strftime("%Y/%m/%d")
            resp = s3.list_objects_v2(
                Bucket=BUCKET, Prefix=f"{prefix}{day}/", MaxKeys=1_000
            )
            items = resp.get("Contents", [])
            if delta == 0:
                today_count = len(items)
            for obj in items:
                ts = obj["LastModified"]
                if latest is None or ts > latest:
                    latest = ts
            if latest:
                break
        return {"ok": True, "today_count": today_count, "last_ts": latest}
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}


def check_flink() -> dict:
    try:
        r  = requests.get(f"{FL_URL}/jobs/overview", timeout=5)
        r.raise_for_status()
        jobs    = r.json().get("jobs", [])
        running = sum(1 for j in jobs if j.get("state") == "RUNNING")
        r2  = requests.get(f"{FL_URL}/taskmanagers", timeout=5)
        tms = len(r2.json().get("taskmanagers", [])) if r2.ok else 0
        return {"ok": True, "running_jobs": running, "task_managers": tms}
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}


def check_airflow() -> dict:
    try:
        r = requests.get(f"{AF_URL}/health", timeout=5)
        r.raise_for_status()
        body     = r.json()
        db_ok    = body.get("metadatabase", {}).get("status") == "healthy"
        sched_ok = body.get("scheduler",    {}).get("status") == "healthy"
        dags = []
        for dag_id in DAGS:
            try:
                rr = requests.get(
                    f"{AF_URL}/api/v1/dags/{dag_id}/dagRuns",
                    params={"limit": 1, "order_by": "-start_date"},
                    auth=AF_AUTH, timeout=5,
                )
                runs  = rr.json().get("dag_runs", []) if rr.ok else []
                state = runs[0].get("state", "?") if runs else "never run"
                start = runs[0].get("start_date", "—") if runs else "—"
            except Exception:
                state, start = "?", "?"
            dags.append({"DAG": dag_id, "Last state": state, "Last run": start})
        return {
            "ok": db_ok and sched_ok,
            "metadatabase": db_ok,
            "scheduler":    sched_ok,
            "dags":         dags,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}


def check_spark() -> dict:
    try:
        r = requests.get(f"{SP_URL}/json/", timeout=5)
        r.raise_for_status()
        d = r.json()
        return {
            "ok":           True,
            "alive_workers": d.get("aliveworkers", 0),
            "cores":        d.get("cores", 0),
            "memory_gb":    round(d.get("memory", 0) / 1024, 1),
            "active_apps":  len(d.get("activeapps", [])),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}


def check_dwh() -> dict:
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASS, connect_timeout=4,
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), MAX(processing_date) FROM gold.earthquake_events")
        ev_cnt, last_date = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM monitoring.events")
        mon_cnt = cur.fetchone()[0]
        conn.close()
        return {
            "ok":              True,
            "events":          ev_cnt,
            "last_date":       str(last_date) if last_date else None,
            "monitoring_rows": mon_cnt,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}


# ── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Pipeline Monitor",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st_autorefresh(interval=30_000, key="pipeline_monitor_refresh")

# ── Header ───────────────────────────────────────────────────────────────────

col_title, col_ts, col_btn = st.columns([3, 5, 1])
with col_title:
    st.title("🔍 Pipeline Monitor")
with col_ts:
    st.caption(
        f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d  %H:%M:%S UTC')}"
        "  ·  auto-refresh every 30 s"
    )
with col_btn:
    if st.button("🔄 Refresh"):
        st.rerun()

st.divider()

# ── Run all checks in parallel ───────────────────────────────────────────────

with st.spinner("Polling pipeline components…"):
    with concurrent.futures.ThreadPoolExecutor(max_workers=9) as pool:
        futures = {
            "kafka":  pool.submit(check_kafka),
            "mongo":  pool.submit(check_mongodb),
            "bronze": pool.submit(check_s3, "bronze/events/"),
            "silver": pool.submit(check_s3, "silver/events/"),
            "gold_s3":pool.submit(check_s3, "gold/"),
            "flink":  pool.submit(check_flink),
            "airflow":pool.submit(check_airflow),
            "spark":  pool.submit(check_spark),
            "dwh":    pool.submit(check_dwh),
        }
        r = {k: f.result() for k, f in futures.items()}

# ── Bronze Layer ─────────────────────────────────────────────────────────────

st.header("🥉 Bronze Layer — Ingestion")
b1, b2, b3 = st.columns(3)

with b1:
    st.subheader(f"{_dot(r['kafka']['ok'])} Kafka")
    if r["kafka"]["ok"]:
        st.metric("Messages in topic", f"{r['kafka']['messages']:,}")
        st.caption(f"Topic `{TOPIC}` · {r['kafka']['partitions']} partition(s)")
    else:
        st.error(r["kafka"]["error"])

with b2:
    st.subheader(f"{_dot(r['mongo']['ok'])} MongoDB")
    if r["mongo"]["ok"]:
        st.metric("Raw events (total)", f"{r['mongo']['count']:,}")
        st.caption(f"Last received: **{_ago(r['mongo']['last_ts'])}**")
    else:
        st.error(r["mongo"]["error"])

with b3:
    st.subheader(f"{_dot(r['bronze']['ok'])} MinIO  bronze/")
    if r["bronze"]["ok"]:
        st.metric("Files written today", r["bronze"]["today_count"])
        st.caption(f"Last file: **{_ago(r['bronze']['last_ts'])}**")
    else:
        st.error(r["bronze"]["error"])

st.divider()

# ── Silver Layer ─────────────────────────────────────────────────────────────

st.header("🥈 Silver Layer — Enrichment")
s1, s2 = st.columns(2)

with s1:
    st.subheader(f"{_dot(r['flink']['ok'])} Flink Cluster")
    if r["flink"]["ok"]:
        st.metric("Running jobs", r["flink"]["running_jobs"])
        st.caption(f"Task managers online: {r['flink']['task_managers']}")
    else:
        st.error(r["flink"]["error"])

with s2:
    st.subheader(f"{_dot(r['silver']['ok'])} MinIO  silver/")
    if r["silver"]["ok"]:
        st.metric("Enriched files today", r["silver"]["today_count"])
        st.caption(f"Last file: **{_ago(r['silver']['last_ts'])}**")
    else:
        st.error(r["silver"]["error"])

st.divider()

# ── Gold Layer ───────────────────────────────────────────────────────────────

st.header("🥇 Gold Layer — Aggregation & DWH")
g1, g2, g3 = st.columns(3)

with g1:
    st.subheader(f"{_dot(r['spark']['ok'])} Spark")
    if r["spark"]["ok"]:
        alive = r["spark"]["alive_workers"]
        ok    = alive >= 1
        st.metric("Alive workers", alive)
        st.caption(
            f"Cores: {r['spark']['cores']}  "
            f"· Memory: {r['spark']['memory_gb']} GB  "
            f"· Active apps: {r['spark']['active_apps']}"
        )
        if not ok:
            st.warning("No alive workers — Spark jobs cannot run.")
    else:
        st.error(r["spark"]["error"])

with g2:
    st.subheader(f"{_dot(r['gold_s3']['ok'])} MinIO  gold/")
    if r["gold_s3"]["ok"]:
        st.metric("Gold files today", r["gold_s3"]["today_count"])
        st.caption(f"Last file: **{_ago(r['gold_s3']['last_ts'])}**")
    else:
        st.error(r["gold_s3"]["error"])

with g3:
    st.subheader(f"{_dot(r['dwh']['ok'])} PostgreSQL DWH")
    if r["dwh"]["ok"]:
        st.metric("Gold events", f"{r['dwh']['events']:,}")
        st.caption(
            f"Last processed: {r['dwh']['last_date'] or '—'}  "
            f"· Monitoring rows: {r['dwh']['monitoring_rows']:,}"
        )
    else:
        st.error(r["dwh"]["error"])

# Airflow DAG status table
st.subheader(f"{_dot(r['airflow']['ok'])} Airflow")
if r["airflow"]["ok"]:
    ac1, ac2 = st.columns([1, 3])
    with ac1:
        st.markdown(
            f"**Metadatabase:** {'✅ healthy' if r['airflow']['metadatabase'] else '❌ unhealthy'}  \n"
            f"**Scheduler:**    {'✅ healthy' if r['airflow']['scheduler']    else '❌ unhealthy'}"
        )
    with ac2:
        import pandas as pd
        _state_icon = {
            "success":  "🟢", "running": "🔵", "failed": "🔴",
            "queued":   "🟡", "never run": "⚪", "?": "⚪",
        }
        df = pd.DataFrame(r["airflow"]["dags"])
        df["Last state"] = df["Last state"].apply(
            lambda s: f"{_state_icon.get(s, '⚪')} {s}"
        )
        st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.error(r["airflow"]["error"])

"""
db.py  ·  Shared database connection for Streamlit pages
Reads credentials from environment variables.
All pages import get_conn() from here — never hardcode credentials.
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()


def get_conn():
    """Return a new psycopg2 connection to the Gold DWH."""
    return psycopg2.connect(
        host=os.getenv("DWH_HOST", "postgres-dwh"),
        port=int(os.getenv("DWH_PORT", "5432")),
        dbname=os.getenv("DWH_DB", "earthquake"),
        user=os.getenv("DWH_USER", "eq_user"),
        password=os.getenv("DWH_PASSWORD", "eq_password_change_me"),
        cursor_factory=RealDictCursor,
    )


def query(sql: str, params=None):
    """Execute a SQL query and return all rows as a list of dicts."""
    import pandas as pd
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
            rows = cur.fetchall()
    return pd.DataFrame([dict(r) for r in rows])

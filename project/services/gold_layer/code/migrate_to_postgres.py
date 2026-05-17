"""
migrate_to_postgres.py
Reads all existing silver Parquet files and writes them into the PostgreSQL gold tables.

Run inside the gold_layer container:
  docker exec gold_layer python /app/migrate_to_postgres.py
"""

from io import BytesIO

import pandas as pd

from s3_reader import make_s3_client, S3_BUCKET, SINGLE_EQ_KEY, SINGLE_COUNTRY_KEY
from processor import process_event, process_country
from postgres_writer import (
    make_postgres_conn,
    ensure_tables,
    upsert_event,
    refresh_top10,
    refresh_top_historical,
    upsert_countries,
    TABLE,
    TABLE_TOP10,
    TABLE_TOP_HISTORICAL,
    TABLE_COUNTRIES,
)


def migrate_earthquake_events(s3, conn) -> None:
    print("Reading silver/earthquake_events.parquet ...", flush=True)
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=SINGLE_EQ_KEY)
        df  = pd.read_parquet(BytesIO(obj["Body"].read()))
    except Exception as e:
        print(f"  ERROR reading earthquake_events.parquet: {e}", flush=True)
        return

    total = len(df)
    print(f"  {total} events found — upserting into earthquake_events_gold ...", flush=True)

    for i, row in enumerate(df.to_dict(orient="records"), start=1):
        try:
            record = process_event(row)
            upsert_event(conn, record)
            if i % 50 == 0 or i == total:
                print(f"  {i}/{total} upserted", flush=True)
        except Exception as e:
            print(f"  ERROR on row {i} (unid={row.get('unid')}): {e}", flush=True)

    print("  Refreshing top10_earthquakes_24h ...", flush=True)
    refresh_top10(conn)

    print("  Refreshing top_historical_earthquakes ...", flush=True)
    refresh_top_historical(conn)

    print(f"  Done: {total} events migrated.", flush=True)


def migrate_countries(s3, conn) -> None:
    print("Reading silver/countries.parquet ...", flush=True)
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=SINGLE_COUNTRY_KEY)
        df  = pd.read_parquet(BytesIO(obj["Body"].read()))
    except Exception as e:
        print(f"  ERROR reading countries.parquet: {e}", flush=True)
        return

    rows = df.to_dict(orient="records")
    print(f"  {len(rows)} countries found — upserting into country_summary ...", flush=True)

    try:
        records = [process_country(r) for r in rows]
        upsert_countries(conn, records)
        print(f"  Done: {len(records)} countries migrated.", flush=True)
    except Exception as e:
        print(f"  ERROR upserting countries: {e}", flush=True)


def truncate_all(conn) -> None:
    print("Truncating all gold tables ...", flush=True)
    with conn.cursor() as cur:
        cur.execute("SET lock_timeout = '5s';")
        cur.execute(
            f"TRUNCATE {TABLE_TOP10}, {TABLE_TOP_HISTORICAL}, {TABLE_COUNTRIES}, {TABLE};"
        )
    conn.commit()
    print("  All tables cleared.", flush=True)


def main():
    s3   = make_s3_client()
    conn = make_postgres_conn()
    ensure_tables(conn)

    truncate_all(conn)
    migrate_earthquake_events(s3, conn)
    migrate_countries(s3, conn)

    conn.close()
    print("Migration complete.", flush=True)


if __name__ == "__main__":
    main()

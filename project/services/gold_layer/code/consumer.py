from kafka_client import get_kafka_consumer
from s3_reader import make_s3_client, read_silver_event, read_silver_countries
from processor import process_event, process_country
from postgres_writer import make_postgres_conn, ensure_tables, upsert_event, refresh_top10, refresh_top_historical, upsert_countries


def run():
    consumer = get_kafka_consumer()
    s3       = make_s3_client()
    conn     = make_postgres_conn()
    ensure_tables(conn)

    for message in consumer:
        try:
            unid = message.value["unid"]

            row = read_silver_event(s3, unid)
            if row is None:
                print(f"Skipping {unid}: not found in silver Parquet", flush=True)
                continue
            
            # extract the parameters from row
            record = process_event(row)

            # insert in 'earthquake_events_gold' (postgresql table)
            upsert_event(conn, record)
            print(f"[{unid}] written to earthquake_events_gold", flush=True)

            # delete & insert in 'top10_earthquakes_24h' (postgresql table)
            refresh_top10(conn)
            print(f"[{unid}] top10_earthquakes_24h refreshed", flush=True)

            # delete & insert in 'top_historical_earthquakes' (postgresql table)
            refresh_top_historical(conn)
            print(f"[{unid}] top_historical_earthquakes refreshed", flush=True)

            country_rows = read_silver_countries(s3)
            country_records = [process_country(r) for r in country_rows]

            # insert in 'country_summary' (postgresql table)
            upsert_countries(conn, country_records)
            print(f"[{unid}] country_summary updated ({len(country_records)} countries)", flush=True)

        except Exception as e:
            print(f"Error processing message: {e}", flush=True)
            conn.rollback()


if __name__ == "__main__":
    run()

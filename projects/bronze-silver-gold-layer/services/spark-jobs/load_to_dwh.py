"""
load_to_dwh.py  ·  GOLD → DWH loader
Reads Gold layer files and writes to PostgreSQL via JDBC.
Loads: earthquake_events, regional_stats, daily_summary.
"""
import argparse, sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DateType, BooleanType, DoubleType

parser = argparse.ArgumentParser()
parser.add_argument("--date",         required=True)
parser.add_argument("--bucket",       default="eq-monitor")
parser.add_argument("--dwh-host",     default="postgres-dwh")
parser.add_argument("--dwh-port",     default="5432")
parser.add_argument("--dwh-db",       default="earthquake")
parser.add_argument("--dwh-user",     default="eq_user")
parser.add_argument("--dwh-password", default="eq_password_change_me")
args = parser.parse_args()

spark = SparkSession.builder.appName("eq-load-to-dwh") \
    .config("spark.sql.adaptive.enabled", "true").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

JDBC_URL   = f"jdbc:postgresql://{args.dwh_host}:{args.dwh_port}/{args.dwh_db}"
JDBC_PROPS = {"user": args.dwh_user, "password": args.dwh_password,
              "driver": "org.postgresql.Driver"}
date_path  = args.date.replace("-", "/")

def write_jdbc(df, table: str):
    count = df.count()
    if count == 0:
        print(f"  No data — skipping {table}", flush=True)
        return
    df.write.mode("append").jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)
    print(f"  {count} rows loaded into {table}", flush=True)

# ── earthquake_events ────────────────────────────────────────────────────────
path = f"s3a://{args.bucket}/gold/impact_scores/{date_path}/"
print(f"Loading {path} → gold.earthquake_events", flush=True)
df = spark.read.json(path)
if df.count() > 0:
    df = df \
        .withColumn("enriched_at",          F.col("enriched_at").cast(TimestampType())) \
        .withColumn("event_time",           F.col("event_time").cast(TimestampType())) \
        .withColumn("scored_at",            F.col("scored_at").cast(TimestampType())) \
        .withColumn("processing_date",      F.col("processing_date").cast(DateType())) \
        .withColumn("ge_validation_passed", F.col("ge_validation_passed").cast(BooleanType())) \
        .withColumn("population_50km",      F.lit(None).cast(DoubleType()))
    write_jdbc(df, "gold.earthquake_events")

# ── regional_stats ───────────────────────────────────────────────────────────
path = f"s3a://{args.bucket}/gold/regional_stats/{date_path}/"
print(f"Loading {path} → gold.regional_stats", flush=True)
df = spark.read.json(path)
if df.count() > 0:
    df = df \
        .withColumn("aggregated_at",   F.col("aggregated_at").cast(TimestampType())) \
        .withColumn("processing_date", F.col("processing_date").cast(DateType()))
    write_jdbc(df, "gold.regional_stats")

# ── daily_summary ────────────────────────────────────────────────────────────
path = f"s3a://{args.bucket}/gold/daily_summary/{date_path}/"
print(f"Loading {path} → gold.daily_summary", flush=True)
df = spark.read.json(path)
if df.count() > 0:
    df = df \
        .withColumn("aggregated_at",   F.col("aggregated_at").cast(TimestampType())) \
        .withColumn("processing_date", F.col("processing_date").cast(DateType()))
    write_jdbc(df, "gold.daily_summary")

print(f"DWH load complete for {args.date}", flush=True)
spark.stop()

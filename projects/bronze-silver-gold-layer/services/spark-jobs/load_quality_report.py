"""
load_quality_report.py  ·  Quality profiles → DWH
Reads quality profile JSONs for all three layers,
loads into gold.quality_profiles in the DWH.
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--date",         required=True)
parser.add_argument("--bucket",       default="eq-monitor")
parser.add_argument("--dwh-host",     default="postgres-dwh")
parser.add_argument("--dwh-port",     default="5432")
parser.add_argument("--dwh-db",       default="earthquake")
parser.add_argument("--dwh-user",     default="eq_user")
parser.add_argument("--dwh-password", default="eq_password_change_me")
args = parser.parse_args()

spark = SparkSession.builder.appName("eq-load-quality-report").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

JDBC_URL   = f"jdbc:postgresql://{args.dwh_host}:{args.dwh_port}/{args.dwh_db}"
JDBC_PROPS = {"user": args.dwh_user, "password": args.dwh_password,
              "driver": "org.postgresql.Driver"}

path = f"s3a://{args.bucket}/gold/quality_profiles/{args.date.replace('-', '/')}/"
df   = spark.read.json(path)

if df.count() == 0:
    print(f"No quality profiles for {args.date} — skipping", flush=True)
    spark.stop(); exit(0)

df.select(F.col("layer"), F.col("date"), F.col("profiled_at"),
          F.col("row_count"), F.col("distinct_ids"), F.col("duplicate_count")) \
  .write.mode("append").jdbc(url=JDBC_URL, table="gold.quality_profiles", properties=JDBC_PROPS)
print(f"Quality profiles loaded for {args.date}", flush=True)
spark.stop()

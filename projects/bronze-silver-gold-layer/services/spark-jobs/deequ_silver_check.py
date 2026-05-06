"""
deequ_silver_check.py  ·  SILVER quality gate
Dataset-level checks before Gold aggregation is allowed.
Exits with code 1 on failure — blocks the entire Gold write.
"""
import argparse, sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--date",   required=True)
parser.add_argument("--bucket", default="eq-monitor")
args = parser.parse_args()

spark = SparkSession.builder.appName("eq-deequ-silver-check").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver_path = f"s3a://{args.bucket}/silver/events/{args.date.replace('-', '/')}/"
df    = spark.read.json(silver_path)
total = df.count()

if total == 0:
    print(f"No Silver data for {args.date} — skipping checks", flush=True)
    spark.stop(); sys.exit(0)

events = df.select(
    F.col("data.properties.unid").alias("event_id"),
    F.col("data.properties.mag").cast("double").alias("magnitude"),
    F.col("data.properties.depth").cast("double").alias("depth"),
    F.col("data.properties.lat").cast("double").alias("lat"),
    F.col("data.properties.lon").cast("double").alias("lon"),
    F.col("ge_validation_passed"),
)

failures = []

null_ids = events.filter(F.col("event_id").isNull()).count()
if null_ids > 0: failures.append(f"FAIL: {null_ids} null event IDs")

bad_mag = events.filter(F.col("magnitude").isNull() | (F.col("magnitude") < 0) | (F.col("magnitude") > 10)).count()
if bad_mag > 0: failures.append(f"FAIL: {bad_mag} invalid magnitudes")

bad_depth = events.filter(F.col("depth").isNull() | (F.col("depth") < 0)).count()
if bad_depth > 0: failures.append(f"FAIL: {bad_depth} invalid depths")

bad_coords = events.filter(
    F.col("lat").isNull() | F.col("lon").isNull() |
    (F.col("lat") < -90) | (F.col("lat") > 90) |
    (F.col("lon") < -180) | (F.col("lon") > 180)).count()
if bad_coords > 0: failures.append(f"FAIL: {bad_coords} invalid coordinates")

distinct_ids = events.select("event_id").distinct().count()
if total != distinct_ids: failures.append(f"FAIL: {total - distinct_ids} duplicate event IDs")

failed_ge = events.filter(F.col("ge_validation_passed") != True).count()
if failed_ge > 0: failures.append(f"WARN: {failed_ge} records with ge_validation_passed != True")

print(f"\n=== Silver Quality Report — {args.date} ===", flush=True)
print(f"Total records: {total}", flush=True)
if failures:
    for f in failures: print(f, flush=True)
    print("Silver quality checks FAILED — Gold write blocked", flush=True)
    spark.stop(); sys.exit(1)
else:
    print("All Silver quality checks PASSED ✓", flush=True)
spark.stop()

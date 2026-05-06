"""
deequ_gold_check.py  ·  GOLD quality gate
Dataset-level checks before DWH load is allowed.
Exits with code 1 on failure — blocks the DWH load.
"""
import argparse, sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--date",   required=True)
parser.add_argument("--bucket", default="eq-monitor")
args = parser.parse_args()

spark = SparkSession.builder.appName("eq-deequ-gold-check").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

gold_path = f"s3a://{args.bucket}/gold/impact_scores/{args.date.replace('-', '/')}/"
df    = spark.read.json(gold_path)
total = df.count()

if total == 0:
    print(f"No Gold data for {args.date} — skipping checks", flush=True)
    spark.stop(); sys.exit(0)

failures = []
score_cols = ["tsunami_risk", "building_vulnerability",
              "population_exposure", "infrastructure_risk", "composite_score"]

for col in score_cols:
    bad = df.filter(F.col(col).isNull() | (F.col(col) < 0) | (F.col(col) > 10)).count()
    if bad > 0: failures.append(f"FAIL: {bad} invalid values in {col}")

distinct_ids = df.select("event_id").distinct().count()
if total != distinct_ids: failures.append(f"FAIL: {total - distinct_ids} duplicate event IDs in Gold")

bad_composite = df.filter(
    F.abs(F.col("composite_score") - (
        F.col("tsunami_risk")           * F.lit(0.30) +
        F.col("building_vulnerability") * F.lit(0.25) +
        F.col("population_exposure")    * F.lit(0.25) +
        F.col("infrastructure_risk")    * F.lit(0.20)
    )) > F.lit(0.01)).count()
if bad_composite > 0: failures.append(f"FAIL: {bad_composite} inconsistent composite_score values")

print(f"\n=== Gold Quality Report — {args.date} ===", flush=True)
print(f"Total records: {total}", flush=True)
if failures:
    for f in failures: print(f, flush=True)
    print("Gold quality checks FAILED — DWH load blocked", flush=True)
    spark.stop(); sys.exit(1)
else:
    print("All Gold quality checks PASSED ✓", flush=True)
spark.stop()

"""
impact_scoring.py  ·  GOLD layer
Reads Silver events, computes 4 impact scores + composite,
writes to s3://eq-monitor/gold/impact_scores/YYYY/MM/DD/

Scores (all 0-10):
  tsunami_risk           = min(10, max(0, (magnitude-4) * 2.5 * (1-depth/300)))
  building_vulnerability = min(10, max(0, 1.5*magnitude - 0.0133*depth + 1.3))
  population_exposure    = min(10, max(0, population_50km / 1_000_000))
  infrastructure_risk    = min(10, max(0, magnitude*0.8 + (1-depth/300)*2))
  composite_score        = tsunami*0.30 + building*0.25 + population*0.25 + infra*0.20
"""
import argparse, sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

parser = argparse.ArgumentParser()
parser.add_argument("--date",   required=True)
parser.add_argument("--bucket", default="eq-monitor")
args = parser.parse_args()

spark = SparkSession.builder \
    .appName("eq-impact-scoring") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver_path = f"s3a://{args.bucket}/silver/events/{args.date.replace('-', '/')}/"
print(f"Reading Silver: {silver_path}", flush=True)
df = spark.read.json(silver_path)
if df.count() == 0:
    print(f"No Silver data for {args.date} — skipping", flush=True)
    spark.stop(); sys.exit(0)

df = df.select(
    F.col("data.properties.unid").alias("event_id"),
    F.col("data.properties.lat").cast(DoubleType()).alias("lat"),
    F.col("data.properties.lon").cast(DoubleType()).alias("lon"),
    F.col("data.properties.mag").cast(DoubleType()).alias("magnitude"),
    F.col("data.properties.depth").cast(DoubleType()).alias("depth"),
    F.col("data.properties.time").alias("event_time"),
    F.col("data.properties.flynn_region").alias("region"),
    F.col("enriched_at"),
    F.lit(None).cast(DoubleType()).alias("population_50km"),  # population_data not enriched
    F.col("usgs_nearby.count").cast(DoubleType()).alias("usgs_nearby_count"),
    F.col("usgs_nearby.max_magnitude").cast(DoubleType()).alias("usgs_max_magnitude"),
    F.col("ge_validation_passed"),
)

df = df.withColumn("tsunami_risk",
    F.least(F.lit(10.0), F.greatest(F.lit(0.0),
        (F.col("magnitude") - F.lit(4.0)) * F.lit(2.5) *
        (F.lit(1.0) - F.col("depth") / F.lit(300.0))
    )).cast(DoubleType()))

df = df.withColumn("mmi_estimate",
    F.lit(1.5)*F.col("magnitude") - F.lit(0.0133)*F.col("depth") + F.lit(1.3)
).withColumn("building_vulnerability",
    F.least(F.lit(10.0), F.greatest(F.lit(0.0), F.col("mmi_estimate"))).cast(DoubleType()))

df = df.withColumn("population_exposure",
    F.least(F.lit(10.0), F.greatest(F.lit(0.0),
        F.coalesce(F.col("population_50km"), F.lit(0.0)) / F.lit(1_000_000.0)
    )).cast(DoubleType()))

df = df.withColumn("infrastructure_risk",
    F.least(F.lit(10.0), F.greatest(F.lit(0.0),
        F.col("magnitude") * F.lit(0.8) +
        (F.lit(1.0) - F.col("depth") / F.lit(300.0)) * F.lit(2.0)
    )).cast(DoubleType()))

df = df.withColumn("composite_score",
    (F.col("tsunami_risk")           * F.lit(0.30) +
     F.col("building_vulnerability") * F.lit(0.25) +
     F.col("population_exposure")    * F.lit(0.25) +
     F.col("infrastructure_risk")    * F.lit(0.20)).cast(DoubleType()))

df = df.withColumn("processing_date", F.lit(args.date)) \
       .withColumn("scored_at", F.current_timestamp())

gold_path = f"s3a://{args.bucket}/gold/impact_scores/{args.date.replace('-', '/')}/"
df.write.mode("overwrite").json(gold_path)
print(f"Impact scoring complete: {df.count()} events → {gold_path}", flush=True)
spark.stop()

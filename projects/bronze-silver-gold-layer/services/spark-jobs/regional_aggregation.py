"""
regional_aggregation.py  ·  GOLD layer
Groups Gold impact scores by region, produces daily aggregations.
Writes regional_stats and daily_summary to Gold layer.
"""
import argparse, sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--date",   required=True)
parser.add_argument("--bucket", default="eq-monitor")
args = parser.parse_args()

spark = SparkSession.builder.appName("eq-regional-aggregation") \
    .config("spark.sql.adaptive.enabled", "true").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

gold_path = f"s3a://{args.bucket}/gold/impact_scores/{args.date.replace('-', '/')}/"
df = spark.read.json(gold_path)
if df.count() == 0:
    print(f"No Gold data for {args.date} — skipping", flush=True)
    spark.stop(); sys.exit(0)

regional = df.groupBy("region").agg(
    F.count("event_id").alias("event_count"),
    F.countDistinct("event_id").alias("unique_events"),
    F.avg("magnitude").alias("avg_magnitude"),
    F.max("magnitude").alias("max_magnitude"),
    F.avg("composite_score").alias("avg_composite_score"),
    F.max("composite_score").alias("max_composite_score"),
    F.avg("tsunami_risk").alias("avg_tsunami_risk"),
    F.max("tsunami_risk").alias("max_tsunami_risk"),
    F.avg("population_exposure").alias("avg_population_exposure"),
    F.lit(None).cast("double").alias("total_population_exposed"),
).withColumn("processing_date", F.lit(args.date)) \
 .withColumn("aggregated_at", F.current_timestamp())

global_summary = df.agg(
    F.count("event_id").alias("total_events"),
    F.max("magnitude").alias("max_magnitude_global"),
    F.max("composite_score").alias("max_composite_score_global"),
    F.countDistinct("region").alias("regions_affected"),
    F.lit(None).cast("double").alias("total_population_exposed_global"),
).withColumn("processing_date", F.lit(args.date)) \
 .withColumn("aggregated_at", F.current_timestamp()) \
 .withColumn("region", F.lit("GLOBAL"))

regional_path = f"s3a://{args.bucket}/gold/regional_stats/{args.date.replace('-', '/')}/"
summary_path  = f"s3a://{args.bucket}/gold/daily_summary/{args.date.replace('-', '/')}/"
regional.write.mode("overwrite").json(regional_path)
global_summary.write.mode("overwrite").json(summary_path)
print(f"Regional aggregation: {regional.count()} regions → {regional_path}", flush=True)
spark.stop()

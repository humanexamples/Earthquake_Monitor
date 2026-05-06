"""
profile_layer.py  ·  Data quality profiling
Profiles any medallion layer — row counts, null rates, duplicate counts.
Writes JSON profile to gold/quality_profiles/YYYY/MM/DD/{layer}_profile.json
"""
import argparse, json, os
from datetime import datetime, timezone
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--layer",  required=True, choices=["bronze", "silver", "gold"])
parser.add_argument("--date",   required=True)
parser.add_argument("--bucket", default="eq-monitor")
args = parser.parse_args()

spark = SparkSession.builder.appName(f"eq-profile-{args.layer}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

paths = {
    "bronze": f"s3a://{args.bucket}/bronze/events/{args.date.replace('-', '/')}/",
    "silver": f"s3a://{args.bucket}/silver/events/{args.date.replace('-', '/')}/",
    "gold":   f"s3a://{args.bucket}/gold/impact_scores/{args.date.replace('-', '/')}/",
}

df    = spark.read.json(paths[args.layer])
total = df.count()

if total == 0:
    print(f"No data in {args.layer} for {args.date}", flush=True)
    spark.stop(); exit(0)

null_rates = {}
for col in df.columns:
    null_rates[col] = round(df.filter(F.col(col).isNull()).count() / total, 4)

id_col = "data.properties.unid" if args.layer in ("bronze", "silver") else "event_id"
try:
    distinct_ids    = df.select(F.col(id_col)).distinct().count()
    duplicate_count = total - distinct_ids
except Exception:
    distinct_ids    = 0
    duplicate_count = 0

profile = {
    "layer": args.layer, "date": args.date,
    "profiled_at": datetime.now(timezone.utc).isoformat(),
    "row_count": total, "distinct_ids": distinct_ids,
    "duplicate_count": duplicate_count,
    "null_rates": null_rates, "columns": df.columns,
}

s3 = boto3.client("s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY"))
key = f"gold/quality_profiles/{args.date.replace('-', '/')}/{args.layer}_profile.json"
s3.put_object(Bucket=args.bucket, Key=key,
    Body=json.dumps(profile, indent=2), ContentType="application/json")
print(f"Profile written: s3://{args.bucket}/{key}", flush=True)
spark.stop()

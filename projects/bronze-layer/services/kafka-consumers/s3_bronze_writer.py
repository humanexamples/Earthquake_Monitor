"""
s3_bronze_writer.py  ·  BRONZE layer consumer
Reads raw events from Kafka and writes them to S3/MinIO as JSON files.
Path: bronze/events/YYYY/MM/DD/{unid}.json
Immutable: files are never modified or deleted after writing.
"""
import json
import os
from datetime import datetime
import boto3
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
S3_ENDPOINT   = os.getenv("S3_ENDPOINT_URL")
S3_ACCESS     = os.getenv("S3_ACCESS_KEY")
S3_SECRET     = os.getenv("S3_SECRET_KEY")
S3_BUCKET     = os.getenv("S3_BUCKET", "eq-monitor")

consumer = KafkaConsumer(
    "raw-seismic-events",
    bootstrap_servers=KAFKA_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="s3-bronze-writer",
)

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS,
    aws_secret_access_key=S3_SECRET,
)

print(f"S3 Bronze writer started — bucket: {S3_BUCKET}", flush=True)

for message in consumer:
    event     = message.value
    unid      = event.get("data", {}).get("properties", {}).get("unid", "unknown")
    date_path = datetime.utcnow().strftime("%Y/%m/%d")
    key       = f"bronze/events/{date_path}/{unid}.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(event),
        ContentType="application/json",
    )
    print(f"Bronze: s3://{S3_BUCKET}/{key}", flush=True)

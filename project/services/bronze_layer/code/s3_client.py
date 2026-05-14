import datetime
import json
import os
import boto3
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET   = os.getenv("S3_BUCKET", "eq-monitor")


def make_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
    )


def upload_to_s3(s3, unid: str, earthquake_event, historical_earthquakes, infrastructure) -> None:
    date_path = datetime.datetime.now(datetime.timezone.utc).strftime("%Y/%m/%d")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"bronze/events/{date_path}/{unid}/earthquake_event.json",
        Body=json.dumps(earthquake_event),
        ContentType="application/json",
    )
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"bronze/events/{date_path}/{unid}/historical_earthquakes.json",
        Body=json.dumps(historical_earthquakes),
        ContentType="application/json",
    )
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"bronze/events/{date_path}/{unid}/infrastructure.json",
        Body=json.dumps(infrastructure),
        ContentType="application/json",
    )


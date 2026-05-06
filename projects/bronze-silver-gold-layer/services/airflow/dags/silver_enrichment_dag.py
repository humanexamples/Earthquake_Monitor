"""
silver_enrichment_dag.py  ·  SILVER → GOLD gate
Triggered every 15 minutes.
Waits for new Silver files, then runs the Deequ Silver quality check.
If checks pass, the Gold aggregation DAG is unblocked for its next run.
"""
import os
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

S3_BUCKET      = os.getenv("S3_BUCKET", "eq-monitor")
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_ACCESS      = os.getenv("S3_ACCESS_KEY", "minio_access_key")
S3_SECRET      = os.getenv("S3_SECRET_KEY", "minio_secret_key_change_me")

SPARK_CONF = {
    "spark.hadoop.fs.s3a.endpoint":                  MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key":                S3_ACCESS,
    "spark.hadoop.fs.s3a.secret.key":                S3_SECRET,
    "spark.hadoop.fs.s3a.path.style.access":         "true",
    "spark.hadoop.fs.s3a.impl":                      "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider":  "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.jars.ivy":                                "/tmp/ivy-cache",
}

default_args = {
    "owner":            "eq-monitor",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

@dag(
    dag_id="silver_enrichment_pipeline",
    default_args=default_args,
    description="Detects new Silver files and runs Deequ Silver quality checks",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "quality"],
)
def silver_enrichment_pipeline():

    wait_for_silver = S3KeySensor(
        task_id="wait_for_silver_files",
        bucket_name=S3_BUCKET,
        bucket_key="silver/events/{{ ds_nodash }}/*.json",
        aws_conn_id="minio_s3",
        timeout=600,
        poke_interval=30,
        mode="poke",
    )

    deequ_silver_check = SparkSubmitOperator(
        task_id="deequ_silver_quality_check",
        application="/opt/spark-jobs/deequ_silver_check.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        application_args=["--date", "{{ ds }}", "--bucket", S3_BUCKET],
    )

    wait_for_silver >> deequ_silver_check

silver_enrichment_pipeline()

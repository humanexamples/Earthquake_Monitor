"""
gold_aggregation_dag.py  ·  GOLD layer producer
Hourly DAG: Silver → impact scores + regional aggregation
           → Deequ Gold check → DWH load.
impact_scoring and regional_aggregation run in parallel.
Deequ Gold check gates the DWH load.
"""
import os
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

S3_BUCKET      = os.getenv("S3_BUCKET", "eq-monitor")
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_ACCESS      = os.getenv("S3_ACCESS_KEY", "minio_access_key")
S3_SECRET      = os.getenv("S3_SECRET_KEY", "minio_secret_key_change_me")
DWH_HOST       = os.getenv("DWH_HOST", "postgres-dwh")
DWH_PORT       = os.getenv("DWH_PORT", "5432")
DWH_DB         = os.getenv("DWH_DB", "earthquake")
DWH_USER       = os.getenv("DWH_USER", "eq_user")
DWH_PASSWORD   = os.getenv("DWH_PASSWORD", "eq_password_change_me")

PACKAGES_JDBC = "org.postgresql:postgresql:42.7.1"

SPARK_CONF = {
    "spark.hadoop.fs.s3a.endpoint":                  MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key":                S3_ACCESS,
    "spark.hadoop.fs.s3a.secret.key":                S3_SECRET,
    "spark.hadoop.fs.s3a.path.style.access":         "true",
    "spark.hadoop.fs.s3a.impl":                      "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider":  "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.jars.ivy":                                "/tmp/ivy-cache",
}

DWH_ARGS = [
    "--dwh-host",     DWH_HOST,
    "--dwh-port",     DWH_PORT,
    "--dwh-db",       DWH_DB,
    "--dwh-user",     DWH_USER,
    "--dwh-password", DWH_PASSWORD,
]

default_args = {
    "owner":            "eq-monitor",
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}

@dag(
    dag_id="gold_aggregation_pipeline",
    default_args=default_args,
    description="Silver → Gold: impact scoring, aggregation, quality check, DWH load",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "aggregation", "dwh"],
)
def gold_aggregation_pipeline():

    compute_impact_scores = SparkSubmitOperator(
        task_id="compute_impact_scores",
        application="/opt/spark-jobs/impact_scoring.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        application_args=["--date", "{{ ds }}", "--bucket", S3_BUCKET],
    )

    aggregate_regional = SparkSubmitOperator(
        task_id="aggregate_regional_stats",
        application="/opt/spark-jobs/regional_aggregation.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        application_args=["--date", "{{ ds }}", "--bucket", S3_BUCKET],
    )

    deequ_gold_check = SparkSubmitOperator(
        task_id="deequ_gold_quality_check",
        application="/opt/spark-jobs/deequ_gold_check.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        application_args=["--date", "{{ ds }}", "--bucket", S3_BUCKET],
    )

    load_to_dwh = SparkSubmitOperator(
        task_id="load_gold_to_dwh",
        application="/opt/spark-jobs/load_to_dwh.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=PACKAGES_JDBC,
        application_args=["--date", "{{ ds }}", "--bucket", S3_BUCKET] + DWH_ARGS,
    )

    compute_impact_scores >> aggregate_regional >> deequ_gold_check >> load_to_dwh

gold_aggregation_pipeline()

"""
data_quality_dag.py  ·  Daily quality report
Profiles Bronze, Silver, and Gold layers in parallel.
Loads all three profiles into DWH gold.quality_profiles.
Powers the Streamlit data profiling page.
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

default_args = {
    "owner":            "eq-monitor",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

@dag(
    dag_id="data_quality_report",
    default_args=default_args,
    description="Daily quality profiling across Bronze, Silver, Gold layers",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quality", "profiling"],
)
def data_quality_report():

    def profile_task(layer: str):
        return SparkSubmitOperator(
            task_id=f"profile_{layer}_layer",
            application="/opt/spark-jobs/profile_layer.py",
            conn_id="spark_default",
            conf=SPARK_CONF,
            application_args=["--layer", layer, "--date", "{{ ds }}", "--bucket", S3_BUCKET],
        )

    bronze_profile = profile_task("bronze")
    silver_profile = profile_task("silver")
    gold_profile   = profile_task("gold")

    load_quality_report = SparkSubmitOperator(
        task_id="load_quality_report_to_dwh",
        application="/opt/spark-jobs/load_quality_report.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=PACKAGES_JDBC,
        application_args=[
            "--date", "{{ ds }}", "--bucket", S3_BUCKET,
            "--dwh-host", DWH_HOST, "--dwh-port", DWH_PORT,
            "--dwh-db", DWH_DB, "--dwh-user", DWH_USER,
            "--dwh-password", DWH_PASSWORD,
        ],
    )

    [bronze_profile, silver_profile, gold_profile] >> load_quality_report

data_quality_report()

## Why Spark is used in the bronze-silver-gold-layer project

Airflow acts as the **scheduler/orchestrator** — it decides *when* and *in what order* tasks run. Spark is the **compute engine** — it does the actual heavy data processing. They serve different roles.

### What Spark does in this project

Airflow uses `SparkSubmitOperator` to trigger Spark jobs for all the computationally intensive work:

| Spark Job                                                    | Purpose                                                      |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| [impact_scoring.py](vscode-webview://0hs34rpns60nkimdq53kub5hrqrjkl10cf9ne2p6atdd3hhqu54l/_Course\myprojects\Earthquake_Monitor\projects\bronze-silver-gold-layer\services\spark-jobs\impact_scoring.py) | Reads Silver layer events, computes 5 impact scores (tsunami risk, building vulnerability, etc.), writes to Gold layer in S3 |
| [regional_aggregation.py](vscode-webview://0hs34rpns60nkimdq53kub5hrqrjkl10cf9ne2p6atdd3hhqu54l/_Course\myprojects\Earthquake_Monitor\projects\bronze-silver-gold-layer\services\spark-jobs\regional_aggregation.py) | Computes regional statistics across earthquake data          |
| [deequ_gold_check.py](vscode-webview://0hs34rpns60nkimdq53kub5hrqrjkl10cf9ne2p6atdd3hhqu54l/_Course\myprojects\Earthquake_Monitor\projects\bronze-silver-gold-layer\services\spark-jobs\deequ_gold_check.py) | Runs **PyDeequ** data quality checks on Gold layer data — Deequ is a Spark-native library |
| [profile_layer.py](vscode-webview://0hs34rpns60nkimdq53kub5hrqrjkl10cf9ne2p6atdd3hhqu54l/_Course\myprojects\Earthquake_Monitor\projects\bronze-silver-gold-layer\services\spark-jobs\profile_layer.py) | Profiles Bronze/Silver/Gold layers (completeness, uniqueness, distributions) |
| [load_to_dwh.py](vscode-webview://0hs34rpns60nkimdq53kub5hrqrjkl10cf9ne2p6atdd3hhqu54l/_Course\myprojects\Earthquake_Monitor\projects\bronze-silver-gold-layer\services\spark-jobs\load_to_dwh.py) | Loads Gold data into PostgreSQL DWH via JDBC                 |

### The key reasons Spark is the right tool here

1. **Distributed processing of S3 data** — Spark reads Parquet/JSON files from S3 (MinIO) at scale using its native `s3a://` connector. Airflow itself can't do this.
2. **Deequ requires Spark** — The data quality checks use [AWS Deequ](https://github.com/awslabs/python-deequ), which is a Spark library. There's no way to run Deequ without Spark.
3. **Complex columnar transformations** — The impact scoring logic ([impact_scoring.py:73-105](vscode-webview://0hs34rpns60nkimdq53kub5hrqrjkl10cf9ne2p6atdd3hhqu54l/_Course\myprojects\Earthquake_Monitor\projects\bronze-silver-gold-layer\services\spark-jobs\impact_scoring.py#L73-L105)) applies formulas across potentially millions of rows using `pyspark.sql.functions` — this is Spark's core strength.
4. **JDBC bulk loading** — Spark efficiently bulk-loads large datasets into the PostgreSQL DWH using JDBC, which is much faster than row-by-row inserts.

**In short:** Airflow orchestrates *when* things run; Spark handles *the actual data transformation, quality checking, and loading* at scale.



## What is Deequ

**Deequ** is an open-source data quality library built on top of Apache Spark, originally developed by Amazon/AWS. The name stands for **"Data Quality"**.

### What it does

It lets you define **data quality constraints** (rules) on a dataset, then automatically checks whether the data satisfies them. Think of it as unit tests, but for your data.

### Example checks it can run

| Check type            | Example                                         |
| --------------------- | ----------------------------------------------- |
| Completeness          | "Column `magnitude` must never be null"         |
| Uniqueness            | "Column `event_id` must be 100% unique"         |
| Range validation      | "All magnitude values must be between 0 and 10" |
| Distribution          | "At least 90% of rows must have a valid region" |
| Referential integrity | "All `event_id`s in Gold must exist in Silver"  |

### Why it needs Spark

Deequ computes quality metrics by running **aggregations over your entire dataset** (e.g., count nulls, count distinct values, min/max). Spark handles this efficiently across millions of rows in a distributed way — that's why Deequ is a Spark library and can't run without it.

### How it's used in your project

In [deequ_gold_check.py](vscode-webview://0hs34rpns60nkimdq53kub5hrqrjkl10cf9ne2p6atdd3hhqu54l/_Course\myprojects\Earthquake_Monitor\projects\bronze-silver-gold-layer\services\spark-jobs\deequ_gold_check.py) and [deequ_silver_check.py](vscode-webview://0hs34rpns60nkimdq53kub5hrqrjkl10cf9ne2p6atdd3hhqu54l/_Course\myprojects\Earthquake_Monitor\projects\bronze-silver-gold-layer\services\spark-jobs\deequ_silver_check.py), it acts as a **quality gate** in the Airflow pipeline:



```
impact_scoring → regional_aggregation → deequ_gold_check → load_to_dwh
```

If the Deequ checks fail, the DAG stops and data is **not** loaded into the DWH — preventing bad data from reaching the final layer.
FROM apache/airflow:2.9.0-python3.11

USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Pre-download S3A JARs into pyspark's jars directory so DAGs need no runtime --packages.
# hadoop-aws 3.4.1 requires both v1 SDK (SimpleAWSCredentialsProvider) and v2 SDK (S3AFileSystem).
RUN python -c "print('import sys; sys.exit(0)')" > /tmp/dummy.py && \
    PYSPARK_JARS=$(python -c \
        "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") && \
    /home/airflow/.local/bin/spark-submit \
        --master "local[1]" \
        --packages "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-core:1.12.770,com.amazonaws:aws-java-sdk-s3:1.12.770,joda-time:joda-time:2.12.5,software.amazon.awssdk:s3:2.26.25,software.amazon.awssdk:sts:2.26.25,software.amazon.awssdk:s3-transfer-manager:2.26.25" \
        --exclude-packages "com.amazonaws:aws-java-sdk-bundle,software.amazon.awssdk:bundle,org.apache.hadoop:hadoop-common,org.apache.hadoop:hadoop-annotations,org.apache.hadoop:hadoop-auth" \
        --conf "spark.jars.ivy=/tmp/ivy-s3a" \
        /tmp/dummy.py && \
    find /tmp/ivy-s3a/cache -name "*.jar" ! -name "*sources*" ! -name "*javadoc*" \
        -exec cp -n {} "${PYSPARK_JARS}/" \; && \
    rm -rf /tmp/ivy-s3a /tmp/dummy.py

ENV PYTHONUNBUFFERED=1

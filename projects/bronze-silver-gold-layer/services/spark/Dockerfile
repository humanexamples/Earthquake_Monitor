FROM apache/spark:4.0.2-python3
USER root

# The base image does NOT include hadoop-aws or AWS SDKs needed for s3a:// paths.
# Use Spark's built-in Ivy resolver (spark-submit --packages) to download the full
# dependency tree, then copy the resolved JARs into /opt/spark/jars/ for permanent use.
# Uses the bundled SparkPi example to run a minimal job without needing Python/PySpark.

RUN /opt/spark/bin/spark-submit \
        --master "local[1]" \
        --class org.apache.spark.examples.SparkPi \
        --packages "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-core:1.12.770,com.amazonaws:aws-java-sdk-s3:1.12.770,joda-time:joda-time:2.12.5,software.amazon.awssdk:s3:2.26.25,software.amazon.awssdk:sts:2.26.25,software.amazon.awssdk:s3-transfer-manager:2.26.25" \
        --exclude-packages "com.amazonaws:aws-java-sdk-bundle,software.amazon.awssdk:bundle,org.apache.hadoop:hadoop-common,org.apache.hadoop:hadoop-annotations,org.apache.hadoop:hadoop-auth" \
        --conf "spark.jars.ivy=/tmp/ivy-s3a" \
        /opt/spark/examples/jars/spark-examples_2.13-4.0.2.jar 1 && \
    find /tmp/ivy-s3a/cache -name "*.jar" ! -name "*sources*" ! -name "*javadoc*" \
        -exec cp -n {} /opt/spark/jars/ \; && \
    rm -rf /tmp/ivy-s3a

USER spark

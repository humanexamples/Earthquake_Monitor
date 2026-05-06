FROM flink:1.18-scala_2.12-java11

USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
COPY enrichment_job.py .
COPY validators.py .

ENV PYTHONUNBUFFERED=1
CMD ["python", "enrichment_job.py"]

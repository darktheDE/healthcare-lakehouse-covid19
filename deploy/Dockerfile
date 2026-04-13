FROM apache/airflow:2.7.1-python3.9

USER root
# Cài đặt Java JRE để hỗ trợ PySpark / SparkSubmit (Yêu cầu bắt buộc của Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==2.7.1" \
    apache-airflow-providers-apache-spark \
    psycopg2-binary \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.9.txt"
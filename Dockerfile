FROM apache/airflow:3.3.0

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt

RUN python -m pip install --upgrade pip \
    && pip install -r /requirements.txt

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark pyspark
# docker image [gcp]
# https://console.cloud.google.com/gcr/images/spark-operator

FROM gcr.io/spark-operator/spark-py:v3.1.1-hadoop3
MAINTAINER Luan Moreno M. Maciel <luan.moreno@owshq.com>

# using root user
USER root:root

# create directory for apps
RUN mkdir -p /app

# pip install
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir delta-spark

# copy spark program
COPY etl-enriched-users-analysis.py /app/

# copy jar files
COPY ../iceberg/kubernetes/jars /opt/spark/jars

# set work directory
WORKDIR /app

# user
USER 1001
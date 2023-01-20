# docker image [gcp]
# https://console.cloud.google.com/gcr/images/spark-operator
# gcr.io/datamechanics/spark:platform-3.2-latest
FROM gcr.io/datamechanics/spark:platform-3.2-latest

# using root user
USER root:root

# create directory for apps
RUN mkdir -p /app

# pip install
RUN pip install --no-cache-dir --upgrade pip

# copy spark program
COPY prod/jobs/ /app/

# copy jar files
COPY jars/ /opt/spark/jars

# set work directory
WORKDIR /app

# user
USER 1001
#!/bin/bash

spark-submit \
  --conf "spark.jars.ivy=/opt/bitnami/spark/.ivy2" \
  --conf "spark.executor.extraJavaOptions=-Djava.security.krb5.conf=/path/to/krb5.conf" \
  --conf "spark.driver.extraJavaOptions=-Djava.security.krb5.conf=/path/to/krb5.conf" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 \
  /opt/bitnami/spark/jobs/spark_job.py stocks

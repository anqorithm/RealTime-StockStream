#!/bin/bash
set -e

/opt/bitnami/scripts/kafka/entrypoint.sh /opt/bitnami/scripts/kafka/run.sh &

echo "Waiting for Kafka to be up..."
sleep 10

kafka-topics.sh --create --topic stocks --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

wait
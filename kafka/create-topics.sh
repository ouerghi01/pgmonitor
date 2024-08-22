#!/bin/bash

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
cub kafka-ready -b localhost:9092 1 20

# Create topics
echo "Creating topics..."
kafka-topics --create --topic db-monitoring --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
kafka-topics --create --topic query-monitoring --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181

echo "Topics created."

# Start Kafka
exec /opt/bitnami/scripts/kafka/run.sh

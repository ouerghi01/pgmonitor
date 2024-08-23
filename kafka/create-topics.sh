#!/bin/bash

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 4

# Create topics if they don't already exist
echo "Creating topics..."
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic db-monitoring --if-not-exists
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic query-monitoring --if-not-exists

echo "Topics created."

# Start Kafka using the default run script
exec /opt/bitnami/scripts/kafka/run.sh

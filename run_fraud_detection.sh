#!/bin/bash

# Spark Job Runner for Fraud Detection

echo "Starting Spark Fraud Detection Job..."

# Check if Kafka is running
if ! docker ps | grep -q kafka; then
    echo "ERROR: Kafka is not running. Please start with: ./kafka-manager.sh start"
    exit 1
fi

# Check if Spark container exists
if ! docker ps | grep -q spark-master; then
    echo "Starting Spark container..."
    docker-compose up -d spark
    echo "Waiting for Spark to be ready..."
    sleep 15
fi

echo "Submitting Spark job..."
echo "Spark UI available at: http://localhost:8081"
echo "Press Ctrl+C to stop"

# Submit the Spark job
docker exec spark-master spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    --master local[*] \
    /opt/spark_apps/fraud_detector.py

echo "Spark job completed!"

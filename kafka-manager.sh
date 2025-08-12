#!/bin/bash

# Kafka Management Script for Fraud Detection Project

# Check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        echo "ERROR: Docker is not running!"
        echo "Please start Docker Desktop and try again"
        exit 1
    fi
}

# Determine docker compose command 
DOCKER_COMPOSE_CMD="docker compose"
if ! command -v docker compose &> /dev/null; then
    if command -v docker-compose &> /dev/null; then
        DOCKER_COMPOSE_CMD="docker-compose"
    else
        echo "ERROR: Neither 'docker compose' nor 'docker-compose' found!"
        echo "Please install Docker with Compose support"
        exit 1
    fi
fi

case "$1" in
    "start")
        check_docker
        echo "Starting Kafka infrastructure..."
        $DOCKER_COMPOSE_CMD up -d
        echo "Waiting for Kafka to be ready..."
        sleep 15
        echo "Kafka should be ready!"
        echo "Kafka UI available at: http://localhost:8080"
        ;;
    "stop")
        echo "Stopping Kafka infrastructure..."
        $DOCKER_COMPOSE_CMD down
        ;;
    "status")
        echo "Kafka infrastructure status:"
        $DOCKER_COMPOSE_CMD ps
        ;;
    "logs")
        echo "Showing Kafka logs..."
        $DOCKER_COMPOSE_CMD logs kafka
        ;;
    "create-topic")
        echo "Creating 'transactions' topic..."
        docker exec kafka kafka-topics --create \
            --topic transactions \
            --bootstrap-server localhost:9092 \
            --partitions 3 \
            --replication-factor 1
        echo "Topic created!"
        ;;
    "list-topics")
        echo "Listing Kafka topics..."
        docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
        ;;
    "consume")
        echo "Consuming messages from 'transactions' topic..."
        echo "Press Ctrl+C to stop"
        docker exec -it kafka kafka-console-consumer \
            --topic transactions \
            --bootstrap-server localhost:9092 \
            --from-beginning
        ;;
    *)
        echo "Kafka Management Script"
        echo "Usage: $0 {start|stop|status|logs|create-topic|list-topics|consume}"
        echo ""
        echo "Commands:"
        echo "  start        - Start Kafka infrastructure"
        echo "  stop         - Stop Kafka infrastructure" 
        echo "  status       - Show status of containers"
        echo "  logs         - Show Kafka logs"
        echo "  create-topic - Create the transactions topic"
        echo "  list-topics  - List all Kafka topics"
        echo "  consume      - Listen to transactions in real-time"
        echo ""
        echo "Prerequisites:"
        echo "  - Docker Desktop must be running"
        echo "  - Docker Compose must be available"
        ;;
esac

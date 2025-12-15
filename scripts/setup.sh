#!/bin/bash

# Setup script for Real-Time Order & Delivery Event Platform
# This script performs initial setup and validation

set -e

COLOR_GREEN='\033[0;32m'
COLOR_RED='\033[0;31m'
COLOR_YELLOW='\033[1;33m'
COLOR_NC='\033[0m' # No Color

echo -e "${COLOR_GREEN}========================================${COLOR_NC}"
echo -e "${COLOR_GREEN}Real-Time Order & Delivery Platform${COLOR_NC}"
echo -e "${COLOR_GREEN}Initial Setup Script${COLOR_NC}"
echo -e "${COLOR_GREEN}========================================${COLOR_NC}"
echo ""

# Check prerequisites
check_prerequisites() {
    echo -e "${COLOR_YELLOW}Checking prerequisites...${COLOR_NC}"
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        echo -e "${COLOR_RED}ERROR: Docker is not installed${COLOR_NC}"
        exit 1
    fi
    echo -e "${COLOR_GREEN}✓ Docker found: $(docker --version)${COLOR_NC}"
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        echo -e "${COLOR_RED}ERROR: Docker Compose is not installed${COLOR_NC}"
        exit 1
    fi
    echo -e "${COLOR_GREEN}✓ Docker Compose found: $(docker-compose --version)${COLOR_NC}"
    
    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        echo -e "${COLOR_RED}ERROR: Docker daemon is not running${COLOR_NC}"
        exit 1
    fi
    echo -e "${COLOR_GREEN}✓ Docker daemon is running${COLOR_NC}"
    
    # Check Java (optional, for building Flink jobs)
    if command -v java &> /dev/null; then
        echo -e "${COLOR_GREEN}✓ Java found: $(java -version 2>&1 | head -n 1)${COLOR_NC}"
    else
        echo -e "${COLOR_YELLOW}⚠ Java not found (needed for building Flink jobs)${COLOR_NC}"
    fi
    
    # Check Maven (optional)
    if command -v mvn &> /dev/null; then
        echo -e "${COLOR_GREEN}✓ Maven found: $(mvn --version | head -n 1)${COLOR_NC}"
    else
        echo -e "${COLOR_YELLOW}⚠ Maven not found (needed for building Flink jobs)${COLOR_NC}"
    fi
    
    echo ""
}

# Create environment file
setup_env() {
    echo -e "${COLOR_YELLOW}Setting up environment variables...${COLOR_NC}"
    
    if [ ! -f .env ]; then
        cp .env.example .env
        echo -e "${COLOR_GREEN}✓ Created .env file from .env.example${COLOR_NC}"
    else
        echo -e "${COLOR_YELLOW}⚠ .env file already exists, skipping${COLOR_NC}"
    fi
    
    echo ""
}

# Start services
start_services() {
    echo -e "${COLOR_YELLOW}Starting services...${COLOR_NC}"
    echo "This may take 2-3 minutes for first-time setup..."
    echo ""
    
    docker-compose up -d
    
    echo ""
    echo -e "${COLOR_GREEN}✓ Services started${COLOR_NC}"
    echo ""
}

# Wait for services to be ready
wait_for_services() {
    echo -e "${COLOR_YELLOW}Waiting for services to be ready...${COLOR_NC}"
    echo "This may take 1-2 minutes..."
    
    local max_attempts=60
    local attempt=1
    
    # Wait for Kafka
    while [ $attempt -le $max_attempts ]; do
        if docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &> /dev/null; then
            echo -e "${COLOR_GREEN}✓ Kafka is ready${COLOR_NC}"
            break
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    if [ $attempt -gt $max_attempts ]; then
        echo -e "${COLOR_RED}ERROR: Kafka failed to start${COLOR_NC}"
        return 1
    fi
    
    # Wait for MinIO
    attempt=1
    while [ $attempt -le $max_attempts ]; do
        if curl -sf http://localhost:9000/minio/health/live &> /dev/null; then
            echo -e "${COLOR_GREEN}✓ MinIO is ready${COLOR_NC}"
            break
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    # Wait for Prometheus
    attempt=1
    while [ $attempt -le $max_attempts ]; do
        if curl -sf http://localhost:9090/-/healthy &> /dev/null; then
            echo -e "${COLOR_GREEN}✓ Prometheus is ready${COLOR_NC}"
            break
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    # Wait for Grafana
    attempt=1
    while [ $attempt -le $max_attempts ]; do
        if curl -sf http://localhost:3000/api/health &> /dev/null; then
            echo -e "${COLOR_GREEN}✓ Grafana is ready${COLOR_NC}"
            break
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo ""
}

# Create Kafka topics
create_topics() {
    echo -e "${COLOR_YELLOW}Creating Kafka topics...${COLOR_NC}"
    
    docker-compose exec -T kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 3 \
        --topic order-events \
        --if-not-exists
    
    docker-compose exec -T kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 3 \
        --topic delivery-events \
        --if-not-exists
    
    echo -e "${COLOR_GREEN}✓ Kafka topics created${COLOR_NC}"
    echo ""
}

# Show service URLs
show_urls() {
    echo ""
    echo -e "${COLOR_GREEN}========================================${COLOR_NC}"
    echo -e "${COLOR_GREEN}Setup Complete!${COLOR_NC}"
    echo -e "${COLOR_GREEN}========================================${COLOR_NC}"
    echo ""
    echo "Service URLs:"
    echo "  Grafana:         http://localhost:3000 (admin/admin)"
    echo "  Prometheus:      http://localhost:9090"
    echo "  Flink Web UI:    http://localhost:8082"
    echo "  Airflow:         http://localhost:8083 (airflow/airflow)"
    echo "  MinIO Console:   http://localhost:9001 (admin/password)"
    echo "  Trino:           http://localhost:8080"
    echo "  Schema Registry: http://localhost:8081"
    echo ""
    echo "Next steps:"
    echo "  1. Build Flink jobs: make build-flink"
    echo "  2. Submit jobs: make submit-kpi-aggregator"
    echo "  3. Test Kafka: make test-kafka"
    echo "  4. View logs: make logs"
    echo ""
    echo "For more commands, run: make help"
    echo ""
}

# Main execution
main() {
    check_prerequisites
    setup_env
    start_services
    wait_for_services
    create_topics
    show_urls
}

# Run main function
main

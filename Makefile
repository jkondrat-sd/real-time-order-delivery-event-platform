.PHONY: help up down restart logs clean build-flink test-kafka create-topics

# Default target
help:
	@echo "Real-Time Order & Delivery Event Platform - Makefile Commands"
	@echo "=============================================================="
	@echo ""
	@echo "Infrastructure Commands:"
	@echo "  make up              - Start all services"
	@echo "  make down            - Stop all services"
	@echo "  make restart         - Restart all services"
	@echo "  make logs            - View logs from all services"
	@echo "  make logs-kafka      - View Kafka logs"
	@echo "  make logs-flink      - View Flink logs"
	@echo "  make logs-airflow    - View Airflow logs"
	@echo "  make clean           - Stop services and remove volumes"
	@echo ""
	@echo "Build Commands:"
	@echo "  make build-flink     - Build Flink jobs"
	@echo "  make build-all       - Build all components"
	@echo ""
	@echo "Kafka Commands:"
	@echo "  make create-topics   - Create Kafka topics"
	@echo "  make test-kafka      - Test Kafka producer/consumer"
	@echo "  make list-topics     - List all Kafka topics"
	@echo ""
	@echo "Monitoring Commands:"
	@echo "  make open-grafana    - Open Grafana dashboard"
	@echo "  make open-prometheus - Open Prometheus UI"
	@echo "  make open-flink      - Open Flink Web UI"
	@echo "  make open-airflow    - Open Airflow Web UI"
	@echo "  make open-minio      - Open MinIO Console"
	@echo ""
	@echo "Development Commands:"
	@echo "  make status          - Check status of all services"
	@echo "  make shell-kafka     - Open Kafka container shell"
	@echo "  make shell-flink     - Open Flink JobManager shell"
	@echo ""

# Infrastructure Commands
up:
	@echo "Starting all services..."
	docker-compose up -d
	@echo ""
	@echo "Services are starting up. Please wait a moment..."
	@echo "You can check status with: make status"
	@echo ""
	@echo "Service URLs:"
	@echo "  Grafana:        http://localhost:3000 (admin/admin)"
	@echo "  Prometheus:     http://localhost:9090"
	@echo "  Flink:          http://localhost:8082"
	@echo "  Airflow:        http://localhost:8083 (airflow/airflow)"
	@echo "  MinIO Console:  http://localhost:9001 (admin/password)"
	@echo "  Trino:          http://localhost:8080"
	@echo "  Schema Registry: http://localhost:8081"

down:
	@echo "Stopping all services..."
	docker-compose down

restart: down up

logs:
	docker-compose logs -f

logs-kafka:
	docker-compose logs -f kafka

logs-flink:
	docker-compose logs -f flink-jobmanager flink-taskmanager

logs-airflow:
	docker-compose logs -f airflow-webserver airflow-scheduler

clean:
	@echo "Stopping services and removing volumes..."
	@echo "WARNING: This will delete all data!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v; \
		echo "Cleanup complete"; \
	else \
		echo "Cleanup cancelled"; \
	fi

# Build Commands
build-flink:
	@echo "Building Flink KPI Aggregator..."
	cd flink/kpi-aggregator && mvn clean package
	@echo ""
	@echo "Building Flink Complex Event Detector..."
	cd flink/complex-event-detector && mvn clean package
	@echo ""
	@echo "Flink jobs built successfully!"

build-all: build-flink

# Kafka Commands
create-topics:
	@echo "Creating Kafka topics..."
	docker-compose exec kafka kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--replication-factor 1 \
		--partitions 3 \
		--topic order-events \
		--if-not-exists
	docker-compose exec kafka kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--replication-factor 1 \
		--partitions 3 \
		--topic delivery-events \
		--if-not-exists
	@echo "Topics created successfully!"

list-topics:
	docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

test-kafka:
	@echo "Testing Kafka - sending test message to order-events topic..."
	@echo '{"order_id":"TEST-001","customer_id":"CUST-001","amount":99.99,"timestamp":"'$$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}' | \
		docker-compose exec -T kafka kafka-console-producer \
		--broker-list localhost:9092 \
		--topic order-events
	@echo ""
	@echo "Message sent! To consume messages, run:"
	@echo "docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic order-events --from-beginning --max-messages 1"

# Monitoring Commands
open-grafana:
	@echo "Opening Grafana at http://localhost:3000"
	@echo "Default credentials: admin/admin"
	@command -v xdg-open > /dev/null && xdg-open http://localhost:3000 || open http://localhost:3000 || echo "Please open http://localhost:3000 in your browser"

open-prometheus:
	@echo "Opening Prometheus at http://localhost:9090"
	@command -v xdg-open > /dev/null && xdg-open http://localhost:9090 || open http://localhost:9090 || echo "Please open http://localhost:9090 in your browser"

open-flink:
	@echo "Opening Flink Web UI at http://localhost:8082"
	@command -v xdg-open > /dev/null && xdg-open http://localhost:8082 || open http://localhost:8082 || echo "Please open http://localhost:8082 in your browser"

open-airflow:
	@echo "Opening Airflow at http://localhost:8083"
	@echo "Default credentials: airflow/airflow"
	@command -v xdg-open > /dev/null && xdg-open http://localhost:8083 || open http://localhost:8083 || echo "Please open http://localhost:8083 in your browser"

open-minio:
	@echo "Opening MinIO Console at http://localhost:9001"
	@echo "Default credentials: admin/password"
	@command -v xdg-open > /dev/null && xdg-open http://localhost:9001 || open http://localhost:9001 || echo "Please open http://localhost:9001 in your browser"

# Development Commands
status:
	@echo "Service Status:"
	@docker-compose ps

shell-kafka:
	docker-compose exec kafka /bin/bash

shell-flink:
	docker-compose exec flink-jobmanager /bin/bash

# Submit Flink jobs
submit-kpi-aggregator:
	@echo "Submitting KPI Aggregator job to Flink..."
	docker-compose exec flink-jobmanager flink run \
		/opt/flink/usrlib/kpi-aggregator/target/kpi-aggregator-1.0.0.jar

submit-complex-event-detector:
	@echo "Submitting Complex Event Detector job to Flink..."
	docker-compose exec flink-jobmanager flink run \
		/opt/flink/usrlib/complex-event-detector/target/complex-event-detector-1.0.0.jar

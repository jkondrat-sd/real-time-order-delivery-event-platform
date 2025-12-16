SHELL := /bin/bash

# ==============================================================================
# Real-Time Order Delivery Event Platform - Makefile
# ==============================================================================
# For cross-platform support (Windows), use: python -m tasks <command>
# Or PowerShell scripts in scripts/ folder

.PHONY: up down demo demo-full lint test build doctor clean \
        deploy-jobs start-airflow stop-airflow \
        health screenshot trino-shell flink-ui grafana-ui

COMPOSE_FILE := local/docker-compose.yml
PYTHON := python

# ==============================================================================
# Core Infrastructure Commands
# ==============================================================================

up:
	docker compose -f $(COMPOSE_FILE) up -d
	@echo "Waiting for services to initialize..."
	@sleep 30
	@echo "Services started. Access points:"
	@echo "  - Flink UI:      http://localhost:8081"
	@echo "  - Trino UI:      http://localhost:8082"
	@echo "  - Grafana:       http://localhost:3000"
	@echo "  - Prometheus:    http://localhost:9090"
	@echo "  - MinIO Console: http://localhost:9001"
	@echo "  - Airflow:       http://localhost:8085"

down:
	docker compose -f $(COMPOSE_FILE) down

clean:
	docker compose -f $(COMPOSE_FILE) down -v --remove-orphans
	@echo "All containers and volumes removed"

build:
	docker compose -f $(COMPOSE_FILE) build

# ==============================================================================
# Demo Commands (Reproducible Screenshots)
# ==============================================================================

demo: up
	@echo "============================================"
	@echo "Generating 100 sample events (dry mode)"
	@echo "============================================"
	$(PYTHON) services/event_generator/main.py --events 100 --dry-run

demo-full: up deploy-jobs
	@echo "============================================"
	@echo "FULL DEMO - Generating 500 live events"
	@echo "============================================"
	@echo "Waiting for Flink jobs to be ready..."
	@sleep 30
	$(PYTHON) services/event_generator/main.py --events 500 --topic orders.events --brokers localhost:29092 --rate-limit 10
	@echo ""
	@echo "Events generated! Wait 30s then query:"
	@echo "  make trino-shell"
	@echo "  > SELECT COUNT(*) FROM iceberg.lakehouse.bronze_order_events;"
	@echo ""
	@echo "View dashboards at:"
	@echo "  - Flink UI:  http://localhost:8081"
	@echo "  - Grafana:   http://localhost:3000"

demo-quick:
	@echo "============================================"
	@echo "Quick Demo - 50 events"
	@echo "============================================"
	$(PYTHON) services/event_generator/main.py --events 50 --topic orders.events --brokers localhost:29092 --rate-limit 20

deploy-jobs:
	@echo "============================================"
	@echo "Deploying Flink Jobs"
	@echo "============================================"
	@docker exec local-flink-jobmanager-1 /opt/flink/bin/flink run -d -py /opt/flink/jobs/bronze_writer.py || true
	@sleep 5
	@docker exec local-flink-jobmanager-1 /opt/flink/bin/flink run -d -py /opt/flink/jobs/silver_order_state.py || true
	@sleep 5
	@docker exec local-flink-jobmanager-1 /opt/flink/bin/flink run -d -py /opt/flink/jobs/gold_kpi_aggregates.py || true
	@sleep 5
	@docker exec local-flink-jobmanager-1 /opt/flink/bin/flink run -d -py /opt/flink/jobs/kpi_aggregator.py || true
	@sleep 5
	@docker exec local-flink-jobmanager-1 /opt/flink/bin/flink run -d -py /opt/flink/jobs/alert_detector.py || true
	@echo "All jobs submitted. Check http://localhost:8081 for status."

# ==============================================================================
# Airflow Commands
# ==============================================================================

start-airflow:
	docker compose -f $(COMPOSE_FILE) up -d airflow-init airflow-scheduler airflow-webserver
	@echo "Airflow starting... Access at http://localhost:8085"
	@echo "Credentials: admin / admin"

stop-airflow:
	docker compose -f $(COMPOSE_FILE) stop airflow-scheduler airflow-webserver

# ==============================================================================
# Development Commands
# ==============================================================================

env:
	./scripts/create_conda_env.sh

lint:
	ruff check .

format:
	ruff format .

type-check:
	mypy services/ flink/jobs/ airflow/dags/ --ignore-missing-imports

test:
	pytest tests/ -v

test-integration:
	pytest tests/test_integration.py -v

doctor:
	$(PYTHON) -m tasks doctor

# ==============================================================================
# Interactive Commands
# ==============================================================================

trino-shell:
	@echo "Connecting to Trino..."
	docker exec -it local-trino-1 trino

flink-ui:
	@echo "Opening Flink UI at http://localhost:8081"
	@python -m webbrowser http://localhost:8081 || open http://localhost:8081 || xdg-open http://localhost:8081

grafana-ui:
	@echo "Opening Grafana at http://localhost:3000"
	@python -m webbrowser http://localhost:3000 || open http://localhost:3000 || xdg-open http://localhost:3000

# ==============================================================================
# Health & Status Commands
# ==============================================================================

health:
	@echo "============================================"
	@echo "Service Health Check"
	@echo "============================================"
	@echo "Flink JobManager:"
	@curl -s http://localhost:8081/overview | python -m json.tool 2>/dev/null || echo "  Not responding"
	@echo ""
	@echo "Trino:"
	@curl -s http://localhost:8082/v1/info | python -m json.tool 2>/dev/null || echo "  Not responding"
	@echo ""
	@echo "Redpanda:"
	@docker exec local-redpanda-1 rpk cluster health 2>/dev/null || echo "  Not responding"
	@echo ""
	@echo "Running Containers:"
	@docker compose -f $(COMPOSE_FILE) ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

jobs:
	@echo "============================================"
	@echo "Flink Running Jobs"
	@echo "============================================"
	@curl -s http://localhost:8081/jobs | python -m json.tool 2>/dev/null || echo "Flink not responding"

topics:
	@echo "============================================"
	@echo "Kafka Topics"
	@echo "============================================"
	@docker exec local-redpanda-1 rpk topic list

tables:
	@echo "============================================"
	@echo "Iceberg Tables"
	@echo "============================================"
	@docker exec local-trino-1 trino --execute "SHOW TABLES FROM iceberg.lakehouse" 2>/dev/null || echo "Trino not responding"

# ==============================================================================
# Screenshot Helper Commands (for Portfolio)
# ==============================================================================

screenshot-prep:
	@echo "============================================"
	@echo "Preparing for Screenshots"
	@echo "============================================"
	@echo "1. Starting services and generating data..."
	make up
	@sleep 60
	make deploy-jobs
	@sleep 30
	$(PYTHON) services/event_generator/main.py --events 200 --topic orders.events --brokers localhost:29092 --rate-limit 5
	@sleep 60
	@echo ""
	@echo "2. Ready for screenshots! Open these URLs:"
	@echo "   - Flink UI:     http://localhost:8081 (show 5 running jobs)"
	@echo "   - Grafana:      http://localhost:3000 (import dashboards/orders_kpi_dashboard.json)"
	@echo "   - Trino query:  make trino-shell then run queries from sql/example_queries.sql"
	@echo "   - Airflow:      http://localhost:8085 (show DAGs)"

# ==============================================================================
# CI/CD Commands
# ==============================================================================

ci-lint:
	ruff check . --output-format=github
	ruff format --check .

ci-test:
	pytest tests/ -v --tb=short

ci-all: ci-lint ci-test
	@echo "All CI checks passed!"

# ==============================================================================
# Help
# ==============================================================================

help:
	@echo "Real-Time Order Delivery Event Platform"
	@echo "========================================"
	@echo ""
	@echo "Quick Start:"
	@echo "  make up           - Start all services"
	@echo "  make deploy-jobs  - Deploy Flink streaming jobs"
	@echo "  make demo-full    - Full demo with live events"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make up           - Start Docker services"
	@echo "  make down         - Stop services (keep data)"
	@echo "  make clean        - Stop and remove all data"
	@echo "  make health       - Check service health"
	@echo ""
	@echo "Demo:"
	@echo "  make demo         - Dry run (no Kafka)"
	@echo "  make demo-full    - Full pipeline demo"
	@echo "  make demo-quick   - Quick 50-event test"
	@echo ""
	@echo "Interactive:"
	@echo "  make trino-shell  - Open Trino CLI"
	@echo "  make flink-ui     - Open Flink UI in browser"
	@echo "  make grafana-ui   - Open Grafana in browser"
	@echo ""
	@echo "Development:"
	@echo "  make lint         - Run ruff linter"
	@echo "  make format       - Format code with ruff"
	@echo "  make test         - Run tests"
	@echo "  make type-check   - Run mypy"
	@echo ""
	@echo "Status:"
	@echo "  make jobs         - List Flink jobs"
	@echo "  make topics       - List Kafka topics"
	@echo "  make tables       - List Iceberg tables"


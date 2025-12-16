#!/bin/bash
# Complete demo startup script for Real-Time Order Delivery Event Platform
# 
# Usage: ./scripts/start_demo.sh

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "========================================"
echo " Real-Time Order Delivery Event Platform"
echo " Complete Demo Startup"
echo "========================================"
echo ""

# Step 1: Start Docker services
echo "[1/5] Starting Docker services..."
cd "$PROJECT_ROOT/local"
docker compose up -d --build
echo "✓ Docker services started"
echo ""

# Step 2: Wait for services to be ready
echo "[2/5] Waiting for services to initialize (60 seconds)..."
sleep 60
echo "✓ Services should be ready"
echo ""

# Step 3: Create Kafka topics if not exist
echo "[3/5] Ensuring Kafka topics exist..."
docker exec local-redpanda-1 rpk topic create orders.events --partitions 1 2>/dev/null || true
docker exec local-redpanda-1 rpk topic create alerts.events --partitions 1 2>/dev/null || true
docker exec local-redpanda-1 rpk topic create kpi.minute --partitions 1 2>/dev/null || true
echo "✓ Topics ready (orders.events, alerts.events, kpi.minute)"
echo ""

# Step 4: Deploy Flink jobs
echo "[4/5] Deploying Flink streaming jobs..."

jobs=(
    "/opt/flink/jobs/bronze_writer.py"
    "/opt/flink/jobs/silver_order_state.py"
    "/opt/flink/jobs/gold_kpi_aggregates.py"
    "/opt/flink/jobs/kpi_aggregator.py"
    "/opt/flink/jobs/alert_detector.py"
)

for job in "${jobs[@]}"; do
    job_name=$(basename "$job" .py)
    echo -n "  Submitting $job_name..."
    output=$(docker exec local-flink-jobmanager-1 /opt/flink/bin/flink run -py "$job" 2>&1)
    if echo "$output" | grep -q "Job has been submitted"; then
        echo " ✓"
    else
        echo " ✗"
    fi
done
echo ""

# Step 5: Generate sample events
echo "[5/5] Generating sample events..."
cd "$PROJECT_ROOT"
python services/event_generator/main.py --events 100 --topic orders.events --brokers localhost:29092 2>/dev/null
echo "✓ 100 sample events generated"
echo ""

# Summary
echo "========================================"
echo " Demo Ready!"
echo "========================================"
echo ""
echo "Access the platform:"
echo "  • Flink Dashboard:   http://localhost:8081"
echo "  • Redpanda Console:  http://localhost:8080"
echo "  • Trino SQL:         http://localhost:8082"
echo "  • Grafana:           http://localhost:3000"
echo "  • Prometheus:        http://localhost:9090"
echo "  • Alertmanager:      http://localhost:9093"
echo "  • MinIO Console:     http://localhost:9001"
echo ""
echo "Query Iceberg tables in Trino:"
echo "  docker exec -it local-trino-1 trino"
echo ""
echo "  SELECT * FROM iceberg.lakehouse.bronze_order_events LIMIT 10;"
echo "  SELECT * FROM iceberg.lakehouse.silver_order_state LIMIT 10;"
echo "  SELECT * FROM iceberg.lakehouse.gold_alerts LIMIT 10;"
echo ""
echo "View alerts in real-time:"
echo "  docker exec local-redpanda-1 rpk topic consume alerts.events"
echo ""
echo "To stop: docker compose -f local/docker-compose.yml down"

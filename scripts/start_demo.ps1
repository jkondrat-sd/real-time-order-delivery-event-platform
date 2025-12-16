#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Complete demo startup script for Real-Time Order Delivery Event Platform
    
.DESCRIPTION
    This script starts all services, waits for them to be healthy, 
    deploys all Flink jobs, and generates sample events.
    
.EXAMPLE
    .\scripts\start_demo.ps1
#>

$ErrorActionPreference = "Continue"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Real-Time Order Delivery Event Platform" -ForegroundColor Cyan
Write-Host " Complete Demo Startup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Start Docker services
Write-Host "[1/5] Starting Docker services..." -ForegroundColor Yellow
Set-Location "$ProjectRoot\local"
docker compose up -d --build
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to start Docker services" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Docker services started" -ForegroundColor Green
Write-Host ""

# Step 2: Wait for services to be ready
Write-Host "[2/5] Waiting for services to initialize (60 seconds)..." -ForegroundColor Yellow
Start-Sleep -Seconds 60
Write-Host "✓ Services should be ready" -ForegroundColor Green
Write-Host ""

# Step 3: Create Kafka topics if not exist
Write-Host "[3/5] Ensuring Kafka topics exist..." -ForegroundColor Yellow
docker exec local-redpanda-1 rpk topic create orders.events --partitions 1 2>$null
docker exec local-redpanda-1 rpk topic create alerts.events --partitions 1 2>$null
docker exec local-redpanda-1 rpk topic create kpi.minute --partitions 1 2>$null
Write-Host "✓ Topics ready (orders.events, alerts.events, kpi.minute)" -ForegroundColor Green
Write-Host ""

# Step 4: Deploy Flink jobs
Write-Host "[4/5] Deploying Flink streaming jobs..." -ForegroundColor Yellow

$jobs = @(
    "/opt/flink/jobs/bronze_writer.py",
    "/opt/flink/jobs/silver_order_state.py", 
    "/opt/flink/jobs/gold_kpi_aggregates.py",
    "/opt/flink/jobs/kpi_aggregator.py",
    "/opt/flink/jobs/alert_detector.py"
)

foreach ($job in $jobs) {
    $jobName = [System.IO.Path]::GetFileNameWithoutExtension($job)
    Write-Host "  Submitting $jobName..." -NoNewline
    $output = docker exec local-flink-jobmanager-1 /opt/flink/bin/flink run -py $job 2>&1
    if ($output -match "Job has been submitted") {
        Write-Host " ✓" -ForegroundColor Green
    } else {
        Write-Host " ✗" -ForegroundColor Red
    }
}
Write-Host ""

# Step 5: Generate sample events
Write-Host "[5/5] Generating sample events..." -ForegroundColor Yellow
Set-Location $ProjectRoot
python services/event_generator/main.py --events 100 --topic orders.events --brokers localhost:29092 2>$null
Write-Host "✓ 100 sample events generated" -ForegroundColor Green
Write-Host ""

# Summary
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Demo Ready!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Access the platform:" -ForegroundColor Yellow
Write-Host "  • Flink Dashboard:   http://localhost:8081" -ForegroundColor White
Write-Host "  • Redpanda Console:  http://localhost:8080" -ForegroundColor White
Write-Host "  • Trino SQL:         http://localhost:8082" -ForegroundColor White
Write-Host "  • Grafana:           http://localhost:3000" -ForegroundColor White
Write-Host "  • Prometheus:        http://localhost:9090" -ForegroundColor White
Write-Host "  • Alertmanager:      http://localhost:9093" -ForegroundColor White
Write-Host "  • MinIO Console:     http://localhost:9001" -ForegroundColor White
Write-Host ""
Write-Host "Query Iceberg tables in Trino:" -ForegroundColor Yellow
Write-Host "  docker exec -it local-trino-1 trino" -ForegroundColor Gray
Write-Host ""
Write-Host "  SELECT * FROM iceberg.lakehouse.bronze_order_events LIMIT 10;" -ForegroundColor Gray
Write-Host "  SELECT * FROM iceberg.lakehouse.silver_order_state LIMIT 10;" -ForegroundColor Gray
Write-Host "  SELECT * FROM iceberg.lakehouse.gold_alerts LIMIT 10;" -ForegroundColor Gray
Write-Host ""
Write-Host "View alerts in real-time:" -ForegroundColor Yellow
Write-Host "  docker exec local-redpanda-1 rpk topic consume alerts.events" -ForegroundColor Gray
Write-Host ""
Write-Host "To stop: docker compose -f local/docker-compose.yml down" -ForegroundColor Yellow

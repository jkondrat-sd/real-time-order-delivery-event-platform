# Complete Phase 2 Pipeline Test Script
# Automates the entire workflow: start services → generate events → deploy jobs → query lakehouse

param(
    [int]$NumEvents = 1000,
    [switch]$SkipRestart,
    [switch]$SkipEvents,
    [switch]$SkipJobs,
    [switch]$SkipQueries
)

$ErrorActionPreference = "Continue"

Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 59) -ForegroundColor Cyan
Write-Host "🚀 Phase 2 Complete Pipeline Test" -ForegroundColor Cyan
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 59) -ForegroundColor Cyan
Write-Host ""

# Debug: show flags for visibility
Write-Host "Flags: SkipRestart=$SkipRestart SkipEvents=$SkipEvents SkipJobs=$SkipJobs SkipQueries=$SkipQueries" -ForegroundColor DarkGray

# Step 1: Restart services
if (-not $PSBoundParameters.ContainsKey('SkipRestart')) {
    Write-Host "📦 Step 1: Restarting Docker services..." -ForegroundColor Yellow
    python -m tasks down
    Start-Sleep -Seconds 3
    python -m tasks up
    
    Write-Host "⏳ Waiting for services to initialize..." -ForegroundColor Gray
    Write-Host "   Checking Redpanda health..." -ForegroundColor Gray
    
    # Wait up to 60 seconds for Redpanda to be ready
    $maxWait = 60
    $waited = 0
    $redpandaReady = $false
    
    while ($waited -lt $maxWait) {
        $result = docker exec local-redpanda-1 rpk cluster health 2>$null
        if ($result -match "Healthy:\s*true") {
            $redpandaReady = $true
            break
        }
        Start-Sleep -Seconds 5
        $waited += 5
        Write-Host "   Waited $waited seconds..." -ForegroundColor Gray
    }
    
    if ($redpandaReady) {
        Write-Host "   ✅ Redpanda is ready" -ForegroundColor Green
        Write-Host "   Waiting 10 more seconds for other services..." -ForegroundColor Gray
        Start-Sleep -Seconds 10
    } else {
        Write-Host "   ⚠️  Redpanda may not be ready yet, continuing anyway..." -ForegroundColor Yellow
    }
    
    Write-Host "✅ Services restarted" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "⏭️  Skipping service restart" -ForegroundColor Gray
    Write-Host ""
}

# Step 2: Generate events
# Debug: show parameter binding keys and decision
Write-Host "DEBUG: PSBoundParameters keys: $($PSBoundParameters.Keys -join ',')" -ForegroundColor DarkGray
$shouldGenerate = -not $PSBoundParameters.ContainsKey('SkipEvents')
Write-Host "DEBUG: shouldGenerate = $shouldGenerate" -ForegroundColor DarkGray
if ($shouldGenerate) {
    Write-Host "📤 Step 2: Generating $NumEvents test events..." -ForegroundColor Yellow
    
    # Verify Kafka is accessible before generating events
    Write-Host "   Checking Kafka connectivity..." -ForegroundColor Gray
    $kafkaReady = $false
    $maxRetries = 3
    $retry = 0
    
    while ($retry -lt $maxRetries) {
        $topics = docker exec local-redpanda-1 rpk topic list 2>$null
        if ($topics) {
            $kafkaReady = $true
            Write-Host "   ✅ Kafka is accessible" -ForegroundColor Green
            break
        }
        $retry++
        if ($retry -lt $maxRetries) {
            Write-Host "   Retry $retry/$maxRetries..." -ForegroundColor Gray
            Start-Sleep -Seconds 5
        }
    }
    
    if (-not $kafkaReady) {
        Write-Host "   ⚠️  Kafka not accessible, events may fail" -ForegroundColor Yellow
        Write-Host "   Tip: Check 'docker logs local-redpanda-1'" -ForegroundColor Gray
    }
    
    python services/event_generator/main.py `
        --events $NumEvents `
        --topic orders.events `
        --brokers localhost:29092
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Events generated successfully" -ForegroundColor Green
    } else {
        Write-Host "❌ Event generation failed - Kafka may not be ready" -ForegroundColor Red
        Write-Host "   Try waiting longer or check: docker logs local-redpanda-1" -ForegroundColor Yellow
    }
    Write-Host ""
} else {
    Write-Host "⏭️  Skipping event generation" -ForegroundColor Gray
    Write-Host ""
}

# Step 3: Deploy Flink jobs
if (-not $PSBoundParameters.ContainsKey('SkipJobs')) {
    Write-Host "🔧 Step 3: Deploying PyFlink jobs to cluster..." -ForegroundColor Yellow
    $deployOutput = & ".\scripts\deploy_flink_jobs.ps1" 2>&1
    Write-Host "$deployOutput" -ForegroundColor Gray

    Write-Host "⏳ Waiting 20 seconds for jobs to start processing..." -ForegroundColor Gray
    Start-Sleep -Seconds 20

    # Check for running jobs
    $flinkJobs = docker exec local-flink-jobmanager-1 /opt/flink/bin/flink list 2>$null
    if ($flinkJobs -and $flinkJobs -notmatch "No running jobs") {
        Write-Host "✅ Flink jobs are running" -ForegroundColor Green
    } else {
        Write-Host "⚠️  No Flink jobs running. Check deploy script output and Flink logs." -ForegroundColor Yellow
        Write-Host "  Hint: ensure connector and runtime JARs are available in /opt/flink/lib on JobManager and TaskManager." -ForegroundColor Gray
    }
    Write-Host ""
} else {
    Write-Host "⏭️  Skipping job deployment" -ForegroundColor Gray
    Write-Host ""
}

# Step 4: Query lakehouse
if (-not $PSBoundParameters.ContainsKey('SkipQueries')) {
    Write-Host "📊 Step 4: Querying Iceberg lakehouse..." -ForegroundColor Yellow
    Write-Host ""
    
    # Query 1: Bronze table count
    Write-Host "  Query 1: Bronze table row count" -ForegroundColor Cyan
    $bronze_count = docker exec -i trino trino --execute "SELECT COUNT(*) FROM iceberg_catalog.lakehouse.bronze_order_events" 2>$null
    if ($bronze_count) {
        Write-Host "  Result: $bronze_count rows in Bronze table" -ForegroundColor White
    } else {
        Write-Host "  ⚠️  Bronze table not ready yet" -ForegroundColor Yellow
    }
    Write-Host ""
    
    # Query 2: Event type distribution
    Write-Host "  Query 2: Event type distribution" -ForegroundColor Cyan
    $event_dist = docker exec -i trino trino --execute "SELECT event_type, COUNT(*) as cnt FROM iceberg_catalog.lakehouse.bronze_order_events GROUP BY event_type ORDER BY cnt DESC" 2>$null
    if ($event_dist) {
        Write-Host "  Result:" -ForegroundColor White
        $event_dist | ForEach-Object { Write-Host "    $_" -ForegroundColor White }
    } else {
        Write-Host "  ⚠️  Query failed (table may be empty)" -ForegroundColor Yellow
    }
    Write-Host ""
    
    # Query 3: Silver table count
    Write-Host "  Query 3: Silver table (order state) count" -ForegroundColor Cyan
    $silver_count = docker exec -i trino trino --execute "SELECT COUNT(*) FROM iceberg_catalog.lakehouse.silver_order_state" 2>$null
    if ($silver_count) {
        Write-Host "  Result: $silver_count unique orders in Silver table" -ForegroundColor White
    } else {
        Write-Host "  ⚠️  Silver table not ready yet" -ForegroundColor Yellow
    }
    Write-Host ""
    
    # Query 4: Gold table count
    Write-Host "  Query 4: Gold table (hourly KPIs) count" -ForegroundColor Cyan
    $gold_count = docker exec -i trino trino --execute "SELECT COUNT(*) FROM iceberg_catalog.lakehouse.gold_hourly_kpis" 2>$null
    if ($gold_count) {
        Write-Host "  Result: $gold_count hourly windows in Gold table" -ForegroundColor White
    } else {
        Write-Host "  ⚠️  Gold table not ready yet" -ForegroundColor Yellow
    }
    Write-Host ""
    
    Write-Host "✅ Queries completed" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "⏭️  Skipping queries" -ForegroundColor Gray
    Write-Host ""
}

# Summary
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 59) -ForegroundColor Cyan
Write-Host "🎯 Pipeline Test Complete!" -ForegroundColor Green
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 59) -ForegroundColor Cyan
Write-Host ""

Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "  • View Flink jobs: http://localhost:8081" -ForegroundColor White
Write-Host "  • Query with Trino CLI: docker exec -it trino trino" -ForegroundColor White
Write-Host "  • Check MinIO data: http://localhost:9001" -ForegroundColor White
Write-Host "  • View events: http://localhost:8080" -ForegroundColor White
Write-Host ""

Write-Host "Troubleshooting:" -ForegroundColor Yellow
Write-Host "  • If queries show 0 rows, wait 1-2 minutes for data to land" -ForegroundColor White
Write-Host "  • Check Flink logs: docker logs flink-taskmanager" -ForegroundColor White
Write-Host "  • Check Trino logs: docker logs trino" -ForegroundColor White
Write-Host "  • Re-run with --SkipRestart to avoid service downtime" -ForegroundColor White
Write-Host ""

Write-Host "Full documentation: docs/runbooks/phase2_quickstart.md" -ForegroundColor Cyan

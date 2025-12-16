# Troubleshooting Guide: Service Startup Issues

## Issue: "NoBrokersAvailable" or Services Not Ready

### Symptoms
- Event generator fails with `kafka.errors.NoBrokersAvailable`
- Trino queries return empty results
- Services show "not found" in health checks

### Root Cause
Services need time to initialize after `docker compose up`. Redpanda (Kafka) takes 30-60 seconds to become fully operational.

### Solution

#### 1. Check Docker is Running

```powershell
docker ps
```

If this fails, start Docker Desktop and wait for it to fully initialize.

#### 2. Start Services with Proper Wait Time

```powershell
# Start all services
python -m tasks up

# Wait for services to initialize (1-2 minutes)
Start-Sleep -Seconds 90

# Check health
.\scripts\check_health.ps1
```

#### 3. Verify Redpanda is Ready

```powershell
# Check Redpanda cluster health
docker exec redpanda rpk cluster health

# Should show: HEALTHY
```

#### 4. Verify Kafka Topics

```powershell
# List topics
docker exec redpanda rpk topic list

# Should show: orders.events, kpi.minute (if created)
```

#### 5. Test Event Generation

```powershell
# Generate a small batch first
python services/event_generator/main.py --events 10 --topic orders.events --brokers localhost:29092

# If successful, generate more
python services/event_generator/main.py --events 1000 --topic orders.events --brokers localhost:29092
```

## Common Errors and Fixes

### Error: "NoBrokersAvailable"

**Cause**: Redpanda not ready or port 29092 not accessible

**Fix**:
```powershell
# Check Redpanda logs
docker logs redpanda -f

# Restart Redpanda if needed
docker restart redpanda
Start-Sleep -Seconds 30

# Verify port is listening
docker exec redpanda rpk cluster health
```

### Error: "Trino table not found"

**Cause**: Flink jobs haven't created tables yet, or Hive Metastore not synced

**Fix**:
```powershell
# Check Flink jobs are running
docker exec flink-jobmanager /opt/flink/bin/flink list

# Deploy jobs if not running
.\scripts\deploy_flink_jobs.ps1

# Wait for tables to be created (30-60 seconds)
Start-Sleep -Seconds 60

# Restart Trino to refresh catalog
docker restart trino
Start-Sleep -Seconds 20
```

### Error: "Connection refused" for Trino/Hive Metastore

**Cause**: Services haven't started or failed initialization

**Fix**:
```powershell
# Check service logs
docker logs hive-metastore | Select-String -Pattern "ERROR"
docker logs trino | Select-String -Pattern "ERROR"

# Restart in dependency order
docker restart hive-metastore
Start-Sleep -Seconds 30
docker restart trino
Start-Sleep -Seconds 20
```

## Best Practices

### 1. Always Wait After Starting Services

```powershell
python -m tasks up

# Don't proceed immediately! Wait for initialization:
Write-Host "Waiting for services to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 90

# Then check health
.\scripts\check_health.ps1
```

### 2. Use Health Checks Before Operations

```powershell
# Before generating events
$health = docker exec redpanda rpk cluster health 2>$null
if ($health -notmatch "HEALTHY") {
    Write-Host "Redpanda not ready, waiting..."
    Start-Sleep -Seconds 30
}

# Then proceed
python services/event_generator/main.py ...
```

### 3. Run Phase 2 Test Script (Already Has Proper Waits)

```powershell
# Automated test with built-in health checks
.\scripts\test_phase2_pipeline.ps1

# Or skip restart if services already running
.\scripts\test_phase2_pipeline.ps1 -SkipRestart
```

## Quick Diagnostic Commands

```powershell
# Check all container statuses
docker compose -f local/docker-compose.yml ps

# Check specific service logs (last 50 lines)
docker logs redpanda --tail 50
docker logs flink-taskmanager --tail 50
docker logs trino --tail 50

# Check which ports are listening
docker ps --format "table {{.Names}}\t{{.Ports}}"

# Restart everything cleanly
python -m tasks down
Start-Sleep -Seconds 5
python -m tasks up
Start-Sleep -Seconds 90
.\scripts\check_health.ps1
```

## Timing Guidelines

| Operation | Wait Time | Check |
|-----------|-----------|-------|
| `python -m tasks up` | 90 seconds | `.\scripts\check_health.ps1` |
| `docker restart redpanda` | 30 seconds | `docker exec redpanda rpk cluster health` |
| `docker restart hive-metastore` | 30 seconds | `docker logs hive-metastore` |
| `docker restart trino` | 20 seconds | `docker exec -it trino trino --execute "SHOW CATALOGS"` |
| Deploy Flink jobs | 20 seconds | `docker exec flink-jobmanager /opt/flink/bin/flink list` |
| Event generation → Iceberg | 30-60 seconds | Trino query `SELECT COUNT(*)` |

## Recovery Procedure

If nothing works, full reset:

```powershell
# 1. Stop everything
python -m tasks down

# 2. Clean volumes (⚠️ DELETES ALL DATA)
docker volume prune -f

# 3. Restart Docker Desktop (Windows)
# Close Docker Desktop, wait 10 seconds, reopen

# 4. Start fresh
python -m tasks up

# 5. Wait for full initialization
Write-Host "Waiting 2 minutes for services..."
Start-Sleep -Seconds 120

# 6. Check health
.\scripts\check_health.ps1

# 7. Generate events
python services/event_generator/main.py --events 1000 --topic orders.events --brokers localhost:29092

# 8. Deploy jobs
.\scripts\deploy_flink_jobs.ps1

# 9. Wait for data
Start-Sleep -Seconds 60

# 10. Query lakehouse
docker exec -it trino trino --execute "SELECT COUNT(*) FROM iceberg_catalog.lakehouse.bronze_order_events"
```

## Prevention

Always follow this sequence:

1. ✅ Start services → **WAIT 90 seconds**
2. ✅ Check health → **Verify all green**
3. ✅ Generate events → **Verify no errors**
4. ✅ Deploy Flink jobs → **Check job status**
5. ✅ Wait for ingestion → **30-60 seconds**
6. ✅ Query lakehouse → **Verify results**

Don't skip wait times - they prevent 90% of issues!

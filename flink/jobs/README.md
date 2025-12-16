# PyFlink Streaming Jobs

This directory contains all Apache Flink streaming jobs for the Real-Time Order Delivery Event Platform.

## Overview

The platform runs **4 PyFlink jobs** that process events from Kafka and write to various sinks (Kafka topics, Iceberg tables).

## Jobs Summary

| Job | Phase | Source | Sink | Purpose |
|-----|-------|--------|------|---------|
| `kpi_aggregator.py` | 1 | Kafka `orders.events` | Kafka `kpi.minute` | Real-time KPIs for Grafana |
| `bronze_writer.py` | 2 | Kafka `orders.events` | Iceberg Bronze | Raw event ingestion |
| `silver_order_state.py` | 2 | Kafka `orders.events` | Iceberg Silver | Current order state |
| `gold_kpi_aggregates.py` | 2 | Kafka `orders.events` | Iceberg Gold | Hourly business KPIs |

## Job Details

### 1. kpi_aggregator.py (Phase 1)
Computes orders_created_per_minute KPI for Grafana dashboards.
- **Window**: 1-minute tumbling event-time windows
- **Filter**: Only "created" events
- **Watermark**: 10-second bounded out-of-orderness
- **Output**: JSON to `kpi.minute` topic

### 2. bronze_writer.py (Phase 2)
Writes raw events to Iceberg lakehouse (Bronze layer).
- **Format**: Parquet with Gzip compression
- **Mode**: Append-only (immutable audit trail)
- **Table**: `iceberg_catalog.lakehouse.bronze_order_events`
- **Schema**: Flattened event structure with metadata

### 3. silver_order_state.py (Phase 2)
Maintains current state for each order (Silver layer).
- **Mode**: Upsert (Iceberg v2)
- **Deduplication**: One row per `order_id`
- **Table**: `iceberg_catalog.lakehouse.silver_order_state`
- **Computed**: `is_delivered`, `is_cancelled`, `is_returned` flags

### 4. gold_kpi_aggregates.py (Phase 2)
Precomputes hourly business KPIs (Gold layer).
- **Window**: 1-hour tumbling windows
- **Group By**: `region`, `warehouse_id`
- **Table**: `iceberg_catalog.lakehouse.gold_hourly_kpis`
- **Metrics**: Order counts, SLA rates, revenue, conversion

## Deployment

### Automated (Recommended)

**PowerShell (Windows)**:
```powershell
.\scripts\deploy_flink_jobs.ps1
```

**Bash (Linux/Mac)**:
```bash
./scripts/deploy_flink_jobs.sh
```

### Manual

Jobs are automatically mounted in the Flink containers via docker-compose at `/opt/flink/jobs/`.

```bash
# Submit job to cluster
docker exec local-flink-jobmanager-1 /opt/flink/bin/flink run \
    -py /opt/flink/jobs/<job_name>.py

# Check running jobs
docker exec local-flink-jobmanager-1 /opt/flink/bin/flink list
```

## Requirements

The custom Flink Docker image (`local/flink/Dockerfile`) includes:
- Python 3 and PyFlink
- All required JARs pre-installed in `/opt/flink/lib/`:
  - `flink-connector-kafka-1.16.3.jar`
  - `iceberg-flink-runtime-1.16-1.4.2.jar`
  - `iceberg-aws-bundle-1.4.2.jar`
  - Hive Metastore dependencies
  - Hadoop S3 support

## Key Configuration

All Iceberg jobs include:
- **Checkpointing**: 60-second intervals for data commits
- **Classloader**: Parent-first to avoid conflicts
- **S3FileIO**: Uses Iceberg's native S3 client for MinIO

## Monitoring

- **Flink Dashboard**: http://localhost:8081
- **Logs**: `docker logs local-flink-taskmanager-1 -f`

## Testing

```bash
# Generate test events
python services/event_generator/main.py --events 100 --topic orders.events --brokers localhost:29092

# Run integration test
python tests/test_integration.py

# Quick pipeline test
.\scripts\test_phase2_pipeline.ps1
```

## Documentation

See detailed job documentation:
- **Phase 2 Guide**: `docs/runbooks/phase2_quickstart.md`
- **Quick Reference**: `docs/QUICK_REFERENCE.md`
- **SQL Queries**: `sql/example_queries.sql`

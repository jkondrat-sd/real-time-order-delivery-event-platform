# Real-Time Order Delivery Event Platform - Architecture

## Overview

This platform demonstrates a production-grade streaming data architecture for real-time order tracking and analytics. It showcases modern data engineering practices including stream processing, lakehouse architecture, and operational observability.

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         Real-Time Order Delivery Event Platform                      │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                       │
│   ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐   │
│   │    Event     │────▶│   Redpanda   │────▶│    Flink     │────▶│   Iceberg    │   │
│   │  Generator   │     │   (Kafka)    │     │  Processing  │     │   Lakehouse  │   │
│   └──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘   │
│         │                     │                    │                     │           │
│         │                     │                    │                     │           │
│         ▼                     ▼                    ▼                     ▼           │
│   ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐   │
│   │   Airflow    │     │  Prometheus  │     │   Grafana    │     │    Trino     │   │
│   │   (Backfill) │     │   (Metrics)  │     │ (Dashboards) │     │  (Queries)   │   │
│   └──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘   │
│                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Architecture

### Data Ingestion Layer

```
┌─────────────────────────────────────────────────────────────────┐
│                      Event Generator                             │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  • Simulates order lifecycle events                      │   │
│  │  • Generates: order_created → accepted → in_delivery    │   │
│  │  •            → delivered/cancelled                       │   │
│  │  • Configurable rate: --rate-limit (events/sec)          │   │
│  │  • Produces to: orders.events topic                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                            │                                     │
│                            ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Redpanda (Kafka-compatible)                             │   │
│  │  ├── orders.events      (raw order events)               │   │
│  │  ├── kpi.minute         (aggregated KPIs)                │   │
│  │  └── alerts.events      (alert notifications)            │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Stream Processing Layer (Apache Flink)

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              Flink Job Pipeline                                       │
│                                                                                       │
│   orders.events ──┬──▶ [bronze_writer.py] ──▶ iceberg.lakehouse.bronze_order_events │
│                   │                                                                   │
│                   ├──▶ [silver_order_state.py] ──▶ iceberg.lakehouse.silver_orders  │
│                   │                                                                   │
│                   ├──▶ [gold_kpi_aggregates.py] ──▶ iceberg.lakehouse.gold_kpis     │
│                   │                                                                   │
│                   ├──▶ [kpi_aggregator.py] ──▶ kpi.minute topic                      │
│                   │                                                                   │
│                   └──▶ [alert_detector.py] ──▶ alerts.events + iceberg.alerts        │
│                                                                                       │
├─────────────────────────────────────────────────────────────────────────────────────┤
│  Job Details:                                                                         │
│                                                                                       │
│  bronze_writer.py        │ Append-only raw event storage                             │
│  silver_order_state.py   │ Deduplication + state enrichment                          │
│  gold_kpi_aggregates.py  │ 1-minute tumbling window aggregations                     │
│  kpi_aggregator.py       │ Real-time KPIs to Kafka for dashboards                    │
│  alert_detector.py       │ Pattern detection: stuck orders, SLA breaches             │
│                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Lakehouse Storage Layer

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        Apache Iceberg + MinIO (S3-compatible)                        │
│                                                                                       │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │                           lakehouse (schema)                                   │ │
│   │  ┌─────────────────────────────────────────────────────────────────────────┐ │ │
│   │  │  BRONZE LAYER (Raw Data)                                                 │ │ │
│   │  │  ├── bronze_order_events                                                 │ │ │
│   │  │  │   └── Columns: event_id, order_id, event_type, timestamp,            │ │ │
│   │  │  │                restaurant_id, customer_id, driver_id,                 │ │ │
│   │  │  │                delivery_location, amount, kafka_timestamp             │ │ │
│   │  │  └── Partitioned by: event_date (daily)                                  │ │ │
│   │  └─────────────────────────────────────────────────────────────────────────┘ │ │
│   │  ┌─────────────────────────────────────────────────────────────────────────┐ │ │
│   │  │  SILVER LAYER (Cleaned + Enriched)                                       │ │ │
│   │  │  ├── silver_orders                                                       │ │ │
│   │  │  │   └── Columns: order_id, current_status, created_at, updated_at,     │ │ │
│   │  │  │                restaurant_id, customer_id, driver_id, amount,        │ │ │
│   │  │  │                time_to_accept, time_to_deliver                        │ │ │
│   │  │  └── UPSERT semantics on order_id                                        │ │ │
│   │  └─────────────────────────────────────────────────────────────────────────┘ │ │
│   │  ┌─────────────────────────────────────────────────────────────────────────┐ │ │
│   │  │  GOLD LAYER (Aggregated)                                                 │ │ │
│   │  │  ├── gold_kpis                                                           │ │ │
│   │  │  │   └── Columns: window_start, window_end, total_orders,                │ │ │
│   │  │  │                completed_orders, cancelled_orders,                    │ │ │
│   │  │  │                avg_delivery_time_sec, p95_delivery_time               │ │ │
│   │  │  └── 1-minute aggregation windows                                        │ │ │
│   │  └─────────────────────────────────────────────────────────────────────────┘ │ │
│   │  ┌─────────────────────────────────────────────────────────────────────────┐ │ │
│   │  │  ALERTS TABLE                                                            │ │ │
│   │  │  ├── alerts                                                              │ │ │
│   │  │  │   └── Columns: alert_id, alert_type, order_id, severity,             │ │ │
│   │  │  │                message, detected_at, context                          │ │ │
│   │  │  └── Types: stuck_order, high_cancellation_rate, sla_breach             │ │ │
│   │  └─────────────────────────────────────────────────────────────────────────┘ │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                       │
│   Storage: MinIO (S3-compatible) at s3://lakehouse/                                  │
│   Catalog: Hive Metastore                                                            │
│   Format: Apache Parquet with Iceberg metadata                                       │
│                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Query Engine Layer

```
┌─────────────────────────────────────────────────────────────────┐
│                         Trino Query Engine                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Catalogs:                                                │   │
│  │  ├── iceberg     → Iceberg tables in MinIO               │   │
│  │  │   └── iceberg.lakehouse.bronze_order_events          │   │
│  │  │   └── iceberg.lakehouse.silver_orders                 │   │
│  │  │   └── iceberg.lakehouse.gold_kpis                     │   │
│  │  │   └── iceberg.lakehouse.alerts                        │   │
│  │  │                                                        │   │
│  │  └── memory      → In-memory tables for testing          │   │
│  │                                                           │   │
│  │  Features:                                                │   │
│  │  • Time-travel queries (SELECT * FROM table FOR VERSION) │   │
│  │  • Schema evolution support                               │   │
│  │  • Partition pruning                                      │   │
│  │  • Predicate pushdown                                     │   │
│  └─────────────────────────────────────────────────────────┘   │
│  Port: 8082                                                      │
└─────────────────────────────────────────────────────────────────┘
```

### Orchestration Layer (Apache Airflow)

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              Apache Airflow                                          │
│  Port: 8085 | Executor: LocalExecutor | Backend: SQLite                             │
│                                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐│
│  │  DAG: backfill_range                                                             ││
│  │  Schedule: @once (manual trigger)                                                ││
│  │  Purpose: Reprocess historical time windows                                       ││
│  │                                                                                   ││
│  │  Flow: validate_dates → query_source_data → create_staging_table →              ││
│  │        compute_gold_aggregates → execute_aggregation →                           ││
│  │        validate_backfill_results → swap_tables → cleanup                         ││
│  └─────────────────────────────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────────────────────────────┐│
│  │  DAG: data_sanity_checks                                                         ││
│  │  Schedule: @daily                                                                ││
│  │  Purpose: Data quality monitoring                                                 ││
│  │                                                                                   ││
│  │  Parallel Checks:                                                                ││
│  │  ├── check_row_counts         (min 100 events/day)                               ││
│  │  ├── check_freshness          (max 2 hour lag)                                   ││
│  │  ├── check_null_rates         (max 5% nulls)                                     ││
│  │  ├── check_duplicates         (max 1% duplicates)                                ││
│  │  └── check_referential_integrity                                                 ││
│  │                                                                                   ││
│  │  Flow: [parallel checks] → evaluate_results → (success OR failure)              ││
│  └─────────────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Observability Layer

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              Monitoring Stack                                        │
│                                                                                       │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐                 │
│  │   Prometheus    │───▶│  Alertmanager   │───▶│  Notifications  │                 │
│  │   Port: 9090    │    │   Port: 9093    │    │  (Slack/Email)  │                 │
│  └────────┬────────┘    └─────────────────┘    └─────────────────┘                 │
│           │                                                                          │
│           │ scrapes                                                                  │
│           ▼                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐│
│  │  Metrics Sources:                                                                ││
│  │  ├── Flink JobManager/TaskManager  (job status, throughput, latency)            ││
│  │  ├── Redpanda                      (topic lag, consumer groups)                 ││
│  │  └── MinIO                         (storage metrics)                             ││
│  └─────────────────────────────────────────────────────────────────────────────────┘│
│                                                                                       │
│  ┌─────────────────┐                                                                │
│  │    Grafana      │    Dashboards:                                                 │
│  │   Port: 3000    │    ├── Orders KPI Dashboard (orders_kpi_dashboard.json)       │
│  │                 │    └── Sample Dashboard (grafana_sample_dashboard.json)        │
│  └─────────────────┘                                                                │
│                                                                                       │
│  Alert Rules (prometheus_alert_rules.yml):                                           │
│  ├── FlinkJobFailed           - Job not running                                     │
│  ├── HighKafkaLag             - Consumer lag > threshold                            │
│  ├── HighCancellationRate     - Cancellations > 15%                                 │
│  ├── SlowDeliveryTime         - P95 delivery > 30 min                               │
│  └── StuckOrders              - Orders in same state > 15 min                       │
│                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow Diagram

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                    DATA FLOW                                            │
│                                                                                          │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐      ┌──────────┐      ┌──────────┐ │
│  │  Event   │─────▶│  Kafka   │─────▶│  Flink   │─────▶│ Iceberg  │─────▶│  Trino   │ │
│  │Generator │      │ (Redpanda)│      │  Jobs    │      │  Tables  │      │ Queries  │ │
│  └──────────┘      └──────────┘      └──────────┘      └──────────┘      └──────────┘ │
│       │                 │                 │                  │                 │        │
│       │                 │                 │                  │                 │        │
│       ▼                 ▼                 ▼                  ▼                 ▼        │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐ │
│  │                              Latency Targets                                       │ │
│  │                                                                                    │ │
│  │  Generator → Kafka:      < 10ms   (network latency)                              │ │
│  │  Kafka → Flink:          < 100ms  (micro-batch)                                  │ │
│  │  Flink → Iceberg:        < 5s     (checkpoint interval)                          │ │
│  │  Iceberg → Trino:        < 1s     (query time)                                   │ │
│  │                                                                                    │ │
│  │  End-to-end:             < 10s    (event to queryable)                           │ │
│  └──────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                          │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Event Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "OrderEvent",
  "type": "object",
  "properties": {
    "event_id": { "type": "string", "format": "uuid" },
    "order_id": { "type": "string", "format": "uuid" },
    "event_type": { 
      "type": "string",
      "enum": ["order_created", "order_accepted", "order_picked_up", 
               "in_delivery", "delivered", "cancelled"]
    },
    "timestamp": { "type": "string", "format": "date-time" },
    "restaurant_id": { "type": "string" },
    "customer_id": { "type": "string" },
    "driver_id": { "type": ["string", "null"] },
    "delivery_location": {
      "type": "object",
      "properties": {
        "lat": { "type": "number" },
        "lng": { "type": "number" }
      }
    },
    "amount": { "type": "number" },
    "currency": { "type": "string", "default": "USD" }
  },
  "required": ["event_id", "order_id", "event_type", "timestamp"]
}
```

---

## Network Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           Docker Network: rtod-network                               │
│                                                                                       │
│  ┌──────────────────────────────────────────────────────────────────────────────┐   │
│  │  Service              │  Internal Port  │  External Port  │  Purpose          │   │
│  ├──────────────────────────────────────────────────────────────────────────────┤   │
│  │  redpanda             │  9092, 29092    │  29092          │  Kafka broker     │   │
│  │  flink-jobmanager     │  8081, 6123     │  8081           │  Flink UI/API     │   │
│  │  flink-taskmanager    │  6121, 6122     │  -              │  Flink workers    │   │
│  │  trino                │  8082           │  8082           │  SQL queries      │   │
│  │  minio                │  9000, 9001     │  9000, 9001     │  S3 storage       │   │
│  │  hive-metastore       │  9083           │  9083           │  Catalog          │   │
│  │  prometheus           │  9090           │  9090           │  Metrics          │   │
│  │  grafana              │  3000           │  3000           │  Dashboards       │   │
│  │  alertmanager         │  9093           │  9093           │  Alerts           │   │
│  │  airflow-webserver    │  8085           │  8085           │  DAG UI           │   │
│  └──────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Layer | Technology | Version | Purpose |
|-------|------------|---------|---------|
| **Event Streaming** | Redpanda | 23.2 | Kafka-compatible message broker |
| **Stream Processing** | Apache Flink | 1.16.3 | Real-time event processing |
| **Table Format** | Apache Iceberg | 1.4.2 | ACID transactions, time-travel |
| **Query Engine** | Trino | 417 | Distributed SQL queries |
| **Object Storage** | MinIO | Latest | S3-compatible storage |
| **Catalog** | Hive Metastore | 3.1.3 | Table metadata |
| **Orchestration** | Apache Airflow | 2.7.3 | DAG scheduling, backfills |
| **Monitoring** | Prometheus | Latest | Metrics collection |
| **Dashboards** | Grafana | Latest | Visualization |
| **Alerting** | Alertmanager | Latest | Alert routing |

---

## Directory Structure

```
real-time-order-delivery-event-platform/
├── airflow/
│   └── dags/
│       ├── backfill_range.py       # Historical reprocessing DAG
│       ├── data_sanity_checks.py   # Data quality DAG
│       └── backfill_example.py     # Simple example
├── dashboards/
│   ├── grafana_sample_dashboard.json
│   └── orders_kpi_dashboard.json
├── docs/
│   ├── architecture.md             # This document
│   └── runbooks/
│       ├── alerts_runbook.md       # Alert response procedures
│       ├── initial-runbook.md      # Setup procedures
│       └── TROUBLESHOOTING_STARTUP.md
├── flink/
│   └── jobs/
│       ├── bronze_writer.py        # Raw event ingestion
│       ├── silver_order_state.py   # State enrichment
│       ├── gold_kpi_aggregates.py  # Aggregations
│       ├── kpi_aggregator.py       # Real-time KPIs
│       └── alert_detector.py       # CEP alerts
├── local/
│   ├── docker-compose.yml          # Full stack definition
│   ├── prometheus.yml              # Prometheus config
│   ├── prometheus_alert_rules.yml  # Alert rules
│   └── alertmanager.yml            # Alert routing
├── schemas/
│   └── order_event.json            # Event schema
├── scripts/
│   ├── deploy_flink_jobs.ps1       # Job deployment
│   └── start_demo.ps1              # Quick start
├── services/
│   └── event_generator/            # Python event producer
└── tests/
    ├── test_event_generator.py
    └── test_integration.py
```

---

## Deployment Phases

### Phase 1: Core Infrastructure
- Docker Compose setup
- Redpanda, MinIO, Hive Metastore
- Event generator

### Phase 2: Streaming Pipeline
- Flink jobs (bronze, silver, gold)
- Iceberg table creation
- Trino integration

### Phase 3: Complex Event Processing
- Alert detection
- Prometheus + Alertmanager
- Alert rules configuration

### Phase 4: Production Polish
- Airflow DAGs for backfills
- CI/CD pipeline
- Documentation
- Demo scripts

---

## Quick Start

```bash
# 1. Start infrastructure
cd local
docker compose up -d

# 2. Wait for services (2-3 minutes)
docker compose logs -f flink-jobmanager

# 3. Deploy Flink jobs
../scripts/deploy_flink_jobs.ps1

# 4. Generate events
python ../services/event_generator/main.py --events 1000

# 5. Query data
docker exec -it local-trino-1 trino
> SELECT * FROM iceberg.lakehouse.bronze_order_events LIMIT 10;
```

---

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

## License

MIT License - See [LICENSE](../LICENSE)

# 🚀 Real-Time Order & Delivery Event Platform

<div align="center">

[![CI Pipeline](https://img.shields.io/github/actions/workflow/status/yourusername/real-time-order-delivery-event-platform/ci.yml?branch=main&label=CI&logo=github)](https://github.com/yourusername/real-time-order-delivery-event-platform/actions)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10+-3776ab.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Flink](https://img.shields.io/badge/Apache%20Flink-1.16-E6526F.svg?logo=apacheflink&logoColor=white)](https://flink.apache.org/)
[![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1.4-0D7377.svg)](https://iceberg.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Production-grade streaming data platform demonstrating modern data engineering best practices**

[Quick Start](#-quick-start) •
[Architecture](#-architecture) •
[Features](#-key-features) •
[Documentation](#-documentation) •
[Demo](#-live-demo)

</div>

---

## 📋 Overview

This project showcases a complete **end-to-end streaming data platform** for real-time order tracking and analytics. Built entirely with open-source technologies, it demonstrates the skills needed for modern data engineering roles:

- **Stream Processing**: Real-time event ingestion and transformation with Apache Flink
- **Lakehouse Architecture**: Bronze → Silver → Gold medallion pattern using Apache Iceberg
- **Complex Event Processing**: SLA violation detection and alerting
- **Operational Observability**: Prometheus metrics, Grafana dashboards, and Alertmanager
- **Data Orchestration**: Airflow DAGs for backfills and data quality checks
- **Production Patterns**: CI/CD, testing, documentation, and runbooks

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                                DATA FLOW                                        │
│                                                                                  │
│   📱 Events    →    📬 Kafka     →    ⚡ Flink    →    🧊 Iceberg   →   📊 Analytics│
│   Generator         (Redpanda)         Jobs            Lakehouse        (Trino)  │
│                                          │                                       │
│                                          ├── Bronze (raw events)                │
│                                          ├── Silver (current state)             │
│                                          └── Gold (KPIs + Alerts)               │
│                                                                                  │
│   🔔 Alertmanager  ←  📈 Prometheus  ←  📊 Grafana                              │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features

| Feature | Description | Technologies |
|---------|-------------|--------------|
| **Real-time Ingestion** | Sub-second event capture from order lifecycle | Kafka/Redpanda |
| **Stream Processing** | 5 PyFlink jobs for ETL and aggregation | Apache Flink 1.16 |
| **Lakehouse Storage** | ACID transactions, time-travel, schema evolution | Apache Iceberg 1.4 |
| **SQL Analytics** | Interactive queries on streaming data | Trino 417 |
| **Complex Event Processing** | SLA breach detection, stuck order alerts | Flink CEP |
| **Monitoring & Alerting** | Real-time dashboards, alert routing | Prometheus, Grafana, Alertmanager |
| **Data Quality** | Automated sanity checks, backfill DAGs | Apache Airflow 2.7 |
| **CI/CD Pipeline** | Lint, test, integration tests | GitHub Actions |

---

## 🚀 Quick Start

### Prerequisites

- **Docker Desktop** (Windows/macOS) or **Docker Engine** (Linux) with 8GB+ RAM
- **Python 3.10+** (for event generator and local testing)

### One-Command Demo

```bash
# Clone the repository
git clone https://github.com/yourusername/real-time-order-delivery-event-platform.git
cd real-time-order-delivery-event-platform

# Start all services (takes ~2 minutes)
make up

# Deploy Flink streaming jobs
make deploy-jobs

# Generate sample events and watch the pipeline
make demo-full
```

### Access the Platform

| Service | URL | Credentials |
|---------|-----|-------------|
| **Flink Dashboard** | http://localhost:8081 | — |
| **Trino SQL** | http://localhost:8082 | — |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Airflow** | http://localhost:8085 | admin / admin |
| **Prometheus** | http://localhost:9090 | — |
| **Redpanda Console** | http://localhost:8080 | — |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |

### Query Your Data

```bash
# Open Trino CLI
make trino-shell
```

```sql
-- Count events by type
SELECT event_type, COUNT(*) as count
FROM iceberg.lakehouse.bronze_order_events
GROUP BY event_type
ORDER BY count DESC;

-- View current order states
SELECT current_status, COUNT(*) as orders
FROM iceberg.lakehouse.silver_orders
GROUP BY current_status;

-- Check recent alerts
SELECT alert_type, severity, message, detected_at
FROM iceberg.lakehouse.alerts
ORDER BY detected_at DESC
LIMIT 10;
```

---

## 🏗️ Architecture

### System Overview

```
┌─────────────────┐    ┌──────────────┐    ┌────────────────────────────────────┐
│  Event          │    │   Redpanda   │    │           Apache Flink             │
│  Generator      │───▶│   (Kafka)    │───▶│  ┌────────────────────────────┐   │
│  (Python)       │    │              │    │  │ bronze_writer.py           │   │
└─────────────────┘    └──────────────┘    │  │ silver_order_state.py      │   │
                                           │  │ gold_kpi_aggregates.py     │   │
                                           │  │ kpi_aggregator.py          │   │
                                           │  │ alert_detector.py          │   │
                                           │  └────────────────────────────┘   │
                                           └───────────────┬────────────────────┘
                                                           │
                        ┌──────────────────────────────────▼───────────────────────┐
                        │                Apache Iceberg Lakehouse                   │
                        │  ┌────────────┐  ┌────────────┐  ┌────────────┐          │
                        │  │  Bronze    │  │  Silver    │  │   Gold     │          │
                        │  │ Raw Events │  │ Order State│  │ KPIs+Alerts│          │
                        │  └────────────┘  └────────────┘  └────────────┘          │
                        │                    MinIO (S3)                             │
                        └───────────────────────┬──────────────────────────────────┘
                                                │
                        ┌───────────────────────▼───────────────────────┐
                        │              Query & Analytics                 │
                        │  ┌─────────┐  ┌─────────┐  ┌─────────┐       │
                        │  │  Trino  │  │ Grafana │  │ Airflow │       │
                        │  │   SQL   │  │  Dash   │  │  DAGs   │       │
                        │  └─────────┘  └─────────┘  └─────────┘       │
                        └───────────────────────────────────────────────┘
```

### Medallion Architecture

| Layer | Table | Purpose | Update Pattern |
|-------|-------|---------|----------------|
| **Bronze** | `bronze_order_events` | Raw event storage, immutable audit trail | Append-only |
| **Silver** | `silver_orders` | Deduplicated current state per order | Upsert on order_id |
| **Gold** | `gold_kpis` | Pre-aggregated business metrics | Tumbling windows |
| **Gold** | `alerts` | SLA violations and anomalies | CEP detection |

### Streaming Jobs

| Job | Description | Input | Output |
|-----|-------------|-------|--------|
| `bronze_writer.py` | Raw event ingestion | `orders.events` topic | `bronze_order_events` table |
| `silver_order_state.py` | State enrichment + dedup | Bronze table | `silver_orders` table |
| `gold_kpi_aggregates.py` | 1-minute window aggregations | Silver table | `gold_kpis` table |
| `kpi_aggregator.py` | Real-time KPI streaming | Events topic | `kpi.minute` topic |
| `alert_detector.py` | SLA breach detection | Events topic | `alerts` table + topic |

---

## 📊 Live Demo

### Generate Events

```bash
# Generate 500 events with realistic timing
python services/event_generator/main.py \
  --events 500 \
  --topic orders.events \
  --brokers localhost:29092 \
  --rate-limit 10
```

### Watch the Pipeline

1. **Flink UI** (http://localhost:8081): See 5 jobs running, throughput metrics
2. **Redpanda Console** (http://localhost:8080): Watch events flow through topics
3. **Grafana** (http://localhost:3000): Import `dashboards/orders_kpi_dashboard.json`
4. **Trino**: Query the lakehouse tables in real-time

### Screenshot Preparation

```bash
# Automated demo setup for portfolio screenshots
make screenshot-prep
```

---

## 🧪 Testing & Quality

### Run Tests

```bash
# Unit tests
make test

# Linting
make lint

# Type checking
make type-check

# All CI checks
make ci-all
```

### CI Pipeline

The GitHub Actions workflow runs:
- **Lint**: ruff check + format verification
- **Test**: pytest unit tests
- **Integration**: Full Docker Compose pipeline test
- **DAG Validation**: Airflow DAG syntax check

---

## 📁 Project Structure

```
real-time-order-delivery-event-platform/
├── .github/workflows/     # CI/CD pipelines
├── airflow/dags/          # Backfill + data quality DAGs
│   ├── backfill_range.py
│   └── data_sanity_checks.py
├── dashboards/            # Grafana dashboard exports
├── docs/
│   ├── architecture.md    # Detailed system design
│   └── runbooks/          # Operational procedures
├── flink/jobs/            # PyFlink streaming jobs
│   ├── bronze_writer.py
│   ├── silver_order_state.py
│   ├── gold_kpi_aggregates.py
│   ├── kpi_aggregator.py
│   └── alert_detector.py
├── local/                 # Docker Compose stack
│   ├── docker-compose.yml
│   ├── prometheus.yml
│   └── alertmanager.yml
├── schemas/               # Event JSON schemas
├── scripts/               # Deployment scripts
├── services/              # Python services
│   └── event_generator/
├── sql/                   # Example queries
├── tests/                 # Test suite
├── Makefile               # Development commands
└── pyproject.toml         # Project configuration
```

---

## 📚 Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | Detailed system design with diagrams |
| [Quick Reference](docs/QUICK_REFERENCE.md) | Common commands and URLs |
| [Alerts Runbook](docs/runbooks/alerts_runbook.md) | Alert response procedures |
| [Troubleshooting](docs/runbooks/TROUBLESHOOTING_STARTUP.md) | Common issues and fixes |
| [Phase Summaries](docs/deliverables/) | Implementation details by phase |

---

## 🛠️ Technology Stack

| Category | Technology | Version |
|----------|------------|---------|
| **Event Streaming** | Redpanda (Kafka-compatible) | 23.2 |
| **Stream Processing** | Apache Flink | 1.16.3 |
| **Table Format** | Apache Iceberg | 1.4.2 |
| **Query Engine** | Trino | 417 |
| **Object Storage** | MinIO | Latest |
| **Orchestration** | Apache Airflow | 2.7.3 |
| **Monitoring** | Prometheus + Grafana | Latest |
| **Alerting** | Alertmanager | Latest |
| **Containerization** | Docker Compose | 2.x |

---

## 🎯 Skills Demonstrated

This project demonstrates proficiency in:

- **Data Engineering**: ETL/ELT pipelines, data modeling, medallion architecture
- **Stream Processing**: Apache Flink, Kafka, exactly-once semantics
- **Lakehouse**: Apache Iceberg, time-travel queries, schema evolution
- **SQL**: Complex analytical queries, window functions, aggregations
- **DevOps**: Docker, CI/CD, infrastructure as code
- **Observability**: Metrics, logging, alerting, runbooks
- **Python**: Clean code, testing, type hints, project structure
- **Documentation**: Technical writing, architecture diagrams, runbooks

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

<div align="center">

**Built with ❤️ for the data engineering community**

[⬆ Back to top](#-real-time-order--delivery-event-platform)

</div>

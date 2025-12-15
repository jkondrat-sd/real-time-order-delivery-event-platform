# Real-Time Order & Delivery Event Platform

An industry-grade, production-ready monorepo for a real-time order and delivery event processing platform. This platform demonstrates modern data engineering best practices with stream processing, data lakehouse architecture, and comprehensive observability.

## 🏗️ Architecture Overview

```
┌─────────────┐      ┌──────────┐      ┌───────────────┐      ┌─────────┐
│   Source    │─────▶│  Kafka   │─────▶│  Flink Jobs   │─────▶│ Iceberg │
│  Systems    │      │ + Schema │      │  (KPI/CEP)    │      │ Tables  │
└─────────────┘      │ Registry │      └───────────────┘      └─────────┘
                     └──────────┘              │                     │
                           │                   │                     │
                           ▼                   ▼                     ▼
                     ┌──────────┐      ┌───────────┐         ┌──────────┐
                     │ Airflow  │      │Prometheus │         │  Trino   │
                     │(Backfill)│      │ + Grafana │         │ (Query)  │
                     └──────────┘      └───────────┘         └──────────┘
                                              │
                                              └──────▶ MinIO (S3 Storage)
```

### Components

- **Apache Kafka (v7.6.0)**: Event streaming platform for order and delivery events
- **Schema Registry**: Manages Avro/JSON schemas for event data
- **Apache Flink (v1.18.0)**: Stream processing with two jobs:
  - **KPI Aggregator**: Computes real-time metrics (order counts, revenue, avg order value)
  - **Complex Event Detector**: Detects patterns using Flink CEP (delayed deliveries, failed attempts)
- **Apache Iceberg**: Data lakehouse table format for analytics
- **MinIO**: S3-compatible object storage for Iceberg data files
- **Trino**: Distributed SQL query engine for ad-hoc analytics
- **Apache Airflow (v2.8.0)**: Orchestrates historical data backfills
- **Prometheus + Grafana**: Metrics collection and visualization

## 📁 Repository Structure

```
.
├── docker-compose.yml              # All service definitions
├── Makefile                        # Common development commands
├── README.md                       # This file
│
├── flink/                          # Flink streaming jobs
│   ├── kpi-aggregator/            # Real-time KPI computation
│   │   ├── pom.xml
│   │   └── src/main/java/com/platform/flink/
│   │       └── KpiAggregatorJob.java
│   └── complex-event-detector/    # CEP pattern matching
│       ├── pom.xml
│       └── src/main/java/com/platform/flink/
│           └── ComplexEventDetectorJob.java
│
├── airflow/                        # Airflow DAGs and plugins
│   ├── dags/
│   │   └── order_delivery_backfill.py
│   └── plugins/
│
├── docker/                         # Docker configurations
│   └── configs/
│       ├── kafka/
│       ├── trino/
│       │   ├── config.properties
│       │   └── catalog/
│       │       └── iceberg.properties
│       ├── prometheus/
│       │   └── prometheus.yml
│       ├── grafana/
│       │   ├── datasources/
│       │   │   └── prometheus.yml
│       │   └── dashboards/
│       │       └── dashboard.yml
│       └── minio/
│
├── docs/                           # Additional documentation
└── scripts/                        # Utility scripts
```

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose (v2.0+)
- Java 11+ (for building Flink jobs)
- Maven 3.6+ (for building Flink jobs)
- Make (optional, for convenience commands)

### Starting the Platform

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Josres/real-time-order-delivery-event-platform.git
   cd real-time-order-delivery-event-platform
   ```

2. **Start all services:**
   ```bash
   make up
   # OR
   docker-compose up -d
   ```

3. **Wait for services to initialize (2-3 minutes):**
   ```bash
   make status
   ```

4. **Create Kafka topics:**
   ```bash
   make create-topics
   ```

5. **Build and submit Flink jobs (optional):**
   ```bash
   make build-flink
   make submit-kpi-aggregator
   make submit-complex-event-detector
   ```

### Accessing Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |
| Flink Web UI | http://localhost:8082 | - |
| Airflow | http://localhost:8083 | airflow / airflow |
| MinIO Console | http://localhost:9001 | admin / password |
| Trino | http://localhost:8080 | - |
| Schema Registry | http://localhost:8081 | - |

## 📊 Usage Examples

### Producing Test Events to Kafka

```bash
# Send a test order event
make test-kafka

# Or manually:
docker-compose exec kafka kafka-console-producer \
  --broker-list localhost:9092 \
  --topic order-events

# Then paste JSON events:
{"order_id":"ORD-001","customer_id":"CUST-123","amount":99.99,"status":"placed"}
```

### Running Airflow Backfill

1. Open Airflow UI: http://localhost:8083
2. Enable the `order_delivery_backfill` DAG
3. Trigger with parameters:
   ```json
   {
     "start_date": "2024-01-01",
     "end_date": "2024-01-31",
     "batch_size": 1000
   }
   ```

### Querying Data with Trino

```bash
# Connect to Trino CLI
docker-compose exec trino trino

# Query Iceberg tables
USE iceberg.default_db;
SHOW TABLES;
SELECT * FROM order_kpis LIMIT 10;
```

### Viewing Metrics in Grafana

1. Open Grafana: http://localhost:3000
2. Login with admin/admin
3. Navigate to Explore
4. Select Prometheus datasource
5. Query Flink metrics:
   ```promql
   flink_jobmanager_job_numberOfCompletedCheckpoints
   flink_taskmanager_job_task_numRecordsInPerSecond
   ```

## 🛠️ Development

### Available Make Commands

```bash
make help              # Show all available commands
make up                # Start all services
make down              # Stop all services
make restart           # Restart services
make logs              # View all logs
make clean             # Remove all volumes and data
make build-flink       # Build Flink jobs
make create-topics     # Create Kafka topics
make status            # Check service status
```

### Building Flink Jobs

```bash
# Build both jobs
make build-flink

# Or build individually
cd flink/kpi-aggregator
mvn clean package

cd flink/complex-event-detector
mvn clean package
```

### Monitoring and Debugging

```bash
# View logs for specific services
make logs-kafka
make logs-flink
make logs-airflow

# Check Kafka topics
make list-topics

# Access service shells
make shell-kafka
make shell-flink
```

## 🏭 Production Considerations

### Scalability
- **Kafka**: Increase partitions for higher throughput
- **Flink**: Scale TaskManagers horizontally
- **Trino**: Add worker nodes for query performance
- **MinIO**: Deploy in distributed mode

### High Availability
- Deploy Kafka with multiple brokers and ZooKeeper ensemble
- Configure Flink JobManager HA with ZooKeeper
- Use external metastore for Iceberg (AWS Glue, Hive)
- Configure Airflow with CeleryExecutor or KubernetesExecutor

### Security
- Enable SSL/TLS for Kafka
- Configure authentication (SASL, OAuth)
- Use IAM roles for S3 access
- Enable Ranger/Sentry for data access control
- Rotate credentials regularly

### Monitoring
- Set up alerts in Prometheus/Alertmanager
- Configure log aggregation (ELK, Loki)
- Implement distributed tracing (OpenTelemetry)
- Monitor data quality metrics

## 🧪 Testing

```bash
# Test Kafka connectivity
make test-kafka

# Verify Flink job submissions
make submit-kpi-aggregator

# Run Airflow backfill in test mode
# (via Airflow UI with small date range)
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes
4. Build and test: `make build-all && make up`
5. Commit: `git commit -am 'Add new feature'`
6. Push: `git push origin feature/my-feature`
7. Submit a Pull Request

## 📝 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Software Foundation for Kafka, Flink, Airflow, Iceberg
- Confluent for Kafka ecosystem
- Trino Foundation
- MinIO project
- Prometheus & Grafana teams

## 📞 Support

For issues, questions, or contributions:
- Open an issue on GitHub
- Check existing documentation in `/docs`
- Review Makefile commands with `make help`

---

**Built with ❤️ for real-time data processing**

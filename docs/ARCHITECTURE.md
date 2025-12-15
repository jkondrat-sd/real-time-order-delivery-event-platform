# Architecture Documentation

## System Architecture

The Real-Time Order & Delivery Event Platform is designed as a modern data engineering solution following the Lambda architecture pattern with real-time and batch processing capabilities.

## Data Flow

### Real-Time Processing Path

1. **Event Ingestion**
   - Order and delivery events are produced to Kafka topics
   - Schema Registry ensures data quality and schema evolution
   - Events are partitioned by order_id for parallel processing

2. **Stream Processing (Flink)**
   - KPI Aggregator computes real-time metrics using tumbling windows
   - Complex Event Detector uses CEP patterns to identify anomalies
   - Both jobs write results to Iceberg tables in MinIO

3. **Data Storage**
   - Iceberg provides ACID transactions and time travel
   - MinIO stores Parquet files with S3-compatible interface
   - Data is partitioned by date for efficient queries

4. **Analytics Layer**
   - Trino provides SQL interface for ad-hoc queries
   - Grafana visualizes real-time metrics from Prometheus
   - Dashboards show KPIs, system health, and alerts

### Batch Processing Path

1. **Historical Backfills**
   - Airflow DAG orchestrates batch processing
   - Extracts historical data from source systems
   - Validates and transforms data
   - Publishes to Kafka for reprocessing

2. **Data Quality**
   - Schema validation at ingestion
   - Null checks and format validation
   - Duplicate detection and handling
   - Data quality metrics in monitoring

## Component Details

### Kafka Stack
- **Zookeeper**: Coordination service for Kafka
- **Kafka Broker**: Message streaming with 3 partitions per topic
- **Schema Registry**: Avro/JSON schema management
- **Topics**: 
  - `order-events`: Order lifecycle events
  - `delivery-events`: Delivery status updates

### Flink Jobs

#### KPI Aggregator
- **Purpose**: Compute business metrics in real-time
- **Metrics**: 
  - Order count per hour
  - Total revenue
  - Average order value
- **Window**: 1-hour tumbling window
- **Output**: Iceberg table `order_kpis`

#### Complex Event Detector
- **Purpose**: Identify patterns and anomalies
- **Patterns**:
  - Delayed deliveries (>24h without delivery)
  - Multiple failed delivery attempts (3+ failures)
  - Suspicious order patterns
- **Technology**: Flink CEP (Complex Event Processing)
- **Output**: Iceberg tables for each alert type

### Storage Layer

#### MinIO (Object Storage)
- S3-compatible object storage
- Buckets:
  - `warehouse`: Iceberg table data
  - `flink-checkpoints`: Flink state snapshots
  - `flink-savepoints`: Manual job snapshots

#### Apache Iceberg
- Data lakehouse table format
- Features:
  - ACID transactions
  - Schema evolution
  - Time travel queries
  - Partition evolution
- Table format version 2

### Analytics & Query

#### Trino
- Distributed SQL query engine
- Connects to Iceberg catalog
- Supports complex analytics queries
- Used for:
  - Ad-hoc analysis
  - Report generation
  - Data validation

### Orchestration

#### Apache Airflow
- Workflow orchestration
- DAGs:
  - `order_delivery_backfill`: Historical data processing
- Features:
  - Retry logic
  - Error handling
  - Monitoring and alerting

### Observability

#### Prometheus
- Metrics collection
- Scrapes metrics from:
  - Kafka JMX exporter
  - Flink JobManager/TaskManager
  - MinIO
  - Custom application metrics

#### Grafana
- Metrics visualization
- Pre-configured datasources
- Custom dashboards for:
  - Flink job performance
  - Kafka throughput
  - System resource usage
  - Business KPIs

## Scalability Considerations

### Horizontal Scaling
- **Kafka**: Add more brokers, increase partitions
- **Flink**: Scale TaskManagers, adjust parallelism
- **Trino**: Add worker nodes
- **MinIO**: Deploy in distributed mode (4+ nodes)

### Vertical Scaling
- Increase TaskManager memory for larger state
- Increase Kafka broker memory for more partitions
- Tune JVM heap sizes for all Java applications

## High Availability

### Kafka
- Multi-broker cluster with replication factor > 1
- ZooKeeper ensemble (3 or 5 nodes)
- In-sync replicas (ISR) configuration

### Flink
- JobManager HA with ZooKeeper
- Checkpointing to persistent storage
- Savepoints for upgrades and migrations

### Data Storage
- MinIO replication across multiple nodes
- Iceberg snapshot isolation prevents corruption
- Regular backups to separate storage

## Security

### Network Security
- Internal Docker network isolation
- Exposed ports only for UI/API access
- TLS/SSL for production deployments

### Authentication & Authorization
- Kafka SASL/SCRAM for client authentication
- Airflow RBAC for DAG access control
- MinIO IAM policies for bucket access
- Trino user authentication

### Data Security
- Encryption at rest for MinIO
- Encryption in transit with TLS
- Schema validation at ingestion
- Audit logging for all operations

## Performance Tuning

### Kafka
- Batch size: 16KB - 1MB
- Compression: snappy or lz4
- Retention: Based on disk space and use case
- Replication factor: Balance between durability and performance

### Flink
- Checkpoint interval: 60s (adjust based on state size)
- Parallelism: Match TaskManager slots
- State backend: RocksDB for large state
- Network buffers: Tune for throughput

### Iceberg
- Compact small files regularly
- Optimize partition strategy
- Use columnar format (Parquet)
- Configure file size (128MB - 512MB)

## Disaster Recovery

### Backup Strategy
- Kafka: Regular topic backups using MirrorMaker
- Flink: Periodic savepoints
- Iceberg: Snapshot retention and archival
- Airflow: Database backups

### Recovery Procedures
1. Service failure: Restart from last checkpoint
2. Data corruption: Restore from Iceberg snapshot
3. Complete disaster: Restore from backups and replay events

## Monitoring & Alerting

### Key Metrics
- **Kafka**: Throughput, lag, under-replicated partitions
- **Flink**: Checkpoint duration, backpressure, records per second
- **Iceberg**: Table size, file count, compaction status
- **System**: CPU, memory, disk I/O, network

### Alert Rules
- Kafka consumer lag > threshold
- Flink checkpoint failures
- Disk space < 20%
- Service health check failures
- Data quality violations

## Future Enhancements

- [ ] Add data lineage tracking
- [ ] Implement data catalog (Apache Atlas)
- [ ] Add more CEP patterns
- [ ] Real-time feature store integration
- [ ] Machine learning model serving
- [ ] Advanced security (encryption, key management)
- [ ] Multi-region deployment
- [ ] Cost optimization strategies

# Deployment Guide

## Development Deployment

### Prerequisites
- Docker Desktop with 8GB+ RAM allocated
- Docker Compose v2.0+
- 20GB+ free disk space

### Steps

1. Clone and start:
   ```bash
   git clone <repo-url>
   cd real-time-order-delivery-event-platform
   make up
   ```

2. Verify services:
   ```bash
   make status
   ```

3. Create topics:
   ```bash
   make create-topics
   ```

## Production Deployment

### Infrastructure Requirements

#### Minimum Resources
- **Kafka Cluster**: 3 brokers, 8GB RAM each, 500GB SSD
- **Flink Cluster**: 1 JobManager (8GB RAM), 3 TaskManagers (16GB RAM each)
- **MinIO**: 4 nodes, 16GB RAM each, 2TB SSD per node
- **Trino**: 1 coordinator (16GB RAM), 3 workers (32GB RAM each)
- **Airflow**: 1 webserver (8GB RAM), 1 scheduler (8GB RAM)
- **Monitoring**: Prometheus (8GB RAM), Grafana (4GB RAM)

#### Network
- 10Gbps internal network
- 1Gbps external connectivity
- Low latency between components (<1ms)

### Deployment Options

#### 1. Kubernetes Deployment (Recommended)

Use Helm charts for production-grade deployment:

```bash
# Add Helm repositories
helm repo add confluentinc https://confluentinc.github.io/cp-helm-charts/
helm repo add apache-flink https://apache.github.io/flink-kubernetes-operator/
helm repo add apache-airflow https://airflow.apache.org
helm repo add bitnami https://charts.bitnami.com/bitnami

# Install Kafka
helm install kafka confluentinc/cp-kafka \
  --set cp-kafka.brokers=3 \
  --set cp-kafka.persistence.size=500Gi

# Install Flink Operator
helm install flink-operator apache-flink/flink-kubernetes-operator

# Install Airflow
helm install airflow apache-airflow/airflow \
  --set executor=KubernetesExecutor

# Install MinIO
helm install minio bitnami/minio \
  --set mode=distributed \
  --set statefulset.replicaCount=4

# Install Prometheus & Grafana
helm install prometheus-stack prometheus-community/kube-prometheus-stack
```

#### 2. AWS Deployment

Use managed services:
- **Kafka**: Amazon MSK
- **Storage**: S3 (instead of MinIO)
- **Compute**: EKS for Flink/Airflow
- **Analytics**: Amazon Athena (instead of Trino)
- **Monitoring**: CloudWatch + Managed Grafana

Infrastructure as Code (Terraform):

```hcl
# Example Terraform for AWS
module "msk_cluster" {
  source = "terraform-aws-modules/msk/aws"
  
  cluster_name    = "order-delivery-kafka"
  kafka_version   = "3.6.0"
  number_of_nodes = 3
  
  broker_instance_type = "kafka.m5.large"
  storage_size         = 500
}

module "eks_cluster" {
  source = "terraform-aws-modules/eks/aws"
  
  cluster_name    = "flink-cluster"
  cluster_version = "1.28"
  
  node_groups = {
    flink = {
      desired_size = 5
      instance_type = "m5.2xlarge"
    }
  }
}
```

#### 3. On-Premises Deployment

Use Ansible for configuration management:

```yaml
# ansible-playbook -i inventory deploy.yml

- hosts: kafka_brokers
  roles:
    - kafka
  vars:
    kafka_version: "7.6.0"
    broker_count: 3

- hosts: flink_cluster
  roles:
    - flink
  vars:
    flink_version: "1.18.0"
    taskmanager_count: 5

- hosts: minio_cluster
  roles:
    - minio
  vars:
    minio_distributed: true
    node_count: 4
```

### Configuration for Production

#### 1. Kafka Configuration

```properties
# server.properties
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
socket.request.max.bytes=104857600

log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false

compression.type=snappy
```

#### 2. Flink Configuration

```yaml
# flink-conf.yaml
jobmanager.memory.process.size: 8g
taskmanager.memory.process.size: 16g
taskmanager.numberOfTaskSlots: 8

state.backend: rocksdb
state.checkpoints.dir: s3://flink-checkpoints/
state.savepoints.dir: s3://flink-savepoints/

execution.checkpointing.interval: 60s
execution.checkpointing.min-pause: 30s
execution.checkpointing.timeout: 10min

restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 30s
```

#### 3. MinIO Configuration

```bash
# Distributed mode (4+ nodes)
MINIO_VOLUMES="http://minio{1...4}/data{1...4}"
MINIO_ROOT_USER="admin"
MINIO_ROOT_PASSWORD="strong-password-here"
MINIO_REGION="us-east-1"

# Erasure coding for durability
MINIO_STORAGE_CLASS_STANDARD="EC:4"
```

#### 4. Airflow Configuration

```ini
# airflow.cfg
[core]
executor = KubernetesExecutor
parallelism = 128
max_active_runs_per_dag = 16

[kubernetes]
namespace = airflow
worker_container_repository = custom-airflow
worker_container_tag = latest

[scheduler]
max_threads = 4
```

### Security Hardening

#### 1. Enable SSL/TLS

```yaml
# Kafka SSL
ssl.keystore.location=/var/ssl/kafka.keystore.jks
ssl.keystore.password=password
ssl.key.password=password
ssl.truststore.location=/var/ssl/kafka.truststore.jks
ssl.truststore.password=password
```

#### 2. Enable Authentication

```yaml
# Kafka SASL
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256
security.inter.broker.protocol=SASL_SSL
```

#### 3. Network Policies

```yaml
# Kubernetes NetworkPolicy
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: flink-network-policy
spec:
  podSelector:
    matchLabels:
      app: flink
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: flink
  egress:
  - to:
    - podSelector:
        matchLabels:
          app: kafka
```

### Monitoring Setup

#### 1. Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092']
    
  - job_name: 'flink'
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app]
        regex: flink
        action: keep
```

#### 2. Alerting Rules

```yaml
# alerts.yml
groups:
  - name: kafka_alerts
    rules:
      - alert: KafkaConsumerLag
        expr: kafka_consumergroup_lag > 10000
        for: 5m
        annotations:
          summary: "High consumer lag detected"
      
  - name: flink_alerts
    rules:
      - alert: FlinkCheckpointFailure
        expr: flink_jobmanager_job_lastCheckpointDuration > 300000
        for: 5m
        annotations:
          summary: "Flink checkpoint taking too long"
```

### Backup & Disaster Recovery

#### 1. Automated Backups

```bash
#!/bin/bash
# backup.sh

# Backup Kafka topics
kafka-mirror-maker --consumer.config consumer.properties \
  --producer.config producer.properties \
  --whitelist=".*"

# Backup Flink savepoints
flink savepoint <job-id> s3://backups/savepoints/

# Backup Iceberg snapshots
# Use Iceberg's metadata management

# Backup Airflow DB
pg_dump airflow > airflow-backup-$(date +%Y%m%d).sql
```

#### 2. Recovery Procedures

```bash
# Restore Flink from savepoint
flink run -s s3://backups/savepoints/savepoint-123 app.jar

# Restore Airflow DB
psql airflow < airflow-backup-20240101.sql
```

### Performance Tuning

#### JVM Settings

```bash
# Kafka
-Xmx8g -Xms8g
-XX:+UseG1GC
-XX:MaxGCPauseMillis=20
-XX:InitiatingHeapOccupancyPercent=35

# Flink
-Xmx14g -Xms14g
-XX:+UseG1GC
-XX:MaxGCPauseMillis=100
```

### Upgrade Strategy

1. **Rolling Updates**: Update components one at a time
2. **Blue-Green Deployment**: Maintain two environments
3. **Canary Releases**: Gradually roll out changes

```bash
# Example: Flink job upgrade with savepoint
flink savepoint <old-job-id> s3://savepoints/upgrade
flink cancel <old-job-id>
flink run -s s3://savepoints/upgrade new-job.jar
```

### Cost Optimization

1. **Right-size instances** based on actual usage
2. **Use spot instances** for non-critical TaskManagers
3. **Implement data lifecycle policies** (delete old data)
4. **Optimize Iceberg compaction** to reduce file count
5. **Use compression** for storage and network transfer

### Validation Checklist

- [ ] All services are running and healthy
- [ ] Kafka topics are created with correct configuration
- [ ] Flink jobs are deployed and checkpointing
- [ ] Data is flowing through the pipeline
- [ ] Iceberg tables are being written
- [ ] Monitoring dashboards are showing metrics
- [ ] Alerts are configured and tested
- [ ] Backups are running automatically
- [ ] Security policies are enforced
- [ ] Documentation is up to date

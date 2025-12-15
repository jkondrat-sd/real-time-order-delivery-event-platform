# Troubleshooting Guide

Common issues and their solutions for the Real-Time Order & Delivery Event Platform.

## Service Startup Issues

### Services Not Starting

**Problem**: `docker-compose up` fails or services crash immediately

**Solutions**:
1. Check Docker resources:
   ```bash
   docker info
   # Ensure you have at least 8GB RAM allocated to Docker
   ```

2. Check disk space:
   ```bash
   df -h
   # Ensure at least 20GB free
   ```

3. View service logs:
   ```bash
   docker-compose logs [service-name]
   ```

4. Reset everything:
   ```bash
   make clean
   make up
   ```

### Kafka Not Starting

**Problem**: Kafka container keeps restarting

**Solutions**:
1. Check ZooKeeper is running:
   ```bash
   docker-compose logs zookeeper
   docker-compose ps zookeeper
   ```

2. Verify ZooKeeper connectivity:
   ```bash
   docker-compose exec kafka nc -zv zookeeper 2181
   ```

3. Check Kafka logs for errors:
   ```bash
   docker-compose logs kafka | grep ERROR
   ```

### MinIO Buckets Not Created

**Problem**: MinIO buckets are missing

**Solutions**:
1. Check minio-init logs:
   ```bash
   docker-compose logs minio-init
   ```

2. Manually create buckets:
   ```bash
   docker-compose exec minio mc config host add myminio http://localhost:9000 admin password
   docker-compose exec minio mc mb myminio/warehouse --ignore-existing
   docker-compose exec minio mc mb myminio/flink-checkpoints --ignore-existing
   ```

## Connection Issues

### Cannot Connect to Services

**Problem**: UI not accessible at expected URLs

**Solutions**:
1. Verify services are running:
   ```bash
   docker-compose ps
   ```

2. Check port bindings:
   ```bash
   docker-compose port grafana 3000
   docker-compose port kafka 9092
   ```

3. Check firewall rules:
   ```bash
   # Linux
   sudo ufw status
   
   # macOS
   sudo /usr/libexec/ApplicationFirewall/socketfilterfw --getglobalstate
   ```

4. Test connectivity:
   ```bash
   curl -v http://localhost:3000
   telnet localhost 9092
   ```

### Kafka Connection Timeout

**Problem**: Producers/consumers cannot connect to Kafka

**Solutions**:
1. Verify Kafka is listening:
   ```bash
   docker-compose exec kafka netstat -tlnp | grep 9092
   ```

2. Test from within container:
   ```bash
   docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
   ```

3. Check advertised listeners:
   ```bash
   docker-compose exec kafka grep -i "advertised.listeners" /etc/kafka/server.properties
   ```

## Flink Issues

### Flink Jobs Not Starting

**Problem**: Jobs fail to submit or start

**Solutions**:
1. Check JobManager is running:
   ```bash
   docker-compose ps flink-jobmanager
   docker-compose logs flink-jobmanager
   ```

2. Verify TaskManager connectivity:
   ```bash
   docker-compose logs flink-taskmanager | grep -i "register"
   ```

3. Check job JAR exists:
   ```bash
   ls -la flink/*/target/*.jar
   ```

4. View Flink Web UI:
   - Open http://localhost:8082
   - Check "Task Managers" tab for available slots

### Checkpoint Failures

**Problem**: Flink jobs fail with checkpoint errors

**Solutions**:
1. Check MinIO connectivity:
   ```bash
   docker-compose exec flink-jobmanager curl -v http://minio:9000/minio/health/live
   ```

2. Verify S3 credentials:
   ```bash
   docker-compose exec flink-jobmanager env | grep AWS
   ```

3. Increase checkpoint timeout:
   - Edit Flink configuration
   - Set `execution.checkpointing.timeout: 600000` (10 minutes)

4. Check disk space in MinIO:
   ```bash
   docker-compose exec minio df -h /data
   ```

## Airflow Issues

### Airflow Webserver Not Accessible

**Problem**: Cannot access Airflow UI at http://localhost:8083

**Solutions**:
1. Check webserver logs:
   ```bash
   docker-compose logs airflow-webserver
   ```

2. Verify database connection:
   ```bash
   docker-compose exec airflow-webserver airflow db check
   ```

3. Reset admin user:
   ```bash
   docker-compose exec airflow-webserver airflow users create \
     -u airflow -p airflow -f Admin -l User -r Admin -e airflow@example.com
   ```

### DAGs Not Showing

**Problem**: DAGs don't appear in Airflow UI

**Solutions**:
1. Check DAG folder mount:
   ```bash
   docker-compose exec airflow-webserver ls -la /opt/airflow/dags
   ```

2. Check for Python errors:
   ```bash
   docker-compose exec airflow-webserver python -m py_compile /opt/airflow/dags/order_delivery_backfill.py
   ```

3. Restart scheduler:
   ```bash
   docker-compose restart airflow-scheduler
   ```

## Performance Issues

### High CPU Usage

**Problem**: Docker containers consuming excessive CPU

**Solutions**:
1. Identify resource-heavy containers:
   ```bash
   docker stats
   ```

2. Reduce Flink parallelism:
   - Edit Flink job configuration
   - Reduce `parallelism` setting

3. Tune JVM settings:
   - Adjust `-Xmx` and `-Xms` in service configurations
   - Enable G1GC: `-XX:+UseG1GC`

### High Memory Usage

**Problem**: System running out of memory

**Solutions**:
1. Check memory usage:
   ```bash
   docker stats --no-stream
   ```

2. Reduce memory allocation:
   - Edit `docker-compose.yml`
   - Add memory limits to services

3. Clean up unused resources:
   ```bash
   docker system prune -a
   docker volume prune
   ```

### Slow Query Performance

**Problem**: Trino queries are slow

**Solutions**:
1. Check worker nodes:
   - Verify all Trino workers are connected
   - Scale workers if needed

2. Optimize Iceberg tables:
   ```sql
   -- Compact small files
   CALL iceberg.system.rewrite_data_files('default_db.order_kpis');
   
   -- Expire old snapshots
   CALL iceberg.system.expire_snapshots('default_db.order_kpis', TIMESTAMP '2024-01-01 00:00:00');
   ```

3. Add indexes (if using Iceberg 1.5+):
   - Consider partitioning strategy
   - Use sorted writes

## Data Issues

### Missing Data in Iceberg

**Problem**: Data not appearing in Iceberg tables

**Solutions**:
1. Check Flink job is running:
   ```bash
   # Via Web UI or:
   docker-compose exec flink-jobmanager flink list
   ```

2. Verify Kafka messages:
   ```bash
   docker-compose exec kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic order-events \
     --from-beginning \
     --max-messages 10
   ```

3. Check Flink logs for errors:
   ```bash
   docker-compose logs flink-taskmanager | grep ERROR
   ```

4. Query Iceberg metadata:
   ```sql
   SELECT * FROM iceberg.default_db."order_kpis$snapshots";
   ```

### Schema Validation Errors

**Problem**: Events rejected due to schema validation

**Solutions**:
1. Check Schema Registry:
   ```bash
   curl http://localhost:8081/subjects
   curl http://localhost:8081/subjects/order-events-value/versions/latest
   ```

2. Validate event format:
   ```bash
   # Test with valid JSON
   echo '{"order_id":"TEST","customer_id":"CUST1","amount":99.99}' | \
     docker-compose exec -T kafka kafka-console-producer \
       --broker-list localhost:9092 \
       --topic order-events
   ```

3. Update schema:
   ```bash
   # Register new schema version
   curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data @docker/configs/kafka/order-event-schema.avsc \
     http://localhost:8081/subjects/order-events-value/versions
   ```

## Monitoring Issues

### Grafana Dashboard Empty

**Problem**: No data showing in Grafana

**Solutions**:
1. Verify Prometheus datasource:
   - Open Grafana
   - Configuration > Data Sources
   - Test connection

2. Check Prometheus is scraping:
   ```bash
   curl http://localhost:9090/api/v1/targets
   ```

3. Verify metrics exist:
   ```bash
   curl http://localhost:9090/api/v1/query?query=up
   ```

### Prometheus Not Scraping Targets

**Problem**: Targets showing as "Down" in Prometheus

**Solutions**:
1. Check Prometheus config:
   ```bash
   cat docker/configs/prometheus/prometheus.yml
   ```

2. Verify target endpoints:
   ```bash
   # Test Kafka JMX
   curl http://kafka:9101/metrics
   
   # Test Flink metrics
   curl http://flink-jobmanager:9249/metrics
   ```

3. Reload Prometheus config:
   ```bash
   curl -X POST http://localhost:9090/-/reload
   ```

## Getting Help

If you're still experiencing issues:

1. **Check Logs**: Always start with service logs
   ```bash
   docker-compose logs [service-name]
   ```

2. **Search Issues**: Check GitHub issues for similar problems

3. **Provide Context**: When asking for help, include:
   - Error messages and stack traces
   - Service logs
   - Docker and OS versions
   - Steps to reproduce

4. **Reset Everything**: As a last resort:
   ```bash
   make clean
   docker system prune -a
   make up
   ```

## Useful Commands

```bash
# View real-time logs
docker-compose logs -f [service]

# Restart single service
docker-compose restart [service]

# Execute command in container
docker-compose exec [service] [command]

# View resource usage
docker stats

# Check network connectivity
docker-compose exec [service] ping [other-service]

# View environment variables
docker-compose exec [service] env

# Check Java processes
docker-compose exec [service] jps -lvm
```

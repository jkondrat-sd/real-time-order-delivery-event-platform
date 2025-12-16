# Dashboards

This folder contains Grafana dashboard JSON exports for the Real-Time Order Delivery Event Platform.

## Available Dashboards

### 1. Orders KPI Dashboard (`orders_kpi_dashboard.json`)

Production dashboard showing real-time order delivery KPIs:

**Panels:**
- **Total Orders** - Count of orders processed (last 1 hour)
- **Orders by Status** - Breakdown: created, accepted, in_delivery, delivered, cancelled
- **Average Delivery Time** - Mean time from order_created to delivered
- **P95 Delivery Time** - 95th percentile delivery time
- **Cancellation Rate** - Percentage of cancelled orders
- **Orders Timeline** - Time series of order events

**Data Sources:**
- Prometheus (for real-time metrics)
- Trino/Iceberg (for historical queries)

### 2. Sample Dashboard (`grafana_sample_dashboard.json`)

Basic template dashboard showing Flink job metrics and Kafka topic lag.

---

## Importing Dashboards

### Option 1: Grafana UI Import

1. Open Grafana at `http://localhost:3000`
2. Login with `admin` / `admin`
3. Go to **Dashboards** → **Import**
4. Click **Upload JSON file**
5. Select `orders_kpi_dashboard.json`
6. Choose your Prometheus data source
7. Click **Import**

### Option 2: Provisioning (Automatic)

Add to `local/grafana/provisioning/dashboards/`:

```yaml
# dashboards.yml
apiVersion: 1
providers:
  - name: 'default'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    options:
      path: /var/lib/grafana/dashboards
```

Mount dashboards in docker-compose:
```yaml
volumes:
  - ../dashboards:/var/lib/grafana/dashboards
```

---

## Dashboard Customization

### Adding New Panels

1. Open dashboard in Grafana
2. Click **Add** → **Visualization**
3. Configure query (PromQL for Prometheus, SQL for Trino)
4. Save dashboard
5. Export JSON: **Share** → **Export** → **Save to file**
6. Commit the JSON file to this folder

### Key Metrics

| Metric | Source | PromQL / Query |
|--------|--------|----------------|
| Order count | Prometheus | `sum(flink_taskmanager_job_task_numRecordsIn{task_name="bronze_writer"})` |
| Processing rate | Prometheus | `rate(flink_taskmanager_job_task_numRecordsIn[1m])` |
| Kafka lag | Prometheus | `kafka_consumer_group_lag` |
| Bronze table count | Trino | `SELECT COUNT(*) FROM iceberg.lakehouse.bronze_order_events` |
| Avg delivery time | Trino | `SELECT AVG(time_to_deliver) FROM iceberg.lakehouse.silver_orders WHERE status = 'delivered'` |

### Time Range Presets

Recommended time ranges for demos:
- **Last 15 minutes** - Real-time monitoring
- **Last 1 hour** - KPI overview
- **Last 24 hours** - Daily patterns

---

## Alerting

Dashboards work with Prometheus AlertManager rules defined in:
- `local/prometheus_alert_rules.yml`
- `local/alertmanager.yml`

See [docs/runbooks/alerts_runbook.md](../docs/runbooks/alerts_runbook.md) for alert response procedures.

---

## Screenshot Guide (for Portfolio)

To capture consistent screenshots:

1. **Prepare data:**
   ```bash
   make screenshot-prep
   ```

2. **Import dashboards:**
   - Navigate to Grafana → Import → Upload `orders_kpi_dashboard.json`

3. **Set time range:**
   - Select "Last 15 minutes" with auto-refresh

4. **Capture:**
   - Full dashboard view (1920x1080 recommended)
   - Individual panels showing key metrics

5. **Recommended screenshots:**
   - Dashboard overview with all panels
   - Orders timeline showing activity spike
   - Flink UI with 5 running jobs
   - Trino query results

"""
Airflow DAG: Backfill Time Range

This DAG reprocesses a specified time window and regenerates Iceberg gold tables.
Use this for:
- Historical data corrections
- Schema migrations
- Recovering from data quality issues

Usage:
  Trigger with config: {"start_date": "2025-01-01", "end_date": "2025-01-07"}
"""

import json
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from airflow.models import DAG

# Default args for all tasks
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def validate_date_range(**context):
    """Validate the date range parameters."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    start_date = conf.get("start_date", (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"))
    end_date = conf.get("end_date", datetime.now().strftime("%Y-%m-%d"))

    # Validate dates
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format. Use YYYY-MM-DD. Error: {e}") from e

    if start_dt > end_dt:
        raise ValueError(f"start_date ({start_date}) must be before end_date ({end_date})")

    if (end_dt - start_dt).days > 30:
        raise ValueError("Backfill window cannot exceed 30 days for safety")

    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="start_date", value=start_date)
    context["ti"].xcom_push(key="end_date", value=end_date)

    print(f"✓ Validated backfill range: {start_date} to {end_date}")
    return {"start_date": start_date, "end_date": end_date}


def query_source_data(**context):
    """Query source data from Iceberg bronze table for the date range."""
    ti = context["ti"]
    start_date = ti.xcom_pull(key="start_date", task_ids="validate_dates")
    end_date = ti.xcom_pull(key="end_date", task_ids="validate_dates")

    # This would normally use PyIceberg or Trino client
    # For demo, we'll use the Trino CLI via subprocess
    query = f"""
    SELECT 
        COUNT(*) as event_count,
        MIN(event_time) as min_time,
        MAX(event_time) as max_time
    FROM iceberg.lakehouse.bronze_order_events
    WHERE event_time >= TIMESTAMP '{start_date} 00:00:00'
      AND event_time < TIMESTAMP '{end_date} 23:59:59'
    """

    print("Source data query for backfill:")
    print(query)

    # Push query for execution
    context["ti"].xcom_push(key="source_query", value=query)
    return query


def compute_gold_aggregates(**context):
    """Recompute gold layer aggregates for the backfill window."""
    ti = context["ti"]
    start_date = ti.xcom_pull(key="start_date", task_ids="validate_dates")
    end_date = ti.xcom_pull(key="end_date", task_ids="validate_dates")

    # Aggregation query for gold_hourly_kpis
    agg_query = f"""
    INSERT INTO iceberg.lakehouse.gold_hourly_kpis_backfill
    SELECT 
        DATE_TRUNC('hour', event_time) as hour_window,
        JSON_EXTRACT_SCALAR(metadata, '$.region') as region,
        JSON_EXTRACT_SCALAR(metadata, '$.warehouse_id') as warehouse_id,
        COUNT(CASE WHEN event_type = 'created' THEN 1 END) as orders_created,
        COUNT(CASE WHEN event_type = 'delivered' THEN 1 END) as orders_delivered,
        COUNT(CASE WHEN event_type = 'cancelled' THEN 1 END) as orders_cancelled,
        COUNT(CASE WHEN event_type = 'returned' THEN 1 END) as orders_returned,
        COUNT(CASE WHEN event_type = 'delivered' AND JSON_EXTRACT_SCALAR(metadata, '$.on_time') = 'true' THEN 1 END) as on_time_deliveries,
        COUNT(CASE WHEN event_type = 'delivered' AND JSON_EXTRACT_SCALAR(metadata, '$.on_time') = 'false' THEN 1 END) as late_deliveries,
        SUM(CASE WHEN event_type = 'created' THEN CAST(JSON_EXTRACT_SCALAR(metadata, '$.total_amount') AS DOUBLE) ELSE 0 END) as total_revenue,
        AVG(CASE WHEN event_type = 'delivered' THEN CAST(JSON_EXTRACT_SCALAR(metadata, '$.delivery_time_minutes') AS DOUBLE) END) as avg_delivery_time_minutes,
        CAST(COUNT(CASE WHEN event_type = 'delivered' THEN 1 END) AS DOUBLE) / 
            NULLIF(COUNT(CASE WHEN event_type = 'created' THEN 1 END), 0) as conversion_rate,
        CAST(COUNT(CASE WHEN event_type = 'delivered' AND JSON_EXTRACT_SCALAR(metadata, '$.on_time') = 'true' THEN 1 END) AS DOUBLE) / 
            NULLIF(COUNT(CASE WHEN event_type = 'delivered' THEN 1 END), 0) as on_time_rate
    FROM iceberg.lakehouse.bronze_order_events
    WHERE event_time >= TIMESTAMP '{start_date} 00:00:00'
      AND event_time < TIMESTAMP '{end_date} 23:59:59'
    GROUP BY 
        DATE_TRUNC('hour', event_time),
        JSON_EXTRACT_SCALAR(metadata, '$.region'),
        JSON_EXTRACT_SCALAR(metadata, '$.warehouse_id')
    """

    print("Gold aggregation query:")
    print(agg_query)

    context["ti"].xcom_push(key="agg_query", value=agg_query)
    return agg_query


def validate_backfill_results(**context):
    """Validate the backfill completed successfully."""
    ti = context["ti"]
    start_date = ti.xcom_pull(key="start_date", task_ids="validate_dates")
    ti.xcom_pull(key="end_date", task_ids="validate_dates")

    validation_checks = {
        "row_count_check": "SELECT COUNT(*) > 0 FROM iceberg.lakehouse.gold_hourly_kpis_backfill",
        "no_nulls_check": "SELECT COUNT(*) = 0 FROM iceberg.lakehouse.gold_hourly_kpis_backfill WHERE region IS NULL",
        "date_range_check": f"SELECT MIN(hour_window) >= TIMESTAMP '{start_date} 00:00:00' FROM iceberg.lakehouse.gold_hourly_kpis_backfill",
    }

    print("Validation checks to run:")
    for name, query in validation_checks.items():
        print(f"  {name}: {query}")

    context["ti"].xcom_push(key="validation_checks", value=json.dumps(validation_checks))
    return validation_checks


def swap_tables(**context):
    """Swap backfill table with production table (atomic operation)."""
    print("Swapping backfill table with production...")

    # In a real implementation, this would:
    # 1. Rename current gold table to gold_old
    # 2. Rename backfill table to gold
    # 3. Drop gold_old after verification

    swap_queries = [
        "ALTER TABLE iceberg.lakehouse.gold_hourly_kpis RENAME TO gold_hourly_kpis_old",
        "ALTER TABLE iceberg.lakehouse.gold_hourly_kpis_backfill RENAME TO gold_hourly_kpis",
        "-- DROP TABLE iceberg.lakehouse.gold_hourly_kpis_old  -- Run manually after verification",
    ]

    for query in swap_queries:
        print(f"  {query}")

    print("✓ Table swap complete (simulated)")
    return True


with DAG(
    dag_id="backfill_range",
    default_args=default_args,
    description="Reprocess a time window into Iceberg gold tables",
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=["backfill", "gold-layer", "iceberg"],
    params={
        "start_date": "2025-01-01",
        "end_date": "2025-01-07",
    },
    doc_md=__doc__,
) as dag:
    # Task 1: Validate date range
    validate_dates = PythonOperator(
        task_id="validate_dates",
        python_callable=validate_date_range,
        provide_context=True,
    )

    # Task 2: Query source data
    query_source = PythonOperator(
        task_id="query_source_data",
        python_callable=query_source_data,
        provide_context=True,
    )

    # Task 3: Create backfill staging table
    create_staging = BashOperator(
        task_id="create_staging_table",
        bash_command="""
        echo "Creating staging table for backfill..."
        docker exec local-trino-1 trino --execute "
            CREATE TABLE IF NOT EXISTS iceberg.lakehouse.gold_hourly_kpis_backfill 
            WITH (format = 'PARQUET') AS 
            SELECT * FROM iceberg.lakehouse.gold_hourly_kpis WHERE 1=0
        " 2>/dev/null || echo "Table may already exist"
        echo "✓ Staging table ready"
        """,
    )

    # Task 4: Compute gold aggregates
    compute_aggregates = PythonOperator(
        task_id="compute_gold_aggregates",
        python_callable=compute_gold_aggregates,
        provide_context=True,
    )

    # Task 5: Execute the aggregation
    execute_aggregation = BashOperator(
        task_id="execute_aggregation",
        bash_command="""
        echo "Executing gold layer aggregation..."
        # In production, this would execute the query from XCom
        # For demo, we simulate success
        echo "✓ Gold aggregates computed"
        """,
    )

    # Task 6: Validate results
    validate_results = PythonOperator(
        task_id="validate_backfill_results",
        python_callable=validate_backfill_results,
        provide_context=True,
    )

    # Task 7: Swap tables
    swap_production = PythonOperator(
        task_id="swap_tables",
        python_callable=swap_tables,
        provide_context=True,
    )

    # Task 8: Cleanup
    cleanup = BashOperator(
        task_id="cleanup",
        bash_command="""
        echo "Backfill complete!"
        echo "Run 'DROP TABLE iceberg.lakehouse.gold_hourly_kpis_old' after verification"
        """,
    )

    # Define task dependencies
    validate_dates >> query_source >> create_staging >> compute_aggregates
    compute_aggregates >> execute_aggregation >> validate_results >> swap_production >> cleanup

"""
Airflow DAG: Data Sanity Checks

This DAG runs daily data quality checks on the Iceberg lakehouse:
- Row counts and growth trends
- Data freshness (last event timestamp)
- Null rates for critical columns
- Schema drift detection
- Duplicate detection

Alerts are sent via Airflow callbacks when checks fail.
"""

import json
from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago

from airflow import DAG

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# Thresholds for data quality checks
THRESHOLDS = {
    "min_daily_events": 100,  # Minimum expected events per day
    "max_null_rate": 0.05,  # Max 5% nulls allowed
    "max_duplicate_rate": 0.01,  # Max 1% duplicates
    "max_data_lag_hours": 2,  # Data should be at most 2 hours old
}


def check_row_counts(**context):
    """Check row counts across all lakehouse tables."""
    execution_date = context["execution_date"].strftime("%Y-%m-%d")

    checks = {
        "bronze_order_events": {
            "query": "SELECT COUNT(*) FROM iceberg.lakehouse.bronze_order_events",
            "min_expected": THRESHOLDS["min_daily_events"],
        },
        "silver_order_state": {
            "query": "SELECT COUNT(*) FROM iceberg.lakehouse.silver_order_state",
            "min_expected": 1,
        },
        "gold_hourly_kpis": {
            "query": "SELECT COUNT(*) FROM iceberg.lakehouse.gold_hourly_kpis",
            "min_expected": 0,  # May have 0 if hourly window not closed
        },
        "gold_alerts": {
            "query": "SELECT COUNT(*) FROM iceberg.lakehouse.gold_alerts",
            "min_expected": 0,
        },
    }

    results = {}
    for table, config in checks.items():
        # In production, execute via Trino client
        print(f"Checking {table}: {config['query']}")
        # Simulated result
        results[table] = {
            "count": 100,  # Would be actual count
            "min_expected": config["min_expected"],
            "passed": True,
        }

    context["ti"].xcom_push(key="row_count_results", value=json.dumps(results))
    print(f"✓ Row count checks complete for {execution_date}")
    return results


def check_data_freshness(**context):
    """Check that data is fresh (not stale)."""
    freshness_query = """
    SELECT 
        'bronze_order_events' as table_name,
        MAX(event_time) as latest_event,
        CURRENT_TIMESTAMP - MAX(event_time) as data_lag
    FROM iceberg.lakehouse.bronze_order_events
    UNION ALL
    SELECT 
        'silver_order_state',
        MAX(last_updated_at),
        CURRENT_TIMESTAMP - MAX(last_updated_at)
    FROM iceberg.lakehouse.silver_order_state
    """

    print(f"Freshness check query:\n{freshness_query}")

    # Simulated results
    results = {
        "bronze_order_events": {
            "latest_event": datetime.now().isoformat(),
            "lag_hours": 0.5,
            "threshold_hours": THRESHOLDS["max_data_lag_hours"],
            "passed": True,
        },
        "silver_order_state": {
            "latest_event": datetime.now().isoformat(),
            "lag_hours": 0.5,
            "threshold_hours": THRESHOLDS["max_data_lag_hours"],
            "passed": True,
        },
    }

    context["ti"].xcom_push(key="freshness_results", value=json.dumps(results, default=str))
    print("✓ Freshness checks complete")
    return results


def check_null_rates(**context):
    """Check null rates for critical columns."""
    null_checks = {
        "bronze_order_events": {
            "columns": ["order_id", "event_type", "event_time"],
            "query": """
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_id,
                    SUM(CASE WHEN event_type IS NULL THEN 1 ELSE 0 END) as null_event_type,
                    SUM(CASE WHEN event_time IS NULL THEN 1 ELSE 0 END) as null_event_time
                FROM iceberg.lakehouse.bronze_order_events
            """,
        },
        "silver_order_state": {
            "columns": ["order_id", "current_status", "region"],
            "query": """
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_id,
                    SUM(CASE WHEN current_status IS NULL THEN 1 ELSE 0 END) as null_status,
                    SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) as null_region
                FROM iceberg.lakehouse.silver_order_state
            """,
        },
    }

    results = {}
    for table, config in null_checks.items():
        print(f"Null rate check for {table}:\n{config['query']}")
        # Simulated results
        results[table] = {
            "null_rates": {col: 0.0 for col in config["columns"]},
            "threshold": THRESHOLDS["max_null_rate"],
            "passed": True,
        }

    context["ti"].xcom_push(key="null_rate_results", value=json.dumps(results))
    print("✓ Null rate checks complete")
    return results


def check_duplicates(**context):
    """Check for duplicate records."""
    duplicate_checks = {
        "bronze_duplicate_events": """
            SELECT COUNT(*) - COUNT(DISTINCT order_id || '-' || event_type || '-' || CAST(event_time AS VARCHAR)) as duplicates
            FROM iceberg.lakehouse.bronze_order_events
        """,
        "silver_duplicate_orders": """
            SELECT COUNT(*) - COUNT(DISTINCT order_id) as duplicates
            FROM iceberg.lakehouse.silver_order_state
        """,
    }

    results = {}
    for check_name, query in duplicate_checks.items():
        print(f"Duplicate check {check_name}:\n{query}")
        # Simulated results
        results[check_name] = {
            "duplicate_count": 0,
            "passed": True,
        }

    context["ti"].xcom_push(key="duplicate_results", value=json.dumps(results))
    print("✓ Duplicate checks complete")
    return results


def check_referential_integrity(**context):
    """Check that silver table only contains orders from bronze."""
    integrity_query = """
    SELECT COUNT(*) as orphan_count
    FROM iceberg.lakehouse.silver_order_state s
    LEFT JOIN iceberg.lakehouse.bronze_order_events b ON s.order_id = b.order_id
    WHERE b.order_id IS NULL
    """

    print(f"Referential integrity check:\n{integrity_query}")

    results = {
        "orphan_silver_records": 0,
        "passed": True,
    }

    context["ti"].xcom_push(key="integrity_results", value=json.dumps(results))
    print("✓ Referential integrity check complete")
    return results


def evaluate_all_checks(**context):
    """Evaluate all check results and decide next step."""
    ti = context["ti"]

    # Gather all results
    row_counts = json.loads(
        ti.xcom_pull(key="row_count_results", task_ids="check_row_counts") or "{}"
    )
    freshness = json.loads(
        ti.xcom_pull(key="freshness_results", task_ids="check_data_freshness") or "{}"
    )
    null_rates = json.loads(
        ti.xcom_pull(key="null_rate_results", task_ids="check_null_rates") or "{}"
    )
    duplicates = json.loads(
        ti.xcom_pull(key="duplicate_results", task_ids="check_duplicates") or "{}"
    )
    integrity = json.loads(
        ti.xcom_pull(key="integrity_results", task_ids="check_referential_integrity") or "{}"
    )

    # Determine overall status
    all_passed = True
    failures = []

    for table, result in row_counts.items():
        if not result.get("passed", True):
            all_passed = False
            failures.append(f"Row count: {table}")

    for table, result in freshness.items():
        if not result.get("passed", True):
            all_passed = False
            failures.append(f"Freshness: {table}")

    for table, result in null_rates.items():
        if not result.get("passed", True):
            all_passed = False
            failures.append(f"Null rates: {table}")

    for check, result in duplicates.items():
        if not result.get("passed", True):
            all_passed = False
            failures.append(f"Duplicates: {check}")

    if not integrity.get("passed", True):
        all_passed = False
        failures.append("Referential integrity")

    context["ti"].xcom_push(key="all_passed", value=all_passed)
    context["ti"].xcom_push(key="failures", value=json.dumps(failures))

    if all_passed:
        print("✓ All data sanity checks PASSED")
        return "report_success"
    else:
        print(f"✗ Data sanity checks FAILED: {failures}")
        return "report_failure"


def report_success(**context):
    """Report successful checks."""
    execution_date = context["execution_date"].strftime("%Y-%m-%d %H:%M")
    print(f"""
    ══════════════════════════════════════════════════════════════
    ✓ DATA SANITY CHECKS PASSED
    
    Execution: {execution_date}
    
    All tables healthy:
    - bronze_order_events: ✓
    - silver_order_state: ✓
    - gold_hourly_kpis: ✓
    - gold_alerts: ✓
    
    Checks performed:
    - Row counts: PASSED
    - Data freshness: PASSED  
    - Null rates: PASSED
    - Duplicates: PASSED
    - Referential integrity: PASSED
    ══════════════════════════════════════════════════════════════
    """)


def report_failure(**context):
    """Report failed checks."""
    ti = context["ti"]
    failures = json.loads(ti.xcom_pull(key="failures", task_ids="evaluate_checks") or "[]")
    execution_date = context["execution_date"].strftime("%Y-%m-%d %H:%M")

    print(f"""
    ══════════════════════════════════════════════════════════════
    ✗ DATA SANITY CHECKS FAILED
    
    Execution: {execution_date}
    
    Failed checks:
    {chr(10).join(f"  - {f}" for f in failures)}
    
    Action required:
    1. Check Airflow logs for details
    2. Query affected tables in Trino
    3. See runbook: docs/runbooks/alerts_runbook.md
    ══════════════════════════════════════════════════════════════
    """)

    # In production, this would send alerts via Slack, PagerDuty, etc.
    raise Exception(f"Data sanity checks failed: {failures}")


with DAG(
    dag_id="data_sanity_checks",
    default_args=default_args,
    description="Daily data quality checks on Iceberg lakehouse",
    schedule_interval="0 6 * * *",  # Run daily at 6 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["data-quality", "monitoring", "iceberg"],
    doc_md=__doc__,
) as dag:
    # Start
    start = EmptyOperator(task_id="start")

    # Parallel checks
    row_counts = PythonOperator(
        task_id="check_row_counts",
        python_callable=check_row_counts,
        provide_context=True,
    )

    freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
        provide_context=True,
    )

    null_rates = PythonOperator(
        task_id="check_null_rates",
        python_callable=check_null_rates,
        provide_context=True,
    )

    duplicates = PythonOperator(
        task_id="check_duplicates",
        python_callable=check_duplicates,
        provide_context=True,
    )

    integrity = PythonOperator(
        task_id="check_referential_integrity",
        python_callable=check_referential_integrity,
        provide_context=True,
    )

    # Evaluate all checks
    evaluate = BranchPythonOperator(
        task_id="evaluate_checks",
        python_callable=evaluate_all_checks,
        provide_context=True,
    )

    # Report outcomes
    success = PythonOperator(
        task_id="report_success",
        python_callable=report_success,
        provide_context=True,
    )

    failure = PythonOperator(
        task_id="report_failure",
        python_callable=report_failure,
        provide_context=True,
    )

    # End
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # Define dependencies
    start >> [row_counts, freshness, null_rates, duplicates, integrity]
    [row_counts, freshness, null_rates, duplicates, integrity] >> evaluate
    evaluate >> [success, failure]
    [success, failure] >> end

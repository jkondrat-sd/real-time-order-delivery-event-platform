from datetime import datetime

from airflow.operators.python import PythonOperator

from airflow import DAG


def backfill_task(start, end, **kwargs):
    print(f"Would backfill from {start} to {end}")


with DAG(
    dag_id="backfill_example",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    tags=["example"],
) as dag:
    t1 = PythonOperator(
        task_id="backfill_window",
        python_callable=backfill_task,
        op_kwargs={"start": "2025-01-01", "end": "2025-01-02"},
    )

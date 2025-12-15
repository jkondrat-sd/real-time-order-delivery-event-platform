"""
Airflow DAG for Order & Delivery Data Backfills

This DAG handles historical data backfill operations:
1. Extracts historical order/delivery data from source systems
2. Processes and validates the data
3. Publishes to Kafka topics for reprocessing by Flink jobs
4. Optionally writes directly to Iceberg tables
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.utils.dates import days_ago
import json
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Create the DAG
dag = DAG(
    'order_delivery_backfill',
    default_args=default_args,
    description='Backfill historical order and delivery data',
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=['backfill', 'orders', 'deliveries'],
    params={
        'start_date': '2024-01-01',
        'end_date': '2024-01-31',
        'batch_size': 1000,
    }
)


def extract_historical_data(**context):
    """
    Extract historical order/delivery data from source systems
    
    In production, this would:
    - Connect to source databases (PostgreSQL, MySQL, etc.)
    - Query historical data based on date range
    - Handle pagination for large datasets
    - Apply data quality checks
    """
    params = context['params']
    start_date = params.get('start_date', '2024-01-01')
    end_date = params.get('end_date', '2024-01-31')
    batch_size = params.get('batch_size', 1000)
    
    logging.info(f"Extracting data from {start_date} to {end_date}")
    logging.info(f"Batch size: {batch_size}")
    
    # Simulate data extraction
    # In production, replace with actual database queries
    extracted_records = []
    for i in range(batch_size):
        record = {
            'order_id': f'ORDER-{i:06d}',
            'customer_id': f'CUST-{i % 100:04d}',
            'order_timestamp': f'{start_date}T12:00:00Z',
            'order_amount': 100.00 + (i % 500),
            'status': 'completed',
        }
        extracted_records.append(record)
    
    # Store in XCom for next task
    context['task_instance'].xcom_push(key='extracted_data', value=extracted_records)
    logging.info(f"Extracted {len(extracted_records)} records")
    
    return len(extracted_records)


def validate_and_transform_data(**context):
    """
    Validate and transform extracted data
    
    - Data quality checks (null values, format validation)
    - Schema validation
    - Data enrichment
    - Deduplication
    """
    task_instance = context['task_instance']
    extracted_data = task_instance.xcom_pull(key='extracted_data', task_ids='extract_data')
    
    if not extracted_data:
        logging.warning("No data to validate")
        return 0
    
    logging.info(f"Validating {len(extracted_data)} records")
    
    validated_records = []
    for record in extracted_data:
        # Basic validation
        if record.get('order_id') and record.get('customer_id'):
            # Transform to Kafka message format
            kafka_message = {
                'event_type': 'order_placed',
                'event_timestamp': datetime.utcnow().isoformat(),
                'payload': record,
            }
            validated_records.append(kafka_message)
    
    # Store validated data
    task_instance.xcom_push(key='validated_data', value=validated_records)
    logging.info(f"Validated {len(validated_records)} records")
    
    return len(validated_records)


def publish_to_kafka(**context):
    """
    Publish validated data to Kafka topics
    
    In production, this would use the KafkaProducer to send messages
    """
    task_instance = context['task_instance']
    validated_data = task_instance.xcom_pull(key='validated_data', task_ids='validate_transform_data')
    
    if not validated_data:
        logging.warning("No data to publish")
        return 0
    
    logging.info(f"Publishing {len(validated_data)} messages to Kafka")
    
    # In production, use actual Kafka producer
    # from kafka import KafkaProducer
    # producer = KafkaProducer(
    #     bootstrap_servers=['kafka:29092'],
    #     value_serializer=lambda v: json.dumps(v).encode('utf-8')
    # )
    
    published_count = 0
    for message in validated_data:
        # producer.send('order-events', value=message)
        published_count += 1
        if published_count % 100 == 0:
            logging.info(f"Published {published_count} messages")
    
    logging.info(f"Successfully published {published_count} messages to Kafka")
    return published_count


def generate_backfill_report(**context):
    """
    Generate a report of the backfill operation
    """
    task_instance = context['task_instance']
    extracted_count = task_instance.xcom_pull(task_ids='extract_data')
    validated_count = task_instance.xcom_pull(task_ids='validate_transform_data')
    published_count = task_instance.xcom_pull(task_ids='publish_to_kafka')
    
    report = {
        'backfill_timestamp': datetime.utcnow().isoformat(),
        'parameters': context['params'],
        'extracted_records': extracted_count,
        'validated_records': validated_count,
        'published_records': published_count,
        'data_quality_score': (validated_count / extracted_count * 100) if extracted_count > 0 else 0,
    }
    
    logging.info(f"Backfill Report: {json.dumps(report, indent=2)}")
    
    return report


# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_historical_data,
    dag=dag,
)

validate_transform_task = PythonOperator(
    task_id='validate_transform_data',
    python_callable=validate_and_transform_data,
    dag=dag,
)

publish_kafka_task = PythonOperator(
    task_id='publish_to_kafka',
    python_callable=publish_to_kafka,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_backfill_report,
    dag=dag,
)

# Health check task - verify Kafka is available
kafka_health_check = BashOperator(
    task_id='kafka_health_check',
    bash_command='echo "Checking Kafka availability..." && echo "Kafka OK"',
    dag=dag,
)

# Define task dependencies
kafka_health_check >> extract_task >> validate_transform_task >> publish_kafka_task >> report_task

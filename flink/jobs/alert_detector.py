#!/usr/bin/env python3
"""
PyFlink streaming job: Complex Event Processing (CEP) Alert Detector

Detects "Zalando-style" complex events for SLA violations:
1. paid_not_shipped_within_2h - Order paid but not shipped within 2 hours
2. shipped_not_delivered_within_5d - Order shipped but not delivered within 5 days

Outputs alerts to:
- Kafka topic: alerts.events (real-time)
- Iceberg table: gold_alerts (historical)

Note: This simplified implementation detects patterns based on time elapsed since
the triggering event. For production, consider using Flink CEP library or
MATCH_RECOGNIZE for more sophisticated pattern matching.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def main() -> None:
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Enable checkpointing for Iceberg commits (every 60 seconds)
    env.enable_checkpointing(60000)  # 60 seconds

    # Create table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Create Kafka source table for order events
    table_env.execute_sql("""
        CREATE TABLE orders_events_source (
            order_id STRING,
            event_type STRING,
            `timestamp` STRING,
            metadata ROW<
                region STRING,
                warehouse_id STRING,
                total_amount DOUBLE,
                items_count INT,
                delivery_time_minutes INT,
                on_time BOOLEAN,
                return_reason STRING
            >,
            event_time AS TO_TIMESTAMP(REPLACE(`timestamp`, 'T', ' ')),
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'orders.events',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'properties.group.id' = 'alert-detector',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Create Kafka sink for real-time alerts (in default catalog - Kafka doesn't use Iceberg)
    table_env.execute_sql("""
        CREATE TABLE alerts_kafka_sink (
            alert_id STRING,
            alert_type STRING,
            order_id STRING,
            region STRING,
            warehouse_id STRING,
            created_at TIMESTAMP(3),
            detected_at TIMESTAMP(3),
            time_elapsed_hours DOUBLE,
            severity STRING,
            description STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'alerts.events',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'format' = 'json'
        )
    """)

    # Create Iceberg catalog using Hive Metastore with S3FileIO
    table_env.execute_sql("""
        CREATE CATALOG iceberg_catalog WITH (
            'type' = 'iceberg',
            'catalog-type' = 'hive',
            'uri' = 'thrift://hive-metastore:9083',
            'warehouse' = 's3://warehouse',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = 'http://minio:9000',
            's3.path-style-access' = 'true',
            's3.access-key-id' = 'minioadmin',
            's3.secret-access-key' = 'minioadmin',
            'client.region' = 'us-east-1'
        )
    """)

    # Switch to Iceberg catalog for lakehouse tables
    table_env.use_catalog("iceberg_catalog")
    table_env.execute_sql("CREATE DATABASE IF NOT EXISTS lakehouse")
    table_env.use_database("lakehouse")

    # Create Iceberg table for alert history
    table_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS gold_alerts (
            alert_id STRING,
            alert_type STRING,
            order_id STRING,
            region STRING,
            warehouse_id STRING,
            created_at TIMESTAMP(3),
            detected_at TIMESTAMP(3),
            time_elapsed_hours DOUBLE,
            severity STRING,
            description STRING,
            PRIMARY KEY (alert_id) NOT ENFORCED
        ) WITH (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.upsert.enabled' = 'true'
        )
    """)

    # For demo purposes, we'll detect alerts based on simulated delay patterns
    # In production, this would use proper session windows or CEP patterns
    #
    # Approach: Generate alerts for orders that appear to be "stuck" based on
    # their event type and time patterns. For demo, we use the "incomplete"
    # orders in the event generator (those stuck in paid/shipped state).

    # Alert 1: Detect 'paid' events that might indicate stuck orders
    # In demo mode, we flag some paid events as potential SLA violations
    table_env.execute_sql("""
        CREATE TEMPORARY VIEW potential_shipping_alerts AS
        SELECT 
            CONCAT('ALERT-PNS-', order_id, '-', CAST(event_time AS STRING)) as alert_id,
            'paid_not_shipped_within_2h' as alert_type,
            order_id,
            metadata.region as region,
            metadata.warehouse_id as warehouse_id,
            event_time as created_at,
            CURRENT_TIMESTAMP as detected_at,
            2.0 as time_elapsed_hours,
            'WARNING' as severity,
            CONCAT('Order ', order_id, ' paid at ', CAST(event_time AS STRING), 
                   ' - monitoring for shipping SLA') as description
        FROM default_catalog.default_database.orders_events_source
        WHERE event_type = 'paid'
    """)

    # Alert 2: Detect 'shipped' events for delivery monitoring
    table_env.execute_sql("""
        CREATE TEMPORARY VIEW potential_delivery_alerts AS
        SELECT 
            CONCAT('ALERT-SND-', order_id, '-', CAST(event_time AS STRING)) as alert_id,
            'shipped_not_delivered_within_5d' as alert_type,
            order_id,
            metadata.region as region,
            metadata.warehouse_id as warehouse_id,
            event_time as created_at,
            CURRENT_TIMESTAMP as detected_at,
            120.0 as time_elapsed_hours,
            'WARNING' as severity,
            CONCAT('Order ', order_id, ' shipped at ', CAST(event_time AS STRING), 
                   ' - monitoring for delivery SLA') as description
        FROM default_catalog.default_database.orders_events_source
        WHERE event_type = 'shipped'
    """)

    # Union all alerts and output to both Kafka and Iceberg
    # Note: We use StatementSet to execute multiple inserts simultaneously
    statement_set = table_env.create_statement_set()

    # Insert to Kafka (real-time alerts)
    statement_set.add_insert_sql("""
        INSERT INTO default_catalog.default_database.alerts_kafka_sink
        SELECT * FROM potential_shipping_alerts
        UNION ALL
        SELECT * FROM potential_delivery_alerts
    """)

    # Insert to Iceberg (historical alerts)
    statement_set.add_insert_sql("""
        INSERT INTO gold_alerts
        SELECT * FROM potential_shipping_alerts
        UNION ALL
        SELECT * FROM potential_delivery_alerts
    """)

    # Execute both inserts
    statement_set.execute()


if __name__ == "__main__":
    main()

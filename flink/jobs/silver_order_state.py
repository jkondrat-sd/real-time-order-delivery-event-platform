#!/usr/bin/env python3
"""
PyFlink streaming job: Silver Table - Latest Order State
Maintains the current state of each order by deduplicating events
and keeping only the most recent status for each order_id.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def main():
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Enable checkpointing for Iceberg commits (every 60 seconds)
    env.enable_checkpointing(60000)  # 60 seconds

    # Create table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Create Kafka source table
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
            'properties.group.id' = 'silver-writer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
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

    table_env.use_catalog("iceberg_catalog")
    table_env.execute_sql("CREATE DATABASE IF NOT EXISTS lakehouse")
    table_env.use_database("lakehouse")

    # Create Silver table with upsert capability
    table_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS silver_order_state (
            order_id STRING PRIMARY KEY NOT ENFORCED,
            current_status STRING,
            created_at TIMESTAMP(3),
            last_updated_at TIMESTAMP(3),
            region STRING,
            warehouse_id STRING,
            total_amount DOUBLE,
            items_count INT,
            is_delivered BOOLEAN,
            is_cancelled BOOLEAN,
            is_returned BOOLEAN,
            delivery_time_minutes INT,
            on_time BOOLEAN
        ) WITH (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.upsert.enabled' = 'true'
        )
    """)

    # Upsert into Silver table - keeps latest state per order
    table_env.execute_sql("""
        INSERT INTO silver_order_state
        SELECT 
            order_id,
            event_type as current_status,
            CASE WHEN event_type = 'created' THEN event_time 
                 ELSE LAG(event_time) OVER (PARTITION BY order_id ORDER BY event_time) 
            END as created_at,
            event_time as last_updated_at,
            metadata.region,
            metadata.warehouse_id,
            CASE WHEN event_type = 'created' THEN metadata.total_amount ELSE NULL END as total_amount,
            CASE WHEN event_type = 'created' THEN metadata.items_count ELSE NULL END as items_count,
            CASE WHEN event_type IN ('delivered', 'returned') THEN TRUE ELSE FALSE END as is_delivered,
            CASE WHEN event_type = 'cancelled' THEN TRUE ELSE FALSE END as is_cancelled,
            CASE WHEN event_type = 'returned' THEN TRUE ELSE FALSE END as is_returned,
            metadata.delivery_time_minutes,
            metadata.on_time
        FROM default_catalog.default_database.orders_events_source
    """)


if __name__ == "__main__":
    main()

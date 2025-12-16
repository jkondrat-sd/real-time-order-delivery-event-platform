#!/usr/bin/env python3
"""
PyFlink streaming job: Bronze Table Writer
Writes raw events from orders.events topic to Iceberg bronze_order_events table.
This creates an append-only lakehouse table for all historical events.
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

    # Create Kafka source table for reading events
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
            'properties.group.id' = 'bronze-writer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Create Iceberg catalog using Hive Metastore with S3FileIO
    # Using S3FileIO instead of S3AFileSystem to avoid Hadoop dependency issues
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

    # Use Iceberg catalog
    table_env.use_catalog("iceberg_catalog")

    # Create database if not exists
    table_env.execute_sql("CREATE DATABASE IF NOT EXISTS lakehouse")
    table_env.use_database("lakehouse")

    # Create Bronze Iceberg table (append-only)
    table_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS bronze_order_events (
            order_id STRING,
            event_type STRING,
            event_timestamp STRING,
            region STRING,
            warehouse_id STRING,
            total_amount DOUBLE,
            items_count INT,
            delivery_time_minutes INT,
            on_time BOOLEAN,
            return_reason STRING,
            ingestion_time TIMESTAMP(3)
        ) WITH (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.metadata.compression-codec' = 'gzip'
        )
    """)

    # Insert data from Kafka source to Iceberg bronze table
    table_env.execute_sql("""
        INSERT INTO bronze_order_events
            SELECT 
            order_id,
            event_type,
            `timestamp` as event_timestamp,
            metadata.region,
            metadata.warehouse_id,
            metadata.total_amount,
            metadata.items_count,
            metadata.delivery_time_minutes,
            metadata.on_time,
            metadata.return_reason,
            CURRENT_TIMESTAMP as ingestion_time
        FROM default_catalog.default_database.orders_events_source
    """)


if __name__ == "__main__":
    main()

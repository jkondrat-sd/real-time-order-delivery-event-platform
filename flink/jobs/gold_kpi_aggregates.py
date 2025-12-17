#!/usr/bin/env python3
"""
PyFlink streaming job: Gold Table - KPI Aggregates
Computes business KPIs: delivery SLAs, conversion rates, hourly metrics.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Enable checkpointing for Iceberg commits (every 60 seconds)
    env.enable_checkpointing(60000)  # 60 seconds

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Kafka source
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
                on_time BOOLEAN
            >,
            event_time AS TO_TIMESTAMP(REPLACE(`timestamp`, 'T', ' ')),
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'orders.events',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'properties.group.id' = 'gold-kpi-writer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Iceberg catalog using Hive Metastore with S3FileIO
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

    # Gold table for hourly KPIs
    table_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS gold_hourly_kpis (
            hour_window TIMESTAMP(3),
            region STRING,
            warehouse_id STRING,
            orders_created BIGINT,
            orders_delivered BIGINT,
            orders_cancelled BIGINT,
            orders_returned BIGINT,
            on_time_deliveries BIGINT,
            late_deliveries BIGINT,
            total_revenue DOUBLE,
            avg_delivery_time_minutes DOUBLE,
            conversion_rate DOUBLE,
            on_time_rate DOUBLE,
            PRIMARY KEY (hour_window, region, warehouse_id) NOT ENFORCED
        ) WITH (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.upsert.enabled' = 'true'
        )
    """)

    # Compute hourly aggregations
    table_env.execute_sql("""
        INSERT INTO gold_hourly_kpis
        SELECT 
            TUMBLE_START(event_time, INTERVAL '1' HOUR) as hour_window,
            metadata.region,
            metadata.warehouse_id,
            SUM(CASE WHEN event_type = 'created' THEN 1 ELSE 0 END) as orders_created,
            SUM(CASE WHEN event_type = 'delivered' THEN 1 ELSE 0 END) as orders_delivered,
            SUM(CASE WHEN event_type = 'cancelled' THEN 1 ELSE 0 END) as orders_cancelled,
            SUM(CASE WHEN event_type = 'returned' THEN 1 ELSE 0 END) as orders_returned,
            SUM(CASE WHEN event_type = 'delivered' AND metadata.on_time = TRUE THEN 1 ELSE 0 END) as on_time_deliveries,
            SUM(CASE WHEN event_type = 'delivered' AND metadata.on_time = FALSE THEN 1 ELSE 0 END) as late_deliveries,
            SUM(CASE WHEN event_type = 'created' THEN metadata.total_amount ELSE 0 END) as total_revenue,
            AVG(CASE WHEN event_type = 'delivered' THEN metadata.delivery_time_minutes ELSE NULL END) as avg_delivery_time_minutes,
            CAST(SUM(CASE WHEN event_type = 'delivered' THEN 1 ELSE 0 END) AS DOUBLE) / 
                NULLIF(SUM(CASE WHEN event_type = 'created' THEN 1 ELSE 0 END), 0) as conversion_rate,
            CAST(SUM(CASE WHEN event_type = 'delivered' AND metadata.on_time = TRUE THEN 1 ELSE 0 END) AS DOUBLE) / 
                NULLIF(SUM(CASE WHEN event_type = 'delivered' THEN 1 ELSE 0 END), 0) as on_time_rate
        FROM default_catalog.default_database.orders_events_source
        GROUP BY 
            TUMBLE(event_time, INTERVAL '1' HOUR),
            metadata.region,
            metadata.warehouse_id
    """)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
PyFlink streaming job: KPI Aggregator
Computes orders_created_per_minute from orders.events topic.
Sinks results to kpi.minute Kafka topic.

Uses Flink Table API (SQL) for simpler window aggregation.
"""

from pyflink.table import EnvironmentSettings, TableEnvironment


def main() -> None:
    # Create Table environment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    # Set configuration
    table_env.get_config().set("parallelism.default", "1")
    table_env.get_config().set("table.exec.source.idle-timeout", "10000")

    # Create Kafka source table
    table_env.execute_sql("""
        CREATE TABLE orders_events (
            order_id STRING,
            event_type STRING,
            `timestamp` STRING,
            event_time AS TO_TIMESTAMP(REPLACE(`timestamp`, 'T', ' ')),
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'orders.events',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'properties.group.id' = 'kpi-aggregator',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Create Kafka sink table
    table_env.execute_sql("""
        CREATE TABLE kpi_output (
            metric STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            `count` BIGINT,
            event_time TIMESTAMP(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'kpi.minute',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'format' = 'json'
        )
    """)

    # Aggregate created events by minute and insert into sink
    table_env.execute_sql("""
        INSERT INTO kpi_output
        SELECT
            'orders_created_per_minute' AS metric,
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
            COUNT(*) AS `count`,
            NOW() AS event_time
        FROM orders_events
        WHERE event_type = 'created'
        GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE)
    """)

    print("KPI Aggregator job submitted successfully!")


if __name__ == "__main__":
    main()

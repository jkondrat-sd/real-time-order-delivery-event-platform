package com.platform.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Flink Job: KPI Aggregator
 * 
 * Reads order events from Kafka, computes KPIs (order counts, revenue, average order value)
 * and writes aggregated results to Iceberg tables in MinIO.
 */
public class KpiAggregatorJob {
    private static final Logger LOG = LoggerFactory.getLogger(KpiAggregatorJob.class);
    
    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:29092";
    private static final String KAFKA_TOPIC = "order-events";
    private static final String KAFKA_GROUP_ID = "kpi-aggregator";
    
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(60000); // Checkpoint every 60 seconds
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        
        // Create Table Environment for SQL operations
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // Configure Iceberg catalog
        configureCatalog(tableEnv);
        
        // Create Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopics(KAFKA_TOPIC)
            .setGroupId(KAFKA_GROUP_ID)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        
        // Create data stream from Kafka
        DataStream<String> orderStream = env.fromSource(
            source,
            WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withIdleness(Duration.ofMinutes(1)),
            "Kafka Order Source"
        );
        
        // Log incoming messages for debugging
        orderStream.map(value -> {
            LOG.info("Received order event: {}", value);
            return value;
        });
        
        // Convert JSON to Table and parse order events
        // NOTE: This is a simplified example for demonstration purposes.
        // In production, you should:
        // 1. Use Flink's JSON format with proper schema definition
        // 2. Parse the JSON to extract order_id, timestamp, amount, etc.
        // 3. Use event time from the order data, not CURRENT_TIMESTAMP
        // 4. Implement proper error handling and dead-letter queue
        tableEnv.createTemporaryView("orders_raw", orderStream);
        
        // Create Iceberg table for KPI results if not exists
        createKpiTable(tableEnv);
        
        // Example aggregation query - compute hourly KPIs
        // TODO: Implement proper JSON parsing and event time extraction
        // The current query is commented out as it requires proper JSON format parsing
        // Uncomment and implement the following after adding JSON deserializer:
        /*
        String kpiQuery = 
            "INSERT INTO iceberg_catalog.default_db.order_kpis " +
            "SELECT " +
            "  TUMBLE_START(event_time, INTERVAL '1' HOUR) as window_start, " +
            "  COUNT(*) as order_count, " +
            "  SUM(order_amount) as total_revenue, " +
            "  AVG(order_amount) as avg_order_value " +
            "FROM orders_parsed " +
            "GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR)";
        
        tableEnv.executeSql(kpiQuery);
        */
        
        LOG.info("Starting KPI Aggregator Job...");
        
        // Execute the Flink job
        env.execute("Order KPI Aggregator");
    }
    
    /**
     * Configure Iceberg catalog to connect to MinIO
     */
    private static void configureCatalog(StreamTableEnvironment tableEnv) {
        String createCatalog = 
            "CREATE CATALOG iceberg_catalog WITH (\n" +
            "  'type' = 'iceberg',\n" +
            "  'catalog-type' = 'hadoop',\n" +
            "  'warehouse' = 's3a://warehouse',\n" +
            "  'property-version' = '1',\n" +
            "  's3.endpoint' = 'http://minio:9000',\n" +
            "  's3.path-style-access' = 'true',\n" +
            "  's3.access-key-id' = 'admin',\n" +
            "  's3.secret-access-key' = 'password'\n" +
            ")";
        
        tableEnv.executeSql(createCatalog);
        tableEnv.executeSql("USE CATALOG iceberg_catalog");
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS default_db");
        tableEnv.executeSql("USE default_db");
        
        LOG.info("Iceberg catalog configured successfully");
    }
    
    /**
     * Create Iceberg table for storing KPI results
     */
    private static void createKpiTable(StreamTableEnvironment tableEnv) {
        String createTable = 
            "CREATE TABLE IF NOT EXISTS iceberg_catalog.default_db.order_kpis (\n" +
            "  window_start TIMESTAMP(3),\n" +
            "  order_count BIGINT,\n" +
            "  total_revenue DOUBLE,\n" +
            "  avg_order_value DOUBLE,\n" +
            "  PRIMARY KEY (window_start) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'format-version' = '2',\n" +
            "  'write.upsert.enabled' = 'true'\n" +
            ")";
        
        tableEnv.executeSql(createTable);
        LOG.info("KPI table created or already exists");
    }
}

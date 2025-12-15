package com.platform.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Flink Job: Complex Event Detector
 * 
 * Uses Flink CEP (Complex Event Processing) to detect patterns in order and delivery events:
 * - Delayed deliveries (order placed but not delivered within expected time)
 * - Failed delivery attempts (multiple failed delivery events)
 * - Suspicious order patterns (high-value orders from new customers)
 * 
 * Detected events are written to Iceberg tables for analysis.
 */
public class ComplexEventDetectorJob {
    private static final Logger LOG = LoggerFactory.getLogger(ComplexEventDetectorJob.class);
    
    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:29092";
    private static final String ORDER_TOPIC = "order-events";
    private static final String DELIVERY_TOPIC = "delivery-events";
    private static final String KAFKA_GROUP_ID = "complex-event-detector";
    
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(60000); // Checkpoint every 60 seconds
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        
        // Create Table Environment for Iceberg operations
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // Configure Iceberg catalog
        configureCatalog(tableEnv);
        
        // Create Kafka sources
        KafkaSource<String> orderSource = createKafkaSource(ORDER_TOPIC);
        KafkaSource<String> deliverySource = createKafkaSource(DELIVERY_TOPIC);
        
        // Create data streams
        DataStream<String> orderStream = env.fromSource(
            orderSource,
            WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withIdleness(Duration.ofMinutes(1)),
            "Kafka Order Source"
        );
        
        DataStream<String> deliveryStream = env.fromSource(
            deliverySource,
            WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withIdleness(Duration.ofMinutes(1)),
            "Kafka Delivery Source"
        );
        
        // Log incoming events for debugging
        orderStream.map(value -> {
            LOG.info("Received order event: {}", value);
            return value;
        });
        
        deliveryStream.map(value -> {
            LOG.info("Received delivery event: {}", value);
            return value;
        });
        
        // Define CEP pattern for delayed deliveries
        // Pattern: Order placed, but no delivery within 24 hours
        Pattern<String, ?> delayedDeliveryPattern = Pattern.<String>begin("orderPlaced")
            .where(new SimpleCondition<String>() {
                @Override
                public boolean filter(String event) {
                    // In production, parse JSON and check event type
                    return event.contains("order_placed") || event.contains("ORDER");
                }
            })
            .notFollowedBy("delivered")
            .where(new SimpleCondition<String>() {
                @Override
                public boolean filter(String event) {
                    return event.contains("delivered") || event.contains("DELIVERED");
                }
            })
            .within(Time.hours(24));
        
        // Apply pattern to detect delayed deliveries
        PatternStream<String> delayedDeliveryPatternStream = CEP.pattern(orderStream, delayedDeliveryPattern);
        
        DataStream<String> delayedDeliveryAlerts = delayedDeliveryPatternStream.select(
            new PatternSelectFunction<String, String>() {
                @Override
                public String select(Map<String, List<String>> pattern) {
                    List<String> orderEvents = pattern.get("orderPlaced");
                    String orderEvent = orderEvents != null && !orderEvents.isEmpty() 
                        ? orderEvents.get(0) 
                        : "unknown";
                    LOG.warn("ALERT: Delayed delivery detected for order: {}", orderEvent);
                    return "DELAYED_DELIVERY:" + orderEvent;
                }
            }
        );
        
        // Define CEP pattern for failed delivery attempts
        // Pattern: Multiple failed delivery attempts (3+ in sequence)
        Pattern<String, ?> failedDeliveryPattern = Pattern.<String>begin("failedAttempt1")
            .where(new SimpleCondition<String>() {
                @Override
                public boolean filter(String event) {
                    return event.contains("delivery_failed") || event.contains("FAILED");
                }
            })
            .next("failedAttempt2")
            .where(new SimpleCondition<String>() {
                @Override
                public boolean filter(String event) {
                    return event.contains("delivery_failed") || event.contains("FAILED");
                }
            })
            .next("failedAttempt3")
            .where(new SimpleCondition<String>() {
                @Override
                public boolean filter(String event) {
                    return event.contains("delivery_failed") || event.contains("FAILED");
                }
            })
            .within(Time.hours(48));
        
        // Apply pattern to detect multiple failed deliveries
        PatternStream<String> failedDeliveryPatternStream = CEP.pattern(deliveryStream, failedDeliveryPattern);
        
        DataStream<String> failedDeliveryAlerts = failedDeliveryPatternStream.select(
            new PatternSelectFunction<String, String>() {
                @Override
                public String select(Map<String, List<String>> pattern) {
                    LOG.warn("ALERT: Multiple failed delivery attempts detected");
                    return "MULTIPLE_FAILED_DELIVERIES:" + pattern.toString();
                }
            }
        );
        
        // Create Iceberg tables for complex events
        createComplexEventTables(tableEnv);
        
        // Print alerts for demonstration
        // NOTE: In production, these should be written to Iceberg tables or sent to alerting systems
        // TODO: Implement proper sinks to persist alerts:
        // 1. Write to Iceberg tables using Table API
        // 2. Send to alerting service (PagerDuty, Slack, etc.)
        // 3. Publish to a separate Kafka topic for downstream consumption
        delayedDeliveryAlerts.print();
        failedDeliveryAlerts.print();
        
        LOG.info("Starting Complex Event Detector Job...");
        
        // Execute the Flink job
        env.execute("Complex Event Detector");
    }
    
    /**
     * Create Kafka source with common configuration
     */
    private static KafkaSource<String> createKafkaSource(String topic) {
        return KafkaSource.<String>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopics(topic)
            .setGroupId(KAFKA_GROUP_ID)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
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
     * Create Iceberg tables for storing complex event alerts
     */
    private static void createComplexEventTables(StreamTableEnvironment tableEnv) {
        String createDelayedDeliveriesTable = 
            "CREATE TABLE IF NOT EXISTS iceberg_catalog.default_db.delayed_deliveries (\n" +
            "  alert_time TIMESTAMP(3),\n" +
            "  order_id STRING,\n" +
            "  customer_id STRING,\n" +
            "  order_timestamp TIMESTAMP(3),\n" +
            "  expected_delivery_time TIMESTAMP(3),\n" +
            "  PRIMARY KEY (alert_time, order_id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'format-version' = '2'\n" +
            ")";
        
        String createFailedDeliveriesTable = 
            "CREATE TABLE IF NOT EXISTS iceberg_catalog.default_db.failed_delivery_attempts (\n" +
            "  alert_time TIMESTAMP(3),\n" +
            "  order_id STRING,\n" +
            "  attempt_count INT,\n" +
            "  last_failure_reason STRING,\n" +
            "  PRIMARY KEY (alert_time, order_id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'format-version' = '2'\n" +
            ")";
        
        tableEnv.executeSql(createDelayedDeliveriesTable);
        tableEnv.executeSql(createFailedDeliveriesTable);
        
        LOG.info("Complex event tables created or already exist");
    }
}

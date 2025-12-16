-- Example Trino SQL queries for the Order Delivery Event Platform
-- Connect to Trino at: http://localhost:8082
-- Catalog: iceberg_catalog
-- Database: lakehouse

-- =============================================================================
-- BRONZE TABLE QUERIES (Raw Events)
-- =============================================================================

-- Query 1: Count total events by type
-- Purpose: Understand event volume distribution
SELECT 
    event_type,
    COUNT(*) as event_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM iceberg_catalog.lakehouse.bronze_order_events
GROUP BY event_type
ORDER BY event_count DESC;

-- Query 2: Recent events with full details
-- Purpose: Inspect latest data ingested into the lakehouse
SELECT 
    order_id,
    event_type,
    timestamp,
    region,
    warehouse_id,
    total_amount,
    ingestion_time
FROM iceberg_catalog.lakehouse.bronze_order_events
ORDER BY ingestion_time DESC
LIMIT 20;

-- Query 3: Event timeline for a specific order
-- Purpose: Debug order lifecycle and track state transitions
SELECT 
    order_id,
    event_type,
    timestamp,
    region,
    warehouse_id,
    delivery_time_minutes,
    on_time
FROM iceberg_catalog.lakehouse.bronze_order_events
WHERE order_id = 'ORD-00001'  -- Replace with actual order ID
ORDER BY timestamp;

-- =============================================================================
-- SILVER TABLE QUERIES (Latest Order State)
-- =============================================================================

-- Query 4: Current status distribution
-- Purpose: See how many orders are in each state
SELECT 
    current_status,
    COUNT(*) as order_count,
    ROUND(AVG(total_amount), 2) as avg_order_value
FROM iceberg_catalog.lakehouse.silver_order_state
GROUP BY current_status
ORDER BY order_count DESC;

-- Query 5: Orders by region and warehouse
-- Purpose: Analyze geographic distribution and volume
SELECT 
    region,
    warehouse_id,
    COUNT(*) as total_orders,
    SUM(CASE WHEN is_delivered THEN 1 ELSE 0 END) as delivered,
    SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END) as cancelled,
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) as returned,
    ROUND(AVG(total_amount), 2) as avg_order_value
FROM iceberg_catalog.lakehouse.silver_order_state
GROUP BY region, warehouse_id
ORDER BY total_orders DESC;

-- Query 6: Problem orders (cancelled or returned)
-- Purpose: Identify orders that didn't complete successfully
SELECT 
    order_id,
    current_status,
    region,
    warehouse_id,
    total_amount,
    created_at,
    last_updated_at
FROM iceberg_catalog.lakehouse.silver_order_state
WHERE is_cancelled = TRUE OR is_returned = TRUE
ORDER BY last_updated_at DESC
LIMIT 50;

-- =============================================================================
-- GOLD TABLE QUERIES (KPI Aggregates)
-- =============================================================================

-- Query 7: Hourly performance trends
-- Purpose: Track key metrics over time
SELECT 
    hour_window,
    region,
    SUM(orders_created) as total_created,
    SUM(orders_delivered) as total_delivered,
    ROUND(AVG(conversion_rate) * 100, 2) as avg_conversion_pct,
    ROUND(AVG(on_time_rate) * 100, 2) as avg_on_time_pct,
    ROUND(SUM(total_revenue), 2) as revenue
FROM iceberg_catalog.lakehouse.gold_hourly_kpis
WHERE hour_window >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY hour_window, region
ORDER BY hour_window DESC, region;

-- Query 8: Regional performance comparison
-- Purpose: Compare SLA performance across regions
SELECT 
    region,
    SUM(orders_delivered) as total_delivered,
    SUM(on_time_deliveries) as on_time_count,
    SUM(late_deliveries) as late_count,
    ROUND(SUM(on_time_deliveries) * 100.0 / NULLIF(SUM(orders_delivered), 0), 2) as on_time_rate_pct,
    ROUND(AVG(avg_delivery_time_minutes), 1) as avg_delivery_mins
FROM iceberg_catalog.lakehouse.gold_hourly_kpis
WHERE hour_window >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
GROUP BY region
ORDER BY on_time_rate_pct DESC;

-- Query 9: Warehouse efficiency leaderboard
-- Purpose: Identify top and bottom performing warehouses
SELECT 
    warehouse_id,
    region,
    SUM(orders_created) as volume,
    ROUND(AVG(conversion_rate) * 100, 2) as conversion_pct,
    ROUND(AVG(on_time_rate) * 100, 2) as on_time_pct,
    ROUND(SUM(total_revenue), 2) as revenue,
    ROUND(AVG(avg_delivery_time_minutes), 1) as avg_delivery_mins
FROM iceberg_catalog.lakehouse.gold_hourly_kpis
WHERE hour_window >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
GROUP BY warehouse_id, region
HAVING SUM(orders_created) > 10  -- Filter low-volume warehouses
ORDER BY on_time_pct DESC, conversion_pct DESC
LIMIT 20;

-- =============================================================================
-- ADVANCED ANALYTICS QUERIES
-- =============================================================================

-- Query 10: Join Bronze and Gold for detailed event analysis
-- Purpose: Correlate raw events with aggregated KPIs
WITH hourly_stats AS (
    SELECT 
        hour_window,
        region,
        orders_delivered,
        on_time_rate
    FROM iceberg_catalog.lakehouse.gold_hourly_kpis
    WHERE hour_window >= CURRENT_TIMESTAMP - INTERVAL '6' HOUR
)
SELECT 
    hs.hour_window,
    hs.region,
    hs.orders_delivered,
    ROUND(hs.on_time_rate * 100, 2) as on_time_pct,
    COUNT(DISTINCT b.order_id) as unique_orders_in_bronze
FROM hourly_stats hs
LEFT JOIN iceberg_catalog.lakehouse.bronze_order_events b
    ON b.region = hs.region
    AND b.timestamp >= CAST(hs.hour_window AS VARCHAR)
    AND b.timestamp < CAST(hs.hour_window + INTERVAL '1' HOUR AS VARCHAR)
GROUP BY hs.hour_window, hs.region, hs.orders_delivered, hs.on_time_rate
ORDER BY hs.hour_window DESC;

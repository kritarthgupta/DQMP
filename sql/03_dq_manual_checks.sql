USE dqmp;

-- ===============================
-- Define current run ID (timestamp)
-- ===============================
SET @current_run = NOW();

-- ===============================
-- VOLUME CHECK : CUSTOMERS
-- ===============================
INSERT INTO dq_results (run_id, run_time, table_name, column_name, metric_type, metric_value, threshold_value, status)
SELECT
    @current_run AS run_id,
    NOW() AS run_time,
    'customers' AS table_name,
    NULL AS column_name,
    'volume' AS metric_type,
    COUNT(*) AS metric_value,
    CONCAT(m.expected_volume_min, '-', m.expected_volume_max) AS threshold_value,
    CASE
        WHEN COUNT(*) BETWEEN m.expected_volume_min AND m.expected_volume_max
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM customers c
JOIN table_metadata m
  ON m.table_name = 'customers'
 AND m.column_name = 'customer_id'
GROUP BY m.expected_volume_min, m.expected_volume_max;

-- ===============================
-- VOLUME CHECK : ORDERS
-- ===============================
INSERT INTO dq_results (run_id, run_time, table_name, column_name, metric_type, metric_value, threshold_value, status)
SELECT
    @current_run,
    NOW(),
    'orders',
    NULL,
    'volume',
    COUNT(*),
    CONCAT(m.expected_volume_min, '-', m.expected_volume_max),
    CASE
        WHEN COUNT(*) BETWEEN m.expected_volume_min AND m.expected_volume_max
        THEN 'PASS'
        ELSE 'FAIL'
    END
FROM orders o
JOIN table_metadata m
  ON m.table_name = 'orders'
 AND m.column_name = 'order_id'
GROUP BY m.expected_volume_min, m.expected_volume_max;

-- ===============================
-- NULL % CHECK : CUSTOMERS.email
-- ===============================
INSERT INTO dq_results (run_id, run_time, table_name, column_name, metric_type, metric_value, threshold_value, status)
SELECT
    @current_run,
    NOW(),
    'customers',
    'email',
    'null_pct',
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS metric_value,
    'NULL_ALLOWED',
    CASE
        WHEN SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) > 0
        THEN 'FAIL'
        ELSE 'PASS'
    END
FROM customers;

-- ===============================
-- NULL % CHECK : ORDERS.customer_id
-- ===============================
INSERT INTO dq_results (run_id, run_time, table_name, column_name, metric_type, metric_value, threshold_value, status)
SELECT
    @current_run,
    NOW(),
    'orders',
    'customer_id',
    'null_pct',
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS metric_value,
    'NOT_NULL',
    CASE
        WHEN SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) > 0
        THEN 'FAIL'
        ELSE 'PASS'
    END
FROM orders;

-- ===============================
-- FRESHNESS CHECK : ORDERS.updated_at
-- ===============================
INSERT INTO dq_results (run_id, run_time, table_name, column_name, metric_type, metric_value, threshold_value, status)
SELECT
    @current_run,
    NOW(),
    'orders',
    'updated_at',
    'freshness_minutes',
    TIMESTAMPDIFF(MINUTE, MAX(o.updated_at), NOW()) AS metric_value,
    m.freshness_sla_minutes AS threshold_value,
    CASE
        WHEN TIMESTAMPDIFF(MINUTE, MAX(o.updated_at), NOW()) <= m.freshness_sla_minutes
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM orders o
JOIN table_metadata m
  ON m.table_name = 'orders'
 AND m.column_name = 'order_id'
GROUP BY m.freshness_sla_minutes;
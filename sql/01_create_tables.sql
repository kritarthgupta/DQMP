USE dqmp;

CREATE TABLE customers (
    customer_id INT,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    created_at DATETIME
);
ALTER TABLE customers
ADD COLUMN is_primary_key TINYINT(1) DEFAULT 0;

CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    order_amount FLOAT,
    updated_at DATETIME
);

CREATE TABLE table_metadata (
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    is_nullable TINYINT(1),
    expected_volume_min INT,
    expected_volume_max INT,
    freshness_sla_minutes INT
);
ALTER TABLE table_metadata
ADD COLUMN is_primary_key TINYINT(1) DEFAULT 0,
ADD COLUMN reference_table VARCHAR(100) DEFAULT NULL,
ADD COLUMN reference_column VARCHAR(100) DEFAULT NULL;

CREATE TABLE dq_results (
    run_id DATETIME,
    run_time DATETIME,
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    metric_type VARCHAR(50),
    metric_value FLOAT,
    threshold_value VARCHAR(100),
    status VARCHAR(10)
); ALTER TABLE dq_results ADD COLUMN details VARCHAR(255);

CREATE TABLE dq_alerts (
    alert_id INT AUTO_INCREMENT PRIMARY KEY,
    alert_time DATETIME NOT NULL,
    run_id DATETIME NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    metric_type VARCHAR(50) NOT NULL,
    severity VARCHAR(10) NOT NULL,
    message VARCHAR(255) NOT NULL
);


CREATE TABLE table_schema_baseline (
    table_name VARCHAR(100),
    column_name VARCHAR(100)
);
INSERT INTO table_schema_baseline
SELECT table_name, column_name
FROM information_schema.columns
WHERE table_schema = DATABASE();
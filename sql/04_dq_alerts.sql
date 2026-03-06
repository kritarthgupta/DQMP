USE dqmp;

-- Find the latest run_id
SET @latest_run = (SELECT MAX(run_id) FROM dq_results);

INSERT INTO dq_alerts (alert_time, run_id, table_name, column_name, metric_type, severity, message)
SELECT
    NOW() AS alert_time,
    run_id,
    table_name,
    column_name,
    metric_type,
    CASE
        WHEN metric_type = 'volume' AND status = 'FAIL' THEN 'CRITICAL'
        WHEN metric_type = 'null_pct' AND status = 'FAIL' THEN 'WARNING'
        WHEN metric_type = 'freshness_minutes' AND status = 'FAIL' THEN 'CRITICAL'
        ELSE 'INFO'
    END AS severity,
    CONCAT('DQ check failed for ', table_name, 
           IF(column_name IS NOT NULL, CONCAT('.', column_name), ''), 
           ' on ', metric_type, '. Status: ', status) AS message
FROM dq_results
WHERE status = 'FAIL' 
  AND run_id = @latest_run;  -- <-- only current run
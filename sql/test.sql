USE dqmp;

select * from orders;
select * from customers;
select * from table_metadata;
select * from table_schema_baseline;

select * from dq_results order by run_ID desc, metric_type desc, table_name;
select * from dq_alerts;

SELECT column_name
FROM information_schema.columns
WHERE table_schema = DATABASE()
AND table_name = 'customers';

SELECT column_name
FROM table_metadata
WHERE table_name = 'customers';


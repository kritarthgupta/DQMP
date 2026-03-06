USE dqmp;

INSERT INTO customers VALUES
(1, 'Rahul', 'rahul@gmail.com', NOW()),
(2, 'Priya', NULL, NOW()),
(3, 'Amit', 'amit@gmail.com', NOW()),
(4, NULL, 'test@gmail.com', NOW());

INSERT INTO orders VALUES
(101, 1, 250.0, NOW() - INTERVAL 30 MINUTE),
(102, 2, 300.0, NOW() - INTERVAL 25 MINUTE),
(103, NULL, 150.0, NOW() - INTERVAL 20 MINUTE),
(104, 3, 400.0, NOW() - INTERVAL 15 MINUTE),
(104, 3, 100.0, NOW() - INTERVAL 15 MINUTE)
;

TRUNCATE TABLE table_metadata;
INSERT INTO table_metadata VALUES
('customers','customer_id',0,3,10,NULL,1,NULL,NULL),
('customers','email',1,3,10,NULL,0,NULL,NULL),
('orders','order_id',0,5,100,NULL,1,NULL,NULL),
('orders','customer_id',0,5,100,NULL,0,'customers','customer_id'),
('orders','updated_at',0,5,100,60,0,NULL,NULL);
-- 1. Select all users

-- 2. Select orders for a specific user
SELECT * FROM orders WHERE user_id = 272307690;
SELECT * FROM discounts;

-- 3. Select products with stock less than 10
SELECT * FROM products WHERE stock < 10;

-- 4. Join orders with order_items to get details
SELECT o.order_id, o.order_date, oi.product_id, oi.quantity, oi.price
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id;

-- 5. Insert a new user
INSERT INTO users (username, email, created_at, firstname, address, age, gender, color, colo)
VALUES ('dave', 'dave@example.com', '2024-08-07 11:00:00', 'Dave', '101 Maple St', 28, 'M', 'Black', 'Charcoal');

-- 6. Insert a new order
INSERT INTO orders (user_id, order_date, total_amount)
VALUES (272307692, '2024-08-07 16:00:00', 100.00);

-- 7. Insert a new product
INSERT INTO products (name, price, stock)
VALUES ('Tablet', 299.99, 30);

SELECT * FROM discounts WHERE start_date <= CURRENT_DATE AND end_date >= CURRENT_DATE;
SELECT * FROM discounts WHERE end_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days';




SELECT product_id, COUNT(*) AS discount_count
FROM discounts
GROUP BY product_id;

SELECT product_id, MAX(discount_percent) AS max_discount
FROM discounts
GROUP BY product_id;


SELECT pid, query, state, EXTRACT(EPOCH FROM (now() - query_start)) AS duration_seconds
FROM pg_stat_activity
WHERE state = 'active'
  AND EXTRACT(EPOCH FROM (now() - query_start)) >  60  -- Queries running longer than 60 seconds
ORDER BY duration_seconds DESC;

SELECT pid, mode, COUNT(*) AS lock_count
FROM pg_locks
GROUP BY pid, mode
HAVING COUNT(*) > 10;  -- Adjust the threshold as needed


SELECT relname AS table_name,
       pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC;

SELECT relname AS table_name,
       seq_scan AS sequential_scans,
       idx_scan AS index_scans
FROM pg_stat_user_tables
ORDER BY seq_scan DESC;

SELECT pid, wait_event_type, wait_event, state, query
FROM pg_stat_activity
WHERE wait_event IS NOT NULL;


SELECT relname AS table_name,
       pg_size_pretty(pg_total_relation_size(pg_class.oid)) AS total_size,
       pg_size_pretty(pg_relation_size(pg_class.oid)) AS table_size,
       pg_size_pretty(pg_total_relation_size(pg_class.oid) - pg_relation_size(pg_class.oid)) AS index_size
FROM pg_class
JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
WHERE pg_namespace.nspname = 'public'
  AND pg_class.relkind = 'r'  -- 'r' stands for ordinary table
ORDER BY pg_total_relation_size(pg_class.oid) DESC;


SELECT COUNT(*) AS active_connections
FROM pg_stat_activity
WHERE state = 'active';

SELECT relname AS table_name,
       seq_scan AS sequential_scans,
       idx_scan AS index_scans
FROM pg_stat_user_tables
ORDER BY (seq_scan + idx_scan) DESC;

-- The data behind testdata/limits (issue #2115): 2,000,000 orders spread evenly
-- over two years (one every 31.5 s), 100,000 customers. Loaded as is into MySQL
-- 8.4.9, MariaDB 11.4.13 and MariaDB 10.11.19; every file under mysql84/,
-- mariadb114/ and mariadb1011/ is that server's EXPLAIN FORMAT=JSON for the
-- statement of the same name in statements.tsv.
DROP DATABASE IF EXISTS r2115; CREATE DATABASE r2115; USE r2115;
CREATE TABLE d (n INT PRIMARY KEY); INSERT INTO d VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
CREATE TABLE customers (id INT NOT NULL PRIMARY KEY, country_code CHAR(2) NOT NULL, name VARCHAR(40), KEY k_country (country_code));
INSERT INTO customers SELECT x.i, ELT(1 + (x.i % 20 < 12) * 0 + (x.i % 20 >= 12) * (1 + x.i % 5), 'US','AR','BR','DE','FR','JP'), CONCAT('c', x.i)
 FROM (SELECT 1 + a.n + 10*b.n + 100*c.n + 1000*e.n + 10000*f.n AS i FROM d a, d b, d c, d e, d f) x;
CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, customer_id INT NOT NULL, status VARCHAR(16) NOT NULL, amount DECIMAL(10,2) NOT NULL,
 created_at DATETIME NOT NULL, note VARCHAR(64), KEY k_customer (customer_id), KEY k_status (status), KEY k_created (created_at));
INSERT INTO orders SELECT x.i, 1 + (x.i * 7919) % 100000,
 CASE WHEN x.i % 1000 = 0 THEN 'refunded' WHEN x.i % 10 = 3 THEN 'pending' ELSE 'paid' END,
 (x.i * 37 % 100000) / 100, TIMESTAMPADD(SECOND, FLOOR(x.i * 31.5), '2024-10-01 00:00:00'),
 CASE WHEN x.i = 1234567 THEN 'rare' ELSE CONCAT('n', x.i % 1000) END
 FROM (SELECT 1 + a.n + 10*b.n + 100*c.n + 1000*e.n + 10000*f.n + 100000*g.n + 1000000*h.n AS i FROM d a, d b, d c, d e, d f, d g, d h WHERE h.n < 2) x;
ANALYZE TABLE customers, orders;
ALTER TABLE orders DROP KEY k_customer, ADD KEY k_cust_created (customer_id, created_at);
ANALYZE TABLE orders;
-- The a* and b* plans were captured here. The p* plans (MySQL only) after
-- these three more objects:
ALTER TABLE orders ADD KEY k_note2 (note(2));
CREATE VIEW v_orders AS SELECT * FROM orders WHERE amount >= 0;
CREATE TABLE porders (id INT NOT NULL PRIMARY KEY, v INT) PARTITION BY HASH(id) PARTITIONS 4;
INSERT INTO porders SELECT id, customer_id FROM orders WHERE id <= 200000;
ANALYZE TABLE orders, porders;

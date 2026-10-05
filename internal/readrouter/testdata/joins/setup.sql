-- The dataset the plans of this directory were captured over: orders
-- 2,000,000 rows, order_items 4,000,000, customers 100,000, products 2,000,
-- countries 50. Deterministic: every value is a function of the row number.
-- Each <server>/<name>.json is the output of EXPLAIN FORMAT=JSON for the
-- statement of that name in statements.tsv (name, a tab, the statement; a
-- leading "SET ...; " was run in the same session before the EXPLAIN), on
-- MariaDB 11.4.13, MariaDB 10.11.19 and MySQL 8.4.9 with default settings.
DROP DATABASE IF EXISTS shop; CREATE DATABASE shop; USE shop;
CREATE TABLE seq (n INT NOT NULL PRIMARY KEY);
INSERT INTO seq SELECT a.d + 10*b.d + 100*c.d + 1000*e.d + 10000*f.d + 100000*g.d + 1 FROM (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a, (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b, (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c, (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) e, (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) f, (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) g;
CREATE TABLE countries (code CHAR(2) NOT NULL PRIMARY KEY, idx INT NOT NULL, name VARCHAR(64), region VARCHAR(16), UNIQUE KEY (idx));
INSERT INTO countries SELECT ELT(n, 'AR','AT','AU','BE','BO','BR','CA','CH','CL','CN','CO','CR','CZ','DE','DK','EC','EG','ES','FI','FR','GB','GR','HU','ID','IE','IL','IN','IT','JP','KE','KR','MA','MX','NG','NL','NO','NZ','PE','PH','PL','PT','PY','RO','SE','SG','TR','US','UY','VE','ZA'), n - 1, CONCAT('country ', n), ELT(1 + n % 5, 'americas', 'europe', 'asia', 'africa', 'oceania') FROM seq WHERE n <= 50;
CREATE TABLE customers (id INT NOT NULL PRIMARY KEY, name VARCHAR(64), email VARCHAR(64), country_code CHAR(2) NOT NULL, tier VARCHAR(16), created_at DATETIME, KEY (country_code), KEY (email));
INSERT INTO customers SELECT s.n, CONCAT('customer ', s.n), CONCAT('c', s.n, '@example.com'), c.code, ELT(1 + s.n % 3, 'gold', 'silver', 'bronze'), '2023-01-01' + INTERVAL s.n MINUTE FROM seq s JOIN countries c ON c.idx = s.n % 50 WHERE s.n <= 100000;
CREATE TABLE products (id INT NOT NULL PRIMARY KEY, name VARCHAR(64), category_id INT NOT NULL, price DECIMAL(10,2), KEY (category_id));
INSERT INTO products SELECT n, CONCAT('product ', n), 1 + n % 40, (n * 37) % 9000 / 100 + 1 FROM seq WHERE n <= 2000;
CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, customer_id INT NOT NULL, status VARCHAR(16), amount DECIMAL(10,2), note VARCHAR(64), created_at DATETIME, KEY (customer_id), KEY (created_at), KEY (status));
INSERT INTO orders SELECT n, 1 + (n * 7919) % 100000, ELT(1 + n % 5, 'new', 'paid', 'paid', 'shipped', 'refunded'), (n * 37) % 50000 / 100, CONCAT('note ', n % 1000), '2024-01-01' + INTERVAL n * 30 SECOND FROM seq;
INSERT INTO orders SELECT n + 1000000, 1 + ((n + 1000000) * 7919) % 100000, ELT(1 + n % 5, 'new', 'paid', 'paid', 'shipped', 'refunded'), ((n + 1000000) * 37) % 50000 / 100, CONCAT('note ', n % 1000), '2024-01-01' + INTERVAL (n + 1000000) * 30 SECOND FROM seq;
CREATE TABLE order_items (id INT NOT NULL PRIMARY KEY, order_id INT NOT NULL, product_id INT NOT NULL, qty INT NOT NULL, price DECIMAL(10,2), KEY (order_id), KEY (product_id));
INSERT INTO order_items SELECT n, 1 + (n - 1) DIV 2, 1 + (n * 31) % 2000, 1 + n % 5, (n * 13) % 9000 / 100 + 1 FROM seq;
INSERT INTO order_items SELECT n + 1000000, 1 + (n + 999999) DIV 2, 1 + ((n + 1000000) * 31) % 2000, 1 + n % 5, (n * 13) % 9000 / 100 + 1 FROM seq;
INSERT INTO order_items SELECT n + 2000000, 1 + (n + 1999999) DIV 2, 1 + ((n + 2000000) * 31) % 2000, 1 + n % 5, (n * 13) % 9000 / 100 + 1 FROM seq;
INSERT INTO order_items SELECT n + 3000000, 1 + (n + 2999999) DIV 2, 1 + ((n + 3000000) * 31) % 2000, 1 + n % 5, (n * 13) % 9000 / 100 + 1 FROM seq;
ANALYZE TABLE countries, customers, products, orders, order_items;

A real dump, kept as a test fixture. Do not edit the files by hand.

Source: MySQL 8.0.46, schema `shop`: tables `orders` and `customers` (three
rows each) and the view `big_orders` over `orders`.

Tool: mydumper v1.0.3-1 with `--complete-insert --database shop`.

mydumper writes two files for the view: `shop.big_orders-schema.sql`, a
placeholder `CREATE TABLE`, and `shop.big_orders-schema-view.sql`, the view.

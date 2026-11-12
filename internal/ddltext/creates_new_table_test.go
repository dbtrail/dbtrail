package ddltext

import "testing"

// CreatesNewTable is what stands between a recorded CREATE TABLE statement
// and the sentence "the table was created at that moment". The statement an
// application runs at every start, CREATE TABLE IF NOT EXISTS, must never
// pass, however it is spelled.
func TestCreatesNewTable(t *testing.T) {
	cases := []struct {
		stmt string
		want bool
	}{
		{"CREATE TABLE orders (id int primary key)", true},
		{"create table `shop`.`orders` (id int)", true},
		{"CREATE TABLE orders LIKE template", true},
		{"CREATE TABLE orders AS SELECT * FROM old_orders", true},
		{"CREATE TABLE orders (id int) SELECT id FROM old_orders", true},
		{"CREATE TABLE `orders` (`id` int NOT NULL) START TRANSACTION", true},
		{"CREATE OR REPLACE TABLE orders (id int)", true},
		{"/* app */ CREATE TABLE orders (id int)", true},
		// A table whose name starts like the keyword is still a new table.
		{"CREATE TABLE `if` (id int)", true},
		{"CREATE TABLE ifs (id int)", true},

		{"CREATE TABLE IF NOT EXISTS orders (id int primary key)", false},
		{"create table if not exists orders (id int)", false},
		{"CREATE TABLE IF  NOT\n\tEXISTS orders (id int)", false},
		{"CREATE TABLE /* app */ IF /* x */ NOT /* y */ EXISTS orders (id int)", false},
		{"CREATE TABLE /*!32312 IF NOT EXISTS*/ `orders` (id int)", false},
		{"CREATE TABLE /*M!100000 IF NOT EXISTS */ orders (id int)", false},
		{"CREATE TABLE -- note\n IF NOT EXISTS orders (id int)", false},
		{"CREATE TABLE IF NOT EXISTS orders LIKE template", false},
		{"CREATE OR REPLACE TABLE IF NOT EXISTS orders (id int)", false},

		{"CREATE TEMPORARY TABLE orders (id int)", false},
		{"CREATE TEMPORARY TABLE IF NOT EXISTS orders (id int)", false},
		{"CREATE TABLE", false},
		{"CREATE TABLE orders (id int) /* cut short", false},
		{"", false},
		{"(same CREATE TABLE statement as the row for shop.a)", false},
		{"/* dbtrail: inferred from relation shape; not a statement */", false},
		{"ALTER TABLE orders ADD COLUMN x int", false},
		{"RENAME TABLE a TO orders", false},
		{"CREATE VIEW orders AS SELECT 1", false},
		{"CREATE TABLESPACE ts", false},
	}
	for _, tc := range cases {
		if got := CreatesNewTable(tc.stmt); got != tc.want {
			t.Errorf("CreatesNewTable(%q) = %v, want %v", tc.stmt, got, tc.want)
		}
	}
}

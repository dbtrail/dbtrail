package ddltext

import "testing"

// The schema file of a table written from its recorded changes alone carries
// the newest recorded CREATE TABLE. When that is a CREATE TABLE IF NOT EXISTS,
// a loader would load the rows into a table already on the target instead of
// stopping, so the clause is taken out, and only from the head.
func TestWithoutIfNotExists(t *testing.T) {
	cases := []struct {
		stmt, want string
		ok         bool
	}{
		{"CREATE TABLE IF NOT EXISTS orders (id int primary key)", "CREATE TABLE orders (id int primary key)", true},
		{"create table if not exists `orders` (id int)", "CREATE TABLE `orders` (id int)", true},
		{"CREATE TABLE IF  NOT\n\tEXISTS orders (id int)", "CREATE TABLE orders (id int)", true},
		{"CREATE\nTABLE\nIF NOT EXISTS\n`shop`.`orders` (\n  id int\n)", "CREATE TABLE `shop`.`orders` (\n  id int\n)", true},
		{"/* app */ CREATE TABLE IF NOT EXISTS orders (id int)", "/* app */ CREATE TABLE orders (id int)", true},
		{"-- migration 12\nCREATE TABLE IF NOT EXISTS orders (id int)", "-- migration 12\nCREATE TABLE orders (id int)", true},
		{"CREATE TABLE /*!32312 IF NOT EXISTS*/ `orders` (id int)", "CREATE TABLE `orders` (id int)", true},
		{"CREATE TABLE /*M!100000 IF NOT EXISTS */ orders (id int)", "CREATE TABLE orders (id int)", true},
		{"CREATE TABLE IF NOT EXISTS`orders` (id int)", "CREATE TABLE `orders` (id int)", true},
		// The words later in the text belong to the definition.
		{"CREATE TABLE IF NOT EXISTS orders (note varchar(40) DEFAULT 'IF NOT EXISTS' COMMENT 'create table if not exists')",
			"CREATE TABLE orders (note varchar(40) DEFAULT 'IF NOT EXISTS' COMMENT 'create table if not exists')", true},
		{"CREATE TABLE IF NOT EXISTS orders LIKE template", "CREATE TABLE orders LIKE template", true},

		// Not the clause: nothing to take out, and nothing returned.
		{"CREATE TABLE orders (id int)", "", false},
		{"CREATE TABLE orders (note varchar(40) DEFAULT 'IF NOT EXISTS')", "", false},
		{"CREATE TABLE `if` (id int)", "", false},
		{"CREATE OR REPLACE TABLE orders (id int)", "", false},
		{"CREATE TEMPORARY TABLE IF NOT EXISTS orders (id int)", "", false},
		{"ALTER TABLE orders ADD COLUMN x int", "", false},
		{"", "", false},
		// The clause where the head match cannot take it out: refused, never
		// returned with the clause still in it.
		{"CREATE TABLE /* app */ IF /* x */ NOT /* y */ EXISTS orders (id int)", "", false},
		{"CREATE /* x */ TABLE IF NOT EXISTS orders (id int)", "", false},
		{"CREATE TABLE -- note\n IF NOT EXISTS orders (id int)", "", false},
		// Text the reader does not accept.
		{"CREATE TABLE IF NOT EXISTS orders (id int) /* cut short", "", false},
		{"CREATE TABLE IF NOT EXISTS", "", false},
	}
	for _, tc := range cases {
		got, ok := WithoutIfNotExists(tc.stmt)
		if got != tc.want || ok != tc.ok {
			t.Errorf("WithoutIfNotExists(%q) = %q, %v; want %q, %v", tc.stmt, got, ok, tc.want, tc.ok)
		}
	}
}

// HasIfNotExists reads the clause wherever comments sit between its words,
// so a statement WithoutIfNotExists cannot rewrite is still told apart from
// one that never had the clause.
func TestHasIfNotExists(t *testing.T) {
	cases := []struct {
		stmt string
		want bool
	}{
		{"CREATE TABLE IF NOT EXISTS orders (id int)", true},
		{"CREATE /* x */ TABLE IF NOT EXISTS orders (id int)", true},
		{"CREATE TABLE /* app */ IF /* x */ NOT /* y */ EXISTS orders (id int)", true},
		{"CREATE TABLE -- note\n IF NOT EXISTS orders (id int)", true},
		{"CREATE TABLE /*!32312 IF NOT EXISTS*/ orders (id int)", true},
		{"CREATE TABLE orders (id int)", false},
		{"CREATE TABLE orders (note varchar(40) DEFAULT 'IF NOT EXISTS')", false},
		{"CREATE TABLE `if` (id int)", false},
		{"CREATE OR REPLACE TABLE orders (id int)", false},
		{"CREATE TEMPORARY TABLE IF NOT EXISTS orders (id int)", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := HasIfNotExists(tc.stmt); got != tc.want {
			t.Errorf("HasIfNotExists(%q) = %v, want %v", tc.stmt, got, tc.want)
		}
	}
}

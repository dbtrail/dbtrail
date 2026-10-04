package readrouter

import (
	"os"
	"path/filepath"
	"testing"
)

func TestClassify(t *testing.T) {
	cases := map[string]Kind{
		"SELECT 1":                                       KindSelect,
		"  select * from t":                              KindSelect,
		"(SELECT 1) UNION (SELECT 2)":                    KindSelect,
		"WITH x AS (SELECT 1) SELECT * FROM x":           KindSelect,
		"/* app */ SELECT 1":                             KindSelect,
		"-- note\nSELECT 1":                              KindSelect,
		"SET NAMES utf8mb4":                              KindSet,
		"set time_zone = '+00:00'":                       KindSet,
		"SET TRANSACTION ISOLATION LEVEL READ COMMITTED": KindSet,
		"BEGIN":                                    KindTxnBegin,
		"START TRANSACTION READ ONLY":              KindTxnBegin,
		"COMMIT":                                   KindTxnEnd,
		"rollback":                                 KindTxnEnd,
		"SHOW TABLES":                              KindOther,
		"UPDATE t SET a = 1":                       KindWrite,
		"INSERT INTO t VALUES (1)":                 KindWrite,
		"DROP TABLE t":                             KindWrite,
		"CREATE TEMPORARY TABLE x (a INT)":         KindSet,
		"LOCK TABLES t READ":                       KindSet,
		"PREPARE s FROM 'SELECT 1'":                KindSet,
		"/*!40101 SET NAMES utf8mb4 */":            KindSet,
		"/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE */": KindSet,
		"/*!50001 SELECT 1 */":                     KindSelect,
		"EXPLAIN SELECT 1":                         KindOther,
		"USE shop":                                 KindOther,
		"selection":                                KindOther, // not the keyword
	}
	for stmt, want := range cases {
		if got := Classify(stmt); got != want {
			t.Errorf("Classify(%q) = %s, want %s", stmt, got, want)
		}
	}
}

func TestVeto(t *testing.T) {
	vetoed := map[string]string{
		"SELECT GROUP_CONCAT(name) FROM t":                     "GROUP_CONCAT",
		"SELECT * FROM t WHERE ts > NOW() - INTERVAL 1 DAY":    "NOW/CURDATE/CURTIME/CURRENT_TIMESTAMP",
		"SELECT STR_TO_DATE(d, '%Y') FROM t":                   "STR_TO_DATE",
		"SELECT TIMESTAMPDIFF(DAY, a, b) FROM t":               "TIMESTAMPDIFF/DATEDIFF",
		"SELECT * FROM t WHERE name = 'x' COLLATE utf8mb4_bin": "COLLATE",
		"SELECT CAST(a AS UNSIGNED) FROM t":                    "CAST AS UNSIGNED/SIGNED",
		"SELECT a DIV 2 FROM t":                                "DIV",
		"SELECT RAND()":                                        "RAND/UUID",
		"SELECT FOUND_ROWS()":                                  "FOUND_ROWS/LAST_INSERT_ID/ROW_COUNT",
		"SELECT SQL_CALC_FOUND_ROWS * FROM t LIMIT 10":         "FOUND_ROWS/LAST_INSERT_ID/ROW_COUNT",
		"SELECT DATABASE()":                                    "CONNECTION_ID/USER/DATABASE/VERSION",
		"SELECT @@version":                                     "user or system variable",
		"SELECT @x := 1":                                       "user or system variable",
		"SELECT * FROM t FOR UPDATE":                           "locking read",
		"SELECT * FROM t LOCK IN SHARE MODE":                   "locking read",
		"SELECT * FROM t INTO OUTFILE '/tmp/x'":                "INTO (OUTFILE/DUMPFILE/variables)",
		"SELECT * FROM information_schema.tables":              "system schema",
		"SELECT * FROM mysql.user":                             "system schema",
		"SELECT * FROM t WHERE MATCH(body) AGAINST ('x')":      "MATCH AGAINST",
		"SELECT * FROM t WHERE BINARY name = 'A'":              "binary string comparison",
		"SELECT /*+ NO_INDEX(t) */ * FROM t":                   "optimizer hint or MySQL comment",
		"SELECT * FROM t WHERE name LIKE 'a%'":                 "LIKE/REGEXP (case-sensitive on the copy, case-insensitive on MySQL)",
		"SELECT count(DISTINCT status) FROM t":                 "DISTINCT inside an aggregate (not folded by the copy's collation)",
		"SELECT INSTR(name, 'x') FROM t":                       "INSTR/LOCATE/POSITION/STRCMP (case-sensitive on the copy)",
		"SELECT * FROM t WHERE name REGEXP '^a'":               "LIKE/REGEXP (case-sensitive on the copy, case-insensitive on MySQL)",
		"SELECT a || b FROM t":                                 "|| (string concatenation on the copy, logical OR on MySQL)",
		"SELECT 2 ^ 3":                                         "^ (power on the copy, bitwise XOR on MySQL)",
		`SELECT * FROM t WHERE status = "paid"`:                "double-quoted string literal",
		`SELECT "id" FROM t`:                                   "double-quoted string literal",
		// A quote inside a comment must not blank what follows it.
		"SELECT /* it's */ NOW() /* it's */ FROM t": "NOW/CURDATE/CURTIME/CURRENT_TIMESTAMP",
		"SELECT -- don't\n NOW(), 'x' FROM t":       "NOW/CURDATE/CURTIME/CURRENT_TIMESTAMP",
		"SELECT # don't\n NOW() FROM t":             "NOW/CURDATE/CURTIME/CURRENT_TIMESTAMP",
	}
	for stmt, want := range vetoed {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
	clean := []string{
		"SELECT status, count(*) FROM orders GROUP BY status",
		"SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.amount > 100",
		"SELECT * FROM t WHERE note = 'call NOW() at @home DIV user()'", // inside a string literal
		"SELECT `id`, `status` FROM `shop`.`orders` LIMIT 10, 20",
		"SELECT nowhere FROM t",         // not the function
		"SELECT divisor FROM t",         // not the keyword
		"SELECT likes, unlike FROM t",   // not the operator
		"SELECT DISTINCT status FROM t", // SELECT DISTINCT folds; only the aggregate form does not
		"SELECT count(*) FROM t WHERE distinct_id = 1",
		"SELECT user_id FROM t", // not user(
		"SELECT * FROM t WHERE note = '#not a comment -- nor this' AND a = 1",
		"SELECT `a\"b` FROM t",  // a double quote inside a backtick identifier is not a string
		"SELECT 'it''s' FROM t", // a doubled quote does not end the literal
	}
	harmless := map[string]bool{
		"SET NAMES utf8mb4":                            true,
		"SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci": true,
		"SET character_set_results = utf8mb4":          true,
		"SET autocommit = 1":                           true,
		"SET autocommit = 0":                           false,
		"SET time_zone = '+00:00'":                     false,
		"SET sql_mode = ''":                            false,
	}
	for stmt, want := range harmless {
		if got := HarmlessSet(stmt); got != want {
			t.Errorf("HarmlessSet(%q) = %v, want %v", stmt, got, want)
		}
	}
	if got := LeadingKeyword("/*!40101 drop table t */"); got != "DROP" {
		t.Errorf("LeadingKeyword = %q", got)
	}
	for _, stmt := range clean {
		if got := Veto(stmt); got != "" {
			t.Errorf("Veto(%q) = %q, want none", stmt, got)
		}
	}
}

// The five fixtures are real EXPLAIN FORMAT=JSON output from MySQL 8.4.9 on a
// 200,000-row table with a primary key and one secondary index.
func TestParsePlan_fixtures(t *testing.T) {
	cases := []struct {
		file      string
		cost      float64
		tables    int
		fullScans int
		maxRows   int64
		toCopy    bool
	}{
		{"point_lookup.json", 1.0, 1, 0, 1, false},
		{"ref_limit.json", 808.75, 1, 0, 4000, false},
		{"group_by_full_scan.json", 20145.85, 1, 1, 200096, true},
		// GROUP BY served from the secondary index: access_type "index", not
		// "ALL", so it is the COST rule that sends it to the copy.
		{"group_by_filter_sort.json", 20145.85, 1, 0, 200096, true},
		{"join_derived.json", 253574.82, 3, 1, 200096, true},
	}
	pol := DefaultPolicy()
	for _, tc := range cases {
		raw, err := os.ReadFile(filepath.Join("testdata", tc.file))
		if err != nil {
			t.Fatal(err)
		}
		p, err := ParsePlan(raw)
		if err != nil {
			t.Fatalf("%s: %v", tc.file, err)
		}
		if p.Cost != tc.cost || p.Tables != tc.tables || p.FullScans != tc.fullScans || p.MaxScanRows != tc.maxRows {
			t.Errorf("%s: plan = %+v, want cost %v tables %d fullScans %d maxRows %d", tc.file, p, tc.cost, tc.tables, tc.fullScans, tc.maxRows)
		}
		if toCopy, reason := pol.Decide(p); toCopy != tc.toCopy {
			t.Errorf("%s: Decide = %v (%s), want %v", tc.file, toCopy, reason, tc.toCopy)
		}
	}
}

func TestParsePlan_shapes(t *testing.T) {
	// The optimizer's shortcut: no plan, a message.
	p, err := ParsePlan([]byte(`{"query_block": {"select_id": 1, "message": "no matching row in const table"}}`))
	if err != nil || p.Message == "" {
		t.Fatalf("message plan: %+v, %v", p, err)
	}
	if toCopy, reason := DefaultPolicy().Decide(p); toCopy || reason == "" {
		t.Errorf("trivial plan routed to the copy: %v %q", toCopy, reason)
	}
	// Full scan over few rows with a low cost: stays on MySQL; the scan rule
	// needs BOTH a full scan and the row count.
	p, _ = ParsePlan([]byte(`{"query_block": {"cost_info": {"query_cost": "5.0"}, "table": {"table_name": "t", "access_type": "ALL", "rows_examined_per_scan": 40}}}`))
	if toCopy, _ := DefaultPolicy().Decide(p); toCopy {
		t.Error("a 40-row full scan went to the copy")
	}
	// A zero policy never routes to the copy.
	p, _ = ParsePlan([]byte(`{"query_block": {"cost_info": {"query_cost": "9e9"}, "table": {"table_name": "t", "access_type": "ALL", "rows_examined_per_scan": 9000000}}}`))
	if toCopy, _ := (Policy{}).Decide(p); toCopy {
		t.Error("the zero policy routed to the copy")
	}
	if _, err := ParsePlan([]byte(`not json`)); err == nil {
		t.Error("non-JSON accepted")
	}
	if _, err := ParsePlan([]byte(`{"x": 1}`)); err == nil {
		t.Error("JSON without query_block accepted")
	}
}

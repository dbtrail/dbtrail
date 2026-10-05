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
		"SELECT GROUP_CONCAT(name) FROM t":                      "GROUP_CONCAT",
		"SELECT * FROM t WHERE ts > NOW() - INTERVAL 1 DAY":     "NOW/CURDATE/CURTIME/CURRENT_TIMESTAMP",
		"SELECT STR_TO_DATE(d, '%Y') FROM t":                    "STR_TO_DATE",
		"SELECT TIMESTAMPDIFF(DAY, a, b) FROM t":                "TIMESTAMPDIFF/DATEDIFF",
		"SELECT * FROM t WHERE name = 'x' COLLATE utf8mb4_bin":  "COLLATE",
		"SELECT CAST(a AS UNSIGNED) FROM t":                     "CAST AS UNSIGNED/SIGNED",
		"SELECT a DIV 2 FROM t":                                 "DIV",
		"SELECT RAND()":                                         "RAND/UUID",
		"SELECT FOUND_ROWS()":                                   "FOUND_ROWS/LAST_INSERT_ID/ROW_COUNT",
		"SELECT SQL_CALC_FOUND_ROWS * FROM t LIMIT 10":          "FOUND_ROWS/LAST_INSERT_ID/ROW_COUNT",
		"SELECT DATABASE()":                                     "CONNECTION_ID/USER/DATABASE/VERSION",
		"SELECT @@version":                                      "user or system variable",
		"SELECT @x := 1":                                        "user or system variable",
		"SELECT * FROM t FOR UPDATE":                            "locking read",
		`SELECT * FROM t WHERE path = 'a\\b'`:                   "backslash in a string literal (an escape on MySQL, a plain character on the copy)",
		`SELECT * FROM t WHERE name = 'it\'s'`:                  "backslash in a string literal (an escape on MySQL, a plain character on the copy)",
		`SELECT id FROM t WHERE code = 'A\_1' ORDER BY id`:      "backslash in a string literal (an escape on MySQL, a plain character on the copy)",
		`SELECT * FROM t WHERE a = 1 AND note = '\n'`:           "backslash in a string literal (an escape on MySQL, a plain character on the copy)",
		"SELECT JSON_UNQUOTE(JSON_EXTRACT(v, '$.tier')) FROM t": "JSON function or -> operator (missing or different on the copy)",
		"SELECT v->>'$.tier' FROM t":                            "JSON function or -> operator (missing or different on the copy)",
		"SELECT v->'$.tier' FROM t":                             "JSON function or -> operator (missing or different on the copy)",
		"SELECT * FROM t LOCK IN SHARE MODE":                    "locking read",
		"SELECT * FROM t INTO OUTFILE '/tmp/x'":                 "INTO (OUTFILE/DUMPFILE/variables)",
		"SELECT * FROM information_schema.tables":               "system schema",
		"SELECT * FROM mysql.user":                              "system schema",
		"SELECT * FROM t WHERE MATCH(body) AGAINST ('x')":       "MATCH AGAINST",
		"SELECT * FROM t WHERE BINARY name = 'A'":               "binary string comparison",
		"SELECT /*+ NO_INDEX(t) */ * FROM t":                    "optimizer hint or MySQL comment",
		"SELECT * FROM t WHERE name LIKE 'a%'":                  "LIKE/REGEXP (case-sensitive on the copy, case-insensitive on MySQL)",
		"SELECT count(DISTINCT status) FROM t":                  "DISTINCT inside an aggregate (not folded by the copy's collation)",
		"SELECT INSTR(name, 'x') FROM t":                        "INSTR/LOCATE/POSITION/STRCMP (case-sensitive on the copy)",
		"SELECT * FROM t WHERE name REGEXP '^a'":                "LIKE/REGEXP (case-sensitive on the copy, case-insensitive on MySQL)",
		"SELECT * FROM t WHERE x > 5--3 AND y = 1":              "-- without a space after it (two minus signs on MySQL, a comment on the copy)",
		"SELECT a --b\n FROM t":                                 "-- without a space after it (two minus signs on MySQL, a comment on the copy)",
		"SELECT a || b FROM t":                                  "|| (string concatenation on the copy, logical OR on MySQL)",
		"SELECT 2 ^ 3":                                          "^ (power on the copy, bitwise XOR on MySQL)",
		`SELECT * FROM t WHERE status = "paid"`:                 "double-quoted string literal",
		`SELECT "id" FROM t`:                                    "double-quoted string literal",
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
		"SELECT `id`, `status` FROM `shop`.`orders` LIMIT 20 OFFSET 10",
		"SELECT nowhere FROM t",         // not the function
		"SELECT divisor FROM t",         // not the keyword
		"SELECT likes, unlike FROM t",   // not the operator
		"SELECT DISTINCT status FROM t", // SELECT DISTINCT folds; only the aggregate form does not
		"SELECT count(*) FROM t WHERE distinct_id = 1",
		"SELECT user_id FROM t", // not user(
		"SELECT * FROM t WHERE note = '#not a comment -- nor this' AND a = 1",
		"SELECT 'it''s' FROM t",                             // a doubled quote does not end the literal
		"SELECT * FROM t WHERE path = 'C:/tmp' -- not a\\b", // a backslash in a comment is not in a literal
		"SELECT `a\\b` FROM t WHERE c = 'x'",                // nor one in a backtick identifier
		"SELECT json_col, jsonish FROM t",                   // a column named like the functions, no call
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
		// Real plans behind the LIMIT rule (limit_test.go): the cost rule fires
		// on both; DecideStatement is what keeps the index-ordered one on MySQL.
		{"order_by_pk_limit2.json", 272233.65, 1, 0, 2, true},
		{"order_by_filesort_limit20.json", 272233.65, 1, 1, 2654806, true},
		{"order_by_pk_limit2_filtered.json", 272233.65, 1, 0, 20, true},
		{"order_by_pk_limit2_join_filtered.json", 310020.10, 2, 0, 20, true},
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
		if d := pol.Decide(p); d.ToCopy != tc.toCopy {
			t.Errorf("%s: Decide = %v (%s), want %v", tc.file, d.ToCopy, d.Reason, tc.toCopy)
		}
	}
}

func TestParsePlan_shapes(t *testing.T) {
	// The optimizer's shortcut: no plan, a message.
	p, err := ParsePlan([]byte(`{"query_block": {"select_id": 1, "message": "no matching row in const table"}}`))
	if err != nil || p.Message == "" {
		t.Fatalf("message plan: %+v, %v", p, err)
	}
	if d := DefaultPolicy().Decide(p); d.ToCopy || d.Reason == "" || d.Rule != RuleTrivial {
		t.Errorf("trivial plan routed to the copy: %+v", d)
	}
	// Full scan over few rows with a low cost: stays on MySQL; the scan rule
	// needs BOTH a full scan and the row count.
	p, _ = ParsePlan([]byte(`{"query_block": {"cost_info": {"query_cost": "5.0"}, "table": {"table_name": "t", "access_type": "ALL", "rows_examined_per_scan": 40}}}`))
	if DefaultPolicy().Decide(p).ToCopy {
		t.Error("a 40-row full scan went to the copy")
	}
	// A zero policy never routes to the copy.
	p, _ = ParsePlan([]byte(`{"query_block": {"cost_info": {"query_cost": "9e9"}, "table": {"table_name": "t", "access_type": "ALL", "rows_examined_per_scan": 9000000}}}`))
	if (Policy{}).Decide(p).ToCopy {
		t.Error("the zero policy routed to the copy")
	}
	if _, err := ParsePlan([]byte(`not json`)); err == nil {
		t.Error("non-JSON accepted")
	}
	if _, err := ParsePlan([]byte(`{"x": 1}`)); err == nil {
		t.Error("JSON without query_block accepted")
	}
}

// An empty comment in front of a statement that has another comment later
// used to swallow the whole statement: it classified as "other", so a SET
// was forwarded without the port noticing it was one.
func TestClassify_emptyLeadingComment(t *testing.T) {
	cases := map[string]Kind{
		"/**/SET time_zone='+05:00'/**/":         KindSet,
		"/**/ SET time_zone = '+05:00' /* x */":  KindSet,
		"/**//**/SELECT 1 /* x */":               KindSelect,
		"/**/UPDATE t SET a = 1 /**/":            KindWrite,
		"/**/ /* a */ BEGIN /* b */":             KindTxnBegin,
		"/***/SET time_zone='+05:00'/**/":        KindSet,
		"/**/ /*!40101 SET time_zone='+05:00'*/": KindSet,
		// An executable comment that only opens the statement.
		"/*!50000 SET max_join_size = 1, */ time_zone = '+05:00'": KindSet,
		"/*!50000 SEL*/ECT 1": KindSet,
		// MariaDB's executable comment: the same code to MariaDB, a comment
		// to MySQL. Read as the code it may be.
		"/*M! SET time_zone = '+05:00' */":       KindSet,
		"/*M!100100 SET time_zone = '+05:00' */": KindSet,
		"/*M!100100 SET */ time_zone = '+05:00'": KindSet,
		"/*M*/SET time_zone = '+05:00' /* x */":  KindSet,
		"/* Mx */ /*M x*/ SELECT 1 /* y */":      KindSelect,
		// A control byte MySQL reads as white space.
		"\vSET time_zone = '+05:00'":    KindSet,
		"\f\v SET time_zone = '+05:00'": KindSet,
		"\x01SELECT 1":                  KindSelect,
		"\v/* c */\vUPDATE t SET a = 1": KindWrite,
		"\vBEGIN":                       KindTxnBegin,
	}
	for stmt, want := range cases {
		if got := Classify(stmt); got != want {
			t.Errorf("Classify(%q) = %s, want %s", stmt, got, want)
		}
	}
	if got := LeadingKeyword("/**/UPDATE t SET a = 1 /**/"); got != "UPDATE" {
		t.Errorf("LeadingKeyword = %q, want UPDATE", got)
	}
	if ReadOnlyRefusal("/**/UPDATE t SET a = 1 /**/") == "" {
		t.Error("a write behind an empty comment is not refused by read-only mode")
	}
}

// PlainRead: only what is positively a read leaves the session known. The
// read-only screen allows more (SET, USE, transaction control), and none of
// that is a read.
func TestPlainRead(t *testing.T) {
	reads := []string{
		"SELECT 1", "select * from t where a = 'x'", "(SELECT 1) UNION (SELECT 2)", "WITH c AS (SELECT 1) SELECT * FROM c",
		"TABLE t", "VALUES ROW(1)", "SHOW TABLES", "SHOW WARNINGS", "SHOW COUNT(*) WARNINGS", "DESCRIBE t", "DESC t",
		"EXPLAIN SELECT * FROM t", "EXPLAIN FORMAT=JSON SELECT * FROM t", "EXPLAIN ANALYZE SELECT * FROM t",
		"/* c */ SELECT 1 -- x", "\vSELECT 1", "SELECT @a := 1", "SELECT 'it''s; SET x = 1'", "ANALYZE SELECT * FROM t",
	}
	for _, stmt := range reads {
		if !PlainRead(stmt) {
			t.Errorf("PlainRead(%q) = false, want true", stmt)
		}
	}
	notReads := []string{
		"XA START 'x'", "XA COMMIT 'x'", "XA RECOVER", "FROBNICATE the session", "SELEKT 1", "SHOWW TABLES", "TABLEAU t",
		"SET time_zone = '+00:00'", "SET NAMES utf8mb4", "SET autocommit = 1", "SET @x = 1", "\vSET time_zone = 'UTC'",
		"/*!40101 SET time_zone = 'UTC' */", "/*M! SET time_zone = 'UTC' */", "/*M!100100 SET time_zone = 'UTC' */",
		"SELECT /*!40001 SQL_NO_CACHE */ * FROM t", "SELECT /*M! 1, */ 2",
		"USE shop", "BEGIN", "START TRANSACTION", "COMMIT", "ROLLBACK", "SAVEPOINT a", "RELEASE SAVEPOINT a",
		"BEGIN NOT ATOMIC SET time_zone = 'UTC'; END", "EXECUTE IMMEDIATE 'SET time_zone = ''UTC'''", "EXECUTE s", "PREPARE s FROM 'SELECT 1'",
		"CALL p()", "DO f()", "INSERT INTO t VALUES (1)", "UPDATE t SET a = 1", "DELETE FROM t", "REPLACE INTO t VALUES (1)",
		"CREATE TEMPORARY TABLE x (id INT)", "LOCK TABLES t READ", "UNLOCK TABLES", "HANDLER t OPEN", "FLUSH TABLES", "ANALYZE TABLE t",
		"SELECT 1 INTO @x", "SELECT * FROM t INTO OUTFILE '/tmp/x'", "SELECT * FROM t FOR UPDATE", "SELECT GET_LOCK('a', 1)",
		"SELECT 1; SET time_zone = 'UTC'", "WITH c AS (SELECT 1) DELETE FROM t", "EXPLAIN ANALYZE DELETE FROM t",
		"FROBNICATE", "", "SELECT 'unterminated", "SELECT 1 /* unterminated",
	}
	for _, stmt := range notReads {
		if PlainRead(stmt) {
			t.Errorf("PlainRead(%q) = true, want false: it can change the session, or is not known to be a read", stmt)
		}
	}
	// The read-only screen and this agree on every statement that is a read:
	// nothing is a plain read that the screen refuses.
	for _, stmt := range append(reads, notReads...) {
		if PlainRead(stmt) && ReadOnlyRefusal(stmt) != "" {
			t.Errorf("%q is a plain read and refused by read-only mode at once", stmt)
		}
	}
}

// WEEK and YEARWEEK number weeks by default_week_format on MySQL and by ISO
// weeks on the copy, which differ at the default: they stay on MySQL.
// WEEKOFYEAR is ISO on both.
func TestVeto_weekNumbering(t *testing.T) {
	for _, stmt := range []string{"SELECT WEEK(d), count(*) FROM t GROUP BY 1", "SELECT yearweek (d) FROM t", "SELECT YEARWEEK(d, 3) FROM t"} {
		if Veto(stmt) != "WEEK/YEARWEEK" {
			t.Errorf("Veto(%q) = %q, want WEEK/YEARWEEK", stmt, Veto(stmt))
		}
	}
	for _, stmt := range []string{"SELECT EXTRACT(WEEK FROM d), count(*) FROM t GROUP BY 1", "SELECT extract ( week\nFROM d) FROM t"} {
		if Veto(stmt) != "EXTRACT(WEEK ...)" {
			t.Errorf("Veto(%q) = %q, want EXTRACT(WEEK ...)", stmt, Veto(stmt))
		}
	}
	for _, stmt := range []string{"SELECT WEEKOFYEAR(d) FROM t", "SELECT week FROM t", "SELECT 'week(' FROM t", "SELECT weekday FROM t",
		"SELECT EXTRACT(YEAR FROM d) FROM t", "SELECT EXTRACT(DAY FROM week_start) FROM t"} {
		if Veto(stmt) != "" {
			t.Errorf("Veto(%q) = %q, want none", stmt, Veto(stmt))
		}
	}
}

// A recursive CTE runs to the end on the copy, where MySQL stops with an
// error at cte_max_recursion_depth and MariaDB cuts the result at
// max_recursive_iterations (1000 by default on both): it stays on MySQL.
func TestVeto_recursiveCTE(t *testing.T) {
	for _, stmt := range []string{
		"WITH RECURSIVE n AS (SELECT 1 AS i UNION ALL SELECT i + 1 FROM n WHERE i < 50) SELECT count(*) FROM n",
		"with\n recursive n AS (SELECT 1) SELECT * FROM n",
		"SELECT * FROM t WHERE id IN (WITH RECURSIVE n AS (SELECT 1) SELECT * FROM n)",
	} {
		if got := Veto(stmt); got != "WITH RECURSIVE" {
			t.Errorf("Veto(%q) = %q, want WITH RECURSIVE", stmt, got)
		}
	}
	for _, stmt := range []string{"WITH n AS (SELECT 1) SELECT * FROM n", "SELECT recursive FROM t", "SELECT 'with recursive' FROM t"} {
		if got := Veto(stmt); got != "" {
			t.Errorf("Veto(%q) = %q, want none", stmt, got)
		}
	}
}

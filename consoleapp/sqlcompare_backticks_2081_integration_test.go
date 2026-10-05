//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// backtickFixture is one statement of the #2081 comparison: what sql-compare
// must say about it, and why.
type backtickFixture struct {
	stmt    string
	verdict sqlcompare.Verdict
	// kind refines DIFFERENT; empty accepts any.
	kind string
	// names: the result's column names must be the same on both sides.
	names bool
	// veto, when set, is text the router's reason must hold: the statement
	// is kept on MySQL and the copy is not tried.
	veto string
	why  string
}

// Statements written the way drivers and ORMs write them, with every name in
// backticks, against a real source and the port as the copy (#2081). The
// copy holds the same rows, so every DIFFERENT is the copy answering
// differently. Before the rewrite all of these read NOT_ON_COPY.
func TestIntegrationSQLCompareBacktickNames(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	t.Run("mysql", func(t *testing.T) {
		srcDB, srcName := testutil.CreateTestDB(t)
		runBacktickFixtures(t, srcDB, srcName, testutil.BaseDSN()+"/"+srcName, false)
	})
	t.Run("mariadb", func(t *testing.T) {
		srcDB, srcName := testutil.CreateTestMariaDB(t)
		runBacktickFixtures(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName, true)
	})
}

// backtickTables are the source's tables: the DDL without a collation, so
// each engine applies its own default, and the rows.
var backtickTables = []struct {
	name string
	ddl  string
	rows [][]string
}{
	{"customers", "CREATE TABLE `customers` (`id` int NOT NULL, `name` varchar(64) DEFAULT NULL, `country_code` char(2) DEFAULT NULL, PRIMARY KEY (`id`))", [][]string{
		{"1", "Ada", "US"}, {"2", "Bo", "US"}, {"3", "Cy", "AR"}, {"4", "Di", "AR"}, {"5", "Ed", "CO"},
	}},
	{"orders", "CREATE TABLE `orders` (`id` int NOT NULL, `customer_id` int DEFAULT NULL, `status` varchar(16) DEFAULT NULL, `amount` decimal(12,2) DEFAULT NULL, `created_on` date DEFAULT NULL, PRIMARY KEY (`id`), KEY `idx_customer` (`customer_id`))", [][]string{
		{"1", "1", "paid", "10.00", "2026-01-01"}, {"2", "1", "paid", "20.50", "2026-01-02"}, {"3", "2", "open", "5.25", "2026-01-02"},
		{"4", "3", "paid", "7.00", "2026-01-03"}, {"5", "3", "void", "0.00", "2026-01-04"}, {"6", "4", "open", "99.99", "2026-01-05"},
		{"7", "5", "paid", "1.10", "2026-01-05"}, {"8", "5", "paid", "2.20", "2026-01-06"},
	}},
	{"order_items", "CREATE TABLE `order_items` (`id` int NOT NULL, `order_id` int DEFAULT NULL, `sku` varchar(32) DEFAULT NULL, `quantity` int DEFAULT NULL, `unit_price` decimal(10,2) DEFAULT NULL, PRIMARY KEY (`id`), KEY `idx_order` (`order_id`))", [][]string{
		{"1", "1", "A-1", "1", "10.00"}, {"2", "2", "A-1", "2", "10.00"}, {"3", "2", "B-2", "1", "0.50"}, {"4", "3", "C-3", "3", "1.75"},
		{"5", "4", "A-1", "7", "1.00"}, {"6", "6", "D-4", "1", "99.99"}, {"7", "7", "B-2", "2", "0.55"}, {"8", "8", "B-2", "4", "0.55"},
	}},
	// Names a statement can only spell quoted: a keyword, a space, a
	// non-ASCII letter, upper case.
	{"odd names", "CREATE TABLE `odd names` (`id` int NOT NULL, `select` int DEFAULT NULL, `total amount` decimal(10,2) DEFAULT NULL, `año` int DEFAULT NULL, `Mixed` int DEFAULT NULL, `count` int DEFAULT NULL, PRIMARY KEY (`id`))", [][]string{
		{"1", "10", "1.50", "2024", "7", "3"}, {"2", "20", "2.50", "2025", "8", "4"}, {"3", "30", "3.50", "2026", "9", "5"},
	}},
}

func runBacktickFixtures(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string, mariadb bool) {
	copyDSN := backtickRig(t, srcDB, srcName, sourceDSN)
	fixtures := backtickFixtures(srcName, mariadb)
	backtickCompare(t, sourceDSN, copyDSN, fixtures)
}

// backtickRig loads the tables into the source and into a snapshot, serves
// the copy on a port without routing and returns its DSN.
func backtickRig(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) string {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)

	baseDir := t.TempDir()
	snap := time.Now().UTC().Add(-time.Minute).Format("2006-01-02T15-04-05Z")
	for _, tb := range backtickTables {
		if _, err := srcDB.Exec(tb.ddl); err != nil {
			t.Fatalf("create %s: %v", tb.name, err)
		}
		for _, r := range tb.rows {
			args := make([]any, len(r))
			for i, v := range r {
				args[i] = v
			}
			if _, err := srcDB.Exec("INSERT INTO `"+tb.name+"` VALUES (?"+strings.Repeat(",?", len(r)-1)+")", args...); err != nil {
				t.Fatalf("insert %s: %v", tb.name, err)
			}
		}
		if _, err := srcDB.Exec("ANALYZE TABLE `" + tb.name + "`"); err != nil {
			t.Fatal(err)
		}
		// The baseline carries the DDL the source prints, as a real one does.
		var name, ddl string
		if err := srcDB.QueryRow("SHOW CREATE TABLE `"+tb.name+"`").Scan(&name, &ddl); err != nil {
			t.Fatalf("show create %s: %v", tb.name, err)
		}
		writeBacktickBaseline(t, filepath.Join(baseDir, snap, srcName, tb.name+".parquet"), ddl+";\n", tb.rows)
	}

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: sourceDSN, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = serveFlashback(ctx, srv, ln, flashbackConfig{}); close(served) }()
	t.Cleanup(func() { cancel(); <-served })
	return fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, ln.Addr(), srcName)
}

func backtickCompare(t *testing.T, sourceDSN, copyDSN string, fixtures []backtickFixture) {
	t.Helper()
	statements := make([]string, len(fixtures))
	for i, f := range fixtures {
		statements[i] = f.stmt
	}
	rep, err := sqlcompare.Run(context.Background(), sqlcompare.Options{
		SourceDSN: sourceDSN, CopyDSN: copyDSN, Policy: readrouter.Policy{ScanRows: 2},
	}, statements)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	by := map[string]sqlcompare.Result{}
	for _, r := range rep.Results {
		by[r.Statement] = r
	}
	src := openRaw(t, sourceDSN)
	cp := openRaw(t, copyDSN)
	tally := map[sqlcompare.Verdict]int{}
	for _, f := range fixtures {
		r, ok := by[f.stmt]
		if !ok {
			t.Errorf("no result for %q", f.stmt)
			continue
		}
		tally[r.Verdict]++
		copyStmt, refusal := readrouter.ForCopy(f.stmt)
		srcNames, cpNames := columnNames(src, f.stmt), columnNames(cp, copyStmt)
		t.Logf("%-12s %-9s route=%-5s (%s) %s\n    mysql: %s %s\n    copy:  %s %s\n    %s",
			r.Verdict, r.Kind, r.Route, r.RouteReason, f.stmt, srcNames, rawAnswer(src, f.stmt), cpNames, rawAnswer(cp, copyStmt), r.Detail)
		if r.Verdict != f.verdict || (f.kind != "" && r.Kind != f.kind) {
			t.Errorf("%q: got %s/%s (%s), want %s/%s: %s", f.stmt, r.Verdict, r.Kind, r.Detail, f.verdict, f.kind, f.why)
		}
		if f.names && strings.Join(srcNames, "\x00") != strings.Join(cpNames, "\x00") {
			t.Errorf("%q: column names %q on MySQL, %q on the copy", f.stmt, srcNames, cpNames)
		}
		if f.veto != "" && (r.Route != "mysql" || r.RouteRule != "veto" || !strings.Contains(r.RouteReason, f.veto)) {
			t.Errorf("%q: route=%s rule=%s (%s), want it kept on MySQL by the veto %q", f.stmt, r.Route, r.RouteRule, r.RouteReason, f.veto)
		}
		if f.veto == "" && r.RouteRule == "veto" {
			t.Errorf("%q: vetoed (%s); the fixture expects the router to consider it", f.stmt, r.RouteReason)
		}
		// The rewrite and the router are one reading: what ForCopy refuses
		// is vetoed under that very reason.
		if refusal != "" && r.RouteReason != "veto: "+refusal {
			t.Errorf("%q: the rewrite refuses it (%s) but the router says %s (%s)", f.stmt, refusal, r.RouteRule, r.RouteReason)
		}
		// The bar of #2081: nothing the router would send to the copy may
		// answer differently, beyond the differences listed as such.
		if r.Verdict == sqlcompare.Different && r.Route == "copy" && f.verdict != sqlcompare.Different {
			t.Errorf("%q: a copy-routed statement answers differently: %s", f.stmt, r.Detail)
		}
	}
	t.Logf("tally: %d EQUAL, %d DIFFERENT, %d NOT_ON_COPY, %d SOURCE_ERROR of %d", tally[sqlcompare.Equal], tally[sqlcompare.Different], tally[sqlcompare.NotOnCopy], tally[sqlcompare.SourceError], len(fixtures))
}

// columnNames is the result's column names as the client sees them; nil
// when the statement fails.
func columnNames(db *sql.DB, stmt string) []string {
	rows, err := db.Query(stmt)
	if err != nil {
		return nil
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil
	}
	return cols
}

func writeBacktickBaseline(t *testing.T, path, ddl string, rows [][]string) {
	t.Helper()
	schemaFile := filepath.Join(t.TempDir(), "t-schema.sql")
	if err := os.WriteFile(schemaFile, []byte(ddl), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := baseline.ParseSchema(schemaFile)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: ddl}})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, make([]bool, len(r))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// backtickFixtures: the verdicts are the same on MySQL 8.4 and MariaDB 11.4,
// but for the one statement only MariaDB accepts.
func backtickFixtures(db string, mariadb bool) []backtickFixture {
	eq, diff, noc, serr := sqlcompare.Equal, sqlcompare.Different, sqlcompare.NotOnCopy, sqlcompare.SourceError
	q := func(s string) string { return strings.ReplaceAll(s, "{db}", db) }
	// same: EQUAL, and the client reads the same column names.
	same := func(stmt, why string) backtickFixture {
		return backtickFixture{stmt: stmt, verdict: eq, names: true, why: why}
	}
	star := func(stmt string) backtickFixture {
		return same(stmt, "SELECT *: the columns come in the table's order on both sides (#2111)")
	}
	// kept: the router keeps it on MySQL under the named veto.
	kept := func(stmt string, verdict sqlcompare.Verdict, veto, why string) backtickFixture {
		return backtickFixture{stmt: stmt, verdict: verdict, veto: veto, why: why}
	}
	// $$ is a name on MariaDB and refused by MySQL 8.4. The list vetoes it
	// (not the rewrite), so the copy is sent the rewritten text here, and
	// answers: one column named ", 2 AS ".
	dollar := kept("SELECT `a` AS $$, 2 AS $$ FROM (SELECT 1 AS `a`) `t`", serr, "$...$", "MySQL 8.4 refuses a name that starts with $")
	if mariadb {
		dollar = kept(dollar.stmt, diff, "$...$", "two columns named $$ on MariaDB, one on the copy")
		dollar.kind = "columns"
	}
	return []backtickFixture{
		dollar,
		// What GORM sends.
		star("SELECT * FROM `orders` WHERE `orders`.`id` = 1 ORDER BY `orders`.`id` LIMIT 1"),
		star("SELECT * FROM `orders` WHERE `orders`.`customer_id` IN (1,2,3)"),
		star("SELECT * FROM `order_items` WHERE `order_items`.`order_id` IN (1,2,3,4) ORDER BY `order_items`.`id`"),
		star("SELECT `o`.* FROM `orders` `o` WHERE `o`.`id` = 2"),
		same("SELECT `orders`.`id`,`orders`.`customer_id`,`orders`.`status`,`orders`.`amount` FROM `orders` WHERE `orders`.`status` = 'paid' ORDER BY `orders`.`id` LIMIT 20", "a paginated list"),
		same("SELECT `id`,`status` FROM `orders` WHERE status = 'paid' AND `amount` > 2 ORDER BY `id` LIMIT 3 OFFSET 2", "the second page; quoted and bare names mixed"),
		{stmt: "SELECT count(*) FROM `orders` WHERE `orders`.`status` = 'paid'", verdict: eq, why: "the count is equal; an aggregate without an alias is named count(*) on MySQL and count_star() on the copy, quoted table or not"},
		same("SELECT `orders`.`id`,`orders`.`amount`,`Customer`.`id` AS `Customer__id`,`Customer`.`name` AS `Customer__name` FROM `orders` LEFT JOIN `customers` `Customer` ON `orders`.`customer_id` = `Customer`.`id` WHERE `Customer`.`country_code` = 'US' ORDER BY `orders`.`id`", "a joined preload: quoted table alias with upper case"),
		same("SELECT `status`, count(*) AS `n`, sum(`amount`) AS `total amount` FROM `orders` GROUP BY `status` ORDER BY `status`", "a report; an alias with a space"),
		same("SELECT `customers`.`country_code`, sum(`orders`.`amount`) AS `revenue` FROM `orders` JOIN `customers` ON `customers`.`id` = `orders`.`customer_id` GROUP BY `customers`.`country_code` HAVING sum(`orders`.`amount`) > 5 ORDER BY `revenue` DESC", "a joined report with HAVING, ordered by the alias"),
		// What Django sends.
		same("SELECT `orders`.`id`, `orders`.`customer_id`, `orders`.`status`, `orders`.`amount`, `orders`.`created_on` FROM `orders` WHERE `orders`.`status` = 'paid' ORDER BY `orders`.`id` ASC LIMIT 20", "every column named, a DATE among them"),
		same("SELECT `orders`.`id`, `orders`.`status` FROM `orders` ORDER BY `orders`.`id` DESC LIMIT 3 OFFSET 3", "pagination"),
		same("SELECT COUNT(*) AS `__count` FROM `orders`", "the paginator's count"),
		same("SELECT (1) AS `a` FROM `orders` WHERE `orders`.`id` = 3 LIMIT 1", "exists()"),
		same("SELECT `orders`.`id`, `orders`.`amount`, `customers`.`id`, `customers`.`name` FROM `orders` INNER JOIN `customers` ON (`orders`.`customer_id` = `customers`.`id`) WHERE `customers`.`country_code` = 'AR' ORDER BY `orders`.`id` ASC", "select_related: two columns both named id"),
		same("SELECT `orders`.`id` FROM `orders` WHERE `orders`.`id` IN (SELECT U0.`order_id` FROM `order_items` U0 WHERE U0.`quantity` > 1) ORDER BY `orders`.`id` ASC", "an IN subquery with Django's bare U0 alias"),
		same("SELECT `orders`.`status`, COUNT(`orders`.`id`) AS `n`, SUM(`orders`.`amount`) AS `total` FROM `orders` GROUP BY `orders`.`status` ORDER BY `total` DESC", "an annotated report"),
		same("SELECT MAX(`orders`.`amount`) AS `amount__max`, MIN(`orders`.`amount`) AS `amount__min`, SUM(`order_items`.`quantity`) AS `q` FROM `orders` LEFT OUTER JOIN `order_items` ON (`orders`.`id` = `order_items`.`order_id`)", "aggregate()"),
		same("SELECT DISTINCT `orders`.`status` FROM `orders` ORDER BY `orders`.`status` ASC", "distinct()"),
		same("SELECT `orders`.`id` FROM `orders` WHERE (`orders`.`created_on` >= '2026-01-02' AND `orders`.`created_on` < '2026-01-05' AND NOT (`orders`.`status` = 'void')) ORDER BY `orders`.`created_on` ASC, `orders`.`id` ASC", "a date range with exclude()"),
		{stmt: "SELECT AVG(`orders`.`amount`) AS `amount__avg` FROM `orders`", verdict: diff, kind: "precision", names: true, why: "AVG is a DECIMAL with four more decimals on MySQL and a DOUBLE on the copy (#2083), backticks or not"},
		// What still fails on the copy with the names out of the way (#2114).
		{stmt: "SELECT `orders`.`status`, COUNT(`orders`.`id`) AS `n` FROM `orders` GROUP BY `orders`.`status` ORDER BY NULL", verdict: noc, why: "ORDER BY NULL: a binder error on the copy (#2114)"},
		{stmt: "SELECT `orders`.`id`, `orders`.`amount` FROM `orders` ORDER BY `orders`.`id` LIMIT 0, 3", verdict: noc, why: "LIMIT offset, count: a syntax error on the copy (#2114)"},
		kept("SELECT `orders`.`id` FROM `orders` WHERE `orders`.`status` LIKE BINARY 'pa%'", noc, "binary string comparison", "LIKE BINARY: vetoed, and refused by the copy (#2114)"),
		{stmt: "SELECT `id` FROM `orders` USE INDEX (`idx_customer`) WHERE `customer_id` = 1 ORDER BY `id`", verdict: noc, why: "an index hint is MySQL syntax"},
		kept("SELECT `id` # by `id`\nFROM `orders` WHERE `id` < 3 ORDER BY `id`", noc, "# starts a comment", "# opens a comment on MySQL only: vetoed, the backtick inside it left alone, and the copy refuses the text"),
		// Qualified with the database, and names that only exist quoted.
		same(q("SELECT `{db}`.`orders`.`id`, `{db}`.`orders`.`status` FROM `{db}`.`orders` WHERE `{db}`.`orders`.`id` < 4 ORDER BY `{db}`.`orders`.`id`"), "database.table.column: the copy keeps each source database as a schema"),
		same(q("SELECT `o`.`id`, `c`.`name` FROM `{db}`.`orders` AS `o`, `{db}`.`customers` AS `c` WHERE `c`.`id` = `o`.`customer_id` ORDER BY `o`.`id`"), "qualified tables with quoted aliases"),
		same("SELECT `id`, `select`, `total amount`, `año`, `Mixed`, `count` FROM `odd names` ORDER BY `id`", "a keyword, a space, a non-ASCII letter, upper case; a table name with a space"),
		same("SELECT `odd names`.`select` AS `from`, `odd names`.`año` AS `名前` FROM `odd names` WHERE `odd names`.`total amount` > 2 ORDER BY `from`", "keywords and non-ASCII as aliases"),
		same("SELECT sum(`count`) AS `sum`, max(`select`) AS `max`, count(`count`) AS `count` FROM `odd names`", "names that spell functions of the copy, as columns and as aliases"),
		same("SELECT `id` AS `order`, `amount` AS `group` FROM `orders` WHERE `id` < 3 ORDER BY `order`", "keyword aliases, one used in ORDER BY"),
		same("SELECT `id` AS `Total`, `amount` AS `total2` FROM `orders` WHERE `id` = 1", "an alias keeps the case it was written in on both sides"),
		// Case. A column is found whatever its case on both sides, but the
		// client reads the name as written on MySQL and as stored on the copy.
		{stmt: "SELECT `ID`, `Status` FROM `orders` WHERE `ID` < 3 ORDER BY `ID`", verdict: eq, why: "found on both; named ID and Status on MySQL, id and status on the copy (the same without quotes)"},
		{stmt: "SELECT `mixed`, `MIXED` FROM `odd names` ORDER BY `ID`", verdict: eq, why: "found on both; named as written on MySQL, Mixed twice on the copy"},
		{stmt: "SELECT `id` FROM `ORDERS` WHERE `id` = 1", verdict: serr, why: "a table name is case-sensitive on this source (lower_case_table_names=0): MySQL refuses, so the copy, which would find it, is never asked"},
		{stmt: "SELECT `o`.`id` FROM `orders` `O` WHERE `o`.`id` = 1", verdict: serr, why: "and so is a table alias"},
		// An alias that is also a column: GROUP BY, HAVING and ORDER BY pick
		// the same one on both sides, quoted or not.
		same("SELECT `id`, -`amount` AS `amount` FROM `orders` ORDER BY `amount` LIMIT 3", "ORDER BY takes the alias"),
		same("SELECT id, -amount AS amount FROM orders ORDER BY amount LIMIT 3", "the same without quotes"),
		same("SELECT `status`, sum(`amount`) AS `amount` FROM `orders` GROUP BY `status` HAVING `amount` > 20 ORDER BY `status`", "HAVING takes the alias"),
		same("SELECT status, sum(amount) AS amount FROM orders GROUP BY status HAVING amount > 20 ORDER BY status", "the same without quotes"),
		same("SELECT `id`, `amount` * 2 AS `amount` FROM `orders` WHERE `amount` > 15 ORDER BY `id`", "WHERE takes the column"),
		same("SELECT `customer_id` AS `id`, count(*) AS `n` FROM `orders` GROUP BY `customer_id` ORDER BY `id` DESC", "ORDER BY takes the alias over the column of the same name"),
		// Shapes around the names.
		same("SELECT `id` /* the `key` */ FROM `orders` -- by `id`\nWHERE `id` < 3 ORDER BY `id`", "backticks inside comments are left alone"),
		same("SELECT `id` FROM `orders` WHERE `status` = 'pa`id' OR `status` = 'paid' ORDER BY `id`", "a backtick inside a string is data"),
		same("SELECT `id`AS`x` FROM`orders`WHERE`id`<3 ORDER BY`id`", "no white space around the names"),
		same("SELECT `e`.`id` FROM `orders` `e` WHERE `e`.`id` = 1", "an alias named like a string prefix of the copy"),
		same("SELECT `id`, `status` `amount` FROM `orders` WHERE `id` = 1", "an alias without AS"),
		same("SELECT `id` FROM `orders` WHERE `id` IN (1, 2) AND `status` IS NOT NULL ORDER BY 1 DESC", "IN list, ORDER BY position"),
		same("SELECT `t`.`status`, `t`.`n` FROM (SELECT `status`, count(*) AS `n` FROM `orders` GROUP BY `status`) AS `t` ORDER BY `t`.`n` DESC, `t`.`status`", "a derived table"),
		same("WITH `paid` AS (SELECT `id`, `amount` FROM `orders` WHERE `status` = 'paid') SELECT count(*) AS `n`, sum(`amount`) AS `s` FROM `paid`", "a CTE"),
		same("SELECT `id`, CASE WHEN `status` = 'paid' THEN 1 ELSE 0 END AS `is paid` FROM `orders` ORDER BY `id`", "CASE"),
		same("SELECT `id`, row_number() OVER (PARTITION BY `customer_id` ORDER BY `id`) AS `rn` FROM `orders` ORDER BY `id`", "a window function"),
		same("SELECT `orders`.`id` FROM `orders` WHERE EXISTS (SELECT 1 FROM `order_items` WHERE `order_items`.`order_id` = `orders`.`id` AND `order_items`.`sku` = 'B-2') ORDER BY `orders`.`id`", "a correlated EXISTS"),
		same("SELECT `status` FROM `orders` UNION ALL SELECT `sku` FROM `order_items` ORDER BY 1", "UNION ALL"),
		same("SELECT COALESCE(`customers`.`name`, 'x') AS `n`, `orders`.`id` FROM `orders` RIGHT JOIN `customers` ON `customers`.`id` = `orders`.`customer_id` ORDER BY `orders`.`id`, `n`", "RIGHT JOIN"),
		// What the rewrite does not guess: kept on MySQL, the copy not tried.
		kept("SELECT `id` AS `a\"b` FROM `orders` WHERE `id` = 1", noc, "holds a backtick or a double quote", "a double quote inside a name"),
		kept("SELECT `id` AS `a``b` FROM `orders` WHERE `id` = 1", noc, "holds a backtick or a double quote", "a backtick inside a name"),
		kept("SELECT `id` AS `` FROM `orders` WHERE `id` = 1", noc, "empty backtick-quoted name", "MySQL accepts an empty alias; the copy has no empty name"),
		kept("SELECT `abs`(-1), `upper`('a') FROM `orders` WHERE `id` = 1", noc, "right before a parenthesis", "a quoted function name: MySQL calls it"),
		kept("SELECT `count`(*) FROM `orders`", serr, "right before a parenthesis", "MySQL refuses a quoted COUNT; the copy would count"),
		kept("SELECT `sum`(`amount`) FROM `orders`", serr, "right before a parenthesis", "MySQL looks for a stored function named sum; the copy would sum"),
		kept("WITH `p` (`a`, `b`) AS (SELECT `id`, `amount` FROM `orders`) SELECT `a` FROM `p` WHERE `b` > 20 ORDER BY `a`", noc, "right before a parenthesis", "a CTE with a column list has the same shape as a call: kept on MySQL too"),
		kept("SELECT `id` & `customer_id` AS `x`, id U, U&`id` FROM (SELECT id, customer_id, id U FROM orders) t WHERE id = 3", noc, "right after U&", "U&`id` is column U AND column id on MySQL; U&\"id\" is one name on the copy"),
		kept("SELECT `id` FROM `orders` WHERE `status` = \"paid\" ORDER BY `id`", noc, "double-quoted string literal", "a double-quoted string stays a veto"),
		kept("SELECT `id`\"x\" FROM `orders` WHERE `id` = 1", noc, "double-quoted string literal", "a name right against a double-quoted alias"),
		kept("SELECT `table_name` FROM `information_schema`.`tables` WHERE `table_schema` = 'main' LIMIT 1", eq, "system schema", "the copy has an information_schema of its own and answers (here both find nothing): the veto must see through the quotes"),
		kept("SELECT `id` FROM `orders` WHERE `id` = 1 /* a /* nested */ OR `id` = 2 -- */", noc, "/* inside a comment", "MySQL ends the comment at the first */ and returns two rows; the copy nests comments and, given the names in double quotes, would return one"),
		kept("SELECT 1 /* a /* b */ + 1 -- */", diff, "/* inside a comment", "the same without names: 2 on MySQL, 1 on the copy"),
		kept("SELECT `text` 'Label' FROM (SELECT 'body' AS `text`) `t`", noc, "right before a string literal", "the column text under the alias Label on MySQL (body); the constant 'Label' of type text on the copy"),
		kept("SELECT `int`\n'5' FROM (SELECT 9 AS `int`) `t`", noc, "right before a string literal", "9 on MySQL, 5 on the copy"),
		kept("SELECT `a` -- x\r+1\n FROM (SELECT 1 AS `a`) `t`", noc, "carriage return inside a line comment", "the comment runs to the line feed on MySQL (1) and stops at the carriage return on the copy (2)"),
		kept("SELECT `a` /*M! +1 */ FROM (SELECT 1 AS `a`) `t`", noc, "optimizer hint or MySQL comment", "MariaDB runs the comment's text (2); MySQL and the copy do not (1)"),
	}
}

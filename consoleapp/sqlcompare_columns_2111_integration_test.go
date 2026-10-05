//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// orderTable is one table of the #2111 fixture: its definition, its rows on
// the source and (when they differ) on the copy, and whether the copy's file
// carries the definition.
type orderTable struct {
	name, ddl string
	// rows hold the STORED columns' values in declared order: a generated
	// column has none, on either side.
	rows     [][]string
	copyRows [][]string // nil = the same rows as the source
	footer   bool
}

// writeOrderSnapshot writes every table under one snapshot directory, the
// way the baseline writer writes a table: its columns sorted by name, its
// CREATE TABLE in the footer.
func writeOrderSnapshot(t *testing.T, dir, schema string, tables []orderTable) {
	t.Helper()
	snap := time.Now().UTC().Add(-time.Minute).Format("2006-01-02T15-04-05Z")
	for _, tb := range tables {
		cols, err := baseline.ParseSchemaText(tb.ddl)
		if err != nil {
			t.Fatal(err)
		}
		cfg := baseline.WriterConfig{Compression: "none", RowGroupSize: 100}
		if tb.footer {
			cfg.Metadata = map[string]string{baseline.MetaKeyCreateTableSQL: tb.ddl}
		}
		w, err := baseline.NewWriter(filepath.Join(dir, snap, schema, tb.name+".parquet"), cols, cfg)
		if err != nil {
			t.Fatal(err)
		}
		rows := tb.copyRows
		if rows == nil {
			rows = tb.rows
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
}

// answerColumns is the column names a statement returns on one side, in
// order, and its first row as text.
func answerColumns(t *testing.T, db *sql.DB, stmt string) (cols []string, first string) {
	t.Helper()
	rows, err := db.Query(stmt)
	if err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
	defer rows.Close()
	cols, err = rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		cells := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		parts := make([]string, len(cells))
		for i, c := range cells {
			parts[i] = string(c)
		}
		first = strings.Join(parts, "|")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return cols, first
}

// #2111 against a real MySQL: `SELECT *` on the copy returned the table's
// columns sorted by name, where MySQL returns them in the order the table
// declares. The same tables with the same rows on both sides, so a DIFFERENT
// is the copy answering differently; then the same port with read routing on,
// where a star the copy cannot answer as MySQL does must be MySQL's.
func TestIntegrationSQLCompareColumnOrder(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)

	tables := []orderTable{
		{name: "orders", footer: true,
			ddl: "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `customer_id` int NOT NULL,\n  `status` varchar(16) DEFAULT NULL,\n" +
				"  `amount` decimal(12,2) DEFAULT NULL,\n  `channel` varchar(8) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n",
			rows: [][]string{{"1", "70", "paid", "10.50", "web"}, {"2", "71", "open", "2.00", "app"}, {"3", "70", "paid", "7.25", "web"}}},
		{name: "customers", footer: true,
			ddl:  "CREATE TABLE `customers` (\n  `id` int NOT NULL,\n  `name` varchar(16) DEFAULT NULL,\n  `city` varchar(16) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n",
			rows: [][]string{{"70", "ann", "lima"}, {"71", "bob", "cali"}}},
		// A column added AFTER another: the table's order is neither the
		// order of creation nor the alphabet.
		{name: "altered", footer: true,
			ddl:  "CREATE TABLE `altered` (\n  `id` int NOT NULL,\n  `zzz` int DEFAULT NULL,\n  `name` varchar(16) DEFAULT NULL,\n  `aaa` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n",
			rows: [][]string{{"1", "26", "n", "1"}, {"2", "27", "m", "2"}}},
		// No CREATE TABLE in the file: what a snapshot of a PostgreSQL source
		// and one written before the key existed look like.
		{name: "legacy", footer: false,
			ddl:  "CREATE TABLE `legacy` (\n  `id` int NOT NULL,\n  `zeta` int DEFAULT NULL,\n  `alpha` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n",
			rows: [][]string{{"1", "26", "1"}, {"2", "27", "2"}, {"3", "28", "3"}}},
		// A generated column: MySQL returns it, a snapshot does not hold it.
		{name: "gen", footer: true,
			ddl: "CREATE TABLE `gen` (\n  `id` int NOT NULL,\n  `twice` int GENERATED ALWAYS AS ((`id` * 2)) STORED,\n" +
				"  `a` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n",
			rows: [][]string{{"1", "5"}, {"2", "6"}, {"3", "7"}}},
		// An invisible column: a snapshot holds it, MySQL's SELECT * leaves it out.
		{name: "invis", footer: true,
			ddl: "CREATE TABLE `invis` (\n  `id` int NOT NULL,\n  `secret` int DEFAULT NULL /*!80023 INVISIBLE */,\n" +
				"  `a` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n",
			rows: [][]string{{"1", "9", "5"}, {"2", "9", "6"}, {"3", "9", "7"}}},
		// Partners for NATURAL JOIN. MySQL pairs gen with g2 on (id, twice)
		// and finds no row; the copy holds no `twice` in gen, pairs on id
		// alone and finds three. MySQL pairs invis with inv2 on id alone
		// (the invisible column is left out) and finds three; the copy pairs
		// on (id, secret) and finds none.
		{name: "g2", footer: true,
			ddl:  "CREATE TABLE `g2` (\n  `id` int NOT NULL,\n  `twice` int DEFAULT NULL,\n  `b` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n",
			rows: [][]string{{"1", "99", "7"}, {"2", "99", "8"}, {"3", "99", "9"}}},
		{name: "inv2", footer: true,
			ddl:  "CREATE TABLE `inv2` (\n  `id` int NOT NULL,\n  `secret` int DEFAULT NULL,\n  `b` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n",
			rows: [][]string{{"1", "6", "7"}, {"2", "6", "8"}, {"3", "6", "9"}}},
		// For the routing half: the copy's rows say "copy", the source's
		// "live", so a statement tells which side answered it.
		{name: "who", footer: true,
			ddl:      "CREATE TABLE `who` (\n  `id` int NOT NULL,\n  `side` varchar(8) DEFAULT NULL,\n  `alpha` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n",
			rows:     [][]string{{"1", "live", "1"}, {"2", "live", "2"}, {"3", "live", "3"}},
			copyRows: [][]string{{"1", "copy", "1"}, {"2", "copy", "2"}, {"3", "copy", "3"}}},
		{name: "who_legacy", footer: false,
			ddl:      "CREATE TABLE `who_legacy` (\n  `id` int NOT NULL,\n  `side` varchar(8) DEFAULT NULL,\n  `alpha` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n",
			rows:     [][]string{{"1", "live", "1"}, {"2", "live", "2"}, {"3", "live", "3"}},
			copyRows: [][]string{{"1", "copy", "1"}, {"2", "copy", "2"}, {"3", "copy", "3"}}},
	}

	srcDB, srcName := testutil.CreateTestDB(t)
	for _, tb := range tables {
		if _, err := srcDB.Exec(tb.ddl); err != nil {
			t.Fatalf("create %s: %v", tb.name, err)
		}
		cols, err := baseline.ParseSchemaText(tb.ddl)
		if err != nil {
			t.Fatal(err)
		}
		names := make([]string, len(cols))
		marks := make([]string, len(cols))
		for i, c := range cols {
			names[i], marks[i] = "`"+c.Name+"`", "?"
		}
		for _, r := range tb.rows {
			args := make([]any, len(r))
			for i, v := range r {
				args[i] = v
			}
			if _, err := srcDB.Exec(fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", tb.name, strings.Join(names, ", "), strings.Join(marks, ", ")), args...); err != nil {
				t.Fatalf("insert into %s: %v", tb.name, err)
			}
		}
		if _, err := srcDB.Exec("ANALYZE TABLE `" + tb.name + "`"); err != nil {
			t.Fatal(err)
		}
	}
	sourceDSN := testutil.IntegrationDSN(srcName)
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, tables)

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: sourceDSN, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg,
		FlashbackListen: "127.0.0.1:3308", ReadRouting: console.ReadRoutingConfig{MaxCopyAge: time.Hour, ScanRows: 2}})
	if err != nil {
		t.Fatal(err)
	}
	serve := func(cfg flashbackConfig) string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		served := make(chan struct{})
		go func() { _ = serveFlashback(ctx, srv, ln, cfg); close(served) }()
		t.Cleanup(func() { cancel(); <-served })
		return ln.Addr().String()
	}
	policy := readrouter.Policy{ScanRows: 2}
	copyDSN := fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, serve(flashbackConfig{}), srcName)

	eq, diff := sqlcompare.Equal, sqlcompare.Different
	fixtures := []valuesFixture{
		// The issue's statement. DIFFERENT (columns) before the fix.
		{"SELECT * FROM orders ORDER BY id", eq, "", "the table's order: id, customer_id, status, amount, channel"},
		{"SELECT * FROM orders", eq, "", "and with no ORDER BY"},
		{"SELECT o.* FROM orders o ORDER BY o.id", eq, "", "t.*"},
		{"SELECT * FROM orders o JOIN customers c ON c.id = o.customer_id ORDER BY o.id", eq, "", "a join: the left table's columns, then the right's"},
		{"SELECT c.*, o.* FROM orders o JOIN customers c ON c.id = o.customer_id ORDER BY o.id", eq, "", "each star in its place"},
		{"SELECT * FROM (SELECT * FROM orders) q ORDER BY id", eq, "", "through a derived table"},
		{"SELECT * FROM altered ORDER BY id", eq, "", "a column added AFTER another, and one at the end"},
		{"SELECT * FROM orders WHERE id > 100", eq, "", "no rows: the columns are still the answer"},
		// What stays different, each with its line in docs/time-travel-sql.md.
		// Under read routing these three are MySQL's (asserted below).
		{"SELECT * FROM legacy ORDER BY id", diff, "columns", "no CREATE TABLE in the snapshot: the file's order, alphabetical"},
		{"SELECT * FROM gen ORDER BY id", diff, "columns", "MySQL also returns the generated column"},
		{"SELECT * FROM invis ORDER BY id", diff, "columns", "MySQL leaves the invisible column out"},
		// The join orders this one, not the tables: MySQL returns the join's
		// column first, the copy leaves it where the left table has it.
		{"SELECT * FROM altered a JOIN altered b USING (name) ORDER BY name", diff, "columns", "a star over USING: MySQL puts the join column first"},
		{"SELECT * FROM orders o JOIN orders p USING (customer_id, id) ORDER BY id", eq, "", "the join columns already lead the left table: the same on both"},
		// NATURAL JOIN with no star: other ROWS, not other columns.
		{"SELECT count(*) FROM gen NATURAL JOIN g2", diff, "", "the copy pairs on id alone: 3 against MySQL's 0"},
		{"SELECT count(*) FROM invis NATURAL JOIN inv2", diff, "", "the copy pairs on (id, secret): 0 against MySQL's 3"},
		{"SELECT count(*) FROM g2 NATURAL JOIN inv2", eq, "", "both tables have MySQL's columns"},
		{"SELECT id, zeta, alpha FROM legacy ORDER BY id", eq, "", "named columns are the same everywhere"},
		{"SELECT id, a FROM gen ORDER BY id", eq, "", "named columns are the same everywhere"},
	}
	statements := make([]string, len(fixtures))
	for i, f := range fixtures {
		statements[i] = f.stmt
	}
	rep, err := sqlcompare.Run(context.Background(), sqlcompare.Options{SourceDSN: sourceDSN, CopyDSN: copyDSN, Policy: policy}, statements)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	by := map[string]sqlcompare.Result{}
	for _, r := range rep.Results {
		by[r.Statement] = r
	}
	src := openRaw(t, sourceDSN)
	cp := openRaw(t, copyDSN)
	for _, f := range fixtures {
		r, ok := by[f.stmt]
		if !ok {
			t.Errorf("no result for %q", f.stmt)
			continue
		}
		srcCols, _ := answerColumns(t, src, f.stmt)
		cpCols, _ := answerColumns(t, cp, f.stmt)
		t.Logf("%-12s %-9s route=%-5s %s\n    mysql: %v\n    copy:  %v\n    %s", r.Verdict, r.Kind, r.Route, f.stmt, srcCols, cpCols, r.Detail)
		if r.Verdict != f.verdict || (f.kind != "" && r.Kind != f.kind) {
			t.Errorf("%q: got %s/%s (%s), want %s/%s: %s", f.stmt, r.Verdict, r.Kind, r.Detail, f.verdict, f.kind, f.why)
		}
	}

	// The same server with read routing on. Every table has three rows and
	// the scan rule is at two, so a full scan is the copy's unless something
	// keeps it on MySQL.
	routed := openFlashback(t, serve(flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: policy}), ent.ID, "tok", srcName)
	defer routed.Close()
	for _, c := range []struct {
		stmt, why string
		cols      []string
		first     string
	}{
		{"SELECT * FROM who ORDER BY alpha", "the copy answers, in the table's order", []string{"id", "side", "alpha"}, "1|copy|1"},
		{"SELECT w.* FROM who w ORDER BY w.alpha", "t.* too", []string{"id", "side", "alpha"}, "1|copy|1"},
		{"SELECT * FROM who_legacy ORDER BY alpha", "the order is not known: MySQL answers", []string{"id", "side", "alpha"}, "1|live|1"},
		{"SELECT l.* FROM who_legacy l ORDER BY l.alpha", "t.* too", []string{"id", "side", "alpha"}, "1|live|1"},
		{"SELECT * FROM who w JOIN who_legacy l ON l.id = w.id ORDER BY w.alpha", "one table of the join is enough", []string{"id", "side", "alpha", "id", "side", "alpha"}, "1|live|1|1|live|1"},
		{"SELECT id, side FROM who_legacy ORDER BY alpha", "without a star the copy still answers for that table", []string{"id", "side"}, "1|copy"},
		{"SELECT side, count(*) FROM who_legacy GROUP BY side", "count(*) is not a star", []string{"side", "count(*)"}, "copy|3"},
		{"SELECT * FROM who_legacy l JOIN who w USING (alpha) ORDER BY alpha", "a star over USING, by the join alone: MySQL answers", []string{"alpha", "id", "side", "id", "side"}, "1|1|live|1|live"},
		{"SELECT * FROM who a JOIN who w USING (alpha) ORDER BY alpha", "a star over USING, both orders known: still MySQL", []string{"alpha", "id", "side", "id", "side"}, "1|1|live|1|live"},
		{"SELECT a.side, w.alpha FROM who a JOIN who w USING (alpha) ORDER BY w.alpha", "USING without a star is the copy's", []string{"side", "alpha"}, "copy|1"},
		// Only the table a star expands counts.
		{"SELECT w.*, l.id FROM who w JOIN who_legacy l ON l.id = w.id ORDER BY w.alpha", "the star is over who, whose order is known: the copy answers", []string{"id", "side", "alpha", "id"}, "1|copy|1|1"},
		{"SELECT l.*, w.id FROM who w JOIN who_legacy l ON l.id = w.id ORDER BY w.alpha", "the star is over who_legacy: MySQL answers", []string{"id", "side", "alpha", "id"}, "1|live|1|1"},
		{"SELECT w.side FROM who w WHERE EXISTS (SELECT * FROM who_legacy l WHERE l.id = w.id) ORDER BY w.alpha", "a star under EXISTS expands nothing that matters: the copy answers", []string{"side"}, "copy"},
		{"SELECT w.* FROM who w JOIN who x USING (alpha) ORDER BY w.alpha", "a qualified star is not reordered by USING: the copy answers", []string{"id", "side", "alpha"}, "1|copy|1"},
		{"SELECT * FROM (SELECT id, a FROM gen) x ORDER BY a", "a star over a derived table with its columns named: the copy answers", []string{"id", "a"}, "1|5"},
		{"SELECT #2\n alpha FROM who ORDER BY alpha", "# is a comment on MySQL and the second column on the copy: MySQL answers", []string{"alpha"}, "1"},
		{"SELECT count(*) AS n FROM gen NATURAL JOIN g2", "NATURAL JOIN over a table with a generated column: MySQL answers", []string{"n"}, "0"},
		{"SELECT count(*) AS n FROM invis NATURAL JOIN inv2", "NATURAL JOIN over a table with an invisible column: MySQL answers", []string{"n"}, "3"},
		{"SELECT count(*) AS n FROM who NATURAL JOIN who_legacy", "NATURAL JOIN over a table with no definition: MySQL answers", []string{"n"}, "3"},
		{"SELECT max(w.side) AS s FROM who w NATURAL JOIN who x", "NATURAL JOIN over tables with MySQL's columns: the copy answers", []string{"s"}, "copy"},
		{"SELECT * FROM gen ORDER BY a", "MySQL returns the generated column: MySQL answers", []string{"id", "twice", "a"}, "1|2|5"},
		{"SELECT * FROM invis ORDER BY a", "MySQL leaves the invisible column out: MySQL answers", []string{"id", "a"}, "1|5"},
	} {
		// Each statement sorts by a column no index serves, so its plan is a
		// full scan (the copy's, by the scan rule) and its first row is fixed.
		cols, first := answerColumns(t, routed, c.stmt)
		if strings.Contains(c.stmt, "count(*)") {
			// The copy names the aggregate its own way; who answered is what
			// is asserted here.
			cols = c.cols
		}
		if !reflect.DeepEqual(cols, c.cols) || first != c.first {
			t.Errorf("routed %q (%s):\n  got  %v %q\n  want %v %q", c.stmt, c.why, cols, first, c.cols, c.first)
		}
	}

	// Who answered, and why MySQL: the copy declining a star or a NATURAL
	// JOIN is counted under its own reason, never with the copy's faults,
	// and the # comment under the vetoes.
	req := httptest.NewRequest("GET", "http://127.0.0.1/api/flashback", nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	var fb struct {
		Routing struct {
			Servers map[string]struct {
				Reasons map[string]uint64 `json:"reasons"`
			} `json:"servers"`
		} `json:"routing"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &fb); err != nil {
		t.Fatalf("decode /api/flashback: %v (%s)", err, rec.Body.String())
	}
	reasons := fb.Routing.Servers[ent.ID].Reasons
	t.Logf("routing reasons: %v", reasons)
	if reasons["copy_columns_differ"] != 10 || reasons["copy_refused"] != 0 || reasons["veto"] != 1 || reasons["expensive_plan"] != 10 {
		t.Errorf("reasons = %v, want copy_columns_differ 10, copy_refused 0, veto 1 (the # comment), expensive_plan 10", reasons)
	}
}

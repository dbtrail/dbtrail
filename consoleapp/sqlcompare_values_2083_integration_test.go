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

// valuesFixture is one statement of the #2083 comparison: what sql-compare
// must say about it, and why.
type valuesFixture struct {
	stmt    string
	verdict sqlcompare.Verdict
	// kind refines DIFFERENT; empty accepts any.
	kind string
	why  string
}

// How the copy PRINTS and COMPARES values, against a real MySQL (#2083). One
// table, the same rows on both sides, so every DIFFERENT is the copy answering
// differently and not different data. A fixture listed DIFFERENT is a
// documented remaining difference (docs/time-travel-sql.md); the day one
// becomes EQUAL this test fails and the documentation line goes with it.
func TestIntegrationSQLCompareValues(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)

	srcDB, srcName := testutil.CreateTestDB(t)
	ddl := "CREATE TABLE `sales` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `amount` decimal(12,2) DEFAULT NULL,\n" +
		"  `rate` decimal(38,10) DEFAULT NULL,\n" +
		"  `units` decimal(10,0) DEFAULT NULL,\n" +
		"  `qty` int DEFAULT NULL,\n" +
		"  `code` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n" +
		"  `tag` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs DEFAULT NULL,\n" +
		"  `name` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,\n" +
		"  `note` varchar(32) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
	rows := [][]string{
		{"1", "10.00", "1.5000000000", "4", "3", "AB", "x", "bob", "straße"},
		{"2", "2.50", "2.2500000000", "5", "4", "ab", "X", "bob ", "Ａ"},
		{"3", "-7.10", "0.0000000000", "0", "0", "Ab", "x", "BOB", "strasse"},
		{"4", "117329550.00", "12345678901234567890.1234567890", "7", "9", "zz", "y", "AB", "a"},
		{"5", "", "", "", "", "", "", "", ""},
	}
	nullRow := 4
	if _, err := srcDB.Exec(ddl); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i, r := range rows {
		args := make([]any, len(r))
		for j, v := range r {
			if i == nullRow && j > 0 {
				args[j] = nil
			} else {
				args[j] = v
			}
		}
		if _, err := srcDB.Exec("INSERT INTO sales VALUES (?,?,?,?,?,?,?,?,?)", args...); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if _, err := srcDB.Exec("ANALYZE TABLE sales"); err != nil {
		t.Fatal(err)
	}
	sourceDSN := testutil.IntegrationDSN(srcName)

	baseDir := t.TempDir()
	writeValuesBaseline(t, baseDir, srcName, "sales", ddl, rows, nullRow)

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
	defer func() { cancel(); <-served }()
	copyDSN := fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, ln.Addr(), srcName)

	fixtures := valuesFixtures()
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
	for _, f := range fixtures {
		r, ok := by[f.stmt]
		if !ok {
			t.Errorf("no result for %q", f.stmt)
			continue
		}
		t.Logf("%-12s %-9s route=%-5s %s\n    mysql: %s\n    copy:  %s\n    %s",
			r.Verdict, r.Kind, r.Route, f.stmt, rawAnswer(src, f.stmt), rawAnswer(cp, f.stmt), r.Detail)
		if r.Verdict != f.verdict || (f.kind != "" && r.Kind != f.kind) {
			t.Errorf("%q: got %s/%s (%s), want %s/%s: %s", f.stmt, r.Verdict, r.Kind, r.Detail, f.verdict, f.kind, f.why)
		}
	}
}

// valuesFixtures is the #2083 statement set.
func valuesFixtures() []valuesFixture {
	eq, diff := sqlcompare.Equal, sqlcompare.Different
	return []valuesFixture{
		// Decimal text: a DECIMAL prints with its scale, trailing zeros kept.
		{"SELECT ROUND(SUM(amount), 2) FROM sales", eq, "", "117329555.40"},
		{"SELECT SUM(amount) FROM sales", eq, "", "a DECIMAL sum keeps its scale"},
		{"SELECT id, amount FROM sales ORDER BY id", eq, "", "a DECIMAL column keeps its scale (10.00)"},
		{"SELECT id, rate FROM sales ORDER BY id", eq, "", "scale 10, 30 significant digits"},
		{"SELECT id, units FROM sales ORDER BY id", eq, "", "scale 0 prints no point"},
		{"SELECT id, ROUND(amount, 1) FROM sales ORDER BY id", eq, "", "10.0"},
		{"SELECT id, amount * qty FROM sales ORDER BY id", eq, "", "DECIMAL times INT keeps scale 2"},
		{"SELECT id, amount * amount FROM sales WHERE id < 4 ORDER BY id", eq, "", "scales add: 4"},
		{"SELECT id, amount * amount FROM sales ORDER BY id", sqlcompare.NotOnCopy, "", "the product passes the copy's DECIMAL(18): refused, so MySQL answers"},
		{"SELECT id, amount + rate FROM sales ORDER BY id", eq, "", "the wider scale: 10"},
		{"SELECT id, -amount, ABS(amount) FROM sales ORDER BY id", eq, "", "sign and ABS keep the scale"},
		{"SELECT id, COALESCE(amount, 0) FROM sales ORDER BY id", eq, "", "0.00 for the NULL row"},
		// MySQL declares the column DECIMAL with 2 decimals and still prints
		// the integer branch as "4"; the copy prints "4.00", what the
		// declared type says. Documented, not chased.
		{"SELECT id, CASE WHEN id = 1 THEN amount ELSE qty END FROM sales ORDER BY id", diff, "precision", "MySQL prints the integer branch of a CASE without decimals"},
		{"SELECT amount x FROM sales WHERE id = 1 UNION ALL SELECT qty FROM sales WHERE id = 2", eq, "", "UNION widens to scale 2"},
		{"SELECT SUM(amount) FROM sales WHERE id > 100", eq, "", "no rows: NULL on both"},
		{"SELECT id, SUM(amount) FROM sales WHERE id > 100 GROUP BY id", eq, "", "GROUP BY with zero rows"},
		{"SELECT MAX(amount), MIN(amount) FROM sales", eq, "", "MIN and MAX keep the scale"},
		{"SELECT SUM(qty), COUNT(*) FROM sales", eq, "", "an integer sum prints no point"},
		{"SELECT CAST(qty AS DECIMAL(10,3)) FROM sales WHERE id = 1", eq, "", "3.000"},

		// AVG and division: a DOUBLE on the copy, a DECIMAL with the operand's
		// scale plus four on MySQL. Documented; nothing is rewritten for the
		// copy, and no result type carries the operand's scale.
		{"SELECT AVG(amount) FROM sales", diff, "precision", "MySQL: DECIMAL, operand scale + 4; the copy: DOUBLE"},
		{"SELECT ROUND(AVG(amount), 1) FROM sales", diff, "precision", "MySQL keeps one decimal"},
		{"SELECT AVG(qty) FROM sales", diff, "precision", "MySQL: four decimals"},
		{"SELECT id, amount / 3 FROM sales ORDER BY id", diff, "precision", "MySQL: six decimals"},
		{"SELECT id, qty / 3 FROM sales ORDER BY id", diff, "precision", "MySQL: four decimals"},
		{"SELECT id, amount / qty FROM sales ORDER BY id", diff, "precision", "MySQL: six decimals; the zero divisor is NULL on both"},
		// Division and modulo by zero are NULL on both sides.
		{"SELECT id FROM sales WHERE amount / qty IS NULL ORDER BY id", eq, "", "the zero divisor and the NULL row"},
		{"SELECT COUNT(amount / qty), COUNT(qty / 0), COUNT(amount % 0) FROM sales", eq, "", "3, 0, 0"},
		{"SELECT id, qty % 0, qty / 0 FROM sales ORDER BY id", eq, "", "NULL in every row"},

		// A _bin column compares byte by byte, as MySQL declares it: the view
		// gives it COLLATE C, read from the CREATE TABLE in the file's footer.
		{"SELECT id FROM sales WHERE code = 'ab'", eq, "", "utf8mb4_bin: only the exact 'ab'"},
		{"SELECT id FROM sales WHERE code IN ('ab', 'AB') ORDER BY id", eq, "", "two of the three spellings"},
		{"SELECT id FROM sales WHERE code > 'a' ORDER BY id", eq, "", "code point order: upper case sorts before 'a'"},
		{"SELECT code, COUNT(*) FROM sales GROUP BY code", eq, "", "AB, ab and Ab are three groups"},
		{"SELECT DISTINCT code FROM sales", eq, "", "and three distinct values"},
		{"SELECT id, code FROM sales ORDER BY code", eq, "", "NULL, AB, Ab, ab, zz"},
		{"SELECT id, code FROM sales ORDER BY code DESC", eq, "", "and the reverse"},
		{"SELECT MIN(code), MAX(code) FROM sales", eq, "", "AB and zz"},
		{"SELECT a.id, b.id FROM sales a JOIN sales b ON a.code = b.name", eq, "", "_bin against _ci: bytes win on both"},
		// What stays different, each with its line in docs/time-travel-sql.md.
		{"SELECT id FROM sales WHERE code = 'ab '", diff, "rows", "utf8mb4_bin is PAD SPACE: MySQL ignores the trailing space"},
		{"SELECT id FROM sales WHERE tag = 'x'", diff, "rows", "utf8mb4_0900_as_cs: 'X' is not 'x'; the copy folds it"},
		{"SELECT id FROM sales WHERE name = 'bob'", diff, "rows", "utf8mb4_general_ci is PAD SPACE: 'bob ' matches on MySQL"},
		{"SELECT id FROM sales WHERE note = 'strasse'", diff, "rows", "utf8mb4_0900_ai_ci: ß equals ss"},
		{"SELECT id FROM sales WHERE note = 'A'", diff, "rows", "utf8mb4_0900_ai_ci: full-width A equals A"},
	}
}

func writeValuesBaseline(t *testing.T, dir, schema, table, ddl string, rows [][]string, nullRow int) {
	t.Helper()
	snap := time.Now().UTC().Add(-time.Minute).Format("2006-01-02T15-04-05Z")
	path := filepath.Join(dir, snap, schema, table+".parquet")
	schemaFile := filepath.Join(t.TempDir(), schema+"."+table+"-schema.sql")
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
	for i, r := range rows {
		nulls := make([]bool, len(r))
		if i == nullRow {
			for j := 1; j < len(r); j++ {
				nulls[j] = true
			}
		}
		if err := w.WriteRow(r, nulls); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func openRaw(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// rawAnswer is one side's answer as the wire carried it: each column's type
// (and decimals, when it reports them) and every row's text.
func rawAnswer(db *sql.DB, stmt string) string {
	rows, err := db.Query(stmt)
	if err != nil {
		return "ERROR " + err.Error()
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return "ERROR " + err.Error()
	}
	var b strings.Builder
	b.WriteString("[")
	for i, ct := range types {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(ct.DatabaseTypeName())
		if _, scale, ok := ct.DecimalSize(); ok {
			fmt.Fprintf(&b, "/%d", scale)
		}
	}
	b.WriteString("]")
	for rows.Next() {
		cells := make([]sql.RawBytes, len(types))
		ptrs := make([]any, len(types))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "ERROR " + err.Error()
		}
		b.WriteString(" (")
		for i, c := range cells {
			if i > 0 {
				b.WriteString(", ")
			}
			if c == nil {
				b.WriteString("NULL")
			} else {
				b.Write(c)
			}
		}
		b.WriteString(")")
	}
	if err := rows.Err(); err != nil {
		return b.String() + " ERROR " + err.Error()
	}
	return b.String()
}

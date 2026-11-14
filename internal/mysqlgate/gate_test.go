//go:build unix

package mysqlgate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

func TestMain(m *testing.M) {
	if sqlsandbox.IsWorkerProcess() {
		os.Exit(sqlsandbox.WorkerMain(os.Stdin, os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

const goldenFile = "testdata/answers.golden.txt"

// q is one statement asked of the copy. one says it returns a single row
// whatever the tables hold, so it needs no ORDER BY.
type q struct {
	sql string
	one bool
}

func s(sql string) q   { return q{sql: sql} }
func one(sql string) q { return q{sql: sql, one: true} }

// statements is what is asked of the copy. Every one that can return more
// than one row names a total order, so the answer does not depend on how the
// engine happens to read the file; every computed column has a name of its
// own, so the engine's spelling of an expression is not part of the answer.
// One per thing the views or the session decide for a MySQL source. Add to
// it; the golden file follows this order.
var statements = []q{
	// The columns: their order, their names and the type each reads as.
	s("SELECT * FROM items ORDER BY id"),
	s("SELECT * FROM orders ORDER BY id"),
	s(`SELECT * FROM odd ORDER BY "Key"`),
	s("SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'shop' ORDER BY table_name, ordinal_position"),
	// Text: a column under MySQL's default collation compares without
	// regard to case, a _bin or _cs column byte by byte.
	s("SELECT id FROM items WHERE name = 'ALICE' ORDER BY id"),
	s("SELECT id FROM items WHERE name = 'alice' ORDER BY id"),
	s("SELECT id FROM items WHERE code = 'AB' ORDER BY id"),
	s("SELECT id FROM items WHERE code = 'ab' ORDER BY id"),
	s("SELECT id FROM items WHERE name < 'b' ORDER BY id"),
	s("SELECT id FROM items WHERE name IN ('bob', 'ÉMILE') ORDER BY id"),
	s("SELECT id, name FROM items ORDER BY name, id"),
	s("SELECT id, name FROM items ORDER BY name DESC, id"),
	s("SELECT id, code FROM items ORDER BY code, id"),
	one("SELECT count(DISTINCT name) AS names, count(DISTINCT code) AS codes FROM items"),
	s("SELECT lower(name) AS n, count(*) AS c FROM items GROUP BY ALL ORDER BY n"),
	one("SELECT min(lower(name)) AS lo, max(lower(name)) AS hi, min(code) AS lo_code, max(code) AS hi_code FROM items"),
	s("SELECT id, note LIKE 'HEL%' AS likes, note ILIKE 'HEL%' AS ilikes, note = 'HELLO' AS equal FROM items ORDER BY id"),
	s("SELECT id FROM orders WHERE status = 'paid' ORDER BY id"),
	s("SELECT lower(status) AS s, count(*) AS c FROM orders GROUP BY ALL ORDER BY s"),
	one("SELECT 'a' = 'A' AS eq, 'a' < 'B' AS lt, 'a' LIKE 'A' AS likes"),
	s(`SELECT "Key" FROM odd WHERE "my col" = 'A' ORDER BY "Key"`),
	s(`SELECT "Key" FROM odd WHERE cs = 'AB' ORDER BY "Key"`),
	s(`SELECT "Key" FROM odd WHERE cs = 'Ab' ORDER BY "Key"`),
	// NULL: where it sorts, and what a comparison with it gives.
	s("SELECT id, price FROM items ORDER BY price, id"),
	s("SELECT id, price FROM items ORDER BY price DESC, id"),
	s("SELECT id FROM items WHERE name IS NULL ORDER BY id"),
	one("SELECT count(*) AS n, count(name) AS names, count(price) AS prices FROM items"),
	// Numbers: decimals keep their scale, unsigned keeps its range, two
	// integers divide to a fraction, and a division by zero is NULL.
	one("SELECT sum(price) AS total, avg(price) AS mean, min(price) AS lo, max(price) AS hi FROM items"),
	s("SELECT id, price * 2 AS twice, price / 3 AS third, price + qty AS plus FROM items ORDER BY id"),
	s("SELECT id, qty, big, small FROM items ORDER BY id"),
	s("SELECT id, big + 0 AS big0, qty + 1 AS qty1 FROM items ORDER BY id"),
	one("SELECT max(big) AS big, max(qty) AS qty, min(small) AS small FROM items"),
	one("SELECT 9 / 2 AS a, 7 / 2 AS b, -7 / 2 AS c, 7 // 2 AS d, 7 % 2 AS e"),
	s("SELECT id, qty / 2 AS half FROM items ORDER BY id"),
	s("SELECT id, price / qty AS per, qty % 0 AS modzero, ratio / 0 AS fzero, 1 / 0 AS izero FROM items ORDER BY id"),
	s("SELECT id, flag, flag = 1 AS is_one, flag + 1 AS next FROM items ORDER BY id"),
	s("SELECT id, ratio, ratio * 2 AS twice FROM items ORDER BY id"),
	one("SELECT sum(amount) AS total, avg(amount) AS mean FROM orders"),
	s("SELECT id, amount, amount * 3 AS triple FROM orders ORDER BY id"),
	s(`SELECT "Key", f, f * 2 AS twice, typeof(f) AS t, tu, mi, bi FROM odd ORDER BY "Key"`),
	s(`SELECT "Key", wide, typeof(wide) AS t FROM odd ORDER BY "Key"`),
	// Dates and times.
	s("SELECT id, born, seen, stamp, tod, yr FROM items ORDER BY id"),
	s("SELECT id, born + INTERVAL 1 MONTH AS next_month, date_diff('day', born, DATE '2026-03-01') AS days FROM items ORDER BY id"),
	s("SELECT id, year(seen) AS y, month(stamp) AS m, CAST(seen AS DATE) AS d FROM items ORDER BY id"),
	s("SELECT id FROM items WHERE seen >= '2026-01-01' ORDER BY id"),
	s("SELECT id FROM items WHERE born BETWEEN '2024-01-01' AND '2026-06-30' ORDER BY id"),
	one("SELECT min(seen) AS first_seen, max(stamp) AS last_stamp, min(born) AS first_born FROM items"),
	s(`SELECT "Key", zd, zd IS NULL AS zero FROM odd ORDER BY "Key"`),
	// Enumerations, sets, JSON and binary.
	s("SELECT id, kind, tags FROM items ORDER BY id"),
	s("SELECT id FROM items WHERE kind = 'A' ORDER BY id"),
	s("SELECT kind, count(*) AS n FROM items GROUP BY kind ORDER BY kind"),
	s("SELECT id, doc, json_extract_string(doc, '$.k') AS k FROM items ORDER BY id"),
	s("SELECT id, raw, hex(raw) AS h FROM items ORDER BY id"),
	s("SELECT id FROM items WHERE raw = 'AB' ORDER BY id"),
	s(`SELECT "Key", bits, bin, hex(bin) AS h FROM odd ORDER BY "Key"`),
	// Across tables.
	s("SELECT i.id, i.name, sum(o.amount) AS total, count(o.id) AS n FROM items i LEFT JOIN orders o ON o.item_id = i.id GROUP BY i.id, i.name ORDER BY i.id"),
	s("SELECT o.id FROM orders o JOIN items i ON i.id = o.item_id WHERE i.name = 'ALICE' ORDER BY o.id"),
	s("SELECT lower(status) AS v FROM orders UNION SELECT lower(name) FROM items ORDER BY 1"),
	// The clock calls and MySQL's time functions: MySQL's on the MySQL
	// port, the copy's own on the SQL card. Their types and what they
	// compare with, never their value, and never against the fixture's
	// rows: an answer here must be the same on any day the test runs.
	one("SELECT typeof(NOW()) AS t"),
	one("SELECT typeof(CURDATE()) AS t"),
	one("SELECT typeof(UTC_TIMESTAMP()) AS ts, typeof(UTC_DATE()) AS d"),
	one("SELECT NOW() >= TIMESTAMP '2026-01-01 00:00:00' AS later"),
	s("SELECT id, UNIX_TIMESTAMP(stamp) AS u FROM items ORDER BY id"),
	one("SELECT FROM_UNIXTIME(86400) AS t"),
	// Names as a statement may write them.
	one("SELECT ID, Name FROM Items WHERE id = 1"),
	one("SELECT id FROM shop.items WHERE id = 1"),
	s(`SELECT "Key", "my col", "select" FROM odd ORDER BY "Key"`),
}

// sessions are the ones a MySQL copy is read under: the SQL card's (the
// copy's own dialect) and the MySQL port's (MySQL's clock calls), over the
// whole list; then two a connection of the port can set for itself, each
// over the statements it decides.
var sessions = []struct {
	name string
	sess sqlsandbox.Session
	only []string // nil for the whole list
}{
	{"sql card", sqlsandbox.Session{}, nil},
	{"mysql port", sqlsandbox.Session{MySQLClock: true}, nil},
	{"select limit 2", sqlsandbox.Session{MySQLClock: true, SelectLimit: 2}, []string{
		"SELECT * FROM items ORDER BY id",
		"SELECT id FROM items ORDER BY id LIMIT 4",
		"SELECT count(*) AS n FROM items",
	}},
	{"keep join order", sqlsandbox.Session{MySQLClock: true, KeepJoinOrder: true}, []string{
		"SELECT o.id FROM orders o JOIN items i ON i.id = o.item_id WHERE i.name = 'ALICE' ORDER BY o.id",
	}},
}

// asked is the statements a session runs, in order.
func asked(only []string) []string {
	if only != nil {
		return only
	}
	out := make([]string, len(statements))
	for i, st := range statements {
		out[i] = st.sql
	}
	return out
}

// buildCopy converts the fixed dump the way the product does and returns the
// snapshot directory and the views script over it.
func buildCopy(ctx context.Context, t *testing.T) (snapshot, script string) {
	t.Helper()
	out := t.TempDir()
	if _, err := baseline.Run(ctx, baseline.Config{
		InputDir: "testdata/dump", OutputDir: out, Compression: "none", RowGroupSize: 100,
	}); err != nil {
		t.Fatalf("convert the dump: %v", err)
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if _, ok := snapshotdir.ParseTime(e.Name()); ok && e.IsDir() {
			snapshot = filepath.Join(out, e.Name())
		}
	}
	if snapshot == "" {
		t.Fatalf("no snapshot directory in %s: %v", out, entries)
	}
	script, ok, err := views.GenerateSnapshotViews(ctx, snapshot, filepath.ToSlash(snapshot))
	if err != nil || !ok {
		t.Fatalf("views for %s: ok=%v err=%v", snapshot, ok, err)
	}
	return snapshot, script
}

// newRunner is the real SQL worker (this test binary, see TestMain), with
// workers kept between statements as the port runs them: the same path, and
// the list does not pay a process start per statement.
func newRunner(t *testing.T) *sqlsandbox.Runner {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Standby: true, WorkerStatements: sqlsandbox.DefaultWorkerStatements})
	t.Cleanup(runner.Close)
	return runner
}

// answer runs one statement and writes its answer as text: the columns with
// their types, then one JSON line per row. An error is an answer too, and is
// written as one.
func answer(ctx context.Context, t *testing.T, runner *sqlsandbox.Runner, snapshot, script, stmt string, sess sqlsandbox.Session) string {
	t.Helper()
	var b strings.Builder
	res, err := runner.Run(ctx, sqlsandbox.Job{
		CopyDirs: []string{snapshot},
		ViewsSQL: script,
		SQL:      stmt,
		Schema:   "shop",
		Session:  sess,
		Limits:   sqlsandbox.Limits{MaxRows: 1000},
	})
	if err != nil {
		fmt.Fprintf(&b, "error: %s\n\n", errorText(t, err, snapshot))
		return b.String()
	}
	if res.Spool != nil {
		_ = res.Spool.Close()
		t.Fatalf("%s: the answer came in a file nobody asked for", stmt)
	}
	cols := make([]string, len(res.Columns))
	for i, c := range res.Columns {
		cols[i] = c.Name + " " + c.Type
	}
	fmt.Fprintf(&b, "columns: %s\n", strings.Join(cols, " | "))
	if res.Truncated || res.TruncatedCells > 0 {
		fmt.Fprintf(&b, "cut: rows=%v cells=%d\n", res.Truncated, res.TruncatedCells)
	}
	for _, row := range res.Rows {
		line, err := json.Marshal(row)
		if err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return b.String()
}

// answers is the whole text the golden file holds: what the writer recorded
// about each table's columns (which the views and the port decide by), then
// every session's statements, one block each.
func answers(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	snapshot, script := buildCopy(ctx, t)
	runner := newRunner(t)

	var b strings.Builder
	var paths []string
	_ = filepath.WalkDir(snapshot, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			paths = append(paths, path)
		}
		return nil
	})
	sort.Strings(paths)
	footers, err := baseline.TableFootersFor(ctx, paths)
	if err != nil {
		t.Fatalf("read the tables' footers: %v", err)
	}
	for _, path := range paths {
		f, ok := footers[path]
		if !ok {
			t.Fatalf("no footer was read for %s", path)
		}
		line, err := json.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		rel, _ := filepath.Rel(snapshot, path)
		fmt.Fprintf(&b, "== footer == %s\n%s\n\n", filepath.ToSlash(rel), line)
	}
	for _, s := range sessions {
		for _, stmt := range asked(s.only) {
			fmt.Fprintf(&b, "== %s == %s\n", s.name, stmt)
			b.WriteString(answer(ctx, t, runner, snapshot, script, stmt, s.sess))
		}
	}
	return b.String()
}

// errorText is an error as the golden file keeps it. A statement's own error
// (the engine's, or the sandbox's refusal) is its answer and is kept, with
// the temporary directory taken out of it. Anything else (a worker that
// died, a timeout) is about this run, not about the answer, and fails the
// test instead of being written down as one.
func errorText(t *testing.T, err error, snapshot string) string {
	t.Helper()
	var refused *sqlsandbox.RefusedError
	var query *sqlsandbox.QueryError
	var msg string
	switch {
	case errors.As(err, &refused):
		msg = "refused: " + refused.Reason
	case errors.As(err, &query):
		msg = query.Message
	default:
		t.Fatalf("a statement failed for a reason that is not its answer: %v", err)
	}
	// The first line alone: what follows is the engine's hint and its
	// drawing of the statement, which change with the engine and say
	// nothing about the answer.
	msg, _, _ = strings.Cut(msg, "\n")
	msg, _, _ = strings.Cut(msg, " Did you mean")
	msg = strings.ReplaceAll(msg, snapshot, "<snapshot>")
	return strings.Join(strings.Fields(msg), " ")
}

// TestMySQLAnswersAreTheReviewedOnes is the gate. No t.Skip on any path: a
// run that could not build the copy fails.
func TestMySQLAnswersAreTheReviewedOnes(t *testing.T) {
	got := answers(t)
	if path := os.Getenv("BINTRAIL_GATE_WRITE"); path != "" {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %d bytes of answers to %s; copy it over %s once the differences are the ones meant", len(got), path, goldenFile)
	}
	want, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatalf("the reviewed answers cannot be read: %v", err)
	}
	if got == string(want) {
		return
	}
	// Say which statements moved, with both answers: the whole file would
	// bury them.
	gotBlocks, wantBlocks := blocks(got), blocks(string(want))
	moved := 0
	for _, key := range blockOrder(got) {
		if gotBlocks[key] == wantBlocks[key] {
			continue
		}
		moved++
		if _, known := wantBlocks[key]; !known {
			t.Errorf("a statement with no reviewed answer yet:\n%s\n%s", key, gotBlocks[key])
			continue
		}
		t.Errorf("a MySQL answer changed:\n%s\n--- reviewed\n%s--- now\n%s", key, wantBlocks[key], gotBlocks[key])
	}
	for _, key := range blockOrder(string(want)) {
		if _, still := gotBlocks[key]; !still {
			moved++
			t.Errorf("a reviewed statement is no longer asked:\n%s", key)
		}
	}
	if moved == 0 {
		t.Errorf("the answers differ from %s only in their order or spacing; the file follows the order of the statements list", goldenFile)
	}
	t.Logf("if every change above is meant, write the new answers with BINTRAIL_GATE_WRITE=<file> and copy that file over %s in the same pull request", goldenFile)
}

// The list itself: a statement written twice would have one block shadow the
// other; one that can return several rows and names no order could answer in
// any; and a list cut short, with the golden file cut to match, would pass
// with nothing checked.
func TestTheListIsWhole(t *testing.T) {
	const fewest = 68
	if len(statements) < fewest {
		t.Fatalf("%d statements, and there were %d: the gate only grows", len(statements), fewest)
	}
	seen := map[string]bool{}
	for _, st := range statements {
		if seen[st.sql] {
			t.Errorf("listed twice: %s", st.sql)
		}
		seen[st.sql] = true
		if !st.one && !strings.Contains(strings.ToUpper(st.sql), "ORDER BY") {
			t.Errorf("it can return more than one row and has no ORDER BY: %s", st.sql)
		}
	}
	want, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}
	reviewed := blocks(string(want))
	for _, s := range sessions {
		for _, stmt := range asked(s.only) {
			key := fmt.Sprintf("== %s == %s", s.name, stmt)
			if strings.TrimSpace(reviewed[key]) == "" {
				t.Errorf("no reviewed answer for %s", key)
			}
		}
	}
}

// The gate sees what it is for: under PostgreSQL's session rules these
// answers are not the reviewed ones. If this fails, the statements no longer
// tell the two sessions apart, and a change that gave a MySQL source
// PostgreSQL's rules would pass.
func TestTheGateTellsPostgresRulesApart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	snapshot, script := buildCopy(ctx, t)
	runner := newRunner(t)
	want, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}
	reviewed := blocks(string(want))
	for _, stmt := range []string{
		"SELECT id FROM items WHERE name = 'ALICE' ORDER BY id",          // text compared without regard to case
		"SELECT id, price FROM items ORDER BY price, id",                 // NULL first
		"SELECT 9 / 2 AS a, 7 / 2 AS b, -7 / 2 AS c, 7 // 2 AS d, 7 % 2 AS e", // integers divide to a fraction
	} {
		key := "== sql card == " + stmt
		if reviewed[key] == "" {
			t.Fatalf("no reviewed answer for %s", key)
		}
		got := answer(ctx, t, runner, snapshot, script, stmt, sqlsandbox.Session{PostgresRules: true})
		if got == reviewed[key] {
			t.Errorf("under PostgreSQL's rules the answer is the reviewed MySQL one, so the gate would not see them applied:\n%s\n%s", stmt, got)
		}
	}
}

// blocks splits an answers text into its blocks, by header line.
func blocks(text string) map[string]string {
	out := map[string]string{}
	key := ""
	for _, line := range strings.SplitAfter(text, "\n") {
		if strings.HasPrefix(line, "== ") {
			key = strings.TrimRight(line, "\n")
			continue
		}
		if key != "" {
			out[key] += line
		}
	}
	return out
}

func blockOrder(text string) []string {
	var keys []string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "== ") {
			keys = append(keys, line)
		}
	}
	return keys
}

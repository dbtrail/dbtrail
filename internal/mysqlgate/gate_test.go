//go:build unix

package mysqlgate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

// sessions are the two a MySQL copy is read under: the SQL card's (the
// copy's own dialect) and the MySQL port's (MySQL's clock calls).
var sessions = []struct {
	name string
	sess sqlsandbox.Session
}{
	{"sql card", sqlsandbox.Session{}},
	{"mysql port", sqlsandbox.Session{MySQLClock: true}},
}

// statements is what is asked of the copy. Every one names its order, so the
// answer does not depend on how the engine happens to read the file. One per
// thing the views or the session decide for a MySQL source; add to it, never
// reorder it (the golden file follows this order).
var statements = []string{
	// The columns: their order, their names and the type each reads as.
	"SELECT * FROM items ORDER BY id",
	"SELECT * FROM orders ORDER BY id",
	"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'shop' AND table_name = 'items' ORDER BY ordinal_position",
	"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'shop' AND table_name = 'orders' ORDER BY ordinal_position",
	"SELECT typeof(id), typeof(name), typeof(code), typeof(price), typeof(qty), typeof(big), typeof(flag), typeof(small), typeof(ratio) FROM items WHERE id = 1",
	"SELECT typeof(born), typeof(seen), typeof(stamp), typeof(tod), typeof(yr), typeof(kind), typeof(tags), typeof(doc), typeof(raw), typeof(note) FROM items WHERE id = 1",
	// Text: a column under MySQL's default collation compares without
	// regard to case, a _bin column byte by byte.
	"SELECT id FROM items WHERE name = 'ALICE' ORDER BY id",
	"SELECT id FROM items WHERE name = 'alice' ORDER BY id",
	"SELECT id FROM items WHERE code = 'AB' ORDER BY id",
	"SELECT id FROM items WHERE code = 'ab' ORDER BY id",
	"SELECT id FROM items WHERE name < 'b' ORDER BY id",
	"SELECT id FROM items WHERE name IN ('bob', 'ÉMILE') ORDER BY id",
	"SELECT id, name FROM items ORDER BY name, id",
	"SELECT id, name FROM items ORDER BY name DESC, id",
	"SELECT id, code FROM items ORDER BY code, id",
	"SELECT count(DISTINCT name), count(DISTINCT code) FROM items",
	"SELECT lower(name) AS n, count(*) AS c FROM items GROUP BY ALL ORDER BY n",
	"SELECT min(name), max(name), min(code), max(code) FROM items",
	"SELECT id, note LIKE 'HEL%', note ILIKE 'HEL%', note = 'HELLO' FROM items ORDER BY id",
	"SELECT id FROM orders WHERE status = 'paid' ORDER BY id",
	"SELECT lower(status) AS s, count(*) AS c FROM orders GROUP BY ALL ORDER BY s",
	"SELECT 'a' = 'A', 'a' < 'B', 'a' LIKE 'A'",
	// NULL: where it sorts, and what a comparison with it gives.
	"SELECT id, price FROM items ORDER BY price, id",
	"SELECT id, price FROM items ORDER BY price DESC, id",
	"SELECT id FROM items WHERE name IS NULL",
	"SELECT count(*), count(name), count(price) FROM items",
	// Numbers: decimals keep their scale, unsigned keeps its range, two
	// integers divide to a fraction.
	"SELECT sum(price), avg(price), min(price), max(price) FROM items",
	"SELECT id, price * 2, price / 3, price + qty FROM items ORDER BY id",
	"SELECT id, qty, big, small FROM items ORDER BY id",
	"SELECT id, big + 0, qty + 1 FROM items ORDER BY id",
	"SELECT max(big), max(qty), min(small) FROM items",
	"SELECT 9 / 2, 7 / 2, -7 / 2, 7 // 2, 7 % 2",
	"SELECT id, qty / 2 FROM items ORDER BY id",
	"SELECT id, flag, flag = 1, flag + 1 FROM items ORDER BY id",
	"SELECT id, ratio, ratio * 2 FROM items ORDER BY id",
	"SELECT sum(amount), avg(amount) FROM orders",
	"SELECT id, amount, amount * 3 FROM orders ORDER BY id",
	// Dates and times.
	"SELECT id, born, seen, stamp, tod, yr FROM items ORDER BY id",
	"SELECT id, born + INTERVAL 1 MONTH, date_diff('day', born, DATE '2026-03-01') FROM items ORDER BY id",
	"SELECT id, year(seen), month(stamp), CAST(seen AS DATE) FROM items ORDER BY id",
	"SELECT id FROM items WHERE seen >= '2026-01-01' ORDER BY id",
	"SELECT id FROM items WHERE born BETWEEN '2024-01-01' AND '2026-06-30' ORDER BY id",
	"SELECT min(seen), max(stamp), min(born) FROM items",
	// Enumerations, sets, JSON and binary.
	"SELECT id, kind, tags FROM items ORDER BY id",
	"SELECT id FROM items WHERE kind = 'A' ORDER BY id",
	"SELECT kind, count(*) FROM items GROUP BY kind ORDER BY kind",
	"SELECT id, doc FROM items ORDER BY id",
	"SELECT id, raw, hex(raw) FROM items ORDER BY id",
	"SELECT id FROM items WHERE raw = 'AB' ORDER BY id",
	// Across tables.
	"SELECT i.id, i.name, sum(o.amount) AS total, count(o.id) AS n FROM items i LEFT JOIN orders o ON o.item_id = i.id GROUP BY i.id, i.name ORDER BY i.id",
	"SELECT o.id FROM orders o JOIN items i ON i.id = o.item_id WHERE i.name = 'ALICE' ORDER BY o.id",
	"SELECT status FROM orders UNION SELECT name FROM items ORDER BY 1",
	// The clock calls: MySQL's on the MySQL port, the copy's own on the SQL
	// card. Their types and what they compare with, never their value.
	"SELECT typeof(NOW()), typeof(CURDATE())",
	"SELECT NOW() >= TIMESTAMP '2026-01-01 00:00:00', CURDATE() >= DATE '2026-01-01', count(*) FROM items WHERE seen < NOW()",
	// Names as a statement may write them.
	"SELECT ID, Name FROM Items WHERE id = 1",
	"SELECT id FROM shop.items WHERE id = 1",
}

// answers runs every statement under every session and writes the answers
// as text: one block per statement, the columns with their types, then one
// JSON line per row. An error is an answer too, and is written as one.
func answers(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	out := t.TempDir()
	if _, err := baseline.Run(ctx, baseline.Config{
		InputDir: "testdata/dump", OutputDir: out, Compression: "none", RowGroupSize: 100,
	}); err != nil {
		t.Fatalf("convert the dump: %v", err)
	}
	snapshot := ""
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
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}})
	t.Cleanup(runner.Close)

	var b strings.Builder
	for _, s := range sessions {
		for _, stmt := range statements {
			fmt.Fprintf(&b, "== %s == %s\n", s.name, stmt)
			res, err := runner.Run(ctx, sqlsandbox.Job{
				CopyDirs: []string{snapshot},
				ViewsSQL: script,
				SQL:      stmt,
				Schema:   "shop",
				Session:  s.sess,
				Limits:   sqlsandbox.Limits{MaxRows: 1000},
			})
			if err != nil {
				fmt.Fprintf(&b, "error: %s\n\n", errorText(t, err, snapshot))
				continue
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
// other, and one without an ORDER BY could answer in any order.
func TestStatementsAreDistinctAndOrdered(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range statements {
		if seen[s] {
			t.Errorf("listed twice: %s", s)
		}
		seen[s] = true
		up := strings.ToUpper(s)
		oneRow := strings.Contains(up, "WHERE ID = 1") || !strings.Contains(up, " FROM ") ||
			(strings.Contains(up, "(") && !strings.Contains(up, "GROUP BY") && !strings.Contains(up, "ORDER BY") && aggregateOnly(up))
		if !strings.Contains(up, "ORDER BY") && !oneRow && !strings.Contains(up, "IS NULL") {
			t.Errorf("no ORDER BY, and it can return more than one row: %s", s)
		}
	}
}

// aggregateOnly says the statement selects aggregates alone, so it returns
// one row whatever the table holds.
func aggregateOnly(up string) bool {
	for _, f := range []string{"SUM(", "AVG(", "MIN(", "MAX(", "COUNT("} {
		if strings.Contains(up, f) {
			return true
		}
	}
	return false
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

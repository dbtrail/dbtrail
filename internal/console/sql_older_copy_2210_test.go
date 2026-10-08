package console

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// writeOrdersSnapshot writes shop.orders (id, status) with one status per row
// into the snapshot directory dirName under root, and returns its schema
// directory.
func writeOrdersSnapshot(t *testing.T, root, dirName string, statuses ...string) string {
	t.Helper()
	return writeTableSnapshot(t, root, dirName, "orders", statuses...)
}

// writeTableSnapshot is writeOrdersSnapshot for a table of any name, same
// columns.
func writeTableSnapshot(t *testing.T, root, dirName, table string, statuses ...string) string {
	t.Helper()
	schemaDir := filepath.Join(root, dirName, "shop")
	schemaFile := filepath.Join(t.TempDir(), "shop.orders-schema.sql")
	ddl := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	if err := os.WriteFile(schemaFile, []byte(ddl), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := baseline.ParseSchema(schemaFile)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(schemaDir, table+".parquet"), cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	for i, st := range statuses {
		if err := w.WriteRow([]string{string(rune('1' + i)), st}, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return schemaDir
}

// addChain puts a pair beside table in schemaDir whose upserts are not
// Parquet: past a tiny line it is "too heavy", and a statement that did read
// it would fail on its own.
func addChain(t *testing.T, schemaDir, table string) {
	t.Helper()
	for _, suffix := range []string{".000000.posdel", ".000000.upserts"} {
		if err := os.WriteFile(filepath.Join(schemaDir, table+suffix), []byte("not parquet"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// dbtrail answers from a point in time (#2210): when the newest copy holds
// more unmerged changes than a statement merges, the newest earlier copy in
// which the statement's tables fit answers it, and says so.
func TestSQLAPI_answersFromAnEarlierCopy(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	prev := sqlMaxChainBytes
	t.Cleanup(func() { sqlMaxChainBytes = prev })
	sqlMaxChainBytes = 10

	type answer struct {
		code   int
		status string
		older  string
		copyAt time.Time
		body   string
	}
	ask := func(f *sqlFixture, stmt string) answer {
		t.Helper()
		w := f.post(t, `{"sql":"`+stmt+`"}`)
		a := answer{code: w.Code, body: w.Body.String()}
		if w.Code == http.StatusOK {
			var r sqlResponse
			if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil {
				t.Fatal(err)
			}
			if len(r.Rows) == 1 && len(r.Rows[0]) == 1 {
				a.status, _ = r.Rows[0][0].(string)
			}
			a.older = r.OlderCopy
			if r.CopyUpdatedAt != nil {
				a.copyAt = *r.CopyUpdatedAt
			}
		}
		return a
	}
	const stmt = "SELECT status FROM shop.orders WHERE id = 1"
	two := time.Date(2026, 4, 30, 2, 0, 0, 0, time.UTC)
	one := time.Date(2026, 4, 30, 1, 0, 0, 0, time.UTC)

	t.Run("the newest earlier copy that fits answers, and says which and why", func(t *testing.T) {
		f := newSQLFixture(t, runner, false)
		addChain(t, f.schemaDir, "orders")
		writeOrdersSnapshot(t, f.root, "2026-04-30T02-00-00Z", "at two")
		writeOrdersSnapshot(t, f.root, "2026-04-30T01-00-00Z", "at one")
		a := ask(f, stmt)
		if a.code != http.StatusOK || a.status != "at two" || !a.copyAt.Equal(two) {
			t.Fatalf("got %+v", a)
		}
		for _, want := range []string{"Answered from the copy of 2026-04-30 02:00 UTC", "the newest copy (2026-04-30 03:00 UTC)", "shop.orders has 1 MB"} {
			if !strings.Contains(a.older, want) {
				t.Errorf("note %q lacks %q", a.older, want)
			}
		}
	})

	t.Run("a CSV of an earlier copy carries the copy and why as headers", func(t *testing.T) {
		f := newSQLFixture(t, runner, false)
		addChain(t, f.schemaDir, "orders")
		writeOrdersSnapshot(t, f.root, "2026-04-30T02-00-00Z", "at two")
		w := f.post(t, `{"sql":"`+stmt+`"}`, "Accept", "text/csv")
		if w.Code != http.StatusOK || w.Header().Get("X-DBTrail-Copy-At") != "2026-04-30T02:00:00Z" ||
			!strings.HasPrefix(w.Header().Get("X-DBTrail-Older-Copy"), "Answered from the copy of 2026-04-30 02:00 UTC") ||
			!strings.Contains(w.Body.String(), "at two") {
			t.Fatalf("code=%d headers=%v body=%q", w.Code, w.Header(), w.Body.String())
		}
	})

	t.Run("the note names the total when the statement reads several tables", func(t *testing.T) {
		h := &sqlHeavyRun{chain: sqlHeavyChain{Table: "shop.orders", Own: 30 << 20, Total: 55 << 20, WithChanges: 2}, limit: 48 << 20, newest: sqlSnapshotAt}
		if n := sqlOlderCopyNote(h, two); !strings.Contains(n, "the tables this statement reads have 55 MB") || strings.Contains(n, "30 MB") {
			t.Errorf("note: %q", n)
		}
	})

	t.Run("an earlier copy that is too heavy too is skipped", func(t *testing.T) {
		f := newSQLFixture(t, runner, false)
		addChain(t, f.schemaDir, "orders")
		addChain(t, writeOrdersSnapshot(t, f.root, "2026-04-30T02-00-00Z", "at two"), "orders")
		writeOrdersSnapshot(t, f.root, "2026-04-30T01-00-00Z", "at one")
		if a := ask(f, stmt); a.code != http.StatusOK || a.status != "at one" || !a.copyAt.Equal(one) {
			t.Fatalf("got %+v", a)
		}
	})

	t.Run("an earlier copy missing one of the statement's tables is skipped", func(t *testing.T) {
		f := newSQLFixture(t, runner, false)
		addChain(t, f.schemaDir, "orders")
		writeTableSnapshot(t, f.root, sqlSnapshotDirName, "customers", "c now")
		writeOrdersSnapshot(t, f.root, "2026-04-30T02-00-00Z", "at two") // no customers
		writeOrdersSnapshot(t, f.root, "2026-04-30T01-00-00Z", "at one")
		writeTableSnapshot(t, f.root, "2026-04-30T01-00-00Z", "customers", "c at one")
		both := "SELECT o.status FROM shop.orders o JOIN shop.customers c ON c.id = o.id WHERE o.id = 1"
		if a := ask(f, both); a.code != http.StatusOK || a.status != "at one" || !a.copyAt.Equal(one) {
			t.Fatalf("got %+v", a)
		}
	})

	t.Run("no earlier copy with the table: the refusal", func(t *testing.T) {
		f := newSQLFixture(t, runner, false)
		addChain(t, f.schemaDir, "orders")
		writeTableSnapshot(t, f.root, "2026-04-30T02-00-00Z", "customers", "x")
		if a := ask(f, stmt); a.code != http.StatusUnprocessableEntity || !strings.Contains(a.body, "not merged") {
			t.Fatalf("got %+v", a)
		}
	})

	t.Run("an earlier copy that cannot answer leaves the refusal, not its error", func(t *testing.T) {
		f := newSQLFixture(t, runner, false)
		addChain(t, f.schemaDir, "orders")
		dir := filepath.Join(f.root, "2026-04-30T02-00-00Z", "shop")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "orders.parquet"), []byte("not parquet"), 0o644); err != nil {
			t.Fatal(err)
		}
		if a := ask(f, stmt); a.code != http.StatusUnprocessableEntity || !strings.Contains(a.body, "not merged") {
			t.Fatalf("got %+v", a)
		}
	})

	t.Run("no earlier copy: the refusal, as before", func(t *testing.T) {
		f := newSQLFixture(t, runner, false)
		addChain(t, f.schemaDir, "orders")
		if a := ask(f, stmt); a.code != http.StatusUnprocessableEntity || !strings.Contains(a.body, "not merged") {
			t.Fatalf("got %+v", a)
		}
	})

	t.Run("the newest copy fits: no note, the newest answers", func(t *testing.T) {
		f := newSQLFixture(t, runner, false)
		writeOrdersSnapshot(t, f.root, "2026-04-30T02-00-00Z", "at two")
		if a := ask(f, stmt); a.code != http.StatusOK || a.older != "" || !a.copyAt.Equal(sqlSnapshotAt) {
			t.Fatalf("got %+v", a)
		}
	})

	t.Run("a statement that reads events is refused, not answered from two moments", func(t *testing.T) {
		f := newSQLFixture(t, runner, false)
		addChain(t, f.schemaDir, "orders")
		writeOrdersSnapshot(t, f.root, "2026-04-30T02-00-00Z", "at two")
		if a := ask(f, "SELECT o.status FROM shop.orders o, events e WHERE o.id = 1"); a.code == http.StatusOK || a.older != "" {
			t.Fatalf("got %+v", a)
		}
	})

	t.Run("under read routing the refusal stands: it is what sends the statement to MySQL", func(t *testing.T) {
		f := newSQLFixture(t, runner, false)
		addChain(t, f.schemaDir, "orders")
		writeOrdersSnapshot(t, f.root, "2026-04-30T02-00-00Z", "at two")
		b := f.s.cm.boot
		if _, err := f.s.runSQLVouched(context.Background(), b, "u", stmt, "", 0, sqlsandbox.Session{StrictStar: true}, nil); err == nil || !strings.Contains(err.Error(), "not merged") {
			t.Fatalf("routed: %v", err)
		}
		out, err := f.s.runSQLVouched(context.Background(), b, "u", stmt, "", 0, sqlsandbox.Session{}, nil)
		if err != nil || out.OlderCopyNote == "" {
			t.Fatalf("not routed: %v %+v", err, out)
		}
		// The port carries the note to its warnings.
		q := &SQLOnCopy{s: f.s, b: b, user: "u"}
		res, err := q.Run(context.Background(), stmt, "", sqlsandbox.Session{})
		if err != nil || !strings.HasPrefix(res.Note, "Answered from the copy of") {
			t.Fatalf("port: %v %q", err, res.Note)
		}
	})
}

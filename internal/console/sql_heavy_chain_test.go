package console

import (
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

func chainTable(schema, table string, seqs ...int) views.BaselineTable {
	t := views.BaselineTable{Schema: schema, Table: table, Path: "/snap/" + schema + "/" + table + ".parquet"}
	stem := strings.TrimSuffix(t.Path, ".parquet")
	for _, s := range seqs {
		n := "." + strings.Repeat("0", 5) + string(rune('0'+s))
		t.DeltaFiles = append(t.DeltaFiles, baseline.TableDeltaFile{Seq: s, Posdel: stem + n + ".posdel", Upserts: stem + n + ".upserts"})
	}
	t.Delta = len(seqs) > 0
	return t
}

// The size rule of the pre-flight: what counts is the upserts files of every
// table the statement reads, together; a file that is gone or unreadable
// counts as nothing, and never refuses.
func TestSQLChainTooHeavy(t *testing.T) {
	const limit = 100
	sizes := map[string]int64{
		"/snap/shop/a.000000.upserts": 40, "/snap/shop/a.000001.upserts": 60,
		"/snap/shop/b.000000.upserts":   61,
		"/snap/shop/big.000000.upserts": 101,
		"/snap/shop/old.upserts":        500,
		// posdel files hold row numbers, not rows: they are not what a
		// query has to hold in memory.
		"/snap/shop/a.000000.posdel": 1 << 30,
	}
	size := func(p string) (int64, error) {
		if n, ok := sizes[p]; ok {
			return n, nil
		}
		if strings.Contains(p, "denied") {
			return 0, errors.New("permission denied")
		}
		return 0, fs.ErrNotExist
	}
	legacy := views.BaselineTable{Schema: "shop", Table: "old", Path: "/snap/shop/old.parquet", Delta: true, DeltaLegacy: true}
	cases := []struct {
		name      string
		tables    []views.BaselineTable
		wantTable string
		wantBytes int64
	}{
		{"no tables", nil, "", 0},
		{"a table with no chain", []views.BaselineTable{chainTable("shop", "plain")}, "", 0},
		{"exactly at the limit", []views.BaselineTable{chainTable("shop", "a", 0, 1)}, "", 0},
		{"one byte past", []views.BaselineTable{chainTable("shop", "big", 0)}, "shop.big", 101},
		{"two tables under it, together past it: the heavier is named", []views.BaselineTable{chainTable("shop", "b", 0), chainTable("shop", "a", 0, 1)}, "shop.a", 161},
		{"a v0.83.0 pair", []views.BaselineTable{legacy}, "shop.old", 500},
		{"a file that is gone counts as nothing", []views.BaselineTable{chainTable("shop", "gone", 0, 1)}, "", 0},
		{"a file that cannot be read counts as nothing", []views.BaselineTable{chainTable("shop", "denied", 0)}, "", 0},
		{"a copy on S3 is not this check's", []views.BaselineTable{{Schema: "shop", Table: "s", Path: "s3://b/shop/s.parquet", Delta: true, DeltaLegacy: true}}, "", 0},
	}
	for _, c := range cases {
		table, total := sqlChainTooHeavy(c.tables, limit, size)
		if table != c.wantTable || (c.wantTable != "" && total != c.wantBytes) {
			t.Errorf("%s: got (%q, %d), want (%q, %d)", c.name, table, total, c.wantTable, c.wantBytes)
		}
	}
}

// The sentence a user reads, built from real values: it names the table, says
// how much is waiting and what the limit is, and where to go instead.
func TestSQLChainTooHeavyMessage(t *testing.T) {
	got := sqlChainTooHeavyMessage("tpcc.order_line1", 86<<20, 48<<20)
	want := "tpcc.order_line1 has 86 MB of changes not merged into it yet, and a query here reads at most 48 MB of them: " +
		"it runs with 2 GB of memory on the host that captures changes, which is enough for a quick look and not for this. " +
		"Read this table with your own DuckDB instead (Settings, MCP Server, Download a DuckDB schema). " +
		"DBTrail merges the changes into the table on its own, and the table can be read here again after that."
	if got != want {
		t.Errorf("message:\n got %q\nwant %q", got, want)
	}
	// Under a megabyte past a round limit must not print the same number twice.
	if got := sqlChainTooHeavyMessage("a.b", 48<<20+1, 48<<20); !strings.Contains(got, "has 49 MB") {
		t.Errorf("just past the limit: %q", got)
	}
}

// A statement that ran and failed past the memory cap is told the same way out.
func TestSQLOutOfMemoryHint(t *testing.T) {
	oom := &sqlsandbox.QueryError{Message: "Out of Memory Error: could not allocate block of size 256.0 KiB (1.8 GiB/1.8 GiB used)"}
	got := sqlWithMemoryHint(oom)
	var q *sqlsandbox.QueryError
	if !errors.As(got, &q) || !strings.HasPrefix(q.Message, oom.Message) || !strings.Contains(q.Message, "your own DuckDB") {
		t.Errorf("out of memory: %v", got)
	}
	if oom.Message != "Out of Memory Error: could not allocate block of size 256.0 KiB (1.8 GiB/1.8 GiB used)" {
		t.Error("the runner's error was changed in place")
	}
	other := &sqlsandbox.QueryError{Message: "Binder Error: column x not found"}
	if got := sqlWithMemoryHint(other); got != error(other) {
		t.Errorf("another query error must pass untouched: %v", got)
	}
	wrapped := errors.New("not a query error: Out of Memory Error")
	if got := sqlWithMemoryHint(wrapped); got != wrapped {
		t.Errorf("a non-query error must pass untouched: %v", got)
	}
}

// The wiring, through the real worker: a statement that reads a table whose
// chain is past the limit is refused before the views are installed, naming
// the table; one that reads no table is not; and under the limit the check
// stays out of the way (the pair here is not Parquet, so the read then fails
// on its own, with DuckDB's words and not the refusal's).
func TestSQLAPI_refusesATableWithTooManyChangesWaiting(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	for _, suffix := range []string{".000000.posdel", ".000000.upserts"} {
		if err := os.WriteFile(filepath.Join(f.schemaDir, "orders"+suffix), []byte("not parquet"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	prev := sqlMaxChainBytes
	t.Cleanup(func() { sqlMaxChainBytes = prev })

	sqlMaxChainBytes = 10
	w := f.post(t, `{"sql":"SELECT count(*) FROM shop.orders"}`)
	if w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), "shop.orders has 1 MB of changes not merged into it yet") ||
		!strings.Contains(w.Body.String(), "your own DuckDB") {
		t.Fatalf("past the limit: code=%d body=%s", w.Code, w.Body.String())
	}
	if w := f.post(t, `{"sql":"SELECT 1 AS one"}`); w.Code != http.StatusOK {
		t.Fatalf("a statement that reads no table: code=%d body=%s", w.Code, w.Body.String())
	}
	if w := f.post(t, `{"sql":"SHOW TABLES"}`); strings.Contains(w.Body.String(), "not merged") {
		t.Fatalf("a statement whose tables are not known was refused: code=%d body=%s", w.Code, w.Body.String())
	}

	sqlMaxChainBytes = 11
	w = f.post(t, `{"sql":"SELECT count(*) FROM shop.orders"}`)
	if strings.Contains(w.Body.String(), "not merged") || w.Code == http.StatusOK {
		t.Fatalf("at the limit the check must stay out and the unreadable pair must fail the read: code=%d body=%s", w.Code, w.Body.String())
	}
}

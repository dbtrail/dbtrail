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
		name   string
		tables []views.BaselineTable
		want   sqlHeavyChain
	}{
		{"no tables", nil, sqlHeavyChain{}},
		{"a table with no chain", []views.BaselineTable{chainTable("shop", "plain")}, sqlHeavyChain{}},
		{"exactly at the limit", []views.BaselineTable{chainTable("shop", "a", 0, 1)}, sqlHeavyChain{}},
		{"one byte past", []views.BaselineTable{chainTable("shop", "big", 0)}, sqlHeavyChain{"shop.big", 101, 101, 1}},
		{"two tables under it, together past it: the heavier is named with its own share",
			[]views.BaselineTable{chainTable("shop", "b", 0), chainTable("shop", "a", 0, 1)}, sqlHeavyChain{"shop.a", 100, 161, 2}},
		{"a table with nothing waiting beside a heavy one is not counted",
			[]views.BaselineTable{chainTable("shop", "gone", 0), chainTable("shop", "big", 0), chainTable("shop", "plain")}, sqlHeavyChain{"shop.big", 101, 101, 1}},
		{"a v0.83.0 pair", []views.BaselineTable{legacy}, sqlHeavyChain{"shop.old", 500, 500, 1}},
		{"a file that is gone counts as nothing", []views.BaselineTable{chainTable("shop", "gone", 0, 1)}, sqlHeavyChain{}},
		{"a file that cannot be read counts as nothing", []views.BaselineTable{chainTable("shop", "denied", 0)}, sqlHeavyChain{}},
		{"a copy on S3 is not this check's", []views.BaselineTable{{Schema: "shop", Table: "s", Path: "s3://b/shop/s.parquet", Delta: true, DeltaLegacy: true}}, sqlHeavyChain{}},
	}
	for _, c := range cases {
		if got := sqlChainTooHeavy(c.tables, limit, size); got != c.want {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}
}

// The sentence a user reads, built from real values: one table, and several.
// It names the table with ITS megabytes, says the limit and where to go, and
// fits what a MySQL client shows of an error.
func TestSQLChainTooHeavyMessage(t *testing.T) {
	one := sqlChainTooHeavyMessage(sqlHeavyChain{"tpcc.order_line1", 86 << 20, 86 << 20, 1}, 48<<20, "2GB")
	want := "tpcc.order_line1 has 86 MB of changes not merged into it yet, and SQL on the copy merges at most 48 MB: " +
		"it runs with 2 GB of memory on the capture host, for quick looks. " +
		"Read it with your own DuckDB instead (in the web interface: Settings, MCP Server, Download a DuckDB schema). " +
		"DBTrail merges them on its own, within a day while updates run."
	if one != want {
		t.Errorf("one table:\n got %q\nwant %q", one, want)
	}
	several := sqlChainTooHeavyMessage(sqlHeavyChain{"shop.orders", 30 << 20, 55 << 20, 2}, 48<<20, "2GB")
	if !strings.HasPrefix(several, "the tables this statement reads have 55 MB of changes not merged into them yet (shop.orders has 30 MB), and SQL on the copy merges at most 48 MB: ") ||
		!strings.Contains(several, "Read them with your own DuckDB") {
		t.Errorf("several tables: %q", several)
	}
	msgs := map[string]string{"one": one, "several": several}
	// The longest names, the most waiting, every memory shape (#2210 review):
	// the limit as sqlChainLimit gives it, not a fixed one.
	for _, mem := range []string{"2GB", "2048MiB", "12800MiB", "131072MiB", "1048576MiB", "1025MiB"} {
		msgs["longest names at "+mem] = sqlChainTooHeavyMessage(sqlHeavyChain{strings.Repeat("s", 64) + "." + strings.Repeat("t", 64), 99999 << 20, 999999 << 20, 2},
			sqlChainLimit(mem), mem)
	}
	for name, msg := range msgs {
		if len(msg) > 512 {
			t.Errorf("%s: %d bytes, past the 512 a MySQL client shows", name, len(msg))
		}
	}
	// Under a megabyte past a round limit must not print the same number twice.
	if got := sqlChainTooHeavyMessage(sqlHeavyChain{"a.b", 48<<20 + 1, 48<<20 + 1, 1}, 48<<20, "2GB"); !strings.Contains(got, "has 49 MB") {
		t.Errorf("just past the limit: %q", got)
	}
}

// A statement that ran and failed past the memory cap is told the same way out.
func TestSQLOutOfMemoryHint(t *testing.T) {
	oom := &sqlsandbox.QueryError{Message: "Out of Memory Error: could not allocate block of size 256.0 KiB (1.8 GiB/1.8 GiB used)"}
	got := sqlWithMemoryHint(oom, "2GB")
	var q *sqlsandbox.QueryError
	if !errors.As(got, &q) || !strings.HasPrefix(q.Message, oom.Message) || !strings.Contains(q.Message, "your own DuckDB") ||
		!strings.Contains(q.Message, "2 GB of memory") {
		t.Errorf("out of memory: %v", got)
	}
	if got := sqlWithMemoryHint(oom, "6144MiB"); !strings.Contains(got.Error(), "6 GB of memory") {
		t.Errorf("out of memory under --sql-memory 6GB: %v", got)
	}
	if oom.Message != "Out of Memory Error: could not allocate block of size 256.0 KiB (1.8 GiB/1.8 GiB used)" {
		t.Error("the runner's error was changed in place")
	}
	other := &sqlsandbox.QueryError{Message: "Binder Error: column x not found"}
	if got := sqlWithMemoryHint(other, "2GB"); got != error(other) {
		t.Errorf("another query error must pass untouched: %v", got)
	}
	wrapped := errors.New("not a query error: Out of Memory Error")
	if got := sqlWithMemoryHint(wrapped, "2GB"); got != wrapped {
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
	// Shapes whose table set is not certain still read the table they name.
	for _, q := range []string{
		"SELECT count(*) FROM shop.orders, range(3)",
		"SELECT count(*) FROM shop.orders JOIN information_schema.columns c ON true",
		"WITH a AS (SELECT * FROM shop.orders), b AS (SELECT * FROM a) SELECT count(*) FROM b",
		"SELECT count(*) FROM ORDERS",
	} {
		if w := f.post(t, `{"sql":"`+q+`"}`); w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), "not merged") {
			t.Errorf("%s: code=%d body=%s", q, w.Code, w.Body.String())
		}
	}
	if w := f.post(t, `{"sql":"SHOW TABLES"}`); strings.Contains(w.Body.String(), "not merged") {
		t.Fatalf("a statement whose tables are not known was refused: code=%d body=%s", w.Code, w.Body.String())
	}

	// #2210: the limit follows the memory the server runs SQL with, read at
	// each statement: twice the default memory, twice the line, so the same
	// chain is no longer refused.
	f.s.sqlLimits.MemoryLimit = "4GB"
	w = f.post(t, `{"sql":"SELECT count(*) FROM shop.orders"}`)
	if strings.Contains(w.Body.String(), "not merged") {
		t.Fatalf("under twice the memory the same chain was refused: code=%d body=%s", w.Code, w.Body.String())
	}
	f.s.sqlLimits.MemoryLimit = ""

	sqlMaxChainBytes = 11
	w = f.post(t, `{"sql":"SELECT count(*) FROM shop.orders"}`)
	if strings.Contains(w.Body.String(), "not merged") || w.Code == http.StatusOK {
		t.Fatalf("at the limit the check must stay out and the unreadable pair must fail the read: code=%d body=%s", w.Code, w.Body.String())
	}
}

// #2210: the limit follows the worker's memory. It was measured at the
// default 2 GB; twice the memory merges about twice the changes.
func TestSQLChainLimit_followsTheWorkersMemory(t *testing.T) {
	for _, c := range []struct {
		memory string
		want   int64
	}{
		{"2GB", 48 << 20},
		{"", 48 << 20},
		{"2048MiB", 48 << 20},
		{"4096MiB", 96 << 20},
		{"8GB", 192 << 20},
		{"1GB", 24 << 20},
		{"512MiB", 12 << 20},
		{"64GB", 1536 << 20},
		{"not a size", 48 << 20},
	} {
		if got := sqlChainLimit(c.memory); got != c.want {
			t.Errorf("sqlChainLimit(%q) = %d MiB, want %d MiB", c.memory, got>>20, c.want>>20)
		}
	}
	msg := sqlChainTooHeavyMessage(sqlHeavyChain{"tpcc.stock", 120 << 20, 120 << 20, 1}, sqlChainLimit("4096MiB"), "4096MiB")
	if !strings.Contains(msg, "merges at most 96 MB: it runs with 4 GB of memory") {
		t.Errorf("message under 4 GB: %q", msg)
	}
}

package views

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// writeSnapshot writes one baseline snapshot holding shop.orders with the given
// status values, and marks it complete unless marked is false.
func writeSnapshot(t *testing.T, root, stamp string, marked bool, statuses ...string) string {
	t.Helper()
	path := filepath.Join(root, stamp, "shop", "orders.parquet")
	cols, err := baseline.ParseSchema(writeSchemaFile(t))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("baseline writer: %v", err)
	}
	for i, st := range statuses {
		if err := w.WriteRow([]string{string(rune('1' + i)), st}, []bool{false, false}); err != nil {
			t.Fatalf("write row: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if marked {
		marker := filepath.Join(root, stamp, baseline.SuccessMarker)
		if err := os.WriteFile(marker, nil, 0o644); err != nil {
			t.Fatalf("write marker: %v", err)
		}
	}
	return path
}

// newestFixture renders the FollowNewest state view over a LOCAL root.
//
// Follow and Rel are set directly instead of through ApplyFollow, which reaches
// this mode only for an s3:// root. What is under test here is the SQL the mode
// EMITS, and DuckDB reads a local glob and an S3 one through the same code path;
// that ApplyFollow reaches the mode at all is what the golden covers.
//
// Variadic because a test that asserts NOTHING was created needs at least one
// table that would otherwise have been.
func newestFixture(t *testing.T, root string, tableRels ...string) string {
	t.Helper()
	tables := make([]BaselineTable, 0, len(tableRels))
	for _, rel := range tableRels {
		schema, table, _ := strings.Cut(filepath.ToSlash(rel), "/")
		tables = append(tables, BaselineTable{
			Schema: schema,
			Table:  strings.TrimSuffix(table, ".parquet"),
			Path:   filepath.Join(root, "2026-04-30T03-00-00Z", rel),
			Rel:    filepath.ToSlash(rel),
		})
	}
	return Generate(Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		BaselineSource:   root,
		BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Follow:           FollowNewest,
		Baselines:        tables,
	})
}

func execViews(t *testing.T, sqlText string) *sql.DB {
	t.Helper()
	db, err := loadViews(t, sqlText)
	if err != nil {
		t.Fatalf("DuckDB rejected the generated views:\n%v\n\n--- generated ---\n%s", err, sqlText)
	}
	return db
}

// loadViews runs the generated file and HANDS BACK the error, for the cases
// where refusing to load is the behaviour under test.
func loadViews(t *testing.T, sqlText string) (*sql.DB, error) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	_, err = db.Exec(sqlText)
	return db, err
}

// TestNewestStateView_readsTheNewestMarkedSnapshot runs the emitted SQL, which
// the golden cannot do: a file that pins byte-for-byte can still be invalid SQL,
// or valid SQL that selects the wrong file.
//
// The two snapshots hold DIFFERENT rows on purpose. A view that ignored the
// filter and unioned both, or that picked the older one, returns a row count or
// a value this asserts against, so the test cannot pass by reading whatever
// happens to be there.
func TestNewestStateView_readsTheNewestMarkedSnapshot(t *testing.T) {
	root := t.TempDir()
	writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "old-a", "old-b")
	writeSnapshot(t, root, "2026-05-30T03-00-00Z", true, "new-a")

	db := execViews(t, newestFixture(t, root, filepath.Join("shop", "orders.parquet")))

	var n int
	var status string
	if err := db.QueryRow(`SELECT count(*), min("status") FROM shop.orders`).Scan(&n, &status); err != nil {
		t.Fatalf("query state view: %v", err)
	}
	if n != 1 || status != "new-a" {
		t.Errorf("state view read %d row(s) with status %q; want 1 row of \"new-a\" (the newest marked snapshot)", n, status)
	}
}

// TestNewestStateView_ignoresAnUnmarkedNewerSnapshot pins the anchor. A snapshot
// directory with a later name but no marker is a run that failed or is still
// writing, and reading it would serve a half-published table as the current one.
func TestNewestStateView_ignoresAnUnmarkedNewerSnapshot(t *testing.T) {
	root := t.TempDir()
	writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "complete")
	writeSnapshot(t, root, "2026-05-30T03-00-00Z", false, "half-written")

	db := execViews(t, newestFixture(t, root, filepath.Join("shop", "orders.parquet")))

	var status string
	if err := db.QueryRow(`SELECT "status" FROM shop.orders`).Scan(&status); err != nil {
		t.Fatalf("query state view: %v", err)
	}
	if status != "complete" {
		t.Errorf("state view read %q; want \"complete\" (the unmarked newer snapshot must not win)", status)
	}
}

// TestNewestStateView_raisesWhenTheTableLeftTheNewestSnapshot is the guarantee
// this mechanism exists to keep, and the one it is easiest to lose.
//
// The shape this replaced returned ZERO ROWS with no error when its filter
// matched nothing, and an empty table that should have had rows is a worse
// answer than a stale one. Reading one named file cannot fail that way: the
// path is either there or DuckDB refuses it. This asserts the refusal.
// TestNewestStateView_raisesWhenTheTableLeftTheNewestSnapshot pins the refusal a
// DROPped table gets (#1558).
//
// Before the preflight, this asserted DuckDB's own "No files found" naming a
// Parquet path. That message was not wrong, it was the wrong SHAPE: it arrives
// at the view for the missing table, so every view emitted before it already
// exists and every view after it never does. Against a persistent database that
// leaves a partial schema whose contents depend on emission order, and it reads
// as a corrupt snapshot rather than as a table somebody dropped on purpose.
//
// So the assertions are about WHEN and WHAT, not merely that it failed: nothing
// may be created, and the message has to name the table, where it looked, and
// what to do next.
func TestNewestStateView_raisesWhenTheTableLeftTheNewestSnapshot(t *testing.T) {
	root := t.TempDir()
	writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "here")

	writeSnapshot(t, root, "2026-05-30T03-00-00Z", true, "elsewhere")
	if err := os.Rename(filepath.Join(root, "2026-05-30T03-00-00Z", "shop"),
		filepath.Join(root, "2026-05-30T03-00-00Z", "audit")); err != nil {
		t.Fatalf("rename: %v", err)
	}

	// The rename left audit/orders.parquet sitting there, so this file names
	// one table that is gone and one that is present. Without the preflight the
	// present one WOULD get its view, which is what the count below measures.
	db, err := loadViews(t, newestFixture(t, root,
		filepath.Join("shop", "orders.parquet"),
		filepath.Join("audit", "orders.parquet")))
	if err == nil {
		t.Fatal("the generated file loaded cleanly with shop.orders absent from the newest " +
			"snapshot; a table that left must fail loudly, not read as empty")
	}

	if !strings.Contains(err.Error(), "shop/orders.parquet") {
		t.Errorf("failed with %v; want the refusal naming the table that is not there", err)
	}
	if !strings.Contains(err.Error(), "2026-05-30T03-00-00Z") {
		t.Errorf("failed with %v; the message must name the snapshot it looked in", err)
	}
	if !strings.Contains(err.Error(), "download this file again") {
		t.Errorf("failed with %v; the message must say what to do about it", err)
	}

	// The reason for doing this ahead of the views rather than at one of them.
	// A check placed after even one CREATE would leave that one behind, and
	// this fixture has a second table present precisely so "nothing created"
	// cannot pass by there being nothing to create.
	var n int
	if err := db.QueryRow(
		`SELECT count(*) FROM duckdb_views() WHERE NOT internal`).Scan(&n); err != nil {
		t.Fatalf("count views: %v", err)
	}
	if n != 0 {
		t.Errorf("the refused file left %d view(s) defined; a partial schema is the "+
			"failure this check exists to prevent", n)
	}
}

// TestNewestStateView_persistedViewsNameTheMissingVariable pins the CASE in
// every state view body (#1583), driven through the exact scenario it exists
// for: views PERSIST in a database file, the session variable does not.
//
// A reader who opens the same lake.db in a new session (duckdb -ui, or a
// prompt that never ran this file) sees every view listed and, before this,
// got "read_parquet cannot take NULL list as parameter" on the first read —
// true, and naming nothing they can act on. The read must instead name its
// own cause, and the message's own remediation (run the SET VARIABLE
// statement, alone) must heal the session — asserted by running exactly that
// statement, never the whole file, so the message cannot drift into advice
// that does not work.
func TestNewestStateView_persistedViewsNameTheMissingVariable(t *testing.T) {
	root := t.TempDir()
	writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "kept")
	sqlText := newestFixture(t, root, filepath.Join("shop", "orders.parquet"))

	lake := filepath.Join(t.TempDir(), "lake.db")
	first, err := sql.Open("duckdb", lake)
	if err != nil {
		t.Fatalf("open lake.db: %v", err)
	}
	if _, err := first.Exec(sqlText); err != nil {
		first.Close()
		t.Fatalf("DuckDB rejected the generated views:\n%v", err)
	}
	// The CASE must cost nothing in the session that ran the file: the SET
	// VARIABLE precedes every CREATE, so error() never binds against NULL.
	var status string
	if err := first.QueryRow(`SELECT "status" FROM shop.orders`).Scan(&status); err != nil {
		first.Close()
		t.Fatalf("state view unreadable in the session that created it: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first session: %v", err)
	}

	second, err := sql.Open("duckdb", lake)
	if err != nil {
		t.Fatalf("reopen lake.db: %v", err)
	}
	t.Cleanup(func() { second.Close() })
	err = second.QueryRow(`SELECT "status" FROM shop.orders`).Scan(&status)
	if err == nil {
		t.Fatal("a persisted state view read rows in a session that never set the variable; " +
			"there is no snapshot choice for it to have read through")
	}
	if !strings.Contains(err.Error(), "run its SET VARIABLE statement in this session first") {
		t.Errorf("failed with %v; want the read naming its own cause (the unset session variable)", err)
	}
	if strings.Contains(err.Error(), "NULL list as parameter") {
		t.Errorf("failed with DuckDB's raw NULL refusal, which names nothing the reader can act on: %v", err)
	}

	// The remediation the message names, verbatim: the file's own SET VARIABLE
	// statement, extracted from the generated text rather than rebuilt here.
	i := strings.Index(sqlText, "SET VARIABLE "+newestVar)
	if i < 0 {
		t.Fatalf("generated file carries no SET VARIABLE %s statement", newestVar)
	}
	j := strings.Index(sqlText[i:], ";")
	if j < 0 {
		t.Fatalf("could not find the end of the SET VARIABLE statement")
	}
	if _, err := second.Exec(sqlText[i : i+j+1]); err != nil {
		t.Fatalf("the SET VARIABLE statement the message points at failed on its own: %v", err)
	}
	if err := second.QueryRow(`SELECT "status" FROM shop.orders`).Scan(&status); err != nil {
		t.Fatalf("after the SET VARIABLE statement the persisted view still fails: %v", err)
	}
	if status != "kept" {
		t.Errorf("healed view read %q; want \"kept\"", status)
	}
}

// TestNewestStateView_refusesToLoadWhenNothingIsMarked pins the CASE in the
// variable lookup, and it asserts a refusal to LOAD rather than to query.
//
// With no marker under the root, max() is NULL and the variable is NULL, which
// read_parquet reports as "cannot take NULL list as parameter". That is true and
// says nothing about baselines, so the lookup raises first and names the root
// and the marker. Failing at load also beats failing per query: the operator
// learns at the moment they open the file, not at the first thing they ask it.
func TestNewestStateView_refusesToLoadWhenNothingIsMarked(t *testing.T) {
	root := t.TempDir()
	writeSnapshot(t, root, "2026-04-30T03-00-00Z", false, "unmarked")

	_, err := loadViews(t, newestFixture(t, root, filepath.Join("shop", "orders.parquet")))
	if err == nil {
		t.Fatalf("the generated file loaded cleanly with no %s marker under the root; "+
			"there is no snapshot for its state views to read", baseline.SuccessMarker)
	}
	if !strings.Contains(err.Error(), "no completed snapshot under") {
		t.Errorf("failed with %v; want the message naming the root", err)
	}
	if strings.Contains(err.Error(), "NULL list as parameter") {
		t.Errorf("failed with DuckDB's raw NULL refusal, which says nothing about baselines: %v", err)
	}
}

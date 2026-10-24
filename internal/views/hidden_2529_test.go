package views

import (
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/hiderules"
)

func mustRules(t *testing.T, tables, columns []string) *hiderules.Set {
	t.Helper()
	s, err := hiderules.Parse(hiderules.Rules{Tables: tables, Columns: columns})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestApplyHidden(t *testing.T) {
	in := Input{Baselines: []BaselineTable{
		{Schema: "shop", Table: "orders"},
		{Schema: "Shop", Table: "Audit_Log"},
		{Schema: "hr", Table: "salaries"},
		{Schema: "shop", Table: "customers"},
	}}
	ApplyHidden(&in, mustRules(t, []string{"shop.audit_log", "hr.*"}, []string{"shop.customers.email", "shop.*.ssn"}))
	var names []string
	for _, b := range in.Baselines {
		names = append(names, b.Schema+"."+b.Table+":"+strings.Join(b.HiddenColumns, ","))
	}
	want := []string{"shop.orders:ssn", "shop.customers:email,ssn"}
	if !slices.Equal(names, want) {
		t.Fatalf("after ApplyHidden: %q, want %q", names, want)
	}
	if in.Hidden == nil {
		t.Fatal("ApplyHidden must keep the set for the events view")
	}
	if in.HiddenEveryTable {
		t.Error("HiddenEveryTable with two tables left")
	}
	all := Input{Baselines: []BaselineTable{{Schema: "shop", Table: "orders"}}}
	ApplyHidden(&all, mustRules(t, []string{"shop.*"}, nil))
	if !all.HiddenEveryTable {
		t.Error("every table removed: HiddenEveryTable must say so")
	}
	none := Input{}
	ApplyHidden(&none, mustRules(t, []string{"shop.*"}, nil))
	if none.HiddenEveryTable {
		t.Error("no table to remove: the rules did not empty the copy")
	}

	// No rules: nothing changes, and the events view is not wrapped.
	in2 := Input{Baselines: []BaselineTable{{Schema: "shop", Table: "orders"}}}
	ApplyHidden(&in2, nil)
	ApplyHidden(&in2, mustRules(t, nil, nil))
	if len(in2.Baselines) != 1 || in2.Baselines[0].HiddenColumns != nil || in2.Hidden != nil {
		t.Fatalf("empty rules changed the input: %+v", in2)
	}
}

func hiddenFixture(t *testing.T, set *hiderules.Set, follow bool) *sql.DB {
	t.Helper()
	archiveRoot := t.TempDir()
	baselineRoot := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	writeFixtureArchive(t, archiveRoot, id)
	baselinePath := writeFixtureBaseline(t, baselineRoot)
	in := Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		ArchiveSources:   []string{filepath.Join(archiveRoot, "bintrail_id="+id)},
		BaselineSource:   baselineRoot,
		BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Baselines:        []BaselineTable{{Schema: "shop", Table: "orders", Path: baselinePath, Rel: "shop/orders.parquet"}},
	}
	if follow {
		in.Follow = FollowNewest
		marker := filepath.Join(baselineRoot, "2026-04-30T03-00-00Z", baseline.SuccessMarker)
		if err := os.WriteFile(marker, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ApplyHidden(&in, set)
	return execViews(t, Generate(in))
}

func columnsOf(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	return cols
}

// The fixture table is shop.orders(id, status); the archive holds one UPDATE
// of it with changed_columns ["status"].
func TestHiddenColumnLeavesTheStateView(t *testing.T) {
	for _, follow := range []bool{false, true} {
		db := hiddenFixture(t, mustRules(t, nil, []string{"SHOP.Orders.STATUS"}), follow)
		if got := columnsOf(t, db, "SELECT * FROM shop.orders"); !slices.Equal(got, []string{"id"}) {
			t.Errorf("follow=%v: SELECT * columns = %q, want [id]", follow, got)
		}
		var n int
		if err := db.QueryRow("SELECT count(*) FROM shop.orders").Scan(&n); err != nil || n != 2 {
			t.Errorf("follow=%v: row count = %d, %v; hiding a column must keep the rows", follow, n, err)
		}
		if _, err := db.Exec("SELECT status FROM shop.orders"); err == nil {
			t.Errorf("follow=%v: naming the hidden column must fail", follow)
		}
	}
}

func TestHiddenColumnLeavesTheEventsView(t *testing.T) {
	db := hiddenFixture(t, mustRules(t, nil, []string{"shop.orders.status"}), false)
	var before, after, changed string
	var qt sql.NullString
	if err := db.QueryRow(`SELECT row_before::VARCHAR, row_after::VARCHAR, changed_columns::VARCHAR, query_text FROM events`).
		Scan(&before, &after, &changed, &qt); err != nil {
		t.Fatal(err)
	}
	for name, img := range map[string]string{"row_before": before, "row_after": after, "changed_columns": changed} {
		if strings.Contains(strings.ToLower(img), "status") || strings.Contains(img, "paid") || strings.Contains(img, "new") {
			t.Errorf("%s still carries the hidden column: %s", name, img)
		}
	}
	if !strings.Contains(after, `"id"`) {
		t.Errorf("row_after lost the visible column: %s", after)
	}
	if qt.Valid {
		t.Errorf("query_text must be NULL on a server with rules, got %q", qt.String)
	}
}

func TestHiddenTableLeavesBothViews(t *testing.T) {
	db := hiddenFixture(t, mustRules(t, []string{"shop.*"}, nil), false)
	if _, err := db.Exec("SELECT * FROM shop.orders"); err == nil {
		t.Error("a hidden table must have no view")
	}
	var n int
	if err := db.QueryRow("SELECT count(*) FROM events").Scan(&n); err != nil || n != 0 {
		t.Errorf("events rows of a hidden table = %d, %v; want 0", n, err)
	}
}

// Rules that touch other tables leave this one exactly as it was, except the
// statement text, which can name any table. The rules sit in the SAME schema
// and name the SAME column, so a rule matched on the schema alone would
// strip it here.
func TestUnrelatedRulesLeaveRowsAlone(t *testing.T) {
	db := hiddenFixture(t, mustRules(t, []string{"shop.customers"}, []string{"shop.customers.status", "shop.ord.status"}), false)
	if got := columnsOf(t, db, "SELECT * FROM shop.orders"); !slices.Equal(got, []string{"id", "status"}) {
		t.Errorf("SELECT * columns = %q, want [id status]", got)
	}
	var after string
	if err := db.QueryRow(`SELECT row_after::VARCHAR FROM events`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(after, `"status":"paid"`) {
		t.Errorf("row_after of an unruled table changed: %s", after)
	}
}

// The statement text and its digest are blanked on a server with ANY rule:
// one statement can name several tables and carries their literals.
func TestStatementTextIsBlankedUnderAnyRule(t *testing.T) {
	archiveRoot := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	path := filepath.Join(archiveRoot, "bintrail_id="+id, "event_date=2026-05-01", "event_hour=03", "events.parquet")
	w, err := baseline.NewWriter(path, archive.BinlogEventColumns, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	values := []string{
		"1", "binlog.000001", "100", "200", "2026-05-01 03:00:00", "",
		"42", "shop", "orders", "2", "1",
		`["status"]`, `{"id":1,"status":"new"}`, `{"id":1,"status":"paid"}`,
		"1", "UPDATE shop.orders o JOIN shop.customers c SET o.status='paid' WHERE c.email='a@x'", "deadbeef", "1777000000000000",
	}
	nulls := make([]bool, len(archive.BinlogEventColumns))
	nulls[5] = true // gtid
	if err := w.WriteRow(values, nulls); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	gen := func(set *hiderules.Set) *sql.DB {
		in := Input{
			GeneratedAt:    time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
			Version:        "test",
			ArchiveSources: []string{filepath.Join(archiveRoot, "bintrail_id="+id)},
		}
		ApplyHidden(&in, set)
		return execViews(t, Generate(in))
	}
	var qt, qh sql.NullString
	if err := gen(nil).QueryRow(`SELECT query_text, query_hash FROM events`).Scan(&qt, &qh); err != nil || !qt.Valid || !qh.Valid {
		t.Fatalf("fixture: without rules the text must be there: %v %v %v", qt, qh, err)
	}
	if err := gen(mustRules(t, nil, []string{"shop.customers.email"})).QueryRow(`SELECT query_text, query_hash FROM events`).Scan(&qt, &qh); err != nil {
		t.Fatal(err)
	}
	if qt.Valid || qh.Valid {
		t.Errorf("query_text=%v query_hash=%v, want both NULL under any rule", qt, qh)
	}
}

// A column rule on a table hides its pk_values too: the key may be the
// hidden column, and pk_values cannot say which columns it holds.
func TestHiddenColumnBlanksPKValues(t *testing.T) {
	db := hiddenFixture(t, mustRules(t, nil, []string{"shop.orders.id"}), false)
	var pk sql.NullString
	if err := db.QueryRow(`SELECT pk_values FROM events`).Scan(&pk); err != nil {
		t.Fatal(err)
	}
	if pk.Valid {
		t.Errorf("pk_values = %q, want NULL under a column rule on the table", pk.String)
	}
	db = hiddenFixture(t, mustRules(t, nil, []string{"shop.customers.id"}), false)
	if err := db.QueryRow(`SELECT pk_values FROM events`).Scan(&pk); err != nil || !pk.Valid {
		t.Errorf("pk_values of an unruled table = %v, %v; want kept", pk, err)
	}
}

// A table whose every column is hidden is hidden: a view with no column
// fails at CREATE, which would break every statement that installs every view.
func TestEveryColumnHiddenHidesTheTable(t *testing.T) {
	in := Input{Baselines: []BaselineTable{
		{Schema: "shop", Table: "orders", Columns: []string{"id", "status"}},
		{Schema: "shop", Table: "tags", Columns: []string{"ID"}},
	}}
	ApplyHidden(&in, mustRules(t, nil, []string{"shop.*.id"}))
	if len(in.Baselines) != 1 || in.Baselines[0].Table != "orders" {
		t.Fatalf("Baselines = %+v, want only orders", in.Baselines)
	}
}

package views

import (
	"database/sql"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/baseline"
)

// writeNarrowFixtureArchive writes an archived partition the way a build that
// predates a column would have: the REAL writer, over a PREFIX of the real
// column set.
//
// The narrow shape is what makes union_by_name load-bearing on this layout and
// is therefore the only fixture that can tell a correct grouping from a wrong
// one. A fixture where every file has every column passes under any scheme,
// including the two DuckDB behaviours #1535 measured and rejected (silently
// dropping a column with the narrow file first, failing the read with the wide
// file first).
func writeNarrowFixtureArchive(t *testing.T, root, id, date, hour string, drop int) []string {
	t.Helper()
	cols := slices.Clone(archive.BinlogEventColumns)
	cols = cols[:len(cols)-drop]
	path := filepath.Join(root, "bintrail_id="+id, "event_date="+date, "event_hour="+hour, "events.parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("archive writer: %v", err)
	}
	values := []string{
		"7", "binlog.000001", "100", "200", "2026-05-01 " + hour + ":00:00", "",
		"42", "shop", "orders", "1", "7",
		`["status"]`, `{"id":7}`, `{"id":7,"status":"paid"}`,
		"1", "UPDATE orders SET status='paid'", "abc123", "1777000000000000",
	}
	nulls := make([]bool, len(cols))
	nulls[5] = true // gtid
	if err := w.WriteRow(values[:len(cols)], nulls); err != nil {
		t.Fatalf("write archive row: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close archive writer: %v", err)
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

// TestGroupedEvents_executeInDuckDB is the discriminating test for #1535: two
// groups whose column sets DIFFER, executed by the real engine, queried for the
// column only one of them has.
//
// A generator test cannot catch this. The two failure modes the issue measured
// are both SILENT or engine-side: reading both groups' files in one
// union_by_name = false scan either drops query_text from the result with no
// error at all (narrow file first) or fails at read time (wide file first).
// Only running the SQL distinguishes them, and only a fixture where one file
// genuinely lacks the column makes either reachable.
func TestGroupedEvents_executeInDuckDB(t *testing.T) {
	root := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	// Two generations of the same source: an old partition written before
	// query_text/query_hash/commit_ts_us existed, and a current one.
	oldCols := writeNarrowFixtureArchive(t, root, id, "2026-04-01", "01", 3)
	newCols := writeNarrowFixtureArchive(t, root, id, "2026-05-01", "03", 0)
	if len(oldCols) == len(newCols) {
		t.Fatal("the fixture's two groups have the same column set, so it cannot discriminate")
	}

	base := filepath.Join(root, "bintrail_id="+id)
	in := Input{
		GeneratedAt:    time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:        "test",
		ArchiveSources: []string{base},
		ArchiveGroups: []ArchiveGroup{
			{Columns: lower(oldCols), Files: []string{filepath.Join(base, "event_date=2026-04-01", "event_hour=01", "events.parquet")}},
			{Columns: lower(newCols), Files: []string{filepath.Join(base, "event_date=2026-05-01", "event_hour=03", "events.parquet")}},
		},
	}
	sqlText := Generate(in)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(sqlText); err != nil {
		t.Fatalf("DuckDB rejected the grouped views:\n%v\n\n--- generated ---\n%s", err, sqlText)
	}

	// Both groups' rows are in the view, each attributed to its own partition.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 2 {
		t.Fatalf("events has %d row(s), want 2 — one group is missing from the union", n)
	}

	// The column only the NEW group has: present on its row, NULL on the old
	// one. Both halves matter. A wrong grouping that drops the column entirely
	// still returns two rows and still has query_text = NULL on both.
	rows, err := db.Query(`SELECT CAST("event_date" AS VARCHAR), "query_text" FROM events ORDER BY 1`)
	if err != nil {
		t.Fatalf("query query_text: %v", err)
	}
	defer rows.Close()
	got := map[string]sql.NullString{}
	for rows.Next() {
		var d string
		var q sql.NullString
		if err := rows.Scan(&d, &q); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[d] = q
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if q := got["2026-04-01"]; q.Valid {
		t.Errorf("the old partition reports query_text = %q; its file does not have the column, so it must read NULL", q.String)
	}
	if q := got["2026-05-01"]; !q.Valid || q.String != "UPDATE orders SET status='paid'" {
		t.Errorf("the new partition's query_text = %+v, want the statement it stored — "+
			"a group that reads its files without its own column set loses the column silently", q)
	}

	// commit_time is derived from commit_ts_us, which the old group lacks. The
	// derived column has to be padded too, or the two legs project a different
	// number of columns and the UNION lines the wrong pairs up.
	var ct sql.NullTime
	if err := db.QueryRow(`SELECT "commit_time" FROM events WHERE CAST("event_date" AS VARCHAR) = '2026-04-01'`).Scan(&ct); err != nil {
		t.Fatalf("query commit_time: %v", err)
	}
	if ct.Valid {
		t.Errorf("the old partition reports commit_time = %s from a file with no commit_ts_us", ct.Time)
	}

	// Not cosmetic: union_by_name = true here is the 114s bind the issue
	// measured, and it would still pass every assertion above.
	if strings.Contains(sqlText, "union_by_name = true") {
		t.Errorf("a grouped events view still asks for union_by_name; the bind stays O(archived files):\n%s", sqlText)
	}
}

func lower(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = strings.ToLower(n)
	}
	return out
}

// The grouped file names its archives explicitly, so it stops covering anything
// rotated after it was written. The globbed form does not: DuckDB expands the
// glob on every statement.
//
// That difference is invisible in the result — a query over the most recent
// hours just returns nothing for them — so the file has to say which of the two
// it is. The header used to assert the globbed behaviour unconditionally, which
// on a grouped file is a sentence telling the reader the exact opposite of what
// will happen.
func TestGeneratedSQL_saysWhetherItsFileListIsFixed(t *testing.T) {
	base := "/arc/bintrail_id=aaaa"
	globbed := Generate(Input{ArchiveSources: []string{base}})
	grouped := Generate(Input{
		ArchiveSources: []string{base},
		ArchiveGroups: []ArchiveGroup{{
			Columns: []string{"event_id"},
			Files:   []string{base + "/event_date=2026-05-01/event_hour=03/e.parquet"},
		}},
	})

	if !strings.Contains(globbed, "globs below") ||
		!strings.Contains(globbed, "keep picking up newly rotated partitions") {
		t.Errorf("the globbed file no longer states that it follows the layout:\n%s", globbed)
	}
	if strings.Contains(grouped, "keep picking up newly rotated partitions") {
		t.Errorf("the grouped file claims it follows the layout; its file list is fixed:\n%s", grouped)
	}
	for _, want := range []string{
		"NOTHING here updates itself",
		"no error and no warning",
		"schedule your rotation archives on",
	} {
		if !strings.Contains(grouped, want) {
			t.Errorf("the grouped file does not warn that its list is frozen (missing %q):\n%s", want, grouped)
		}
	}
	// And the events view repeats it where the list actually is, because that
	// is what a reader scrolls to when a recent query comes back empty.
	if !strings.Contains(grouped, "The file list is FIXED") {
		t.Errorf("the events view does not state that its file list is fixed:\n%s", grouped)
	}
}

// A registered partition whose file is gone is a MODELED state: reconcile
// reports it and only an explicit --prune clears it. A glob does not match the
// missing file and the query returns the rest; an explicit path list makes
// DuckDB fail the whole read_parquet, and a view binds eagerly, so that failure
// takes down every statement in the script — the events view and every state
// view after it.
//
// Executed rather than asserted on text: which of the two DuckDB does is the
// entire finding, and only the engine can be asked.
func TestGroupedEvents_aMissingFileWouldBreakEveryView(t *testing.T) {
	root := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	writeNarrowFixtureArchive(t, root, id, "2026-05-01", "03", 0)
	base := filepath.Join(root, "bintrail_id="+id)
	present := filepath.Join(base, "event_date=2026-05-01", "event_hour=03", "events.parquet")
	gone := filepath.Join(base, "event_date=2026-04-01", "event_hour=01", "events.parquet")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// What the generator must never emit: a group listing a file that is not
	// there. Proven here so the guard in query.ArchiveGroups has a stated
	// consequence rather than a plausible one.
	grouped := Generate(Input{
		ArchiveSources: []string{base},
		ArchiveGroups:  []ArchiveGroup{{Columns: lower(archiveColumnNames()), Files: []string{present, gone}}},
	})
	if _, err := db.Exec(grouped); err == nil {
		t.Fatal("DuckDB accepted a file list naming a file that does not exist; " +
			"the guard in query.ArchiveGroups would then be protecting against nothing")
	} else if !strings.Contains(err.Error(), "No files found") {
		t.Fatalf("unexpected failure shape: %v", err)
	}

	// The globbed form over the same layout is unaffected: it never names the
	// missing file. This is what the fallback preserves.
	globbed := Generate(Input{ArchiveSources: []string{base}})
	if _, err := db.Exec(globbed); err != nil {
		t.Fatalf("the globbed form failed over a layout with a registered-but-missing file: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("globbed events: n=%d err=%v, want the surviving row", n, err)
	}
}

func archiveColumnNames() []string {
	out := make([]string, len(archive.BinlogEventColumns))
	for i, c := range archive.BinlogEventColumns {
		out[i] = c.Name
	}
	return out
}

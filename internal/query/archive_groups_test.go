package query

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	drivermysql "github.com/go-sql-driver/mysql"
)

const (
	wide   = "commit_ts_us,event_id,query_text"
	narrow = "event_id"
)

// touch creates an empty file at path, making it listable. ArchiveGroups stats
// every LOCAL path it would put in a group: DuckDB fails an explicit
// read_parquet list on one absent entry, where a glob simply does not match it,
// so a row whose file is gone disqualifies grouping instead of poisoning it.
func touch(t *testing.T, path string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// groupRows builds the archive_state read ArchiveGroups performs.
func groupRows(rows ...[4]any) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"bintrail_id", "local_path", "s3_key", "column_set"})
	for _, row := range rows {
		r.AddRow(row[0], row[1], row[2], row[3])
	}
	return r
}

// The grouping itself: two column sets over four partitions become two groups,
// each naming its own files under the base the caller routed that source to.
func TestArchiveGroups_groupsByColumnSet(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const id = "aaaa"
	root := t.TempDir()
	p := func(d, h string) string {
		return touch(t, filepath.Join(root, "bintrail_id="+id, "event_date="+d, "event_hour="+h, "e.parquet"))
	}
	mock.ExpectQuery("FROM archive_state").WillReturnRows(groupRows(
		[4]any{id, p("2026-05-01", "03"), nil, wide},
		[4]any{id, p("2026-04-01", "01"), nil, narrow},
		[4]any{id, p("2026-04-01", "02"), nil, narrow},
		[4]any{id, p("2026-05-01", "04"), nil, wide},
	))

	groups, ungrouped, err := ArchiveGroups(context.Background(), db, []string{filepath.Join(root, "bintrail_id="+id)})
	if err != nil {
		t.Fatal(err)
	}
	if ungrouped != 0 {
		t.Fatalf("ungrouped = %d, want 0", ungrouped)
	}
	if len(groups) != 2 {
		t.Fatalf("%d group(s), want 2 (one per column set)", len(groups))
	}
	// Sorted by the set string, so "commit_ts_us,…" sorts before "event_id".
	if want := []string{"commit_ts_us", "event_id", "query_text"}; !reflect.DeepEqual(groups[0].Columns, want) {
		t.Errorf("group 0 columns = %v, want %v", groups[0].Columns, want)
	}
	if want := []string{p("2026-05-01", "03"), p("2026-05-01", "04")}; !reflect.DeepEqual(groups[0].Files, want) {
		t.Errorf("group 0 files = %v, want %v", groups[0].Files, want)
	}
	if want := []string{p("2026-04-01", "01"), p("2026-04-01", "02")}; !reflect.DeepEqual(groups[1].Files, want) {
		t.Errorf("group 1 files = %v, want %v", groups[1].Files, want)
	}
}

// One unrecorded partition disqualifies the whole grouping, and the count says
// how many. This is the invariant that keeps the file list from silently
// narrowing what the view reads: the caller only groups when the registry can
// account for EVERY partition, and otherwise keeps the glob.
func TestArchiveGroups_oneUnrecordedPartitionIsReported(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const id = "aaaa"
	base := filepath.Join(t.TempDir(), "bintrail_id="+id)
	touch(t, base+"/event_date=2026-05-01/event_hour=03/e.parquet")
	touch(t, base+"/event_date=2026-05-01/event_hour=04/e.parquet")
	touch(t, base+"/event_date=2026-05-01/event_hour=05/e.parquet")
	mock.ExpectQuery("FROM archive_state").WillReturnRows(groupRows(
		[4]any{id, base + "/event_date=2026-05-01/event_hour=03/e.parquet", nil, wide},
		[4]any{id, base + "/event_date=2026-05-01/event_hour=04/e.parquet", nil, nil},
		// An empty string is the same "not recorded" as NULL: a row an older
		// repair wrote with a blank value must not form a group of its own,
		// whose read_parquet would then name no columns at all.
		[4]any{id, base + "/event_date=2026-05-01/event_hour=05/e.parquet", nil, ""},
	))

	groups, ungrouped, err := ArchiveGroups(context.Background(), db, []string{base})
	if err != nil {
		t.Fatal(err)
	}
	if ungrouped != 2 {
		t.Errorf("ungrouped = %d, want 2 (a NULL and an empty column set)", ungrouped)
	}
	// The recorded ones are still returned; the CALLER decides not to use them.
	// Returning nothing here would hide the count's meaning behind an empty
	// slice and make the two states indistinguishable to a test.
	if len(groups) != 1 {
		t.Errorf("%d group(s), want the one recorded set", len(groups))
	}
}

// Each row's file is rebuilt under the base ITS OWN source was routed to, and
// the location matching that base's scheme is preferred. A row carrying both
// locations must not put an s3:// tail under a local root or the reverse.
func TestArchiveGroups_rebuildsUnderTheRoutedBase(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root := t.TempDir()
	locFile := touch(t, filepath.Join(root, "bintrail_id=locsrc", "event_date=2026-05-01", "event_hour=03", "e.parquet"))
	mock.ExpectQuery("FROM archive_state").WillReturnRows(groupRows(
		// Routed to S3: the s3_key tail wins even though a local path exists.
		// The local file is deliberately NOT created — an s3:// base is taken
		// on the registry's word, so this row must still be grouped.
		[4]any{"s3src", "/local/bintrail_id=s3src/event_date=2026-05-01/event_hour=03/local.parquet",
			"deep/prefix/bintrail_id=s3src/event_date=2026-05-01/event_hour=03/remote.parquet", wide},
		// Routed locally: the local tail wins.
		[4]any{"locsrc", locFile,
			"k/bintrail_id=locsrc/event_date=2026-05-01/event_hour=03/other.parquet", wide},
		// A source the caller did not route at all is not this function's to
		// include, and must not count as unrecorded either.
		[4]any{"unrouted", "/arc/bintrail_id=unrouted/event_date=2026-05-01/event_hour=03/e.parquet", nil, wide},
	))

	groups, ungrouped, err := ArchiveGroups(context.Background(), db,
		[]string{"s3://bkt/deep/prefix/bintrail_id=s3src", filepath.Join(root, "bintrail_id=locsrc")})
	if err != nil {
		t.Fatal(err)
	}
	if ungrouped != 0 {
		t.Fatalf("ungrouped = %d, want 0", ungrouped)
	}
	want := []string{
		locFile,
		"s3://bkt/deep/prefix/bintrail_id=s3src/event_date=2026-05-01/event_hour=03/remote.parquet",
	}
	if len(groups) != 1 || !reflect.DeepEqual(groups[0].Files, want) {
		t.Errorf("files = %v, want %v", groups, want)
	}
}

// A registry row whose path escapes its own source root is refused rather than
// normalized, and counts as unrecorded so the caller falls back to the glob.
// reconcile inserts rows from what it scanned, so this value is not
// hand-audited anywhere upstream.
func TestArchiveGroups_refusesATraversalPath(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM archive_state").WillReturnRows(groupRows(
		[4]any{"aaaa", "/arc/bintrail_id=aaaa/../../etc/passwd.parquet", nil, wide},
	))
	groups, ungrouped, err := ArchiveGroups(context.Background(), db, []string{"/arc/bintrail_id=aaaa"})
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 0 || ungrouped != 1 {
		t.Errorf("groups = %v, ungrouped = %d; want the row refused and counted", groups, ungrouped)
	}
}

// An index that predates the column is not an error: the caller keeps the
// globbed leg it has always emitted. Same contract ResolveArchiveSources keeps
// for a missing archive_state table.
func TestArchiveGroups_toleratesAnUnmigratedIndex(t *testing.T) {
	for _, tc := range []struct {
		name string
		code uint16
	}{
		{"no archive_state table", 1146},
		{"no column_set column", 1054},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("FROM archive_state").WillReturnError(&drivermysql.MySQLError{Number: tc.code})
			groups, ungrouped, err := ArchiveGroups(context.Background(), db, []string{"/arc/bintrail_id=aaaa"})
			if err != nil || groups != nil || ungrouped != 0 {
				t.Errorf("groups=%v ungrouped=%d err=%v; want a clean empty result", groups, ungrouped, err)
			}
		})
	}

	// Any OTHER read failure IS an error. Swallowing it would silently drop to
	// the slow bind with nothing said, which is the same class of quiet
	// degradation #383 closed on the source list.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM archive_state").WillReturnError(&drivermysql.MySQLError{Number: 1142})
	if _, _, err := ArchiveGroups(context.Background(), db, []string{"/arc/bintrail_id=aaaa"}); err == nil {
		t.Error("a permission failure was swallowed")
	}
}

// A REGISTERED ROW WHOSE FILE IS GONE is a modeled state here: `archive
// reconcile` reports it and only an explicit --prune clears it. A glob does not
// match the missing file and the query returns the rest; an explicit path list
// makes DuckDB fail the entire read_parquet, and since a view binds eagerly
// that failure takes down every statement in the generated script.
//
// So a local path we cannot stat disqualifies grouping exactly like an
// unrecorded column set does, and the globbed leg — which tolerates it — stays.
func TestArchiveGroups_aRegisteredFileThatIsGoneDisqualifiesGrouping(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const id = "aaaa"
	base := filepath.Join(t.TempDir(), "bintrail_id="+id)
	here := touch(t, base+"/event_date=2026-05-01/event_hour=03/e.parquet")
	gone := base + "/event_date=2026-05-01/event_hour=04/e.parquet"
	mock.ExpectQuery("FROM archive_state").WillReturnRows(groupRows(
		[4]any{id, here, nil, wide},
		[4]any{id, gone, nil, wide},
	))

	groups, ungrouped, err := ArchiveGroups(context.Background(), db, []string{base})
	if err != nil {
		t.Fatal(err)
	}
	if ungrouped != 1 {
		t.Fatalf("ungrouped = %d, want 1 — the missing file must disqualify grouping, "+
			"not be listed in a group DuckDB then refuses to bind", ungrouped)
	}
	for _, g := range groups {
		for _, f := range g.Files {
			if f == gone {
				t.Fatalf("a file that is not on disk was put in a group: %s", f)
			}
		}
	}
}

// An s3:// base is taken on the registry's word. Probing it costs one request
// per row, which is the per-file cost this whole change exists to remove, so
// the trade is deliberate: the SQL panel carries a retry for the half that
// cannot be checked here.
func TestArchiveGroups_doesNotProbeS3(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM archive_state").WillReturnRows(groupRows(
		[4]any{"aaaa", nil, "k/bintrail_id=aaaa/event_date=2026-05-01/event_hour=03/e.parquet", wide},
	))
	groups, ungrouped, err := ArchiveGroups(context.Background(), db, []string{"s3://bkt/k/bintrail_id=aaaa"})
	if err != nil {
		t.Fatal(err)
	}
	if ungrouped != 0 || len(groups) != 1 {
		t.Fatalf("an S3 row was disqualified without being probed: groups=%v ungrouped=%d", groups, ungrouped)
	}
}

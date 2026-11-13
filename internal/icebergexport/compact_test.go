package icebergexport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// These pin the export's own compaction (#2719): the rows a reader sees are
// the same before and after, every delete file is gone, the cursor is
// untouched, the commit is a `replace`, and a compaction that is interrupted
// or loses a race leaves the table as the delta left it.

// nameMappingKey is the table property iceberg-go sets when it rewrites
// files (Iceberg's schema.name-mapping.default).
const nameMappingKey = "schema.name-mapping.default"

// fileCounts returns the live data and delete files of the current snapshot.
func fileCounts(t *testing.T, tbl *table.Table) (data, deletes int) {
	t.Helper()
	d, del, err := liveFiles(context.Background(), tbl)
	if err != nil {
		t.Fatal(err)
	}
	return len(d), len(del)
}

// threeDeltas builds a table with three commits: the load, an update + a
// delete + an insert, and another update. Three delete files' worth of keys
// are touched (the load writes one too, naming keys that did not exist).
func threeDeltas(t *testing.T) (catalog.Catalog, *table.Table, []column, cursor) {
	t.Helper()
	cat, tbl, cols := loadedTable(t)
	c2 := cursor{File: "binlog.000001", Pos: 200, At: t0.Add(time.Hour)}
	tbl = commit(t, tbl, cols, foldOps(t, ordersPK,
		ev(4, event.EventUpdate, "2", orderRow(2, "new", "20.00", "2026-08-28 12:00:00"), orderRow(2, "paid", "22.50", "2026-08-28 13:00:00")),
		ev(5, event.EventDelete, "3", orderRow(3, "new", "30.00", "2026-08-28 12:00:00"), nil),
		ev(6, event.EventInsert, "4", nil, orderRow(4, "new", "40.00", "2026-08-28 13:00:00")),
	), c2)
	c3 := cursor{File: "binlog.000001", Pos: 300, At: t0.Add(2 * time.Hour)}
	tbl = commit(t, tbl, cols, foldOps(t, ordersPK,
		ev(7, event.EventUpdate, "4", orderRow(4, "new", "40.00", "2026-08-28 13:00:00"), orderRow(4, "sent", "40.00", "2026-08-28 14:00:00")),
	), c3)
	return cat, tbl, cols, c3
}

func TestCompact_rewritesLiveRowsAndDropsEveryDeleteFile(t *testing.T) {
	cat, tbl, _, cur := threeDeltas(t)
	want := []string{"1=new", "2=paid", "4=sent"}
	equalRows(t, "before compaction", scanRows(t, tbl), want)
	if _, del := fileCounts(t, tbl); del != 3 {
		t.Fatalf("delete files before = %d, want 3 (one per commit)", del)
	}
	propsBefore := maps.Clone(tbl.Properties())
	parent := tbl.CurrentSnapshot().SnapshotID

	got, c := compactTable(context.Background(), tbl, 3, nil)
	if c == nil || c.Err != nil {
		t.Fatalf("compaction = %+v, want one that committed", c)
	}
	if c.DataFilesBefore != 3 || c.DeleteFilesBefore != 3 || c.DataFilesAfter != 1 || c.Rows != 3 {
		t.Fatalf("compaction = %+v, want 3 data + 3 delete files into 1 data file of 3 rows", c)
	}
	for _, tb := range []*table.Table{got, reload(t, cat)} {
		equalRows(t, "after compaction", scanRows(t, tb), want)
		if data, del := fileCounts(t, tb); data != 1 || del != 0 {
			t.Fatalf("files after = %d data, %d delete; want 1 and 0", data, del)
		}
		snap := tb.CurrentSnapshot()
		if snap.Summary.Operation != table.OpReplace {
			t.Fatalf("operation = %q, want replace (readers must see that the data did not change)", snap.Summary.Operation)
		}
		if snap.ParentSnapshotID == nil || *snap.ParentSnapshotID != parent {
			t.Fatalf("parent = %v, want the delta's snapshot %d", snap.ParentSnapshotID, parent)
		}
		if v := snap.Summary.Properties["total-delete-files"]; v != "0" {
			t.Fatalf("summary total-delete-files = %q, want 0", v)
		}
		if v := snap.Summary.Properties["total-records"]; v != "3" {
			t.Fatalf("summary total-records = %q, want 3", v)
		}
		if *c.SnapshotID != snap.SnapshotID {
			t.Fatalf("reported snapshot %d, table at %d", *c.SnapshotID, snap.SnapshotID)
		}
		// iceberg-go adds the standard name mapping on a rewrite; every
		// other property, the export's own above all, is untouched.
		after := maps.Clone(tb.Properties())
		if _, ok := after[nameMappingKey]; !ok {
			t.Fatalf("no %s after the rewrite; the test no longer knows what changes", nameMappingKey)
		}
		delete(after, nameMappingKey)
		if !maps.Equal(after, propsBefore) {
			t.Fatalf("properties changed:\n got %v\nwant %v", after, propsBefore)
		}
		if rc, err := readCursor(tb.Properties()); err != nil || rc == nil || *rc != cur {
			t.Fatalf("cursor = %v (%v), want %v", rc, err, cur)
		}
	}
	ddb := openDuckDBIceberg(t)
	equalRows(t, "duckdb after compaction", duckRows(t, ddb, got.Location()), want)
}

func TestCompact_belowTheThresholdOrOffDoesNothing(t *testing.T) {
	for _, threshold := range []int{0, -1, 4} {
		t.Run(fmt.Sprint(threshold), func(t *testing.T) {
			_, tbl, _, _ := threeDeltas(t)
			before := len(tbl.Metadata().Snapshots())
			got, c := compactTable(context.Background(), tbl, threshold, nil)
			if c != nil {
				t.Fatalf("compaction = %+v, want none", c)
			}
			if got != tbl || len(got.Metadata().Snapshots()) != before {
				t.Fatal("the table changed")
			}
		})
	}
}

func TestCompact_tableWithoutSnapshotIsLeftAlone(t *testing.T) {
	// A zero-row first load commits the table and its cursor and no data
	// file: there is no snapshot, and iceberg-go refuses to replace files in
	// one.
	_, tbl, _ := newTestTable(t, ordersCols, []string{"id"})
	if got, c := compactTable(context.Background(), tbl, 1, nil); c != nil || got != tbl {
		t.Fatalf("compaction = %+v, want none", c)
	}
}

func TestCompact_everyRowDeleted(t *testing.T) {
	// No live row: the old data files go and no new one comes. Then the
	// table must still take inserts.
	cat, tbl, cols := loadedTable(t)
	c2 := cursor{File: "binlog.000001", Pos: 200, At: t0.Add(time.Hour)}
	tbl = commit(t, tbl, cols, foldOps(t, ordersPK,
		ev(4, event.EventDelete, "1", orderRow(1, "new", "10.00", "2026-08-28 12:00:00"), nil),
		ev(5, event.EventDelete, "2", orderRow(2, "new", "20.00", "2026-08-28 12:00:00"), nil),
		ev(6, event.EventDelete, "3", orderRow(3, "new", "30.00", "2026-08-28 12:00:00"), nil),
	), c2)
	equalRows(t, "before", scanRows(t, tbl), nil)
	tbl, c := compactTable(context.Background(), tbl, 1, nil)
	if c == nil || c.Err != nil {
		t.Fatalf("compaction = %+v", c)
	}
	if c.DataFilesAfter != 0 || c.Rows != 0 {
		t.Fatalf("compaction = %+v, want no data file written", c)
	}
	if data, del := fileCounts(t, reload(t, cat)); data != 0 || del != 0 {
		t.Fatalf("files after = %d data, %d delete; want none", data, del)
	}
	equalRows(t, "after", scanRows(t, reload(t, cat)), nil)
	tbl = commit(t, tbl, cols, foldOps(t, ordersPK,
		ev(7, event.EventInsert, "1", nil, orderRow(1, "back", "1.00", "2026-08-28 14:00:00")),
	), cursor{File: "binlog.000001", Pos: 300, At: t0.Add(2 * time.Hour)})
	equalRows(t, "after a new insert", scanRows(t, tbl), []string{"1=back"})
}

func TestCompact_interruptedBeforeTheCommitLeavesTheTable(t *testing.T) {
	cat, tbl, _, cur := threeDeltas(t)
	snap := tbl.CurrentSnapshot().SnapshotID
	stop := errors.New("killed")
	got, c := compactTable(context.Background(), tbl, 1, func() error { return stop })
	if c == nil || !errors.Is(c.Err, stop) {
		t.Fatalf("compaction = %+v, want the interruption", c)
	}
	if c.SnapshotID != nil || got != tbl {
		t.Fatalf("an interrupted compaction reports snapshot %v", c.SnapshotID)
	}
	fresh := reload(t, cat)
	if fresh.CurrentSnapshot().SnapshotID != snap {
		t.Fatal("the table moved")
	}
	equalRows(t, "after the interruption", scanRows(t, fresh), []string{"1=new", "2=paid", "4=sent"})
	if _, del := fileCounts(t, fresh); del != 3 {
		t.Fatalf("delete files = %d, want the 3 the delta left", del)
	}
	if rc, _ := readCursor(fresh.Properties()); rc == nil || *rc != cur {
		t.Fatalf("cursor = %v, want %v", rc, cur)
	}
	// And the next try compacts normally.
	if _, c := compactTable(context.Background(), fresh, 1, nil); c == nil || c.Err != nil {
		t.Fatalf("retry = %+v", c)
	}
	equalRows(t, "after the retry", scanRows(t, reload(t, cat)), []string{"1=new", "2=paid", "4=sent"})
}

func TestCompact_anotherCommitUnderItIsRefused(t *testing.T) {
	// The compaction read the table, and then another writer committed a
	// delta. Applying the compaction would drop that delta's delete file and
	// resurrect the row it deleted; the guard must refuse it.
	cat, tbl, cols, cur := threeDeltas(t)
	other := cursor{File: "binlog.000001", Pos: 400, At: cur.At.Add(time.Hour)}
	got, c := compactTable(context.Background(), tbl, 1, func() error {
		commit(t, reload(t, cat), cols, foldOps(t, ordersPK,
			ev(8, event.EventDelete, "1", orderRow(1, "new", "10.00", "2026-08-28 12:00:00"), nil),
		), other)
		return nil
	})
	if c == nil || !errors.Is(c.Err, ErrConcurrentCommit) {
		t.Fatalf("compaction = %+v, want ErrConcurrentCommit", c)
	}
	if got != tbl {
		t.Fatal("a refused compaction returned another table")
	}
	fresh := reload(t, cat)
	equalRows(t, "after the refusal", scanRows(t, fresh), []string{"2=paid", "4=sent"})
	if rc, _ := readCursor(fresh.Properties()); rc == nil || *rc != other {
		t.Fatalf("cursor = %v, want the other writer's %v", rc, other)
	}
	if fresh.CurrentSnapshot().Summary.Operation == table.OpReplace {
		t.Fatal("the compaction landed")
	}
}

func TestCompact_commitThatLandedIsSuccess(t *testing.T) {
	// The catalog applied the compaction and then reported an error (a 5xx,
	// a timeout). The id in the snapshot summary makes it a success.
	for _, cerr := range []error{rest.ErrCommitStateUnknown, errors.New("read tcp: i/o timeout")} {
		t.Run(cerr.Error(), func(t *testing.T) {
			cat, _, _, _ := threeDeltas(t)
			g := guard(landsThenFails{Catalog: cat.(guardedCatalog).Catalog, err: cerr})
			got, c := compactTable(context.Background(), reload(t, g), 1, nil)
			if c == nil || c.Err != nil {
				t.Fatalf("a compaction that landed was reported as %+v", c)
			}
			if got.CurrentSnapshot().Summary.Operation != table.OpReplace {
				t.Fatalf("returned table is at %s", got.CurrentSnapshot().Summary.Operation)
			}
		})
	}
}

func TestCompact_anotherWritersCompactionIsNotMistakenForOurs(t *testing.T) {
	// A replace snapshot another writer made is not this compaction's, even
	// though it also changes nothing the cursor shows.
	cat, _, _, _ := threeDeltas(t)
	base := reload(t, cat)
	if _, c := compactTable(context.Background(), reload(t, cat), 1, nil); c == nil || c.Err != nil {
		t.Fatalf("first compaction = %+v", c)
	}
	b := baseOf(base.Metadata())
	b.compaction = "not-the-one"
	if b.landed(reload(t, cat).Metadata()) {
		t.Fatal("another compaction was taken for this one")
	}
}

// TestCompact_deltasAfterACompactionStillMatchTheirKeys is the one that
// matters most: after a compaction rewrote the rows, a later equality delete
// must still find each key in the new files. If the rewrite spelled a key
// differently (a decimal's scale, a timestamp's unit, binary bytes), the
// delete would match nothing and the old row would survive beside the new
// one, for good.
func TestCompact_deltasAfterACompactionStillMatchTheirKeys(t *testing.T) {
	bcols := []baseline.Column{
		{Name: "id", MySQLType: "bigint"},
		{Name: "code", MySQLType: "varbinary"},
		{Name: "amt", MySQLType: "decimal", DecimalPrecision: 10, DecimalScale: 2},
		{Name: "at", MySQLType: "datetime"},
		{Name: "status", MySQLType: "varchar"},
	}
	pk := []metadata.ColumnMeta{
		{Name: "id", IsPK: true, DataType: "bigint"},
		{Name: "code", IsPK: true, DataType: "varbinary"},
		{Name: "amt", IsPK: true, DataType: "decimal"},
		{Name: "at", IsPK: true, DataType: "datetime"},
	}
	cat, tbl, cols := newTestTable(t, bcols, []string{"id", "code", "amt", "at"})
	r := func(id int, code, amt, at, status string) map[string]any {
		return map[string]any{"id": json.Number(fmt.Sprint(id)), "code": []byte(code), "amt": json.Number(amt), "at": at, "status": status}
	}
	key := func(id int, code, amt, at string) string { return fmt.Sprintf("%d|%s|%s|%s", id, code, amt, at) }
	tbl = commit(t, tbl, cols, foldOps(t, pk,
		ev(1, event.EventInsert, key(1, "a\x00b", "1.50", "2026-08-28 12:00:00.123456"), nil, r(1, "a\x00b", "1.50", "2026-08-28 12:00:00.123456", "one")),
		ev(2, event.EventInsert, key(2, "\xff", "-2.00", "2026-08-28 12:00:01"), nil, r(2, "\xff", "-2.00", "2026-08-28 12:00:01", "two")),
		ev(3, event.EventInsert, key(3, "c", "0.01", "2026-08-28 12:00:02"), nil, r(3, "c", "0.01", "2026-08-28 12:00:02", "three")),
	), cursor{File: "binlog.000001", Pos: 100, At: t0})
	tbl = commit(t, tbl, cols, foldOps(t, pk,
		ev(4, event.EventUpdate, key(3, "c", "0.01", "2026-08-28 12:00:02"),
			r(3, "c", "0.01", "2026-08-28 12:00:02", "three"), r(3, "c", "0.01", "2026-08-28 12:00:02", "three!")),
	), cursor{File: "binlog.000001", Pos: 200, At: t0.Add(time.Hour)})

	before := keyCells(t, tbl)
	tbl, c := compactTable(context.Background(), tbl, 1, nil)
	if c == nil || c.Err != nil {
		t.Fatalf("compaction = %+v", c)
	}
	if got := keyCells(t, tbl); !maps.Equal(got, before) {
		t.Fatalf("key cells changed in the rewrite:\n got %v\nwant %v", got, before)
	}
	want := []string{"1=one", "2=two", "3=three!"}
	equalRows(t, "after compaction", scanRows(t, tbl), want)

	// Update the first key, delete the second: both rows live only in the
	// compacted file now.
	tbl = commit(t, tbl, cols, foldOps(t, pk,
		ev(5, event.EventUpdate, key(1, "a\x00b", "1.50", "2026-08-28 12:00:00.123456"),
			r(1, "a\x00b", "1.50", "2026-08-28 12:00:00.123456", "one"), r(1, "a\x00b", "1.50", "2026-08-28 12:00:00.123456", "uno")),
		ev(6, event.EventDelete, key(2, "\xff", "-2.00", "2026-08-28 12:00:01"), r(2, "\xff", "-2.00", "2026-08-28 12:00:01", "two"), nil),
	), cursor{File: "binlog.000001", Pos: 300, At: t0.Add(2 * time.Hour)})
	want = []string{"1=uno", "3=three!"}
	equalRows(t, "iceberg-go after a delta on the compacted rows", scanRows(t, tbl), want)
	equalRows(t, "iceberg-go reloaded", scanRows(t, reload(t, cat)), want)
	ddb := openDuckDBIceberg(t)
	equalRows(t, "duckdb after a delta on the compacted rows", duckRows(t, ddb, tbl.Location()), want)

	if got := keyCells(t, tbl); got["1"] != before["1"] || got["3"] != before["3"] || len(got) != 2 {
		t.Fatalf("key cells after the delta = %v, want rows 1 and 3 as before %v", got, before)
	}
}

// keyCells renders every row's key columns by id, as iceberg-go reads them.
func keyCells(t *testing.T, tbl *table.Table) map[string]string {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer at.Release()
	cells := map[string]string{}
	tr := array.NewTableReader(at, 1024)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		idx := map[string]int{}
		for i, f := range rec.Schema().Fields() {
			idx[f.Name] = i
		}
		for i := 0; i < int(rec.NumRows()); i++ {
			cells[rec.Column(idx["id"]).ValueStr(i)] = fmt.Sprintf("%q %s %s", rec.Column(idx["code"]).(*array.Binary).Value(i),
				rec.Column(idx["amt"]).ValueStr(i), rec.Column(idx["at"]).ValueStr(i))
		}
	}
	if len(cells) == 0 {
		t.Fatal("no rows read; the comparison would prove nothing")
	}
	return cells
}

func TestCompact_reportsTheCommit(t *testing.T) {
	// The outcome carries the compaction, the snapshot it left the table at,
	// and the note; the commit is reported to OnCommit as a compact commit.
	_, tbl, _, cur := threeDeltas(t)
	var commits []Commit
	d := &deps{cfg: Config{CompactAfter: 3, OnCommit: func(c Commit) { commits = append(commits, c) }}, mem: memory.DefaultAllocator}
	c := d.compact(context.Background(), "shop", "orders", tbl, cur)
	if c == nil || c.Err != nil {
		t.Fatalf("compaction = %+v", c)
	}
	if len(commits) != 1 || commits[0].Kind != CommitCompact || commits[0].Rows != 3 || commits[0].SnapshotID == nil || *commits[0].SnapshotID != *c.SnapshotID {
		t.Fatalf("commits = %+v", commits)
	}
	if commits[0].Cursor != cur.String() {
		t.Fatalf("commit cursor = %q, want the unchanged %q", commits[0].Cursor, cur.String())
	}
	if n := c.note(); n != "; compacted 3 data files and 3 delete files into 1 data file (3 rows)" {
		t.Fatalf("note = %q", n)
	}
	for _, tc := range []struct {
		err  error
		want string
	}{
		{fmt.Errorf("commit the compaction: %w", ErrConcurrentCommit), "; compaction not applied: another writer committed"},
		{fmt.Errorf("commit the compaction: %w: %w", errCommitUnknown, rest.ErrCommitStateUnknown), "; the catalog could not say whether the compaction was applied"},
		{errors.New("disk full"), "; compaction failed, the table is as this run's delta left it and the next run tries again: disk full"},
	} {
		n := (&Compaction{Err: tc.err}).note()
		if len(n) < len(tc.want) || n[:len(tc.want)] != tc.want {
			t.Fatalf("note for %v = %q, want it to start %q", tc.err, n, tc.want)
		}
	}
}

// dataDirFiles counts the files under the table's data directory.
func dataDirFiles(t *testing.T, tbl *table.Table) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(filepath.Join(strings.TrimPrefix(tbl.Location(), "file://"), "data"), func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCompact_filesOfAFailedCompactionAreRemoved(t *testing.T) {
	// A compaction certain not to have committed leaves no copy of the
	// table behind: under --every a table that keeps failing would
	// otherwise gain a whole copy per run.
	for _, name := range []string{"interrupted", "refused"} {
		t.Run(name, func(t *testing.T) {
			cat, tbl, cols, cur := threeDeltas(t)
			var before int
			_, c := compactTable(context.Background(), tbl, 1, func() error {
				if name == "refused" {
					commit(t, reload(t, cat), cols, nil, cursor{File: cur.File, Pos: cur.Pos + 1, At: cur.At.Add(time.Minute)})
				}
				before = dataDirFiles(t, tbl)
				if name == "interrupted" {
					return errors.New("killed")
				}
				return nil
			})
			if c == nil || c.Err == nil || c.Status() != CompactionNotApplied {
				t.Fatalf("compaction = %+v, want not applied", c)
			}
			if after := dataDirFiles(t, tbl); after != before-1 {
				t.Fatalf("data files on disk %d -> %d, want the one compacted file removed", before, after)
			}
			equalRows(t, "after", scanRows(t, reload(t, cat)), []string{"1=new", "2=paid", "4=sent"})
		})
	}
}

func TestCompact_unreadableCatalogErrorIsUnknown(t *testing.T) {
	// The catalog answers the commit with a plain error. The commit may or
	// may not have gone through, so the report must not say "not applied",
	// and the files it wrote are kept.
	cat, _, _, _ := threeDeltas(t)
	g := guard(failingCatalog{Catalog: cat.(guardedCatalog).Catalog, err: errors.New("connection reset")})
	tbl := reload(t, g)
	before := dataDirFiles(t, tbl)
	_, c := compactTable(context.Background(), tbl, 1, nil)
	if c == nil || c.Status() != CompactionUnknown {
		t.Fatalf("compaction = %+v, want status unknown", c)
	}
	if n := c.note(); !strings.Contains(n, "could not say whether the compaction was applied") {
		t.Fatalf("note = %q", n)
	}
	if after := dataDirFiles(t, tbl); after != before+1 {
		t.Fatalf("data files on disk %d -> %d, want the compacted file kept", before, after)
	}
}

// landsThenOtherCommits applies the commit, lets another writer commit on
// top, and then reports an error.
type landsThenOtherCommits struct {
	catalog.Catalog
	other func()
}

func (l landsThenOtherCommits) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if _, _, err := l.Catalog.CommitTable(ctx, ident, reqs, updates); err != nil {
		return nil, "", err
	}
	l.other()
	return nil, "", errors.New("read tcp: i/o timeout")
}

func TestCompact_landedThenOvertakenIsApplied(t *testing.T) {
	cat, _, cols, cur := threeDeltas(t)
	raw := cat.(guardedCatalog).Catalog
	g := guard(landsThenOtherCommits{Catalog: raw, other: func() {
		commit(t, reload(t, cat), cols, nil, cursor{File: cur.File, Pos: cur.Pos + 1, At: cur.At.Add(time.Minute)})
	}})
	_, c := compactTable(context.Background(), reload(t, g), 1, nil)
	if c == nil || c.Err != nil {
		t.Fatalf("compaction = %+v, want applied", c)
	}
	s := reload(t, cat).Metadata().SnapshotByID(*c.SnapshotID)
	if s == nil || s.Summary.Operation != table.OpReplace {
		t.Fatalf("reported snapshot %d is %v, want the replace", *c.SnapshotID, s)
	}
}

// timesOutAfterOtherCommit lets another writer move the cursor, does not
// apply this commit, and times out: the guard calls it a conflict, but the
// request went out and could still land, so the new files must stay.
type timesOutAfterOtherCommit struct {
	catalog.Catalog
	other func()
}

func (c timesOutAfterOtherCommit) CommitTable(context.Context, table.Identifier, []table.Requirement, []table.Update) (table.Metadata, string, error) {
	c.other()
	return nil, "", errors.New("read tcp: i/o timeout")
}

func TestCompact_sentThenConflictKeepsTheFiles(t *testing.T) {
	cat, _, cols, cur := threeDeltas(t)
	g := guard(timesOutAfterOtherCommit{Catalog: cat.(guardedCatalog).Catalog, other: func() {
		commit(t, reload(t, cat), cols, nil, cursor{File: cur.File, Pos: cur.Pos + 1, At: cur.At.Add(time.Minute)})
	}})
	tbl := reload(t, g)
	before := dataDirFiles(t, tbl)
	_, c := compactTable(context.Background(), tbl, 1, nil)
	if c == nil || !errors.Is(c.Err, ErrConcurrentCommit) {
		t.Fatalf("compaction = %+v, want ErrConcurrentCommit", c)
	}
	if after := dataDirFiles(t, tbl); after != before+1 {
		t.Fatalf("data files on disk %d -> %d, want the compacted file kept", before, after)
	}
}

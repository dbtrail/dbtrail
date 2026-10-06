package reconstruct

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

func bp(f string, p uint64) *query.BinlogPos { return &query.BinlogPos{File: f, Pos: p} }

// writeRecord2152 writes a snapshot directory's archive record the way a run
// does: through archiveCuts.record and write.
func writeRecord2152(t *testing.T, dir string, entries map[string]*query.BinlogPos) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	c := newArchiveCuts(false)
	for k, v := range entries {
		s, tb, _ := splitSchemaTable(k)
		c.record(s, tb, true, v, nil)
	}
	if err := c.write(dir); err != nil {
		t.Fatal(err)
	}
}

// How far a table was checked through the archives, read back from the
// snapshot directory it is read from (#2152): per table, from the record the
// run that published the directory wrote.
func TestArchiveCuts_forBaseline_2152(t *testing.T) {
	root := t.TempDir()
	snap := filepath.Join(root, "2026-03-01T10-00-00Z")
	writeRecord2152(t, snap, map[string]*query.BinlogPos{
		"shop.orders": bp("binlog.000001", 4),
		"shop.items":  bp("binlog.000003", 77),
	})
	// A newer snapshot the table is missing from: its record does not speak
	// for orders.
	writeRecord2152(t, filepath.Join(root, "2026-03-02T10-00-00Z"), map[string]*query.BinlogPos{"shop.items": bp("binlog.000099", 1)})

	c := newArchiveCuts(false)
	if got := c.forBaseline(filepath.Join(snap, "shop", "orders.parquet"), "shop", "orders"); got == nil || *got != *bp("binlog.000001", 4) {
		t.Fatalf("orders = %+v, want its own value binlog.000001:4, never another table's", got)
	}
	if got := c.forBaseline(filepath.Join(snap, "shop", "items.parquet"), "shop", "items"); got == nil || *got != *bp("binlog.000003", 77) {
		t.Fatalf("items = %+v, want binlog.000003:77", got)
	}
	if got := c.forBaseline(filepath.Join(snap, "shop", "absent.parquet"), "shop", "absent"); got != nil {
		t.Fatalf("a table the record does not name = %+v, want nil", got)
	}
	// Read once per directory: a record rewritten afterwards is not seen.
	writeRecord2152(t, snap, map[string]*query.BinlogPos{"shop.orders": bp("binlog.000009", 1)})
	if got := c.forBaseline(filepath.Join(snap, "shop", "orders.parquet"), "shop", "orders"); got == nil || *got != *bp("binlog.000001", 4) {
		t.Fatalf("second read = %+v, want the cached binlog.000001:4", got)
	}
	if s3 := c.forBaseline("s3://bucket/x/2026-03-01T10-00-00Z/shop/orders.parquet", "shop", "orders"); s3 != nil {
		t.Fatalf("an S3 snapshot = %+v, want nil", s3)
	}
	if off := newArchiveCuts(true).forBaseline(filepath.Join(snap, "shop", "orders.parquet"), "shop", "orders"); off != nil {
		t.Fatalf("on an index `bintrail index` also wrote = %+v, want nil", off)
	}
}

func TestArchiveCuts_recordThatDoesNotRead_2152(t *testing.T) {
	for _, body := range []string{"not json", `{"version":2,"tables":{"shop.orders":{"binlog_file":"binlog.000001","start_pos":4}}}`,
		`{"version":1,"tables":{"shop.orders":{"binlog_file":"","start_pos":4}}}`, `{"version":1,"tables":{"shop.orders":{"binlog_file":"binlog.000001"}}}`} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, archiveCutFile), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if got := newArchiveCuts(false).forBaseline(filepath.Join(dir, "shop", "orders.parquet"), "shop", "orders"); got != nil {
			t.Fatalf("record %q gave %+v, want nil", body, got)
		}
	}
}

// What a run records for each table. A run that checked the archives records
// its cut. Any other run records, per table, the value the table was read
// with: never a value lifted from another table or another folder. This is
// the case of a run that could not read an archive and carried two tables
// forward from folders checked to different positions.
func TestArchiveCuts_record_2152(t *testing.T) {
	cut := bp("binlog.000005", 900)
	for _, tc := range []struct {
		name      string
		checked   bool
		cut       *query.BinlogPos
		inherited map[string]*query.BinlogPos
		want      map[string]*query.BinlogPos
	}{
		{"a run that checked: its cut for every table", true, cut,
			map[string]*query.BinlogPos{"shop.q": bp("binlog.000002", 10), "shop.a": bp("binlog.000004", 10)},
			map[string]*query.BinlogPos{"shop.q": cut, "shop.a": cut}},
		{"a run that did not check: each table keeps its own", false, cut,
			map[string]*query.BinlogPos{"shop.q": bp("binlog.000002", 10), "shop.a": bp("binlog.000004", 10)},
			map[string]*query.BinlogPos{"shop.q": bp("binlog.000002", 10), "shop.a": bp("binlog.000004", 10)}},
		{"a run that did not check, a table with no value: none", false, cut,
			map[string]*query.BinlogPos{"shop.q": nil},
			map[string]*query.BinlogPos{}},
		{"an empty index: no cut, each table keeps its own", true, nil,
			map[string]*query.BinlogPos{"shop.q": bp("binlog.000002", 10)},
			map[string]*query.BinlogPos{"shop.q": bp("binlog.000002", 10)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			c := newArchiveCuts(false)
			for k, inh := range tc.inherited {
				s, tb, _ := splitSchemaTable(k)
				c.record(s, tb, tc.checked, tc.cut, inh)
			}
			if err := c.write(dir); err != nil {
				t.Fatal(err)
			}
			got := readArchiveCutFile(dir)
			if len(got) != len(tc.want) {
				t.Fatalf("record = %+v, want %+v", got, tc.want)
			}
			for k, w := range tc.want {
				if g, ok := got[k]; !ok || g != *w {
					t.Fatalf("%s = %+v, want %+v", k, g, *w)
				}
			}
		})
	}
	// A backfilled index records nothing, and nothing is written.
	dir := t.TempDir()
	c := newArchiveCuts(true)
	c.record("shop", "q", true, cut, nil)
	if err := c.write(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, archiveCutFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a backfilled index wrote a record (stat err %v)", err)
	}
}

// Whether a run's own fetches checked the archives.
func TestRunChecksArchives_2152(t *testing.T) {
	for _, tc := range []struct {
		name       string
		backfilled bool
		allowGaps  bool
		archErr    error
		want       bool
	}{
		{"a stream-only index, sources found", false, false, nil, true},
		{"`bintrail index` also wrote: the cut does not bound what was indexed late", true, false, nil, false},
		{"--allow-gaps: a source it could not read may have been skipped", false, true, nil, false},
		{"archive discovery failed", false, false, errors.New("boom"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := runChecksArchives(tc.backfilled, tc.allowGaps, tc.archErr); got != tc.want {
				t.Fatalf("runChecksArchives = %v, want %v", got, tc.want)
			}
		})
	}
}

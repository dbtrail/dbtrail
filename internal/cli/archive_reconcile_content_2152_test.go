package cli

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/baseline"
)

func writeArchiveFile2152(t *testing.T, path string, rows [][3]string) {
	t.Helper()
	w, err := baseline.NewWriter(path, archive.BinlogEventColumns, baseline.WriterConfig{Compression: "none", RowGroupSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows { // event_id, binlog_file, start_pos
		vals := make([]string, len(archive.BinlogEventColumns))
		nulls := make([]bool, len(archive.BinlogEventColumns))
		for i := range nulls {
			nulls[i] = true
		}
		vals[0], nulls[0] = r[0], false
		vals[1], nulls[1] = r[1], false
		vals[2], nulls[2] = r[2], false
		vals[4], nulls[4] = "2026-03-01 02:30:00", false
		if err := w.WriteRow(vals, nulls); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func changeOf(a archive.Action, col string) (any, bool) {
	for _, c := range a.Changes {
		if c.Column == col {
			return c.Value, true
		}
	}
	return nil, false
}

// `archive reconcile --repair` registers a file whose archive_state row was
// lost. It records the file's content time range, read from the file, and
// NOT its newest change (#2152): a refresh skips an archive whose newest
// change is before the cut it last searched through, and a row lost from
// archive_state was never seen by any refresh, whatever its position. Left
// unrecorded, with archived_at = now, the row counts as one written after
// every older snapshot: each snapshot's next update reads it once.
func TestAddInsertContent_2152(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "p_2026030102.parquet")
	writeArchiveFile2152(t, good, [][3]string{{"5", "binlog.000002", "900"}, {"9", "binlog.000003", "4"}})
	missing := filepath.Join(dir, "p_2026030103.parquet") // never written

	actions := []archive.Action{
		{Kind: archive.ActionInsert, PartitionName: "p_2026030102", Changes: []archive.FieldChange{{Column: "local_path", Value: good}}},
		{Kind: archive.ActionUpdate, PartitionName: "p_2026030102", Changes: []archive.FieldChange{{Column: "local_path", Value: good}}},
		{Kind: archive.ActionInsert, PartitionName: "p_2026030103", Changes: []archive.FieldChange{{Column: "local_path", Value: missing}}},
		{Kind: archive.ActionInsert, PartitionName: "p_2026030104", Changes: []archive.FieldChange{
			{Column: "s3_bucket", Value: "b"}, {Column: "s3_key", Value: "k.parquet"}}},
	}
	got := addInsertContent(context.Background(), actions, false, "")

	for _, c := range []string{"max_event_id", "max_binlog_file", "max_start_pos"} {
		if v, ok := changeOf(got[0], c); ok {
			t.Fatalf("insert: %s = %v; a repaired row must leave the newest change unrecorded", c, v)
		}
	}
	want := time.Date(2026, 3, 1, 2, 30, 0, 0, time.UTC)
	if v, _ := changeOf(got[0], "min_event_ts"); v != want {
		t.Fatalf("insert: min_event_ts = %v, want %v", v, want)
	}
	if _, ok := changeOf(got[1], "min_event_ts"); ok {
		t.Fatal("an update gained the time range: only inserts are filled, so a dry run reports no new drift")
	}
	if len(got[2].Changes) != 1 {
		t.Fatalf("an unreadable file must register as before (no record = looked at): %+v", got[2].Changes)
	}
	if len(got[3].Changes) != 2 {
		t.Fatalf("an S3 object without --deep is not read: %+v", got[3].Changes)
	}
	for _, c := range []string{"min_event_ts", "max_event_ts"} {
		if !reconcileColumns[c] {
			t.Errorf("%s is not reconcile-writable: the repair would refuse the insert", c)
		}
	}
	for _, c := range []string{"max_event_id", "max_binlog_file", "max_start_pos"} {
		if reconcileColumns[c] {
			t.Errorf("%s is reconcile-writable: a repair must never record a newest change", c)
		}
	}
}

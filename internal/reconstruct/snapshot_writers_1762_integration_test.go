//go:build integration

package reconstruct_test

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A snapshot built on top of another one is signed by the writer that
// builds it, not by the writer of the one it builds on (#1762), and the
// older snapshot keeps its own signature. Carry-forward is on, so the
// table file itself is the older snapshot's file, linked.
func TestReconstructParquet_signsWithTheWriterOfThisRun(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	const first, second = "aaaaaaaa-0000-0000-0000-000000000001", "bbbbbbbb-0000-0000-0000-000000000002"

	for _, tc := range []struct {
		name   string
		writer string
		want   []string
	}{
		{"a named writer signs", second, []string{second}},
		// This index has no stream row and no registered source, so it
		// names no writer: the snapshot is published, unsigned.
		{"an index that names no writer publishes unsigned", "", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, dbName := testutil.CreateTestDB(t)
			if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
				t.Fatalf("CreateIndexTables: %v", err)
			}
			if err := indexer.EnsureSchema(db); err != nil {
				t.Fatalf("EnsureSchema: %v", err)
			}
			const schema = "shop"
			base := time.Now().UTC().Truncate(time.Hour)
			seedOrdersSnapshot(t, db, schema, base)
			root := t.TempDir()
			seedSourceBaseline(t, root, base, schema)
			older := filepath.Join(root, base.Format("2006-01-02T15-04-05Z"))
			if err := baseline.WriteWriterMarker(older, first); err != nil {
				t.Fatal(err)
			}
			testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
				base.Add(time.Minute).Format("2006-01-02 15:04:05"), nil,
				schema, "customers", 1, "1", nil, nil, []byte(`{"id":1,"name":"n"}`))

			at := base.Add(30 * time.Minute)
			if _, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
				IndexDSN:              testutil.BaseDSN() + "/" + dbName,
				BaselineSrc:           root,
				Tables:                []string{schema + ".orders"},
				At:                    at,
				OutputDir:             root,
				OutputFormat:          reconstruct.OutputFormatParquet,
				CarryForwardUnchanged: true,
				WriterID:              tc.writer,
			}); err != nil {
				t.Fatalf("ReconstructTables: %v", err)
			}
			newer := filepath.Join(root, reconstruct.SnapshotDirName(at))
			if !baseline.SnapshotComplete(newer) {
				t.Fatal("the new snapshot is not complete")
			}
			if w, bad, err := baseline.ReadSnapshotWriters(newer); err != nil || len(bad) != 0 || !slices.Equal(w, tc.want) {
				t.Fatalf("the new snapshot is signed by %q (unreadable %q, err %v), want %q", w, bad, err, tc.want)
			}
			if w, _, err := baseline.ReadSnapshotWriters(older); err != nil || !slices.Equal(w, []string{first}) {
				t.Fatalf("the older snapshot is signed by %q (err %v), want %q", w, err, first)
			}
			// The signed snapshot is found and listed like any other.
			if _, snapAt, _, err := reconstruct.FindBaseline(ctx, root, schema, "orders", at.Add(time.Minute)); err != nil || !snapAt.Equal(at.Truncate(time.Second)) {
				t.Fatalf("FindBaseline = %s, %v; want the new snapshot at %s", snapAt, err, at)
			}
		})
	}
}

// A snapshot built from recorded changes reads no dump. It carries the
// record of skipped views of the snapshot it was built from, marked as
// carried, with the time of the full read (#1879).
func TestReconstructParquet_carriesTheSkippedViews(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	const schema = "shop"
	base := time.Now().UTC().Truncate(time.Hour)
	seedOrdersSnapshot(t, db, schema, base)
	root := t.TempDir()
	seedSourceBaseline(t, root, base, schema)
	older := filepath.Join(root, base.Format("2006-01-02T15-04-05Z"))
	if err := baseline.WriteViewsSkipped(older, baseline.NewViewsSkipped([]string{"shop.big_orders"}, base)); err != nil {
		t.Fatal(err)
	}
	at := base.Add(30 * time.Minute)
	if _, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
		IndexDSN:              testutil.BaseDSN() + "/" + dbName,
		BaselineSrc:           root,
		Tables:                []string{schema + ".orders"},
		At:                    at,
		OutputDir:             root,
		OutputFormat:          reconstruct.OutputFormatParquet,
		CarryForwardUnchanged: true,
	}); err != nil {
		t.Fatalf("ReconstructTables: %v", err)
	}
	newer := filepath.Join(root, reconstruct.SnapshotDirName(at))
	rec, ok, err := baseline.ReadViewsSkipped(newer)
	if err != nil || !ok {
		t.Fatalf("the new snapshot has no record: ok %v err %v", ok, err)
	}
	if !rec.Carried || rec.Count != 1 || !slices.Equal(rec.Views, []string{"shop.big_orders"}) ||
		rec.ReadAt != base.Format(time.RFC3339) {
		t.Fatalf("record = %+v, want 1 view carried from the full read of %s", rec, base.Format(time.RFC3339))
	}
	if !baseline.SnapshotComplete(newer) {
		t.Fatal("the new snapshot is not complete")
	}
}

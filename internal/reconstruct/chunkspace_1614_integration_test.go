//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// FullTableConfig.SpaceCheck reaches the mydumper writer through the real
// fold over a baseline (#1614): the .sql backup's disk check is not only a
// field on the config.
func TestReconstructTables_spaceCheckRefusesTheMydumperFold(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	const schema = "shop"
	base := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	seedOrdersSnapshot(t, db, schema, base)
	root := t.TempDir()
	seedSourceBaseline(t, root, base, schema)

	full := errors.New("disk full")
	var calls int
	_, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
		IndexDSN:     testutil.BaseDSN() + "/" + dbName,
		BaselineSrc:  root,
		Tables:       []string{schema + ".orders"},
		At:           base.Add(30 * time.Minute),
		OutputDir:    t.TempDir(),
		OutputFormat: reconstruct.OutputFormatMydumper,
		AllowGaps:    true,
		SpaceCheck:   func(string, int64) error { calls++; return full },
	})
	if !errors.Is(err, full) || calls == 0 {
		t.Fatalf("err = %v after %d checks, want the disk refusal from the fold", err, calls)
	}
}

// Through the real Parquet fold with reuse on, only a table that is rewritten
// is checked (#1614). A table with no changes is carried forward for free and
// never reaches the check, so a disk too small for the whole previous backup
// still publishes a backup whose tables mostly did not change; a table that did
// change is refused before it is written.
func TestReconstructParquet_spaceCheckSkipsCarriedTables(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	for _, tc := range []struct {
		name    string
		changed bool
	}{
		{"a table with no changes is carried without a check", false},
		{"a table with changes is checked and refused", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, dbName := testutil.CreateTestDB(t)
			// Hourly partitions, as in the carry-forward tests: otherwise the
			// planner reads the hour as a gap and refuses before the fold.
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
			ts := base.Add(time.Minute).Format("2006-01-02 15:04:05")
			// A neighbour keeps the window covered either way.
			testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil,
				schema, "customers", 1, "1", nil, nil, []byte(`{"id":1,"name":"n"}`))
			if tc.changed {
				testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts, nil,
					schema, "orders", 1, "4", nil, nil, []byte(`{"id":4,"status":"new"}`))
			}

			full := errors.New("disk full")
			var calls int
			reports, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
				IndexDSN:              testutil.BaseDSN() + "/" + dbName,
				BaselineSrc:           root,
				Tables:                []string{schema + ".orders"},
				At:                    base.Add(30 * time.Minute),
				OutputDir:             root,
				OutputFormat:          reconstruct.OutputFormatParquet,
				CarryForwardUnchanged: true,
				SpaceCheck:            func(string, int64) error { calls++; return full },
			})
			if !tc.changed {
				if err != nil || calls != 0 || len(reports) != 1 || !reports[0].CarriedForward {
					t.Fatalf("err=%v checks=%d reports=%d: a table carried for free was counted against the disk", err, calls, len(reports))
				}
				return
			}
			if !errors.Is(err, full) || calls != 1 {
				t.Fatalf("err = %v after %d checks, want one refusal for the table that is rewritten", err, calls)
			}
		})
	}
}

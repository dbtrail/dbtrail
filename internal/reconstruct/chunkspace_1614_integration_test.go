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

// FullTableConfig.ChunkSpace reaches the mydumper writer through the real
// fold over a baseline (#1614): the .sql backup's disk check is not only a
// field on the config.
func TestReconstructTables_chunkSpaceRefusesTheMydumperFold(t *testing.T) {
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
		ChunkSpace:   func(string, int64) error { calls++; return full },
	})
	if !errors.Is(err, full) || calls == 0 {
		t.Fatalf("err = %v after %d checks, want the disk refusal from the fold", err, calls)
	}
}

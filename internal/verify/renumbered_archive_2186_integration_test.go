//go:build integration

package verify

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2186: a pair whose window was rotated into Parquet archives. The live
// index holds none of it, so the check must read the archives: a numbering
// that started over there is named, and an archive with every change after
// the mark in one numbering is no false refusal.
func TestVerifyBaselinePair_archivedWindow_2186(t *testing.T) {
	sameServer := reconstruct.EventMark{ID: 10, File: "binlog.000007", End: 200}.Encode()
	for _, tc := range []renumberCase{
		{name: "steady state", mark: sameServer, want: StatusMatch},
		{name: "numbering started over inside the archived window", mark: sameServer, startOver: true, want: StatusInconclusive},
		{name: "numbering started over in another table only", mark: sameServer, otherTable: true, want: StatusMatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, dbName := testutil.CreateTestDB(t)
			now := time.Now().UTC()
			prevTS := now.Add(-72 * time.Hour).Truncate(time.Hour)
			newTS := prevTS.Add(time.Hour)
			recent := now.Add(-time.Hour).Truncate(time.Hour)
			renumberIndex(t, db, dbName, tc, []time.Time{prevTS.Add(-time.Hour), prevTS, newTS, recent},
				prevTS.Add(-5*time.Minute), prevTS.Add(30*time.Minute), newTS.Add(10*time.Minute), newTS)
			if _, err := rotation.Perform(context.Background(), db, dbName, rotation.Options{
				RetainDur: 48 * time.Hour, RetainRaw: "48h", ArchiveDir: t.TempDir(), ArchiveCompression: "zstd",
				BintrailID: "21860001-dead-beef-dead-beefdeadbeef", Format: "json",
			}); err != nil {
				t.Fatalf("rotation.Perform: %v", err)
			}
			var live int
			if err := db.QueryRow(`SELECT COUNT(*) FROM binlog_events`).Scan(&live); err != nil || live != 0 {
				t.Fatalf("fixture: %d events still live (%v); the window must be archived", live, err)
			}
			// A recent change of another table stays live, as on any index a
			// stream writes: the check takes its usual path, live index first.
			testutil.MustExec(t, db, `INSERT INTO binlog_events
				(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_before, row_after)
				VALUES (100, 'binlog.000007', 9000, 9100, ?, ?, 'other', 2, '1', '{"id":1}', '{"id":1}')`,
				now.Add(-30*time.Minute).Format("2006-01-02 15:04:05"), dbName)

			baseDir := t.TempDir()
			writeMarkedBaseline(t, baseDir, prevTS, dbName, [][]string{{"1", "a"}, {"2", "b"}}, "binlog.000007", 200, tc.mark)
			writeMarkedBaseline(t, baseDir, newTS, dbName, [][]string{{"1", "zzz"}, {"2", "b"}}, changeFile(tc), 400, "")
			resolver, err := metadata.NewResolver(db, 1)
			if err != nil {
				t.Fatal(err)
			}
			cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, ArchiveFetcher: parquetquery.Fetch}
			ctx := context.Background()
			pairs, _, err := FindBaselinePair(ctx, baseDir)
			if err != nil || len(pairs) != 1 {
				t.Fatalf("FindBaselinePair = %d pairs, %v; want 1", len(pairs), err)
			}
			got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
			if err != nil {
				t.Fatalf("VerifyBaselinePair: %v", err)
			}
			checkRenumberedVerdict(t, tc, got, "second full snapshot")
		})
	}
}

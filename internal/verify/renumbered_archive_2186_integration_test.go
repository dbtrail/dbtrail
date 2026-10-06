//go:build integration

package verify

import (
	"context"
	"fmt"
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

// #2186 review: live-source mode with a snapshot older than the index's
// retention (a quiet table keeps its old snapshot file). The changes since it
// are partly in Parquet archives; the check reads them as the fetch does, so
// a healthy table matches and only a restart inside them is named.
func TestVerifyTable_archivedWindow_2186(t *testing.T) {
	sameServer := reconstruct.EventMark{ID: 10, File: "binlog.000007", End: 200}.Encode()
	for _, tc := range []renumberCase{
		{name: "steady state: routine archives", mark: sameServer, want: StatusMatch},
		{name: "numbering started over inside the archived hours", mark: sameServer, startOver: true, want: StatusInconclusive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, dbName := testutil.CreateTestDB(t)
			now := time.Now().UTC()
			snapTS := now.Add(-72 * time.Hour).Truncate(time.Hour)
			// Every hour from before the snapshot to now has a partition, so
			// rotation archives each one it drops (empty ones too) and the
			// read has no uncovered hour.
			var hours []time.Time
			for h := snapTS.Add(-time.Hour); !h.After(now); h = h.Add(time.Hour) {
				hours = append(hours, h)
			}
			renumberIndex(t, db, dbName, tc, hours,
				snapTS.Add(-5*time.Minute), snapTS.Add(30*time.Minute), time.Time{}, now)
			if _, err := rotation.Perform(context.Background(), db, dbName, rotation.Options{
				RetainDur: 48 * time.Hour, RetainRaw: "48h", ArchiveDir: t.TempDir(), ArchiveCompression: "zstd",
				BintrailID: "21860004-dead-beef-dead-beefdeadbeef", Format: "json",
			}); err != nil {
				t.Fatalf("rotation.Perform: %v", err)
			}
			// A recent change of another table stays live.
			testutil.MustExec(t, db, `INSERT INTO binlog_events
				(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_before, row_after)
				VALUES (100, 'binlog.000007', 9000, 9100, ?, ?, 'other', 2, '1', '{"id":1}', '{"id":1}')`,
				now.Add(-30*time.Minute).Format("2006-01-02 15:04:05"), dbName)

			testutil.MustExec(t, db, fmt.Sprintf("CREATE TABLE `%s`.`orders` (`id` INT PRIMARY KEY, `status` VARCHAR(64))", dbName))
			testutil.MustExec(t, db, fmt.Sprintf("INSERT INTO `%s`.`orders` VALUES (1,'zzz'),(2,'b')", dbName))
			var uuid string
			if err := db.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
				t.Fatal(err)
			}
			testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, gtid_set, last_checkpoint, server_id, bintrail_id)
				VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1, 'b1')`, uuid+":1-1000000")

			baseDir := t.TempDir()
			writeMarkedBaseline(t, baseDir, snapTS, dbName, [][]string{{"1", "a"}, {"2", "b"}}, "binlog.000007", 200, tc.mark)
			resolver, err := metadata.NewResolver(db, 1)
			if err != nil {
				t.Fatal(err)
			}
			cfg := Config{SourceDB: db, IndexDB: db, Resolver: resolver, BaselineSource: baseDir, IndexDBName: dbName, ArchiveFetcher: parquetquery.Fetch}
			got, err := VerifyTable(context.Background(), cfg, dbName, "orders")
			if err != nil {
				t.Fatalf("VerifyTable: %v", err)
			}
			checkRenumberedVerdict(t, tc, got)
		})
	}
}

//go:build integration

package cascadebaseline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2186: TestCascade_afterTheBinlogNumberingStartsOver_2177's recovery, with
// the window from the snapshot to the parent's delete rotated into Parquet
// archives. The live index holds none of it: the check must read the
// archives, or child 10 (moved to parent 2 after the snapshot, in a numbering
// that started over) comes back under parent 1 with no caveat.
func TestCascade_archivedWindow_2186(t *testing.T) {
	mark := reconstruct.EventMark{ID: 10, File: "mysql-bin.000009", End: 500}.Encode()
	for _, tc := range []struct {
		name, moveFile string
		renumbered     bool
	}{
		{name: "steady state", moveFile: "mysql-bin.000009"},
		{name: "numbering started over inside the archived window", moveFile: "mysql-bin.000001", renumbered: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, dbName := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			if err := indexer.EnsureSchema(db); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			hourTop := now.Add(-72 * time.Hour).Truncate(time.Hour)
			testutil.SetupPartitionedTable(t, db, dbName, []time.Time{hourTop, now.Add(-time.Hour).Truncate(time.Hour)})
			snapTime := hourTop.Add(5 * time.Minute)
			rootTS := hourTop.Add(20 * time.Minute)
			root := t.TempDir()
			writeChildBaselineParquet(t, root, reconstruct.SnapshotDirName(snapTime), dbName, [][]string{{"10", "1"}, {"11", "1"}},
				map[string]string{baseline.MetaKeyBinlogFile: "mysql-bin.000009", baseline.MetaKeyBinlogPos: "500", baseline.MetaKeyEventMark: mark})
			insertChildEventAt(t, db, dbName, 10, "mysql-bin.000009", 400, snapTime.Add(-time.Minute), "11", 1, 1)
			moveStart := uint64(600)
			if tc.renumbered {
				moveStart = 100
			}
			insertChildEventAt(t, db, dbName, 12, tc.moveFile, moveStart, snapTime.Add(2*time.Minute), "10", 1, 2)
			if _, err := rotation.Perform(context.Background(), db, dbName, rotation.Options{
				RetainDur: 48 * time.Hour, RetainRaw: "48h", ArchiveDir: t.TempDir(), ArchiveCompression: "zstd",
				BintrailID: "21860003-dead-beef-dead-beefdeadbeef", Format: "json",
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
				VALUES (100, 'mysql-bin.000009', 9000, 9100, ?, ?, 'other', 2, '1', '{"id":1}', '{"id":1}')`,
				now.Add(-30*time.Minute).Format("2006-01-02 15:04:05"), dbName)

			fks := []cascade.CascadeFK{{
				Schema: dbName, Table: "child", ConstraintName: "fk", Column: "pid",
				ReferencedSchema: dbName, ReferencedTable: "parent", ReferencedColumn: "id",
				DeleteRule: "CASCADE", UpdateRule: "RESTRICT",
			}}
			parents := []query.ResultRow{{
				SchemaName: dbName, TableName: "parent", EventType: event.EventDelete, PKValues: "1",
				RowBefore: map[string]any{"id": json.Number("1")}, EventTimestamp: rootTS,
			}}
			fetcher := &query.MergedFetcher{DB: db, Engine: query.New(db), DBName: dbName, ArchiveFetcher: parquetquery.Fetch}
			res, err := cascade.SynthesizeVictims(context.Background(), fetcher, fks, parents,
				cascade.Options{Baseline: New(Source(root), childResolver(dbName), db)})
			if err != nil {
				t.Fatalf("SynthesizeVictims: %v", err)
			}
			got := map[string]bool{}
			for _, v := range res.Victims {
				got[v.PKValues] = true
			}
			named := false
			for _, msg := range res.Incomplete {
				if strings.HasPrefix(msg, dbName+".child's baseline snapshot is not used") && strings.Contains(msg, "binary log started again") {
					named = true
				}
			}
			if tc.renumbered {
				if !named || got["10"] {
					t.Fatalf("victims %v, incomplete %v; want the renumbering named and child 10 not restored", got, res.Incomplete)
				}
				return
			}
			if named || got["10"] || !got["11"] {
				t.Fatalf("victims %v, incomplete %v; want child 11 only, no renumbering (%s)", got, res.Incomplete, fmt.Sprint(res.Complete()))
			}
		})
	}
}

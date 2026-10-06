//go:build integration

package shim

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2186: `_snapshot` with an AS OF whose window was rotated into Parquet
// archives. The read takes those hours from the archives by position, so the
// check must read them too: a numbering that started over there refuses, an
// archive whose changes after the mark are in one numbering answers.
func TestSnapshot_archivedWindow_2186(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		startOver, otherTable bool
		refuse                bool
	}{
		{name: "steady state"},
		{name: "numbering started over inside the archived window", startOver: true, refuse: true},
		{name: "numbering started over in another table only", otherTable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, dbName := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			if err := indexer.EnsureSchema(db); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			hourTop := now.Add(-72 * time.Hour).Truncate(time.Hour)
			addHourlyPartition(t, db, hourTop)
			addHourlyPartition(t, db, now.Add(-time.Hour).Truncate(time.Hour))
			snapTime := hourTop.Add(5 * time.Minute)
			asOf := hourTop.Add(10 * time.Minute)
			seedUsersSnapshot(t, db, snapTime)

			root := t.TempDir()
			dir := filepath.Join(root, reconstruct.SnapshotDirName(snapTime), "myapp")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			w, err := baseline.NewWriter(filepath.Join(dir, "users.parquet"), usersBaselineCols(), baseline.WriterConfig{
				Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
					baseline.MetaKeyBinlogFile: "mysql-bin.000009",
					baseline.MetaKeyBinlogPos:  "500",
					baseline.MetaKeyEventMark:  reconstruct.EventMark{ID: 10, File: "mysql-bin.000009", End: 500}.Encode(),
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range [][]string{{"1", "alice"}, {"2", "dave"}} {
				if err := w.WriteRow(r, []bool{false, false}); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			insertUsersEventAt(t, db, 10, "mysql-bin.000009", 400, snapTime.Add(-time.Minute), "2", "x", "dave")
			insertUsersEventAt(t, db, 11, "mysql-bin.000009", 500, snapTime.Add(time.Minute), "1", "alice", "bob")
			if tc.startOver {
				insertUsersEventAt(t, db, 12, "mysql-bin.000001", 100, snapTime.Add(2*time.Minute), "1", "bob", "carol")
			}
			if tc.otherTable {
				testutil.MustExec(t, db, `INSERT INTO binlog_events
					(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_before, row_after)
					VALUES (12, 'mysql-bin.000001', 100, 200, ?, 'myapp', 'other', 2, '1', '{"id":1}', '{"id":1}')`,
					snapTime.Add(2*time.Minute).Format("2006-01-02 15:04:05"))
			}
			if _, err := rotation.Perform(context.Background(), db, dbName, rotation.Options{
				RetainDur: 48 * time.Hour, RetainRaw: "48h", ArchiveDir: t.TempDir(), ArchiveCompression: "zstd",
				BintrailID: "21860002-dead-beef-dead-beefdeadbeef", Format: "json",
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
				VALUES (100, 'mysql-bin.000009', 9000, 9100, ?, 'myapp', 'other', 2, '1', '{"id":1}', '{"id":1}')`,
				now.Add(-30*time.Minute).Format("2006-01-02 15:04:05"))

			h := NewHandlerWithConfig(db, Config{AllowGaps: true, IndexDBName: dbName, BaselineDir: root}, slog.Default())
			for _, q := range []TimeTravelQuery{
				{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf, PKColumn: "id", PKValue: "1"},
				{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf},
			} {
				res, err := h.runSnapshot(q)
				if tc.refuse {
					var me *mysql.MyError
					if !errors.As(err, &me) || me.Code != mysql.ER_NO_PARTITION_FOR_GIVEN_VALUE || !strings.Contains(me.Message, "binary log started again") {
						t.Fatalf("pk column %q: err = %v; want the renumbering refusal (%d)", q.PKColumn, err, mysql.ER_NO_PARTITION_FOR_GIVEN_VALUE)
					}
					continue
				}
				if err != nil {
					t.Fatalf("pk column %q: %v", q.PKColumn, err)
				}
				// bob is change 11, which only the archive holds: the read went
				// there, and the check let it through.
				cells := rowCells(t, res.Resultset)
				if len(cells) == 0 || cells[0][1] != "bob" {
					t.Fatalf("pk column %q: %v; want id 1 = bob", q.PKColumn, cells)
				}
			}
		})
	}
}

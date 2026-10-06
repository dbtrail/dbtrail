//go:build integration

package shim

import (
	"context"
	"database/sql"
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
	"github.com/dbtrail/dbtrail/internal/serverid"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2174: the source's binary log started again after the snapshot (RESET
// BINARY LOGS AND GTIDS, a replaced server). Every later change sorts below
// the snapshot's position, so a window read from that position never sees
// it. _snapshot must refuse, for one row and for the whole table, with the
// same check a refresh runs (#2160), instead of answering the snapshot's old
// rows. The restart is simulated the way #2160's tests do it: changes indexed
// after the snapshot's event mark in a lower-numbered file.

const (
	renumberedOldUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	renumberedNewUUID = "4f22ab58-71ca-11e1-9e33-c80aa9429563"
)

// insertUsersEventAt writes one UPDATE of myapp.users with the id and binlog
// coordinates chosen by the caller.
func insertUsersEventAt(t *testing.T, db *sql.DB, id uint64, file string, start uint64, at time.Time, pk, before, after string) {
	t.Helper()
	testutil.MustExec(t, db, `INSERT INTO binlog_events
		(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_before, row_after)
		VALUES (?, ?, ?, ?, ?, 'myapp', 'users', 2, ?, ?, ?)`,
		id, file, start, start+100, at.Format("2006-01-02 15:04:05"), pk,
		[]byte(`{"id":`+pk+`,"name":"`+before+`"}`), []byte(`{"id":`+pk+`,"name":"`+after+`"}`))
}

func TestSnapshot_afterTheBinlogNumberingStartsOver_2174(t *testing.T) {
	sameServer := reconstruct.EventMark{ID: 10, File: "mysql-bin.000009", End: 500}.Encode()
	withServer := reconstruct.EventMark{ID: 10, File: "mysql-bin.000009", End: 500, ServerUUID: renumberedOldUUID}.Encode()
	for _, tc := range []struct {
		name string
		// mark is the snapshot's event mark; "" writes none.
		mark string
		// startOver indexes a change in mysql-bin.000001 after the mark,
		// before AS OF; startOverLater indexes it after AS OF.
		startOver, startOverLater bool
		// backfilled leaves a row in index_state, as `bintrail index` does.
		backfilled bool
		// otherTable indexes a change of another table in mysql-bin.000001
		// before AS OF: the numbering started over, this table's read did not.
		otherTable bool
		// captureReads, when set, is the server_uuid capture reads now.
		captureReads string
		// switchVia records when capture moved to captureReads: "change" (a
		// bintrail_server_changes row, same address) or "record" (a new
		// bintrail_servers record); "" records nothing. switchAfter puts it
		// after AS OF, else before.
		switchVia   string
		switchAfter bool
		refuse      bool
	}{
		{name: "no event mark: today's behavior", startOver: true},
		{name: "mark, same numbering", mark: sameServer},
		{name: "mark, numbering started over", mark: sameServer, startOver: true, refuse: true},
		{name: "mark, numbering started over after AS OF", mark: sameServer, startOverLater: true},
		{name: "mark, backfilled index", mark: sameServer, startOver: true, backfilled: true},
		{name: "mark names the server, capture reads it", mark: withServer, captureReads: renumberedOldUUID},
		{name: "mark names the server, capture reads another, no record of when", mark: withServer, captureReads: renumberedNewUUID, refuse: true},
		{name: "same address, new server before AS OF", mark: withServer, captureReads: renumberedNewUUID, switchVia: "change", refuse: true},
		{name: "same address, new server after AS OF", mark: withServer, captureReads: renumberedNewUUID, switchVia: "change", switchAfter: true},
		{name: "new address before AS OF", mark: withServer, captureReads: renumberedNewUUID, switchVia: "record", refuse: true},
		{name: "new address after AS OF", mark: withServer, captureReads: renumberedNewUUID, switchVia: "record", switchAfter: true},
		{name: "numbering started over in another table only", mark: sameServer, otherTable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, dbName := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			if err := indexer.EnsureSchema(db); err != nil {
				t.Fatalf("EnsureSchema: %v", err)
			}
			hourTop := time.Now().UTC().Truncate(time.Hour)
			addHourlyPartition(t, db, hourTop)
			snapTime := hourTop.Add(5 * time.Minute)
			asOf := hourTop.Add(10 * time.Minute)
			seedUsersSnapshot(t, db, snapTime)

			root := t.TempDir()
			dir := filepath.Join(root, reconstruct.SnapshotDirName(snapTime), "myapp")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			md := map[string]string{
				baseline.MetaKeyBinlogFile: "mysql-bin.000009",
				baseline.MetaKeyBinlogPos:  "500",
			}
			if tc.mark != "" {
				md[baseline.MetaKeyEventMark] = tc.mark
			}
			w, err := baseline.NewWriter(filepath.Join(dir, "users.parquet"), usersBaselineCols(), baseline.WriterConfig{
				Compression: "none", RowGroupSize: 100, Metadata: md,
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

			// The mark's own row (the snapshot holds it), then a change after
			// the snapshot in the same numbering.
			insertUsersEventAt(t, db, 10, "mysql-bin.000009", 400, snapTime.Add(-time.Minute), "2", "x", "dave")
			insertUsersEventAt(t, db, 11, "mysql-bin.000009", 500, snapTime.Add(time.Minute), "1", "alice", "bob")
			if tc.startOver {
				insertUsersEventAt(t, db, 12, "mysql-bin.000001", 100, snapTime.Add(2*time.Minute), "1", "bob", "carol")
			}
			if tc.startOverLater {
				// The window up to AS OF is in one numbering: readable.
				insertUsersEventAt(t, db, 12, "mysql-bin.000001", 100, asOf.Add(2*time.Minute), "1", "bob", "carol")
			}
			if tc.otherTable {
				testutil.MustExec(t, db, `INSERT INTO binlog_events
					(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_before, row_after)
					VALUES (12, 'mysql-bin.000001', 100, 200, ?, 'myapp', 'other', 2, '1', '{"id":1}', '{"id":1}')`,
					snapTime.Add(2*time.Minute).Format("2006-01-02 15:04:05"))
			}
			if tc.backfilled {
				testutil.MustExec(t, db, `INSERT INTO index_state (binlog_file, file_size, last_position, events_indexed, status, started_at, completed_at)
					VALUES ('mysql-bin.000005', 1, 150, 1, 'completed', UTC_TIMESTAMP(), UTC_TIMESTAMP())`)
			}
			if tc.captureReads != "" {
				testutil.MustExec(t, db, serverid.DDLBintrailServers)
				testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, last_checkpoint, server_id, bintrail_id)
					VALUES (1, 'position', 'mysql-bin.000009', 600, UTC_TIMESTAMP(), 1, 'b1')`)
				switchAt := asOf.Add(-2 * time.Minute)
				if tc.switchAfter {
					switchAt = asOf.Add(2 * time.Minute)
				}
				created := time.Now()
				if tc.switchVia == "record" {
					created = switchAt
					testutil.MustExec(t, db, `INSERT INTO bintrail_servers (bintrail_id, server_uuid, host, port, username, created_at)
						VALUES ('b0', ?, 'db-old', 3306, 'u', FROM_UNIXTIME(?))`, renumberedOldUUID, snapTime.Add(-time.Hour).Unix())
				}
				testutil.MustExec(t, db, `INSERT INTO bintrail_servers (bintrail_id, server_uuid, host, port, username, created_at)
					VALUES ('b1', ?, 'db', 3306, 'u', FROM_UNIXTIME(?))`, tc.captureReads, created.Unix())
				if tc.switchVia == "change" {
					testutil.MustExec(t, db, serverid.DDLBintrailServerChanges)
					testutil.MustExec(t, db, `INSERT INTO bintrail_server_changes (bintrail_id, field_changed, old_value, new_value, detected_at)
						VALUES ('b1', 'server_uuid', ?, ?, FROM_UNIXTIME(?))`, renumberedOldUUID, tc.captureReads, switchAt.Unix())
				}
			}

			h := NewHandlerWithConfig(db, Config{
				AllowGaps:   true,
				NoArchive:   true,
				IndexDBName: dbName,
				BaselineDir: root,
			}, slog.Default())
			row := TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf, PKColumn: "id", PKValue: "1"}
			table := TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf}

			// The seam pgshim reads (pgResolveError): the error, by its sentinel.
			got, rowErr := h.ResolveSnapshotRow(context.Background(), row)
			if tc.refuse {
				if !errors.Is(rowErr, reconstruct.ErrBinlogRenumbered) {
					t.Fatalf("one row = %v, %v; want ErrBinlogRenumbered", got, rowErr)
				}
			} else if rowErr != nil || got["name"] != "bob" {
				// bob proves the change after the snapshot was read: the check
				// ran and let the window through, rather than nothing being read.
				t.Fatalf("one row = %v, %v; want name=bob", got, rowErr)
			}

			// What a MySQL client gets, for one row and for the table.
			for _, q := range []TimeTravelQuery{row, table} {
				res, err := h.runSnapshot(q)
				if !tc.refuse {
					if err != nil {
						t.Fatalf("%q: %v", q.PKColumn, err)
					}
					if q.PKColumn == "" {
						cells := rowCells(t, res.Resultset)
						if len(cells) != 2 || cells[0][1] != "bob" || cells[1][1] != "dave" {
							t.Fatalf("the table = %v; want [[1 bob] [2 dave]]", cells)
						}
					}
					continue
				}
				var me *mysql.MyError
				if !errors.As(err, &me) || me.Code != mysql.ER_NO_PARTITION_FOR_GIVEN_VALUE {
					t.Fatalf("pk column %q: err = %#v (%v); want a MySQL error %d", q.PKColumn, err, err, mysql.ER_NO_PARTITION_FOR_GIVEN_VALUE)
				}
				for _, want := range []string{"resolve _snapshot", "new full snapshot is needed", "bintrail baseline", "use _flashback"} {
					if !strings.Contains(me.Message, want) {
						t.Errorf("pk column %q: the refusal does not say %q: %s", q.PKColumn, want, me.Message)
					}
				}
				if tc.captureReads == "" && !strings.Contains(me.Message, "binary log started again") {
					t.Errorf("pk column %q: the refusal does not name the restart: %s", q.PKColumn, me.Message)
				}
				if tc.captureReads != "" && !strings.Contains(me.Message, renumberedNewUUID) {
					t.Errorf("pk column %q: the refusal does not name the server capture reads now: %s", q.PKColumn, me.Message)
				}
			}
		})
	}
}

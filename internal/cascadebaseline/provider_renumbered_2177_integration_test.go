//go:build integration

package cascadebaseline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/serverid"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	renumberedOldUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	renumberedNewUUID = "4f22ab58-71ca-11e1-9e33-c80aa9429563"
)

func insertChildEventAt(t *testing.T, db *sql.DB, schema string, id uint64, file string, start uint64, at time.Time, pk string, pidBefore, pidAfter int) {
	t.Helper()
	testutil.MustExec(t, db, `INSERT INTO binlog_events
		(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_before, row_after)
		VALUES (?, ?, ?, ?, ?, ?, 'child', 2, ?, ?, ?)`,
		id, file, start, start+100, at.Format("2006-01-02 15:04:05"), schema, pk,
		[]byte(fmt.Sprintf(`{"id":%s,"pid":%d}`, pk, pidBefore)), []byte(fmt.Sprintf(`{"id":%s,"pid":%d}`, pk, pidAfter)))
}

// TestCascade_afterTheBinlogNumberingStartsOver_2177 drives the real provider
// and the real cascade engine over a real index. The baseline holds children
// 10 and 11 under parent 1; after the snapshot child 10 moves to parent 2;
// then parent 1 is deleted. A recovery that reads the move restores child 11
// only. One that misses it (the move sorts before the snapshot's position
// because the binary log started again) also restores child 10 under parent
// 1, which is wrong SQL with no caveat: the no-mark case reproduces that.
func TestCascade_afterTheBinlogNumberingStartsOver_2177(t *testing.T) {
	sameServer := reconstruct.EventMark{ID: 10, File: "mysql-bin.000009", End: 500}.Encode()
	withServer := reconstruct.EventMark{ID: 10, File: "mysql-bin.000009", End: 500, ServerUUID: renumberedOldUUID}.Encode()
	type outcome int
	const (
		correct    outcome = iota // child 11 only, complete
		stale                     // child 10 restored too, complete (today's behavior without a mark)
		renumbered                // the renumbering is named, child 10 not restored
		checkFail                 // the check failed: named as a failed lookup
		untilLater                // the move is after the recovery's time: 10 and 11, complete
		unchecked                 // #2186: the check could not tell; 10 and 11, incomplete with its note
	)
	for _, tc := range []struct {
		name        string
		mark        string
		moveFile    string // binlog file of child 10's move
		moveAfter   bool   // the move is after the parent delete
		backfilled  bool
		noIndexSt   bool
		captureRead string
		switchAfter bool
		// markGone / markMoved: the mark's event deleted while an older row
		// remains / its id now another event (#2186). note: what the
		// incomplete marker must say.
		markGone, markMoved bool
		note                string
		want                outcome
	}{
		{name: "no event mark: today's behavior", moveFile: "mysql-bin.000001", want: stale},
		{name: "mark, same numbering", mark: sameServer, moveFile: "mysql-bin.000009", want: correct},
		{name: "mark, numbering started over inside the window", mark: sameServer, moveFile: "mysql-bin.000001", want: renumbered},
		{name: "mark, numbering started over after the window", mark: sameServer, moveFile: "mysql-bin.000001", moveAfter: true, want: untilLater},
		{name: "mark, backfilled index: not checked", mark: sameServer, moveFile: "mysql-bin.000001", backfilled: true, want: unchecked,
			note: "`bintrail index` also wrote into this index"},
		{name: "mark's event deleted while older rows remain", mark: sameServer, moveFile: "mysql-bin.000001", markGone: true, want: unchecked,
			note: "was deleted from the index while older ones remain"},
		{name: "mark's id names another event", mark: sameServer, moveFile: "mysql-bin.000001", markMoved: true, want: unchecked,
			note: "is now another event"},
		{name: "mark, index_state unreadable", mark: sameServer, moveFile: "mysql-bin.000009", noIndexSt: true, want: checkFail},
		{name: "mark names the server capture still reads", mark: withServer, moveFile: "mysql-bin.000009", captureRead: renumberedOldUUID, want: correct},
		{name: "capture moved to another server, no record of when", mark: withServer, moveFile: "mysql-bin.000009", captureRead: renumberedNewUUID, want: renumbered},
		{name: "capture moved to another server after the window", mark: withServer, moveFile: "mysql-bin.000009", captureRead: renumberedNewUUID, switchAfter: true, want: correct},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, dbName := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			if err := indexer.EnsureSchema(db); err != nil {
				t.Fatalf("EnsureSchema: %v", err)
			}
			hourTop := time.Now().UTC().Truncate(time.Hour)
			testutil.MustExec(t, db, fmt.Sprintf(
				"ALTER TABLE binlog_events REORGANIZE PARTITION p_future INTO (PARTITION %s VALUES LESS THAN (TO_SECONDS('%s')), PARTITION p_future VALUES LESS THAN MAXVALUE)",
				hourTop.Format("p_2006010215"), hourTop.Add(time.Hour).Format("2006-01-02 15:04:05")))
			snapTime := hourTop.Add(5 * time.Minute)
			rootTS := hourTop.Add(20 * time.Minute)

			root := t.TempDir()
			md := map[string]string{
				baseline.MetaKeyBinlogFile: "mysql-bin.000009",
				baseline.MetaKeyBinlogPos:  "500",
			}
			if tc.mark != "" {
				md[baseline.MetaKeyEventMark] = tc.mark
			}
			writeChildBaselineParquet(t, root, reconstruct.SnapshotDirName(snapTime), dbName,
				[][]string{{"10", "1"}, {"11", "1"}}, md)

			// The event the mark names: child 11, before the snapshot.
			switch {
			case tc.markGone:
				insertChildEventAt(t, db, dbName, 9, "mysql-bin.000009", 400, snapTime.Add(-time.Minute), "11", 1, 1)
			case tc.markMoved:
				insertChildEventAt(t, db, dbName, 10, "mysql-bin.000009", 300, snapTime.Add(-time.Minute), "11", 1, 1)
			default:
				insertChildEventAt(t, db, dbName, 10, "mysql-bin.000009", 400, snapTime.Add(-time.Minute), "11", 1, 1)
			}
			moveAt := snapTime.Add(2 * time.Minute)
			if tc.moveAfter {
				moveAt = rootTS.Add(2 * time.Minute)
			}
			moveStart := uint64(600)
			if tc.moveFile == "mysql-bin.000001" {
				moveStart = 100
			}
			insertChildEventAt(t, db, dbName, 12, tc.moveFile, moveStart, moveAt, "10", 1, 2)

			if tc.backfilled {
				testutil.MustExec(t, db, `INSERT INTO index_state (binlog_file, file_size, last_position, events_indexed, status, started_at, completed_at)
					VALUES ('mysql-bin.000005', 1, 150, 1, 'completed', UTC_TIMESTAMP(), UTC_TIMESTAMP())`)
			}
			if tc.noIndexSt {
				testutil.MustExec(t, db, `DROP TABLE index_state`)
			}
			if tc.captureRead != "" {
				testutil.MustExec(t, db, serverid.DDLBintrailServers)
				testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, last_checkpoint, server_id, bintrail_id)
					VALUES (1, 'position', 'mysql-bin.000009', 700, UTC_TIMESTAMP(), 1, 'b1')`)
				testutil.MustExec(t, db, `INSERT INTO bintrail_servers (bintrail_id, server_uuid, host, port, username, created_at)
					VALUES ('b1', ?, 'db', 3306, 'u', FROM_UNIXTIME(?))`, tc.captureRead, time.Now().Unix())
				if tc.switchAfter {
					testutil.MustExec(t, db, serverid.DDLBintrailServerChanges)
					testutil.MustExec(t, db, `INSERT INTO bintrail_server_changes (bintrail_id, field_changed, old_value, new_value, detected_at)
						VALUES ('b1', 'server_uuid', ?, ?, FROM_UNIXTIME(?))`, renumberedOldUUID, tc.captureRead, rootTS.Add(2*time.Minute).Unix())
				}
			}

			fks := []cascade.CascadeFK{{
				Schema: dbName, Table: "child", ConstraintName: "fk", Column: "pid",
				ReferencedSchema: dbName, ReferencedTable: "parent", ReferencedColumn: "id",
				DeleteRule: "CASCADE", UpdateRule: "RESTRICT",
			}}
			parents := []query.ResultRow{{
				SchemaName: dbName, TableName: "parent", EventType: event.EventDelete, PKValues: "1",
				RowBefore: map[string]any{"id": json.Number("1")}, EventTimestamp: rootTS,
			}}
			prov := New(Source(root), childResolver(dbName), db)
			res, err := cascade.SynthesizeVictims(context.Background(), query.New(db), fks, parents, cascade.Options{Baseline: prov})
			if err != nil {
				t.Fatalf("SynthesizeVictims: %v", err)
			}
			got := map[string]bool{}
			for _, v := range res.Victims {
				got[v.PKValues] = true
			}
			var named, failed bool
			var uncheckedMsg string
			for _, msg := range res.Incomplete {
				if strings.Contains(msg, "binlog numbering not checked") {
					uncheckedMsg = msg
				}
				if strings.HasPrefix(msg, dbName+".child's baseline snapshot is not used") {
					named = true
					if tc.captureRead == "" && !strings.Contains(msg, "binary log started again") {
						t.Errorf("the caveat does not name the restart: %s", msg)
					}
					if tc.captureRead != "" && !strings.Contains(msg, renumberedNewUUID) {
						t.Errorf("the caveat does not name the server capture reads now: %s", msg)
					}
					if !strings.Contains(msg, "A new full snapshot is needed") {
						t.Errorf("the caveat does not say what makes the baseline usable again: %s", msg)
					}
				}
				if strings.Contains(msg, "baseline lookup failed for "+dbName+".child") && strings.Contains(msg, "check the binlog numbering") {
					failed = true
				}
			}
			switch tc.want {
			case correct:
				if !res.Complete() || len(got) != 1 || !got["11"] {
					t.Fatalf("victims %v, incomplete %v; want child 11 only, complete", got, res.Incomplete)
				}
			case stale:
				// Today's behavior, kept: child 10 comes back under parent 1.
				if !res.Complete() || !got["10"] || !got["11"] {
					t.Fatalf("victims %v, incomplete %v; want 10 and 11 (no check ran)", got, res.Incomplete)
				}
			case untilLater:
				if !res.Complete() || len(got) != 2 {
					t.Fatalf("victims %v, incomplete %v; want 10 and 11, complete (the move is after the delete)", got, res.Incomplete)
				}
			case renumbered:
				if !named || res.Complete() {
					t.Fatalf("incomplete %v; want the renumbering named", res.Incomplete)
				}
				if got["10"] {
					t.Errorf("child 10 moved to parent 2 after the snapshot and must not be restored under parent 1: %v", got)
				}
				if !got["11"] {
					t.Errorf("child 11 is in the binlog inside the lookback window and must still be recovered: %v", got)
				}
			case unchecked:
				// The baseline is used as before, and the result says the window
				// could not be checked: never reported complete.
				if res.Complete() || !got["10"] || !got["11"] {
					t.Fatalf("victims %v, incomplete %v; want 10 and 11, incomplete", got, res.Incomplete)
				}
				for _, w := range []string{dbName + ".child's baseline snapshot is used", tc.note, "may be partial"} {
					if !strings.Contains(uncheckedMsg, w) {
						t.Errorf("the marker does not say %q: %v", w, res.Incomplete)
					}
				}
				if strings.Contains(uncheckedMsg, "--") {
					t.Errorf("the marker names a flag (MCP clients get it): %s", uncheckedMsg)
				}
			case checkFail:
				if !failed || named {
					t.Fatalf("incomplete %v; want the failed check as a failed baseline lookup", res.Incomplete)
				}
			}
			if tc.want != unchecked && uncheckedMsg != "" {
				t.Errorf("unexpected cannot-check marker: %v", res.Incomplete)
			}
			if tc.want != renumbered && named {
				t.Errorf("unexpected renumbering caveat: %v", res.Incomplete)
			}
		})
	}
}

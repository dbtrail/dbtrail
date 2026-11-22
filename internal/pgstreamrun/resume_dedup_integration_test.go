//go:build integration

package pgstreamrun

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A capture that stops between making rows durable and saving its position
// leaves rows past the saved position, and PostgreSQL sends them again from
// there. A resumed run removes them first, or each would be in the index
// twice for good; what is at or before the position stays.
func TestIntegrationPGResumeRemovesWhatWasIndexedPastThePosition(t *testing.T) {
	ctx := context.Background()
	lsnsLeft := func(t *testing.T, db *sql.DB) string {
		t.Helper()
		rs, err := db.Query(`SELECT start_pos, pk_values FROM binlog_events ORDER BY event_id`)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		out := ""
		for rs.Next() {
			var pos uint64
			var pk string
			if err := rs.Scan(&pos, &pk); err != nil {
				t.Fatal(err)
			}
			out += fmt.Sprintf("%X:%s ", pos, pk)
		}
		return out
	}
	// saved: the checkpoint row as a capture leaves it, at lsn, reaching the
	// event eventID (0: an index from before that was recorded).
	saved := func(t *testing.T, db *sql.DB, lsn uint64, eventID int64) {
		t.Helper()
		id := "NULL"
		if eventID > 0 {
			id = fmt.Sprint(eventID)
		}
		testutil.MustExec(t, db, fmt.Sprintf(`INSERT INTO stream_state (id, mode, binlog_file, binlog_position, flavor, events_indexed, last_checkpoint, server_id, pg_checkpoint_event_id)
			VALUES (1, 'gtid', '0/%X', %d, 'postgres', 1, UTC_TIMESTAMP(), 1, %s)
			ON DUPLICATE KEY UPDATE binlog_position = VALUES(binlog_position), pg_checkpoint_event_id = VALUES(pg_checkpoint_event_id)`, lsn, lsn, id))
	}
	for name, withBound := range map[string]bool{"with the event the position reached": true, "an index that records none": false} {
		t.Run(name, func(t *testing.T) {
			db, _ := testutil.CreateTestDB(t)
			if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
				t.Fatalf("CreateIndexTables: %v", err)
			}
			leftAt(t, db, 0x100, 1)
			leftAt(t, db, 0x200, 2)
			reached := leftAt(t, db, 0x200, 3) // the same transaction, at the position: stays
			// Past the position: a whole transaction committed and not
			// saved, and part of one still arriving.
			leftAt(t, db, 0x300, 4)
			leftAt(t, db, 0x300, 5)
			leftAt(t, db, 0x400, 6)
			if !withBound {
				reached = 0
			}
			saved(t, db, 0x200, reached)

			n, floor, err := deleteEventsPastCheckpointPG(ctx, db, 0x200)
			if err != nil || n != 3 {
				t.Fatalf("removed %d rows, %v; want the 3 past 200", n, err)
			}
			if got := lsnsLeft(t, db); got != "100:1 200:2 200:3 " {
				t.Fatalf("the index holds %q; want the three rows at or before the position", got)
			}
			if want := uint64(max(reached, 0)); floor != want {
				t.Fatalf("returned event %d; want %d, the one the save recorded", floor, want)
			}
			// Again: nothing is past the position now.
			if n, _, err := deleteEventsPastCheckpointPG(ctx, db, 0x200); err != nil || n != 0 {
				t.Fatalf("a second pass removed %d rows, %v", n, err)
			}
		})
	}

	t.Run("no position to resume from removes nothing", func(t *testing.T) {
		db, _ := testutil.CreateTestDB(t)
		if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
			t.Fatalf("CreateIndexTables: %v", err)
		}
		leftAt(t, db, 0x300, 4)
		if n, _, err := deleteEventsPastCheckpointPG(ctx, db, 0); err != nil || n != 0 {
			t.Fatalf("removed %d rows, %v", n, err)
		}
	})

	// A run told to start before the saved position (an operator's rewind):
	// everything past where it starts arrives again, and the recorded event
	// belongs to the later position, so it bounds nothing.
	t.Run("a start before the saved position", func(t *testing.T) {
		db, _ := testutil.CreateTestDB(t)
		if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
			t.Fatalf("CreateIndexTables: %v", err)
		}
		leftAt(t, db, 0x100, 1)
		leftAt(t, db, 0x200, 2)
		reached := leftAt(t, db, 0x300, 3)
		saved(t, db, 0x300, reached)
		if n, floor, err := deleteEventsPastCheckpointPG(ctx, db, 0x100); err != nil || n != 2 || floor != 0 {
			t.Fatalf("removed %d rows (event %d), %v; want the 2 past 100 and no event: the save is of another position", n, floor, err)
		}
		if got := lsnsLeft(t, db); got != "100:1 " {
			t.Fatalf("the index holds %q", got)
		}
	})

	// Rows AT the position written after the event its save recorded: a
	// part of that transaction indexed again (a table taken since) by a run
	// that stopped before its commit. They go; the rows the save covered stay.
	t.Run("rows at the position past the recorded event", func(t *testing.T) {
		db, _ := testutil.CreateTestDB(t)
		if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
			t.Fatalf("CreateIndexTables: %v", err)
		}
		leftAt(t, db, 0x100, 1)
		reached := leftAt(t, db, 0x200, 2)
		leftAt(t, db, 0x200, 3) // written again after the save
		leftAt(t, db, 0x300, 4)
		saved(t, db, 0x200, reached)
		n, floor, err := deleteEventsPastCheckpointPG(ctx, db, 0x200)
		if err != nil || n != 2 || floor != uint64(reached) {
			t.Fatalf("removed %d rows (event %d), %v; want 2 and event %d", n, floor, err, reached)
		}
		if got := lsnsLeft(t, db); got != "100:1 200:2 " {
			t.Fatalf("the index holds %q", got)
		}
	})
}

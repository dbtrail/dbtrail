//go:build integration

package pgstreamrun

import (
	"database/sql"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
)

// waitSaved waits up to 5 s for the saved column of stream_state to reach
// want, and fails with what it holds.
func waitSaved(t *testing.T, db *sql.DB, column string, want uint64, why string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var got sql.NullInt64
		err := db.QueryRow(`SELECT ` + column + ` FROM stream_state WHERE id = 1`).Scan(&got)
		if err == nil && got.Valid && uint64(got.Int64) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s is %v (%v) 5 s later, want %d: %s", column, got, err, want, why)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// #2985: a transaction whose rows fill a batch is committed inside the
// writer, which stops the commit-within timer, so before this only the
// checkpoint ticker (an hour here, 10 s in watch) saved its position. The
// PostgreSQL port's wait for the source reads that saved position.
func TestIntegrationPGAFullBatchHasItsPositionSavedWithinTheWait_2985(t *testing.T) {
	prev := commitWithin
	commitWithin = func() time.Duration { return 200 * time.Millisecond }
	t.Cleanup(func() { commitWithin = prev })

	send, finish, db := runPGLoop(t) // batch size 1000, checkpoint every hour
	for n := 1; n <= 1000; n++ {
		send(pgRow(n))
	}
	send(event.Event{EventType: event.EventCommit, EndPos: 7000})
	send(event.Event{EventType: event.EventGTID}) // taken only once the commit is handled
	if seen := pgCount(t, db); seen != 1000 {
		t.Fatalf("after the commit a reader sees %d rows, want 1000 (the batch filled)", seen)
	}
	waitSaved(t, db, "binlog_position", 7000,
		"a filled batch must not wait for the checkpoint ticker to have its position saved")
	finish()
}

// The save follows the position on its own, as the MySQL capture's saveSoon
// (#2645) does, not on a timer of the commit wait: with that wait off
// (BINTRAIL_COMMIT_WITHIN=off) a filled batch still has its position saved
// at once, not at the hourly checkpoint.
func TestIntegrationPGAFullBatchPositionIsSavedWithTheCommitWaitOff_2985(t *testing.T) {
	prev := commitWithin
	commitWithin = func() time.Duration { return 0 }
	t.Cleanup(func() { commitWithin = prev })

	send, finish, db := runPGLoop(t)
	for n := 1; n <= 1000; n++ {
		send(pgRow(n))
	}
	send(event.Event{EventType: event.EventCommit, EndPos: 8000})
	send(event.Event{EventType: event.EventGTID})
	waitSaved(t, db, "binlog_position", 8000,
		"a filled batch's position must be saved as soon as it moves, whatever the commit wait")
	finish()
}

// The same for a quiet source: the walsender's position moves with no
// commit, and it too is saved within the wait, not at the next checkpoint.
func TestIntegrationPGASentPositionIsSavedWithinTheWait_2985(t *testing.T) {
	prev := commitWithin
	commitWithin = func() time.Duration { return 200 * time.Millisecond }
	t.Cleanup(func() { commitWithin = prev })

	send, finish, db := runPGLoop(t)
	send(pgRow(1))
	send(event.Event{EventType: event.EventCommit, EndPos: 5000})
	waitSaved(t, db, "binlog_position", 5000, "the commit-within wait saves a quiet transaction's position (#2666)")
	send(event.Event{EventType: event.EventSentThrough, EndPos: 9000})
	waitSaved(t, db, "pg_sent_through_lsn", 9000,
		"the walsender's position on a quiet source must not wait for the checkpoint ticker")
	finish()
}

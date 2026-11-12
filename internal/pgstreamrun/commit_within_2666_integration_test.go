//go:build integration

package pgstreamrun

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
)

// #2666 for a PostgreSQL source: a transaction that does not fill a batch is
// in the index after commitWithin, with the checkpoint an hour away, AND its
// LSN is saved in the same step. A resume deletes nothing here, so a row
// written ahead of the saved LSN would be indexed twice after a crash.
func TestIntegrationPGAQuietTransactionIsVisibleAndItsLSNSavedWithinTheWait_2666(t *testing.T) {
	prev := commitWithin
	commitWithin = func() time.Duration { return 200 * time.Millisecond }
	t.Cleanup(func() { commitWithin = prev })

	send, finish, db := runPGLoop(t)
	send(pgRow(1))
	send(event.Event{EventType: event.EventCommit, EndPos: 5000})
	send(event.Event{EventType: event.EventGTID}) // taken only once the commit is handled

	deadline := time.Now().Add(10 * time.Second)
	for pgCount(t, db) != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("a reader sees %d rows 10 s after the transaction ended, want 1", pgCount(t, db))
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The row and the LSN are written in one step of the loop, the row first:
	// give the second write a moment, then it must be there.
	var lsn uint64
	for {
		err := db.QueryRow(`SELECT binlog_position FROM stream_state WHERE id = 1`).Scan(&lsn)
		if err == nil && lsn == 5000 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the row is visible and the saved LSN is %d (%v), want 5000: rows ahead of the saved LSN are indexed twice after a crash", lsn, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	finish()
}

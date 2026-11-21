package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/query"
)

// pgCaptureStartFunc reads where the capture of req's server started reading
// its source from, off the server's index (query.PGCaptureStartRead, bounded
// to 5 seconds: the snapshot transaction is still open on the source while it
// is asked). ok false with a nil error is "nothing recorded"; a non-nil error
// is a read that failed. A variable so a test answers it without an index.
var pgCaptureStartFunc = func(req console.BaselineRequest) (uint64, bool, error) {
	if req.IndexDSN == "" {
		return 0, false, nil
	}
	db, err := config.Connect(req.IndexDSN)
	if err != nil {
		return 0, false, fmt.Errorf("could not reach the index: %w", err)
	}
	defer db.Close()
	return query.PGCaptureStartRead(context.Background(), db)
}

// pgKnownGapError is pgRefuseKnownGap's refusal: capture started reading the
// source at from, after floor where the snapshot's changes start.
type pgKnownGapError struct{ from, floor uint64 }

func (e *pgKnownGapError) Error() string {
	return fmt.Sprintf("the snapshot is not published: capture of this server started reading the source at %s, after %s where this snapshot's changes start, so what was committed between the two was not captured and an update of this snapshot would miss it. Run the snapshot again: capture is attached now, and the new snapshot starts from where it reads",
		pglogrepl.LSN(e.from), pglogrepl.LSN(e.floor))
}

// summary is the refusal in the failure card's words, with what to do, short
// enough for the card (refusalSummary cuts at 300 characters).
func (e *pgKnownGapError) summary() string {
	return fmt.Sprintf("Not published: capture of this server started at %s, after %s where this snapshot starts, so the changes between the two were never captured. Run the snapshot again: it will start where capture reads from.",
		pglogrepl.LSN(e.from), pglogrepl.LSN(e.floor))
}

// pgRefuseKnownGap is asked when a PostgreSQL full read has its tables and
// before it is published (pgbaseline.Config.BeforePublish, #3078). The index
// records where capture started reading the source; a snapshot whose changes
// start before that has a gap that is known now: what was committed between
// the two was never captured, and every update built on this snapshot would
// carry rows the source no longer has. It is refused, so the folder is not
// published and is removed like any failed read.
//
// It can only happen when the slot the snapshot was anchored on is not the
// one capture started from: the slot was replaced while the tables were read.
// A slot that is merely still being created is waited for before anything is
// read (pgcapture.AnchorOnSlot). With no start on record (capture has not
// saved a position yet) there is nothing to compare and the snapshot is
// published: its floor is then the slot's own position, which is where a
// capture that starts later reads from.
//
// A read of the index that failed (unreachable, denied, a missing table, no
// answer within 5 seconds) is said at warning level and the snapshot IS
// published. Refusing would throw away a whole read of the source over the
// index, which is the rarer of the two to be wrong; and the gap is not lost
// by publishing: every update of this snapshot asks the index the same
// question about each table (reconstruct's pgRefuseFileBeforeCapture), and
// there a failed read refuses the update instead of letting the table
// through, so a snapshot with the gap is never carried forward unseen.
func pgRefuseKnownGap(req console.BaselineRequest, stats pgbaseline.Stats) error {
	from, ok, err := pgCaptureStartFunc(req)
	if err != nil {
		slog.Warn("where capture of this server started could not be read from its index, so the snapshot is published without being compared with it; each update of the snapshot compares it again, and refuses while the index cannot answer",
			"server", req.ServerID, "snapshot_from", pglogrepl.LSN(stats.DeltaStartLSN), "error", err)
		return nil
	}
	if !ok || from <= stats.DeltaStartLSN {
		return nil
	}
	return &pgKnownGapError{from: from, floor: stats.DeltaStartLSN}
}

// pgFailureSummary is the failure card's short text for the two PostgreSQL
// refusals of #3078 whose full message is longer than the card shows, so the
// card says what happened and what to do; "" for any other error. Only the
// refusal before anything is read counts as a slot "still being created":
// CheckSlotStillAnchors also matches ErrSlotNotReady, after the tables were
// read, and "nothing was read" would be false of it.
func pgFailureSummary(err error) string {
	var notReady *pgcapture.SlotNotReadyError
	if errors.As(err, &notReady) {
		return fmt.Sprintf("Not read: the replication slot %q was still being created after %s. PostgreSQL finishes it once the source's open write transactions end (pg_stat_activity, oldest xact_start). Nothing was read: run the snapshot again then.",
			notReady.Slot, waitInWords(notReady.Wait))
	}
	var gap *pgKnownGapError
	if errors.As(err, &gap) {
		return gap.summary()
	}
	return ""
}

// waitInWords is a wait as the card says it: "5 minutes", "1 minute", or
// Go's own form for anything that is not whole minutes.
func waitInWords(d time.Duration) string {
	if d >= time.Minute && d%time.Minute == 0 {
		if d == time.Minute {
			return "1 minute"
		}
		return strconv.Itoa(int(d/time.Minute)) + " minutes"
	}
	return d.String()
}

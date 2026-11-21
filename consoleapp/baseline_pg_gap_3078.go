package consoleapp

import (
	"fmt"
	"log/slog"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// pgCaptureStartFunc reads where the capture of req's server started reading
// its source from, off the server's index, and whether the index records it
// (query.PGCaptureStartLSN). A variable so a test answers it without an index.
var pgCaptureStartFunc = func(req console.BaselineRequest) (uint64, bool) {
	if req.IndexDSN == "" {
		return 0, false
	}
	db, err := config.Connect(req.IndexDSN)
	if err != nil {
		slog.Warn("could not reach the index to compare the snapshot's floor with where capture started; the snapshot is published without that check",
			"server", req.ServerID, "error", err)
		return 0, false
	}
	defer db.Close()
	return query.PGCaptureStartLSN(db)
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
// saved a position yet, or the index is unreachable) there is nothing to
// compare and the snapshot is published: its floor is then the slot's own
// position, which is where a capture that starts later reads from.
func pgRefuseKnownGap(req console.BaselineRequest, stats pgbaseline.Stats) error {
	from, ok := pgCaptureStartFunc(req)
	if !ok || from <= stats.DeltaStartLSN {
		return nil
	}
	return fmt.Errorf("the snapshot is not published: capture of this server started reading the source at %s, after %s where this snapshot's changes start, so what was committed between the two was not captured and an update of this snapshot would miss it. Run the snapshot again: capture is attached now, and the new snapshot starts from where it reads",
		pglogrepl.LSN(from), pglogrepl.LSN(stats.DeltaStartLSN))
}

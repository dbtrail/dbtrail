package consoleapp

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/status"
)

// The snapshot schedule's capture probe (#1791) for a PostgreSQL source
// (#2844): what it asks when nothing was indexed since an old snapshot.
//
// One rule sets this apart from the MySQL probe: only a loss on record takes
// a full read. The source being ahead is not evidence here. Its WAL moves
// for the whole PostgreSQL cluster, and the capture saves its position on a
// timer, so one read finds a healthy capture behind; and a capture that
// stopped is fixed by starting it, not by reading the source in full. So:
//
//   - a loss stamped at or after the snapshot (a lost replication slot):
//     the full read, as for MySQL. Read from the index alone, without a
//     connection to the source;
//   - the two positions equal: confirmed caught up;
//   - the source ahead, the capture past it, a capture that stopped saving,
//     a position that cannot be compared, a source that did not answer: the
//     source cannot be asked. The slot is skipped while the snapshot can
//     wait, and the reason is on the page.
//
// It never answers console.CaptureBehind.

// probeCapturePG is a package variable for the reason probeCapture is.
var probeCapturePG = probeCapturePGFromDBs

// probeCapturePGFromDBs is capturePGFromDBs within windowProbeTimeout, with
// the side that was being read named when the bound runs out (timedOutSide).
func probeCapturePGFromDBs(ctx context.Context, indexDSN, sourceDSN string, anchor, sourceRead time.Time) captureProbeResult {
	var indexRead atomic.Bool
	r := boundedCaptureProbe(ctx, windowProbeTimeout, func(ctx context.Context) captureProbeResult {
		return capturePGFromDBs(ctx, indexDSN, sourceDSN, anchor, sourceRead, &indexRead)
	})
	return timedOutSide(r, indexRead.Load())
}

// capturePGFromDBs reads the capture's record and its position from the
// index, then the source's position, and compares them.
// sourceRead is not used: it dates dropped rows, which a PostgreSQL capture
// does not record.
func capturePGFromDBs(ctx context.Context, indexDSN, sourceDSN string, anchor, _ time.Time, indexRead *atomic.Bool) captureProbeResult {
	idx, err := config.Connect(probeDSN(indexDSN))
	if err != nil {
		return captureProbeResult{detail: captureDetailIndexSilent, cause: config.ScrubDSNError(err, indexDSN, sourceDSN)}
	}
	defer idx.Close()
	st, err := status.LoadStreamState(ctx, idx)
	if err != nil {
		return captureProbeResult{detail: captureDetailCheckpointUnread, cause: config.ScrubDSNError(err, indexDSN, sourceDSN)}
	}
	// No connection to production for a verdict the index already settles.
	if ok, detail := pgCaptureComparable(st, anchor); !ok {
		indexRead.Store(true)
		return captureProbeResult{detail: detail}
	}
	row, err := readPGPositionRow(ctx, idx)
	if err != nil {
		return captureProbeResult{detail: captureDetailCheckpointUnread, cause: config.ScrubDSNError(err, indexDSN, sourceDSN)}
	}
	// The capture's record is read: from here on only the source is slow.
	indexRead.Store(true)
	if ok, detail := pgPositionRefusal(row); !ok {
		return captureProbeResult{detail: detail}
	}
	source, err := pgcapture.SourceFlushedLSN(ctx, sourceDSN)
	if err != nil {
		return captureProbeResult{detail: captureDetailSourceSilent, cause: config.ScrubDSNError(err, indexDSN, sourceDSN)}
	}
	verdict, detail := comparePGPositions(row.sentLSN(), source, row.savedFor())
	return captureProbeResult{verdict: verdict, detail: detail}
}

// pgCaptureComparable is captureComparable for a PostgreSQL capture: what
// the index's own record of the capture says before the source is asked.
// Pure.
//
// Not captureComparable itself, for one reason that a real PostgreSQL index
// showed: a PostgreSQL capture keeps no ledger of dropped rows
// (stream_state.capture_skips stays NULL), and captureComparable reads a
// ledger that is not there as one that cannot be read, which takes a full
// read. Here the loss record is the whole of it: gap_lost_at, which a lost
// replication slot stamps.
func pgCaptureComparable(st *status.StreamStateInfo, anchor time.Time) (ok bool, detail string) {
	switch {
	case st == nil:
		return false, captureDetailNoCapture
	case !st.GapColumnsPresent:
		return false, captureDetailPGNoLossRecord
	case st.GapLostAt.Valid && !st.GapLostAt.Time.Before(anchor):
		return false, captureDetailPGLost
	}
	// A PostgreSQL capture saves mode 'gtid' with its LSN as the set, so
	// this passes once it has saved a checkpoint.
	return checkpointComparable(st)
}

// comparePGPositions is the schedule's verdict on the capture's saved
// sent-through position and the source's flushed WAL position, read after
// it: console.CaptureCaughtUp when they are equal, else "" and why. Pure.
// It never returns console.CaptureBehind (see the top of this file).
func comparePGPositions(sent, source uint64, savedAgo time.Duration) (verdict, detail string) {
	switch {
	case sent == source:
		return console.CaptureCaughtUp, ""
	case sent > source:
		return "", captureDetailPGPast
	case savedAgo > pgSaveFresh:
		return "", captureDetailPGNotSaving
	}
	return "", captureDetailPGAhead
}

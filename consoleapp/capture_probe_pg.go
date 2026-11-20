package consoleapp

import (
	"context"
	"strings"
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
// One rule sets this apart from the MySQL probe: nothing this probe answers
// takes a full read. The source being ahead is not evidence here. Its WAL
// moves for the whole PostgreSQL cluster, and the capture saves its position
// on a timer, so one read finds a healthy capture behind; and a capture that
// stopped is fixed by starting it, not by reading the source in full. So:
//
//   - the two positions equal: confirmed caught up;
//   - the source ahead, the capture past it, a capture that stopped saving,
//     a position that cannot be compared, a source that did not answer: the
//     source cannot be asked. The slot is skipped while the snapshot can
//     wait, and the reason is on the page;
//   - a loss stamped at or after the snapshot (a lost replication slot):
//     said as the reason, from the index alone, and still "cannot be asked".
//     A loss does lead to a full read, by the path that was there before
//     this probe and that has a brake: the snapshot cannot wait
//     (quietSnapshotCanWait), so the update runs, is refused for the gap,
//     and the schedule takes one full read in its place, and no second one
//     while the same refusal repeats (fullReadCannotCure). The brake is why
//     it is not evidence here: a slot that stays lost stamps the same loss
//     again at every retry of the capture, after each new snapshot, and
//     evidence would read the source in full once per stamp.
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
// sourceRead dates dropped changes (pgCaptureComparable).
func capturePGFromDBs(ctx context.Context, indexDSN, sourceDSN string, anchor, sourceRead time.Time, indexRead *atomic.Bool) captureProbeResult {
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
	if ok, detail := pgCaptureComparable(st, anchor, sourceRead); !ok {
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
// Two records of loss: gap_lost_at, which a lost replication slot stamps,
// and the ledger of dropped changes (stream_state.capture_skips), where a
// capture records each change of a table below REPLICA IDENTITY FULL that it
// drops while the other tables keep flowing. A drop is dated against the
// last read of the SOURCE, as on MySQL (captureComparable): an update is
// built from the index and never brings a dropped change back.
//
// Not captureComparable itself, for one reason that a real PostgreSQL index
// showed: an index written by a PostgreSQL capture from before the ledger has
// capture_skips NULL, and captureComparable reads a ledger that is not there
// as one that cannot be read, which takes a full read. Here a NULL ledger is
// no drop (those captures stopped instead of dropping); one that is there and
// does not parse is a record that cannot be read. Both answers are "cannot
// be asked", like every answer of this probe: see the top of this file.
func pgCaptureComparable(st *status.StreamStateInfo, anchor, sourceRead time.Time) (ok bool, detail string) {
	switch {
	case st == nil:
		return false, captureDetailNoCapture
	case !st.GapColumnsPresent:
		return false, captureDetailPGNoLossRecord
	case st.GapLostAt.Valid && !st.GapLostAt.Time.Before(anchor):
		return false, captureDetailPGLost
	}
	if st.CaptureSkips.Valid && strings.TrimSpace(st.CaptureSkips.String) != "" {
		skips, readable := st.ParseCaptureSkips()
		if !readable {
			return false, captureDetailPGNoLossRecord
		}
		for _, sk := range skips {
			if sk.Count > 0 && (sourceRead.IsZero() || sk.LastAt.IsZero() || !sk.LastAt.Before(sourceRead)) {
				return false, captureDetailPGLost
			}
		}
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

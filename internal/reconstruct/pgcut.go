package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// pgFoldBounds is where a PostgreSQL fold stops and the next one starts
// (#1596).
//
// Every row of a PostgreSQL transaction carries its commit LSN as both
// start_pos and end_pos, so the MySQL seam (this fold `end_pos <= C`, the
// next `start_pos >= C`) would hand the transaction at C to both. The
// PostgreSQL seam is: this fold reads `end_pos <= Until` and the next one
// `start_pos >= Next`, with Next = Until + 1.
type pgFoldBounds struct {
	// Until bounds this fold's fetch (UntilPos); nil = the index holds no
	// event, nothing to fold.
	Until *query.BinlogPos
	// Next is the LSN the next fold starts from: the folded snapshot's
	// MetaKeyLSN. Never below the floor: when nothing is folded, the floor.
	Next uint64
	// Since is what the folded snapshot records as MetaKeyPGDeltaSince
	// (pgRunEnd.foldedDeltaSince): carried with Next, so a caller cannot
	// write one without the other.
	Since time.Time
}

// empty reports whether a fold from floor (the baseline's MetaKeyLSN, an
// inclusive start) can fetch nothing: no event, or every event before floor.
func (b pgFoldBounds) empty(floor uint64) bool {
	return b.Until == nil || b.Until.Pos < floor
}

// pgFirstEventPast is firstEventPast, a seam so the unit tests can answer it
// without the partition listing it walks; the integration test runs the real
// one.
var pgFirstEventPast = firstEventPast

// lsnText spells an LSN the way pgcapture writes it (%X/%X). Not pglogrepl:
// this package is linked into the MySQL binary, which must not link the
// PostgreSQL driver (cliapp/pgfree_test.go).
func lsnText(lsn uint64) string { return fmt.Sprintf("%X/%X", lsn>>32, uint32(lsn)) }

// PGDeltaFloor is where a reader of a PostgreSQL snapshot fetches the
// changes after it from: the snapshot's LSN floor (MetaKeyLSN), nil for a
// MySQL snapshot (it records a binlog coordinate, AnchorOf) or a PostgreSQL
// one taken before the floor was recorded. The floor is read before the
// snapshot opens, so a transaction can commit at or after it and before the
// snapshot's time, yet be missing from the snapshot (#771): a reader that
// starts at the snapshot's time drops it, with nothing to say so. Reading
// from the floor applies some changes the snapshot already holds again,
// which is harmless: every change is a full row image.
func PGDeltaFloor(bmeta baseline.DumpMetadata) *query.BinlogPos {
	if bmeta.LSN == 0 || bmeta.BinlogFile != "" {
		return nil
	}
	return lsnPos(bmeta.LSN)
}

// PGDeltaMargin is how far before a PostgreSQL snapshot's time a reader is
// sure to look for the changes it may be missing when the snapshot records no
// bound (MetaKeyPGDeltaSince). The only change a snapshot from `bintrail-pg
// baseline` can miss is one whose commit raced the snapshot's start: its
// commit time is just before the snapshot's, by far less than this. A commit
// held longer than this across the snapshot's start (a synchronous standby
// that stopped answering) is not covered. The fetch itself reads further back
// (query.CoarseSinceFloor under a position); the margin is what the
// coverage check holds the index to, so it stays short: an hour reached
// before a new index's first partition and refused every snapshot taken in
// its first hour.
const PGDeltaMargin = time.Minute

// PGDeltaWindow is where a reader of a snapshot fetches the changes after it
// from: since and from (SincePos), and whether since is a recorded or
// documented bound the fetch must keep (query.Options.SinceProven). For a
// MySQL snapshot, or a PostgreSQL one with no floor: snapshotTime, nil,
// false. For a PostgreSQL one: the floor, and MetaKeyPGDeltaSince (never
// after snapshotTime) or PGDeltaMargin before snapshotTime. A
// MetaKeyPGDeltaSince that does not parse refuses.
func PGDeltaWindow(bmeta baseline.DumpMetadata, snapshotTime time.Time) (since time.Time, from *query.BinlogPos, proven bool, err error) {
	from = PGDeltaFloor(bmeta)
	if from == nil {
		return snapshotTime, nil, false, nil
	}
	since = snapshotTime.Add(-PGDeltaMargin)
	if bmeta.PGDeltaSince != "" {
		t, perr := time.Parse(time.RFC3339, bmeta.PGDeltaSince)
		if perr != nil {
			return time.Time{}, nil, false, fmt.Errorf("the snapshot's bound on how far back its missing changes reach (%s = %q) cannot be read; %s",
				baseline.MetaKeyPGDeltaSince, bmeta.PGDeltaSince, pgRebaseline)
		}
		since = t.UTC()
		if since.After(snapshotTime) {
			since = snapshotTime
		}
	}
	return since, from, true, nil
}

// PGEventCapNote is what a PostgreSQL read adds to a per-row event cap's
// refusal: the count starts at the snapshot's LSN floor, which can be before
// the snapshot's time (#1596), so a nearer target time may not bring it
// under. "" when pg is false.
func PGEventCapNote(pg bool) string {
	if !pg {
		return ""
	}
	return " (on PostgreSQL the count starts at the snapshot's LSN floor, which can be before its time: a newer snapshot brings it down where a nearer time does not)"
}

func lsnPos(lsn uint64) *query.BinlogPos {
	return &query.BinlogPos{File: lsnText(lsn), Pos: lsn}
}

// resolvePGFoldBounds finds the bounds of a PostgreSQL fold to the point in
// time at, from a baseline whose floor is floor and whose snapshot was taken
// at baselineAt, read with the time bound prevSince (PGDeltaWindow):
// resolvePGRunEnd, then boundsFor. A run of several tables calls the two
// apart, the first once (see pgRunEnd).
func resolvePGFoldBounds(ctx context.Context, db *sql.DB, at time.Time, floor uint64, baselineAt, prevSince time.Time) (pgFoldBounds, error) {
	if err := pgPointAfterBaseline(at, baselineAt); err != nil {
		return pgFoldBounds{}, err
	}
	run, err := resolvePGRunEnd(ctx, db, at)
	if err != nil {
		return pgFoldBounds{}, err
	}
	return run.boundsFor(floor, baselineAt, prevSince)
}

// pgRunEnd is where every PostgreSQL fold of one run stops, read once per run
// (#1596): read per table while capture writes, the tables of one snapshot
// would each be folded through a different LSN.
//
//   - The first event committed after at, in commit order (event_id: a
//     PostgreSQL stream writes in commit order), is where the next fold
//     starts; this one stops just before its LSN.
//   - With none after at, this fold takes everything, and the next starts
//     after the newest event, when the stream's checkpoint shows that
//     event's transaction complete. A transaction too large for one index
//     write is visible in parts until its commit is checkpointed (#2391); its
//     LSN is then left whole to the next fold, never split between two.
//   - An LSN that goes backwards is impossible on one cluster: the index now
//     follows another one (a restored or replaced cluster, whose LSNs started
//     over). Refused when it shows: the first event past at with an LSN above
//     the newest event's here, the newest event committed after a baseline
//     with an LSN below its floor in boundsFor. Not caught once the other
//     cluster's LSNs pass the old ones everywhere this looks: no cluster
//     identity is recorded.
//   - An event with LSN 0 is refused: no commit has it, the index row is
//     damaged, and no bound can be placed around it.
//
// The newest event is the index's, of every table: one index database
// follows one source, the premise ResolveSnapshotCut makes for MySQL. An
// index holding a second source's rows would put the end, and the bound
// foldedDeltaSince derives from it, past where this source's capture is.
type pgRunEnd struct {
	at time.Time
	// none: the index holds no event.
	none bool
	// until is the last LSN a fold of this run may take.
	until     uint64
	newestEnd uint64
	newestAt  time.Time
}

// resolvePGRunEnd reads a run's pgRunEnd at the point at.
func resolvePGRunEnd(ctx context.Context, db *sql.DB, at time.Time) (pgRunEnd, error) {
	run := pgRunEnd{at: at}
	err := db.QueryRowContext(ctx, `SELECT end_pos, event_timestamp FROM binlog_events
		WHERE binlog_file IS NOT NULL AND end_pos IS NOT NULL
		ORDER BY event_id DESC LIMIT 1`).Scan(&run.newestEnd, &run.newestAt)
	if errors.Is(err, sql.ErrNoRows) {
		run.none = true
		return run, nil
	}
	if err != nil {
		return pgRunEnd{}, fmt.Errorf("find where a PostgreSQL fold stops (newest indexed event): %w", err)
	}
	if run.newestEnd == 0 {
		return pgRunEnd{}, errors.New("the newest change in the index has LSN 0, which no PostgreSQL commit has: the index row is damaged; a fold cannot place its bounds")
	}
	cut, _, err := pgFirstEventPast(ctx, db, at, 0)
	if err != nil {
		return pgRunEnd{}, err
	}
	if cut != nil {
		switch {
		case cut.Pos == 0:
			return pgRunEnd{}, fmt.Errorf("the first change in the index after %s has LSN 0, which no PostgreSQL commit has: the index row is damaged; a fold cannot place its bounds",
				at.UTC().Format(time.RFC3339))
		case cut.Pos > run.newestEnd:
			return pgRunEnd{}, fmt.Errorf("the first change in the index after %s (LSN %s) is later in the WAL than the newest change (LSN %s): "+
				"the index now follows another PostgreSQL cluster than the one the snapshot was taken from; take a new snapshot (`bintrail-pg baseline`)",
				at.UTC().Format(time.RFC3339), lsnText(cut.Pos), lsnText(run.newestEnd))
		}
		run.until = cut.Pos - 1
		return run, nil
	}
	var checkpoint sql.NullInt64
	err = db.QueryRowContext(ctx, `SELECT binlog_position FROM stream_state WHERE id = 1 AND flavor = 'postgres'`).Scan(&checkpoint)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return pgRunEnd{}, fmt.Errorf("find where a PostgreSQL fold stops (stream checkpoint): %w", err)
	}
	run.until = run.newestEnd - 1
	if checkpoint.Valid && uint64(checkpoint.Int64) >= run.newestEnd {
		run.until = run.newestEnd
	}
	return run, nil
}

// pgPointAfterBaseline refuses an at before the baseline: the baseline
// already holds changes after at, which no fold can take out.
func pgPointAfterBaseline(at, baselineAt time.Time) error {
	if at.Before(baselineAt) {
		return fmt.Errorf("the requested point %s is before the snapshot (taken %s), which already holds later changes; use an older snapshot",
			at.UTC().Format(time.RFC3339), baselineAt.UTC().Format(time.RFC3339))
	}
	return nil
}

// boundsFor is the run's end for one table, from a baseline whose floor is
// floor, whose snapshot was taken at baselineAt and whose changes were read
// from the time prevSince (PGDeltaWindow). A zero prevSince is refused: it
// would leave the folded snapshot no bound, and its reader would fall back to
// a margin before the fold's own time, past changes capture has not
// delivered yet.
func (r pgRunEnd) boundsFor(floor uint64, baselineAt, prevSince time.Time) (pgFoldBounds, error) {
	if err := pgPointAfterBaseline(r.at, baselineAt); err != nil {
		return pgFoldBounds{}, err
	}
	if prevSince.IsZero() {
		return pgFoldBounds{}, errors.New("a PostgreSQL fold was given no time its changes were read from; it cannot record how far back the changes after it reach")
	}
	b := pgFoldBounds{Next: floor}
	if !r.none {
		if r.newestEnd < floor && r.newestAt.After(baselineAt) {
			return pgFoldBounds{}, fmt.Errorf("the newest change in the index (committed %s, LSN %s) is after the snapshot (taken %s) but before its LSN %s: "+
				"the index now follows another PostgreSQL cluster than the one the snapshot was taken from; take a new snapshot (`bintrail-pg baseline`)",
				r.newestAt.UTC().Format(time.RFC3339), lsnText(r.newestEnd), baselineAt.UTC().Format(time.RFC3339), lsnText(floor))
		}
		b = foldFrom(floor, r.until)
	}
	b.Since = r.foldedDeltaSince(b, floor, prevSince)
	return b, nil
}

// foldedDeltaSince is what a snapshot folded to b records as
// MetaKeyPGDeltaSince: no change at or after b.Next committed before it.
// prevSince is the bound the fold read its own changes with (PGDeltaWindow).
//
// When nothing was folded the next fold starts at the same floor, so
// prevSince still holds. Otherwise a change this run did not fold is in the
// index past the run's end, or has not reached the index yet (capture is
// behind, or was stopped). Either one committed after the newest change the
// index holds, and, when a change past at was found, after at: commit times
// follow commit order within PGDeltaMargin, the premise PGDeltaWindow's
// margin already makes. The point alone would not do: a change committed
// before at that capture delivers only after this run has an LSN past the
// next floor and a time before at, and a reader that started at at would
// drop it.
//
// Truncated down to the second, as RFC3339 writes it.
func (r pgRunEnd) foldedDeltaSince(b pgFoldBounds, floor uint64, prevSince time.Time) time.Time {
	if b.Next == floor {
		return prevSince.UTC().Truncate(time.Second)
	}
	since := r.at
	if r.newestAt.Before(since) {
		since = r.newestAt
	}
	return since.Add(-PGDeltaMargin).UTC().Truncate(time.Second)
}

// foldFrom is the bounds of a fold from floor through until: nothing to fold
// when until is below floor, and the next fold starts at the floor again,
// never below it (a Next below the floor would apply again changes the
// baseline already holds).
func foldFrom(floor, until uint64) pgFoldBounds {
	if until < floor {
		return pgFoldBounds{Next: floor}
	}
	return pgFoldBounds{Until: lsnPos(until), Next: until + 1}
}

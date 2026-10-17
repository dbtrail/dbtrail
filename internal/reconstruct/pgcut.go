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
// at baselineAt.
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
//     over). Refused when it shows: the newest event committed after the
//     baseline with an LSN below its floor, or the first event past at with
//     an LSN above the newest event's. Not caught once the other cluster's
//     LSNs pass the old ones everywhere this looks: no cluster identity is
//     recorded.
//   - An at before the baseline is refused: the baseline already holds
//     changes after at, which no fold can take out.
//   - An event with LSN 0 is refused: no commit has it, the index row is
//     damaged, and no bound can be placed around it.
func resolvePGFoldBounds(ctx context.Context, db *sql.DB, at time.Time, floor uint64, baselineAt time.Time) (pgFoldBounds, error) {
	if at.Before(baselineAt) {
		return pgFoldBounds{}, fmt.Errorf("the requested point %s is before the snapshot (taken %s), which already holds later changes; use an older snapshot",
			at.UTC().Format(time.RFC3339), baselineAt.UTC().Format(time.RFC3339))
	}
	var newestEnd uint64
	var newestAt time.Time
	err := db.QueryRowContext(ctx, `SELECT end_pos, event_timestamp FROM binlog_events
		WHERE binlog_file IS NOT NULL AND end_pos IS NOT NULL
		ORDER BY event_id DESC LIMIT 1`).Scan(&newestEnd, &newestAt)
	if errors.Is(err, sql.ErrNoRows) {
		return pgFoldBounds{Next: floor}, nil
	}
	if err != nil {
		return pgFoldBounds{}, fmt.Errorf("find where a PostgreSQL fold stops (newest indexed event): %w", err)
	}
	if newestEnd == 0 {
		return pgFoldBounds{}, errors.New("the newest change in the index has LSN 0, which no PostgreSQL commit has: the index row is damaged; a fold cannot place its bounds")
	}
	if newestEnd < floor && newestAt.After(baselineAt) {
		return pgFoldBounds{}, fmt.Errorf("the newest change in the index (committed %s, LSN %s) is after the snapshot (taken %s) but before its LSN %s: "+
			"the index now follows another PostgreSQL cluster than the one the snapshot was taken from; take a new snapshot (`bintrail-pg baseline`)",
			newestAt.UTC().Format(time.RFC3339), lsnText(newestEnd), baselineAt.UTC().Format(time.RFC3339), lsnText(floor))
	}
	cut, _, err := pgFirstEventPast(ctx, db, at, 0)
	if err != nil {
		return pgFoldBounds{}, err
	}
	if cut != nil {
		switch {
		case cut.Pos == 0:
			return pgFoldBounds{}, fmt.Errorf("the first change in the index after %s has LSN 0, which no PostgreSQL commit has: the index row is damaged; a fold cannot place its bounds",
				at.UTC().Format(time.RFC3339))
		case cut.Pos > newestEnd:
			return pgFoldBounds{}, fmt.Errorf("the first change in the index after %s (LSN %s) is later in the WAL than the newest change (LSN %s): "+
				"the index now follows another PostgreSQL cluster than the one the snapshot was taken from; take a new snapshot (`bintrail-pg baseline`)",
				at.UTC().Format(time.RFC3339), lsnText(cut.Pos), lsnText(newestEnd))
		}
		return foldFrom(floor, cut.Pos-1), nil
	}
	var checkpoint sql.NullInt64
	err = db.QueryRowContext(ctx, `SELECT binlog_position FROM stream_state WHERE id = 1 AND flavor = 'postgres'`).Scan(&checkpoint)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return pgFoldBounds{}, fmt.Errorf("find where a PostgreSQL fold stops (stream checkpoint): %w", err)
	}
	if checkpoint.Valid && uint64(checkpoint.Int64) >= newestEnd {
		return foldFrom(floor, newestEnd), nil
	}
	return foldFrom(floor, newestEnd-1), nil
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

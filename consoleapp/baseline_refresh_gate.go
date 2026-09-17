package consoleapp

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/status"
)

// indexMark is how much the index has EVER been written, as two high-water
// marks: the newest row event and the newest recorded schema change (#1689).
//
// It answers the FIRST of the two questions asked before a refresh cycle
// starts: has anything been indexed since the last fold? A cycle for which the
// answer is no has nothing any table could apply — though that alone does not
// settle it, because not republishing has its own cost; see
// snapshotStillCovered for the second question, which is what makes acting on
// this one safe.
//
// A cycle both answer yes for does not run at all: no snapshot directory, no
// upload, no run record, no third outcome for any surface to misreport. That is
// the whole reason this is a gate rather than a decision taken after folding —
// withholding a finished run's output means teaching every
// surface that reads a refresh outcome about a third case.
//
// AUTO_INCREMENT ids, deliberately, and NOT the run's binlog cut. The cut is a
// POSITION, and a position does not move when older binlogs are indexed after
// the fact — `bintrail index` over earlier files lands rows BELOW the newest
// position, so a cut-based gate would skip real work for as long as the
// backfill lasted. A second source writing into the same index breaks it a
// different way: its positions are not comparable with the first source's at
// all, so there is no "below" to reason about. An auto-increment id rises on
// every INSERT whatever its position, which is the property this needs.
//
// The schema-changes half is not redundant with the events half. A TRUNCATE
// writes no row events — that is exactly why CheckDestructiveDDL exists — so it
// moves no event id, and without this the cycle that would have refused a
// TRUNCATE'd table would never start and the operator would not be told their
// baseline had stopped being faithful.
type indexMark struct {
	events        uint64
	schemaChanges uint64
}

// foldMemo is what one server's last completed fold left behind: the index
// marks it read before folding, and the instant that NAMES the snapshot
// directory it published.
//
// The two are needed together because skipping a cycle has two costs, not one.
// The marks answer "is there anything to apply". publishedAt answers the second
// question, which is the one that makes the first one safe to act on — see
// snapshotStillCovered.
type foldMemo struct {
	mark indexMark
	// publishedAt dates the published snapshot, and it is the DIRECTORY's
	// instant rather than any anchor stamped inside a file. That is not a
	// convenience: findBaselineLocal dates a snapshot by its directory name, so
	// the directory instant is what becomes the delta window's lower bound on
	// the next fold, and therefore the only one coverage is ever checked
	// against. A carried-forward table keeps an OLDER anchor in its own footer,
	// and that anchor IS read — fulltable.go hands it to Options.SincePos, which
	// replaces the exact Since row filter. It is still harmless here: buildQuery
	// floors the coarse bound at Since.Truncate(1h)-1h, derived from the
	// DIRECTORY instant, so an older position can only narrow inside that window
	// and never reach back into hours rotation dropped. query.Plan, which is what
	// grades coverage, takes only times and never sees the position at all.
	publishedAt time.Time
	// destination is where that fold PUBLISHED: the local directory and the
	// bucket, as one string. A memo speaks only for a cycle going to the same
	// place.
	//
	// Two producers write this memo. The daemon-wide --baseline-refresh-interval
	// loop always leaves BaselineS3 empty and publishes locally only; the
	// per-server backup schedule sets it and uploads. Without this, a local-only
	// fold satisfies the gate for the scheduled cycle whose entire job was to get
	// a copy into the bucket, and the bucket silently never receives it. The
	// local directory is in here for the same reason one step down: an operator
	// re-pointing a server at a new mount or a new bucket from the settings panel
	// would otherwise have the gate report that the new destination is up to
	// date, which is true about the DATA and false about the destination.
	destination string
	// indexDSN is which index the marks above were read from, and a memo only
	// speaks for that one. An operator can re-point a server at a different
	// index from the settings panel, and the marks are ordinary auto-increment
	// ids: two unrelated indexes can hold the same pair by coincidence, which
	// would read as "nothing has been indexed" about an index this daemon has
	// never folded from. Compared, never logged.
	indexDSN string
}

// snapshotStillCovered reports whether a published snapshot is still inside the
// window the index can fold from, and whether that could be determined at all.
//
// This is the question that makes the whole gate safe. Skipping a cycle does not
// just save a fold: it stops the snapshot DIRECTORY from advancing, and that
// directory's instant is the lower bound of the next fold's window. The index
// only keeps what rotation has not dropped, so a frozen snapshot with an
// advancing retention floor ends up asking for hours nothing holds any more, and
// the next fold that DOES run refuses for a coverage gap — on a server that was
// merely quiet. The republish this gate calls pointless was doing exactly one
// useful thing, and this is it.
//
// Worse without this check, and worth spelling out because it is not obvious:
// the gate would open at the WORST possible moment. MAX(event_id) is stable
// while the event-bearing partitions live and then drops to zero when rotation
// takes the last one — at about retention age, which is the same moment the
// frozen window start falls below the floor. The mark would "change", the gate
// would open, and the fold it released would be the one guaranteed to refuse.
//
// # Why the LIVE partition floor, and not the one the rest of the product grades
//
// status.OldestDeltaFromDB extends the floor BACKWARDS over contiguous archives,
// and that is right for the question it answers — "how far back can this index
// still restore". It is the wrong input for the question here, which is "how
// long may this server safely do nothing", for two reasons, and the second one
// is a correctness bug rather than a matter of taste:
//
//  1. On an archiving install the extended floor reaches back to the first hour
//     ever archived, so the coverage span is the install's whole age and the
//     keepalive would not fire for YEARS. Measured against the real grading on a
//     400-day install: 4.0 years.
//  2. That floor can JUMP. The archive extension only applies while the archived
//     range still touches the live partitions, and it is dropped entirely when
//     the index cannot attribute its archives to one source. So an operator
//     turning archiving off, or simply registering a SECOND server against the
//     same index, moves the floor from install-era to now-minus-retention in one
//     read — and a snapshot that graded ok the cycle before grades broken, with
//     no aging in between. Aging is the verdict this gate waits for, so it would
//     never see the one state it was watching for, and the fold it finally
//     released would be the refusing one all over again.
//
// The live floor moves smoothly, at the rate rotation drops partitions, and the
// live partitions are SHARED by every source writing into the index — so nothing
// an operator does to archives or to the server list can move it in a step. The
// cost of ignoring the archives is that a quiet archiving server re-anchors on
// the retention clock rather than on its much longer restorable history, which is
// one republish every 0.8 of retention. That is the cost the archives were
// letting it skip, and it was not safe to skip.
//
// The threshold itself is still the product's own (status.BaselineStalenessFor,
// baselineAgingFraction): anything it would already describe as aging or worse
// counts as not covered.
var snapshotStillCovered = snapshotStillCoveredInDB

func snapshotStillCoveredInDB(ctx context.Context, dsn string, publishedAt, now time.Time) (covered, known bool) {
	db, err := config.Connect(dsn)
	if err != nil {
		slog.Debug("baseline refresh: could not open the index to check how far it still reaches", "error", err)
		return false, false
	}
	defer db.Close()
	return snapshotCoveredIn(ctx, db, indexDBName(dsn), publishedAt, now)
}

// snapshotCoveredIn is the half that runs the query, split from the connect so
// it can be tested. It reads the LIVE partitions and nothing else — a test can
// hold it to that, which is the point: reaching for the archive-extended floor
// here is the mistake this split exists to make visible.
func snapshotCoveredIn(ctx context.Context, db *sql.DB, dbName string, publishedAt, now time.Time) (covered, known bool) {
	parts, err := status.LoadPartitionStats(ctx, db, dbName)
	if err != nil {
		slog.Debug("baseline refresh: could not read how far the index still reaches", "error", err)
		return false, false
	}
	floor := status.OldestLivePartitionHour(parts)
	if floor.IsZero() {
		// No parsable partition, so there is no floor to grade against — an
		// unpartitioned binlog_events, or names that have drifted out of the
		// scheme. Reported as NOT KNOWN rather than as a verdict of "not
		// covered": both fold, so nothing changes behaviourally, but only this
		// one reaches reportGateBlind and says so out loud. Grading it would be
		// the last silent way for the gate to be permanently inert.
		slog.Debug("baseline refresh: the index has no partition to measure coverage from")
		return false, false
	}
	return snapshotCoveredBy(floor, publishedAt, now), true
}

// snapshotCoveredBy is the verdict itself, split from the read so the threshold
// is pinned by a test rather than by a stub standing in for one.
//
// A zero floor — an index with no parsable partitions — grades unknown, and so
// does a memo with no published instant; both are meant to fold.
//
// Exactly OK, not merely "not broken". Broken means the snapshot is ALREADY
// outside the window, which is too late — the fold that would re-anchor it is
// the fold that refuses. Aging is the last verdict from which republishing still
// works, so it is the one that has to open the gate.
func snapshotCoveredBy(liveFloor, publishedAt, now time.Time) bool {
	return status.BaselineStalenessFor(publishedAt, liveFloor, now) == status.BaselineOK
}

// readIndexMark is a package variable for the reason foldTables and
// uploadSnapshot are: it opens the index, so nothing at the unit tier could
// otherwise reach the gate that sits above everything this cycle would write. A
// test that replaces it must not restore it until the cycle it started has
// finished — the cycles run in their own goroutines.
var readIndexMark = readIndexMarkFromDB

// readIndexMarkFromDB opens the index and reads both marks. known is false when
// the answer could not be READ — the index cannot be opened, or a query fails —
// and every caller must then FOLD.
//
// Failing toward folding is the only safe direction here. A false "nothing
// changed" skips a refresh that was owed and the backup silently stops moving;
// a false "something changed" costs one fold that applies nothing, which is now
// cheap (#1697, #1701). The asymmetry is the whole design of this gate.
func readIndexMarkFromDB(ctx context.Context, dsn string) (mark indexMark, known bool) {
	db, err := config.Connect(dsn)
	if err != nil {
		slog.Debug("baseline refresh: could not open the index to check for new data; folding", "error", err)
		return indexMark{}, false
	}
	defer db.Close()
	return readIndexMarkFrom(ctx, db)
}

// readIndexMarkFrom is the half that runs the queries, split from the connect so
// it can be tested: readIndexMarkFromDB takes a DSN and opens its own handle, so
// there is no seam to hand a stub to.
func readIndexMarkFrom(ctx context.Context, db *sql.DB) (mark indexMark, known bool) {
	events, ok := readMaxID(ctx, db, "SELECT MAX(event_id) FROM binlog_events")
	if !ok {
		return indexMark{}, false
	}
	changes, ok := readMaxID(ctx, db, "SELECT MAX(id) FROM schema_changes")
	if !ok {
		return indexMark{}, false
	}
	return indexMark{events: events, schemaChanges: changes}, true
}

// readMaxID reads one high-water mark. A NULL — an empty table — is a mark of
// ZERO, and the read is known.
//
// This distinction is the whole correctness of the gate, and reading NULL as
// "unknown" got it wrong in both directions. NULL is not doubt: it is a definite
// answer, and it says the table holds no rows. Only a failed READ is doubt.
//
// Empty is also the NORMAL, PERMANENT state of one of the two tables.
// schema_changes stays empty for as long as the source runs no DDL, which for
// plenty of sources is forever — so treating empty as unknown made the gate
// answer "I cannot tell" on every cycle of those installs, for good, and the
// whole feature silently did nothing. Nothing logged that, because "cannot
// tell" is the ordinary quiet path.
//
// Reading it as a stable zero is correct on its own terms, not just convenient:
// zero is stable while the table stays empty, which is exactly the property the
// gate needs, and the first row ever written moves the mark off zero and folds.
// The same holds for an index that has captured nothing yet — no rows means
// nothing a fold could apply, and the first captured event lifts the mark.
func readMaxID(ctx context.Context, db *sql.DB, q string) (uint64, bool) {
	var v sql.NullInt64
	if err := db.QueryRowContext(ctx, q).Scan(&v); err != nil {
		slog.Debug("baseline refresh: could not read the index high-water mark; folding", "query", q, "error", err)
		return 0, false
	}
	if !v.Valid {
		return 0, true // empty table: a mark of zero, and it is known
	}
	if v.Int64 < 0 {
		// Both columns are UNSIGNED, so this is not a value this can compare
		// against a later one — it is a read that did not come back meaning
		// what it says. Treated as unreadable so the cycle folds.
		slog.Debug("baseline refresh: index high-water mark is negative; folding", "query", q, "value", v.Int64)
		return 0, false
	}
	return uint64(v.Int64), true
}

// unchangedSince reports whether nothing has been indexed since prev.
//
// Equality, never "not greater". A mark that went BACKWARDS is a different
// index than the one that was folded — rotation dropped every partition, or
// `restore-index` rebuilt the table and restarted its ids — and the honest
// answer there is that this gate cannot speak for it, so the cycle folds.
func (m indexMark) unchangedSince(prev indexMark) bool {
	return m.events == prev.events && m.schemaChanges == prev.schemaChanges
}

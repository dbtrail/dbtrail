package console

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/views"
)

// How old an answer read from a PostgreSQL snapshot alone is (#3125).
//
// A PostgreSQL snapshot records no event of the index, so the MySQL rule
// (copyDirMark) cannot place it. What every file of one records, since
// #1596, is the pair its readers start from: an LSN floor (MetaKeyLSN) and
// the bound that goes with it (MetaKeyPGDeltaSince), "no change at or after
// the floor committed before this instant". The file holds every change
// below its floor, so it holds every change committed before the bound. That
// instant is the answer's "through": the one a tail laid over the file starts
// from (pgTailSource.cut), read the same way.
//
// It errs OLD, with one limit named under "The clocks":
//   - it is the bound of each file the statement read, the oldest of them.
//     A table with no change is published again as the file it was
//     (pgCarryForwardEligible), with the bound of the update that last wrote
//     it in its footer; the folder's record of where its update ended
//     (reconstruct.PGRunCutFile) says the update found it unchanged through
//     the update's own bound, and that bound is taken (pgHeldThrough). A
//     folder with no record, or a record from before it named the bound,
//     leaves such a table as old as the update that last wrote its file;
//   - the bound sits PGDeltaMargin (a minute) before the moment it was taken
//     at, so an answer from a snapshot just taken reads a minute old;
//   - a source with no write since the snapshot makes the age grow though
//     nothing is missing: the age is the reader's clock minus the bound,
//     never capture's state. A capture that stopped does the same.
//
// The clocks: a full read's bound is the source's clock (its now() when the
// read began), an update's is the older of the commit time of the newest
// change the index held (the source's clock) and the update's own time (the
// clock of the machine that ran it). A source clock ahead of this machine's
// would make that instant look newer than it is, so it is capped at when the
// file was written, by this machine's clock (pgFileWritten): a file is never
// younger than its last write. What the cap cannot see is the time between
// the read and the write: with a source clock ahead, a file that took
// minutes to write can read younger than its rows by up to the lesser of the
// clock's lead and that time.
//
// A file that records no bound (a full read from before it was recorded),
// one that is not a file of a local snapshot folder, one published over a
// known loss of captured changes, one of a table capture dropped changes of
// since its rows were read (an update can have stamped it with a new bound
// over them), one of a table outside what capture records now (its changes
// are dropped before the record of drops, and an update can stamp it the
// same way), and any file while capture has saved no state for longer than
// pgNoCaptureStateBound have no age: unknown, as before.

// pgFileThrough is the instant the file whose footer is md holds every change committed before, in the snapshot
// folder taken at dirAt, or why it has none.
func pgFileThrough(md baseline.DumpMetadata, dirAt time.Time) (time.Time, string) {
	switch {
	case md.PGDeltaSince == "":
		// PGDeltaWindow would take a margin before the folder's time, which
		// is right for the folder the file was written in and past the truth
		// in any later one.
		return time.Time{}, "records no bound on the changes it may be missing"
	case md.CaptureGap != "":
		return time.Time{}, "was published over a known loss of captured changes"
	}
	since, from, proven, err := reconstruct.PGDeltaWindow(md, dirAt)
	switch {
	case err != nil:
		return time.Time{}, "records a bound on the changes it may be missing that cannot be read"
	case from == nil || !proven || md.LSN == 0:
		return time.Time{}, "records no LSN to continue from"
	}
	return since, ""
}

// pgTableThroughOf is the instant a table holds every change committed
// before, as a statement reads it from the snapshot folder taken at dirAt,
// from its file's footer. A table delta beside the file (#2757) only adds
// changes to what the file holds, so the file's bound is true of the two
// together, and it is the one used: older than the delta's own, never newer.
func pgTableThroughOf(file baseline.DumpMetadata, dirAt time.Time) (time.Time, string) {
	if baseline.ReadConsistencyOf(file) != baseline.ReadConsistent {
		return time.Time{}, "its file does not say it was read at one moment of the source"
	}
	through, why := pgFileThrough(file, dirAt)
	if why != "" {
		return time.Time{}, "its file " + why
	}
	return through, ""
}

// pgLossOf says why a table's file cannot be trusted to hold what its footer
// says, "" when it can: capture dropped changes of the table, or lost
// events, since its rows were read from the source (pgTailCaptureLoss, the
// rule a tail refuses by). An update can lay the changes the index has over
// such a file and stamp it with a new bound, and no footer says so.
type pgLossOf func(t views.BaselineTable, md baseline.DumpMetadata) string

// pgTableThrough is pgTableThroughOf for the table t, its footer read and
// asked of loss. The detail of a refusal names the table, for the log.
func (s *Server) pgTableThrough(t views.BaselineTable, loss pgLossOf) (time.Time, string) {
	name := t.Schema + "." + t.Table
	if t.Delta || len(t.DeltaFiles) > 0 {
		return time.Time{}, name + ": a MySQL table delta is beside its file"
	}
	dir, ok := copySnapshotDir(t)
	if !ok {
		return time.Time{}, name + ": not a table file in a local snapshot folder"
	}
	dirAt, ok := snapshotdir.ParseTime(filepath.Base(filepath.Clean(dir)))
	if !ok {
		return time.Time{}, name + ": its snapshot folder does not say when it was taken"
	}
	md, err := s.copyCuts.footer(t.Path)
	if err != nil {
		return time.Time{}, fmt.Sprintf("%s: its file could not be read: %v", name, err)
	}
	through, why := pgTableThroughOf(md, dirAt)
	if why != "" {
		return time.Time{}, name + ": " + why
	}
	if why := loss(t, md); why != "" {
		return time.Time{}, name + ": " + why
	}
	// Never younger than its last write, whatever the source's clock said.
	written := pgFileWritten(t)
	if written.IsZero() {
		return time.Time{}, name + ": when its file was written could not be told"
	}
	if written.Before(through) {
		through = written
	}
	// A table the folder's update brought to its end, carried unchanged:
	// the update's bound, never newer than the folder.
	if held := s.pgHeldThrough(dir, dirAt, t); held.After(through) {
		through = held
	}
	return through, ""
}

// pgAgeRunCuts remembers the snapshot folders' records of where their update
// ended, for the age of a statement's answer (pgHeldThrough). A published
// folder is never written again.
var pgAgeRunCuts pgRunCutMemo

// pgHeldThrough is the bound the update that published dir brought t to;
// zero when it is later than an update's bound can be (past the folder's
// time dirAt less PGDeltaMargin), when dir's record does not hold t,
// names no bound (written before it did), is for another folder, or cannot be
// read, and for a folder reached through a link or the pointer to the newest
// snapshot. The record is written only for a table the update read over its
// whole window with no loss of captured changes (pgHoldThroughRun), and its
// bound is the one every file the update wrote records.
func (s *Server) pgHeldThrough(dir string, dirAt time.Time, t views.BaselineTable) time.Time {
	clean := filepath.Clean(dir)
	if fi, err := os.Lstat(clean); err != nil || fi.Mode()&os.ModeSymlink != 0 {
		return time.Time{}
	}
	run, ok := pgAgeRunCuts.of(dir, func(err error) {
		s.copyFaults.warn(dir, "Engine Routing: a snapshot folder's record of where its update ended could not be read; the age of an answer that reads one of its PostgreSQL tables unchanged is the age of that table's file",
			"dir", dir, "error", err)
	})
	if !ok || !run.At.Equal(dirAt) || !run.Holds(t.Schema, t.Table) || run.Since.IsZero() {
		return time.Time{}
	}
	// An update's bound sits PGDeltaMargin before its own time at the
	// latest (foldedDeltaSince); one past that is a record that is wrong,
	// and its word is not taken.
	if run.Since.After(dirAt.Add(-reconstruct.PGDeltaMargin)) {
		return time.Time{}
	}
	return run.Since
}

// pgReadThroughWith is the oldest pgTableThrough of the tables a statement
// read, or why one of them has none.
func (s *Server) pgReadThroughWith(read []views.BaselineTable, loss pgLossOf) (time.Time, string) {
	var oldest time.Time
	for _, t := range read {
		at, why := s.pgTableThrough(t, loss)
		if why != "" {
			return time.Time{}, why
		}
		if oldest.IsZero() || at.Before(oldest) {
			oldest = at
		}
	}
	if oldest.IsZero() {
		return time.Time{}, "the statement read no table of the copy"
	}
	return oldest, ""
}

// pgSnapshotLossState is the part of the capture's state the age of a
// snapshot answer depends on, in one read of the index: the lost slot and the
// ledger of dropped changes, both as they are now (a cached record would be
// an optimistic one). nil with no reason: capture has saved no state. Any
// error, an index without these columns included, is a reason: the age is
// then unknown.
func pgSnapshotLossState(ctx context.Context, db *sql.DB) (*status.StreamStateInfo, string) {
	var lost sql.NullTime
	var skips sql.NullString
	err := db.QueryRowContext(ctx, "SELECT gap_lost_at, capture_skips FROM stream_state WHERE id = 1").Scan(&lost, &skips)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ""
	case err != nil:
		pgWarnLossStateUnread(err)
		return nil, "the capture's record of lost and dropped changes could not be read from the index (DBTrail's log has the details)"
	}
	return &status.StreamStateInfo{GapColumnsPresent: true, GapLostAt: lost, CaptureSkips: skips}, ""
}

// pgReadThrough is pgReadThroughWith for server id over what capture records
// now and its record of what it lost, read from b's index
// (pgSnapshotLossState): a server whose capture filter is not known here, or
// an index that cannot be asked, leaves the statement with no instant. So
// does a table outside the filter, and a capture with no saved state past
// pgNoCaptureStateBound, the rule of the port's list of tables not captured.
func (s *Server) pgReadThrough(ctx context.Context, b *bundle, id string, read []views.BaselineTable) (time.Time, string) {
	if len(read) == 0 {
		return time.Time{}, "the statement read no table of the copy"
	}
	if b == nil || b.db == nil {
		return time.Time{}, "the server's index, which records what capture lost, could not be reached"
	}
	captures, _, known := s.tailCapturesKeyed(id)
	if !known {
		return time.Time{}, "which tables capture records is not known here"
	}
	rctx, cancel := context.WithTimeout(ctx, copyUnchangedTimeout)
	st, unreadable := pgSnapshotLossState(rctx, b.db)
	cancel()
	if unreadable != "" {
		return time.Time{}, unreadable
	}
	now := time.Now()
	return s.pgReadThroughWith(read, func(t views.BaselineTable, md baseline.DumpMetadata) string {
		if captures != nil && !captures(t.Schema, t.Table) {
			return "it is outside what capture records"
		}
		lsn, at := reconstruct.PGReadPoint(md)
		if st == nil {
			if age, ok := pgSnapshotAge(now, at, pgFileWritten(t)); !ok || age >= pgNoCaptureStateBound {
				return "capture has saved no state in the index since its snapshot was read, so its dropped changes are not on record"
			}
			return ""
		}
		return pgTailCaptureLoss(st, lsn, at, t.Schema, t.Table)
	})
}

// statementBehindFor is statementBehind for server id's family. A MySQL
// server, and a PostgreSQL one whose statement reads every table with its
// tail, go through statementBehind as before. A PostgreSQL statement that
// reads a table from its snapshot alone is as far behind as the oldest
// pgTableThrough is old, by this machine's clock: capture's state is not
// asked, so a quiet source never reads as caught up here.
//
// The port's own listing of the copy's columns (WithPGCatalogRead) is not
// held to the limit: it reads which columns the copy has, not their rows.
func (s *Server) statementBehindFor(ctx context.Context, b *bundle, id string, in views.Input, read []views.BaselineTable,
	tailThrough time.Time, tailThroughID uint64, allTailed bool) (time.Duration, string) {
	if pgCatalogRead(ctx) {
		return 0, ""
	}
	if allTailed || b == nil || !s.SourceIsPostgres(id) {
		return s.statementBehind(ctx, b, id, in, read, tailThrough, tailThroughID, allTailed)
	}
	cut, why := s.pgReadThrough(ctx, b, id, read)
	if why != "" {
		if ctx.Err() == nil {
			s.copyAges.faults.warn(id, "Engine Routing: how far the copy's snapshot reaches is not known, so statements held to the server's lag limit are answered by the source (or refused on a connection that does not route)",
				"server", id, "reason", why)
		}
		return 0, copyAgeWhyNotPlaced
	}
	// Kept for the caller that asked how far the statement's files reach,
	// so the index is not asked twice for one statement.
	if into, _ := ctx.Value(pgSnapshotAgeKey{}).(*PGSnapshotAge); into != nil {
		into.set(cut)
	}
	return copyAgeOf(time.Now(), olderOf(cut, tailThrough)), ""
}

type pgCatalogReadKey struct{}

// WithPGCatalogRead is ctx for the PostgreSQL port's own listing of the
// copy's columns, which its screen needs before it can send any statement to
// the copy. It is not a statement of the client's and returns no row of a
// table, so the server's lag limit does not hold it: held to it, a copy past
// the limit (or one whose age cannot be told) refused the listing, and every
// statement was then sent to the source as one the screen could not read
// (veto) and never as what it was, a copy behind its limit.
func WithPGCatalogRead(ctx context.Context) context.Context {
	return context.WithValue(ctx, pgCatalogReadKey{}, true)
}

func pgCatalogRead(ctx context.Context) bool {
	v, _ := ctx.Value(pgCatalogReadKey{}).(bool)
	return v
}

// PGSnapshotAge receives, for one run on the copy, the instant the snapshot
// files it read hold every change committed before (WithPGSnapshotAge).
type PGSnapshotAge struct {
	mu      sync.Mutex
	through time.Time
}

func (p *PGSnapshotAge) set(through time.Time) {
	p.mu.Lock()
	p.through = through
	p.mu.Unlock()
}

func (p *PGSnapshotAge) get() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.through
}

type pgSnapshotAgeKey struct{}

// WithPGSnapshotAge is ctx for a run on the copy of a PostgreSQL server whose
// caller wants to know how far the snapshot files it read reach: into is
// filled when the run succeeds and every table it read has a bound, and left
// as it is otherwise.
func WithPGSnapshotAge(ctx context.Context, into *PGSnapshotAge) context.Context {
	if into == nil {
		return ctx
	}
	return context.WithValue(ctx, pgSnapshotAgeKey{}, into)
}

// notePGSnapshotThrough fills the PGSnapshotAge ctx carries, when it carries
// one that the lag limit's own measure did not fill already, with
// pgReadThrough of the tables the statement read. Its caller does not call it
// for a statement that read every table with its tail, whose age is the
// tails': this reads the index on the statement's turn.
func (s *Server) notePGSnapshotThrough(ctx context.Context, b *bundle, read []views.BaselineTable) {
	into, _ := ctx.Value(pgSnapshotAgeKey{}).(*PGSnapshotAge)
	if into == nil || len(read) == 0 || !into.get().IsZero() {
		return
	}
	id, ok := tailsFor(ctx)
	if !ok {
		return
	}
	if through, why := s.pgReadThrough(ctx, b, id, read); why == "" {
		into.set(through)
	}
}

// Place is a with the instant its snapshot files reach, when the run read a
// table without its recent changes and the instant is known: Through is then
// that instant and Snapshot stays set. A statement that read some tables with
// their tails and some without is placed by its files alone, which is true of
// it and can only be older.
// Any other age is returned as it is.
func (p *PGSnapshotAge) Place(a CopyRunAge) CopyRunAge {
	if p == nil || !a.Snapshot {
		return a
	}
	through := p.get()
	if through.IsZero() {
		return a
	}
	a.Through = through
	return a
}

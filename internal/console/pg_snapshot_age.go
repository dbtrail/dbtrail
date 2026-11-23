package console

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
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
// It can only err OLD, and it does:
//   - it is the bound of each file the statement read, the oldest of them,
//     never a bound of the folder: a table with no change is published again
//     as the file it was (pgCarryForwardEligible), with the bound of the
//     update that last wrote it, and nothing in a footer says through which
//     LSN a later update found it unchanged. Such a table reads as old as
//     that update, for as long as it is carried;
//   - the bound sits PGDeltaMargin (a minute) before the moment it was taken
//     at, so an answer from a snapshot just taken reads a minute old;
//   - a source with no write since the snapshot makes the age grow though
//     nothing is missing: the age is the reader's clock minus the bound,
//     never capture's state. A capture that stopped does the same.
//
// The clocks: a full read's bound is the source's clock (its now() when the
// read began), an update's is the older of the commit time of the newest
// change the index held (the source's clock) and the update's own time (the
// clock of the machine that ran it). The age is the reader's clock minus
// that, as every age the router reports: a source clock ahead of the
// reader's reads as less lag, down to zero.
//
// A file that records no bound (a full read from before it was recorded),
// one that is not a file of a local snapshot folder, one published over a
// known loss of captured changes, and one of a table capture dropped changes
// of since its rows were read (an update can have stamped it with a new
// bound over them) have no age: unknown, as before.

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
	return through, ""
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

// pgReadThrough is pgReadThroughWith over capture's record of what it lost,
// read from b's index now (strictly, pgCaptureLossState): an index that
// cannot be asked leaves the statement with no instant. A capture that has
// saved no state yet records no loss.
func (s *Server) pgReadThrough(ctx context.Context, b *bundle, read []views.BaselineTable) (time.Time, string) {
	if len(read) == 0 {
		return time.Time{}, "the statement read no table of the copy"
	}
	if b == nil || b.db == nil {
		return time.Time{}, "the server's index, which records what capture lost, could not be reached"
	}
	rctx, cancel := context.WithTimeout(ctx, copyUnchangedTimeout)
	st, unreadable := pgCaptureLossState(rctx, b.db)
	cancel()
	if unreadable != "" {
		return time.Time{}, unreadable
	}
	return s.pgReadThroughWith(read, func(t views.BaselineTable, md baseline.DumpMetadata) string {
		if st == nil {
			return ""
		}
		lsn, at := reconstruct.PGReadPoint(md)
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
	cut, why := s.pgReadThrough(ctx, b, read)
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
// pgReadThrough of the tables the statement read.
func (s *Server) notePGSnapshotThrough(ctx context.Context, b *bundle, read []views.BaselineTable) {
	into, _ := ctx.Value(pgSnapshotAgeKey{}).(*PGSnapshotAge)
	if into == nil || len(read) == 0 || !into.get().IsZero() {
		return
	}
	if through, why := s.pgReadThrough(ctx, b, read); why == "" {
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

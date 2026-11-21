package console

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"time"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

// A PostgreSQL tail's large values read from the copy (#3049).
//
// On a table below REPLICA IDENTITY FULL, an UPDATE that leaves a large
// value as it was does not carry it. A tail takes it from the row's earlier
// change when it holds one; when it does not, the value is in the published
// cut the tail lies over, and is read there, by key, one read per table for
// every key that waits (reconstruct.PGTail.CopyWants, PGCutValues,
// GiveCopyValues).
//
// Two places read, both PostgreSQL's own:
//
//   - a build, at the end of Fill, in the build's own goroutine: the tail
//     is not the keeper's yet, and the read is the large one (every key of
//     the window that waits), bounded by the build's deadline;
//   - an update of the tails (tailKeeper.pgFillFromCopy, called from
//     refreshLocked for a source that is a pgCopySource and for no other),
//     after the events were folded and before the views are written: what
//     the fold just left waiting, and what a build's catch-up did.
//
// What both hold to, each learned by the MySQL tails first:
//
//   - The copy is never read under the keeper's lock (#2494): the update
//     notes what waits under the lock, lets the lock go for the read, takes
//     it again and gives the values only to a tail that is still the one
//     noted, over the same cut. A tail pruned to a newer cut meanwhile gets
//     nothing: its keys are asked again, of the newer cut.
//   - The cut read is the one the tail lies over when the read starts
//     (tailEntry.table), never one remembered from when the tail was built:
//     after a prune a key's earlier change may be in the newer cut and
//     nowhere else.
//   - One read per table, never one per key, and no more text than the
//     tails' memory budget leaves (tailBudget.Bytes). With no room left the
//     read waits for a later update, as a tail over its budget stops and is
//     not dropped; only a table whose values do not fit the whole budget
//     with every other tail empty is refused. No row is written with part
//     of its values.
//   - A tail is written whole or not at all: while a key waits, the last
//     version stays (reconstruct.ErrPGTailCopyPending), with its own older
//     instant.
//   - An update waits for its reads, so they have a time of their own, all
//     tables together: pgCopyReadMax, or what the update's own deadline
//     leaves when that is less, so that the update still ends in time and
//     publishes the other tables. A read that did not get to run, or was
//     cut short, waits for the next update, and the tails that waited
//     longest are read first. Only a read that had the whole of
//     pgCopyReadMax to itself and did not end loses its tail.
//   - No panic leaves the keeper's lock in the wrong state or ends the
//     process: one in the read refuses the table.

// pgCopySource is a tailSource that can read rows of a table's published
// cut by key. Only pgTailSource is one; the keeper asks nothing of this
// kind of any other source.
type pgCopySource interface {
	// CopyValues is, for each of keys the cut of t holds a row of, the text
	// of cols in that row (nil: a NULL); a key it holds no row of is
	// absent. keyCols are the key's columns in the table's order. maxBytes
	// (0: no bound) is how much text it may read.
	CopyValues(ctx context.Context, t views.BaselineTable, keyCols, keys, cols []string, maxBytes int64) (map[string]map[string]*string, error)
}

// pgCopyTail is a tail that waits for values read from the copy:
// reconstruct.PGTail.
type pgCopyTail interface {
	CopyWants() (keys, cols []string)
	GiveCopyValues(keys, cols []string, rows map[string]map[string]*string) (int, string)
	// DeferCopy and CopyDeferred: the updates in a row that left the read
	// for later, and the room one of them found too small.
	DeferCopy(room int64) int
	CopyDeferred() (times int, room int64)
	PKColumns() []string
	Bytes() int64
}

var _ pgCopyTail = (*reconstruct.PGTail)(nil)

// pgCopyReadMax is how long the reads of ONE update may take, all tables
// together: an update of the tails waits for them, so past it every table's
// answers would be that much older at every update. Less when the update's
// own deadline is nearer: the reads end pgCopyReadMargin before it, so that
// what follows them in the update (a read of the index, the writing of the
// views) still has its time and the other tables are published; and with
// less than pgCopyReadMin to read in, none is started.
//
// A read that was not started, or was cut short by that time, waits for the
// next update, where the tails that waited longest go first. One read alone
// that had all of pgCopyReadMax and did not end is another matter: it would
// take that long of every update for good, so it ends its table's tail (the
// table reads as of its snapshot until the next one).
var (
	pgCopyReadMax    = 10 * time.Second
	pgCopyReadMargin = 500 * time.Millisecond
	pgCopyReadMin    = 200 * time.Millisecond
)

// pgCopyDeferredSaid is how often a read left for later is said at Info: the
// first time, and then once every this many updates in a row.
const pgCopyDeferredSaid = 300

// pgCopyReadSlow is how long a read may take before it is said at Info.
const pgCopyReadSlow = 5 * time.Second

// pgCopyRead is one read of a table's cut for the keys a tail waits for:
// everything the read needs, noted under the lock, so the tail itself is not
// touched while the lock is let go.
type pgCopyRead struct {
	key        string
	entry      *tailEntry
	tail       tailTable
	table      views.BaselineTable
	cut        string
	pk         []string
	keys, cols []string
	rows       map[string]map[string]*string
	err        error
	took       time.Duration
	// late: the read was still running when the time for the update's reads
	// was up.
	late bool
	// deferred and tried are the tail's CopyDeferred when the read was
	// noted; alone is the text the table could read with every other tail
	// empty (the budget less what its own tail holds), and room what this
	// read was allowed.
	deferred    int
	tried       int64
	alone, room int64
}

// pgFillFromCopy reads from the copy the large values the live tails wait
// for and gives them. Called with k.mu held, which it lets go while it
// reads and holds again when it returns.
func (k *tailKeeper) pgFillFromCopy(ctx context.Context, src pgCopySource) {
	var reads []*pgCopyRead
	for key, e := range k.tables {
		if e.refused != "" || e.frozen || e.tail == nil {
			continue
		}
		ct, ok := e.tail.(pgCopyTail)
		if !ok {
			continue
		}
		keys, cols := ct.CopyWants()
		if len(keys) == 0 {
			continue
		}
		r := &pgCopyRead{key: key, entry: e, tail: e.tail, table: e.table, cut: e.cut, pk: ct.PKColumns(), keys: keys, cols: cols}
		r.deferred, r.tried = ct.CopyDeferred()
		r.alone = k.budget.Bytes - ct.Bytes()
		reads = append(reads, r)
	}
	if len(reads) == 0 {
		return
	}
	// The tails that waited longest first, so that the order of their names
	// does not decide which one is left for later at every update.
	slices.SortFunc(reads, func(a, b *pgCopyRead) int {
		if a.deferred != b.deferred {
			return cmp.Compare(b.deferred, a.deferred)
		}
		return cmp.Compare(a.key, b.key)
	})
	// What the tails may still hold, all together: the values read count
	// against it as they come in. Frozen tails count: they are held.
	bounded := k.budget.Bytes > 0
	var left int64
	if bounded {
		left = k.budget.Bytes - k.heldBytesLocked("")
	}
	k.mu.Unlock()
	func() {
		defer k.mu.Lock()
		now := time.Now()
		until := now.Add(pgCopyReadMax)
		if end, ok := ctx.Deadline(); ok && end.Add(-pgCopyReadMargin).Before(until) {
			until = end.Add(-pgCopyReadMargin)
		}
		for _, r := range reads {
			began := time.Now()
			switch {
			case ctx.Err() != nil:
				r.err = ctx.Err()
			case until.Sub(began) < pgCopyReadMin:
				// Not started: the reads before it took the time, or the
				// update has too little left.
				r.err = errPGCopyReadLater
			case bounded && (left <= 0 || left <= r.tried):
				// No room now, or no more than a read of this tail already
				// found too small.
				r.err = errPGCopyNoRoom
			default:
				r.room = left
				rctx, cancel := context.WithDeadline(ctx, until)
				r.rows, r.err = pgCopyValues(rctx, src, r.table, r.pk, r.keys, r.cols, left)
				r.late = r.err != nil && rctx.Err() != nil && ctx.Err() == nil
				cancel()
			}
			r.took = time.Since(began)
			if bounded {
				left -= pgCopyTextBytes(r.rows)
			}
		}
	}()
	for _, r := range reads {
		e := k.tables[r.key]
		if e != r.entry || e.tail != r.tail || e.cut != r.cut || e.refused != "" {
			// Not the tail noted, or not over the cut that was read: what was
			// read is of another cut, and is given to no one.
			continue
		}
		name := e.table.Schema + "." + e.table.Table
		if r.err != nil {
			if why := k.pgCopyReadRefusal(ctx, r); why != "" {
				k.refuse(e, why)
				continue
			}
			var tried int64
			if errors.Is(r.err, pgdelta.ErrBaseValuesTooLarge) {
				tried = r.room
			}
			n := r.tail.(pgCopyTail).DeferCopy(tried)
			log := slog.Debug
			if n == 1 || n%pgCopyDeferredSaid == 0 {
				// Said where an operator sees it, and not at every update:
				// the table's answers are as old as its last version.
				log = slog.Info
			}
			log("Engine Routing: a table's large values were not read from its copy in this update; its last version stays until they are",
				"table", name, "keys", len(r.keys), "updates_in_a_row", n, "took", r.took, "reason", r.err)
			continue
		}
		given, why := r.tail.(pgCopyTail).GiveCopyValues(r.keys, r.cols, r.rows)
		if why != "" {
			k.refuse(e, why)
			continue
		}
		e.dirty = true
		log := slog.Debug
		if r.took >= pgCopyReadSlow {
			// Said where an operator sees it: every table's update waited.
			log = slog.Info
		}
		log("Engine Routing: a table's large values were read from its copy", "table", name, "keys", len(r.keys), "values", given, "took", r.took)
	}
}

// errPGCopyReadLater marks a read that was not started in this update for
// want of time, and errPGCopyNoRoom one not started for want of room in the
// tails' memory budget.
var (
	errPGCopyReadLater = errors.New("left for a later update: the reads before it took the time, or the update had too little left")
	errPGCopyNoRoom    = errors.New("left for a later update: the tails' memory budget has no room for it now (BINTRAIL_SQL_TAIL_MAX_BYTES)")
)

// pgCopyReadRefusal says why a read of the copy that failed ends the
// table's tail, "" when it is left for a later update: a read not started
// (no time, no room), one cut short before it had pgCopyReadMax to itself,
// one whose values did not fit the room left while other tails hold some of
// the budget, and one cut short by the update's own deadline or a closed
// keeper.
func (k *tailKeeper) pgCopyReadRefusal(ctx context.Context, r *pgCopyRead) string {
	switch {
	case errors.Is(r.err, errPGCopyReadLater), errors.Is(r.err, errPGCopyNoRoom):
		return ""
	case errors.Is(r.err, pgdelta.ErrBaseValuesTooLarge):
		if r.room < r.alone {
			// Other tails hold part of the budget: it may fit once they do
			// not.
			return ""
		}
		return fmt.Sprintf("the large values to read from its copy for %d key(s) do not fit the tails' memory budget, with no other table's changes held (%d MiB, BINTRAIL_SQL_TAIL_MAX_BYTES)",
			len(r.keys), k.budget.Bytes>>20)
	case r.late && r.took >= pgCopyReadMax:
		return fmt.Sprintf("reading the large values of %d key(s) from its copy did not end in the %s an update of the recent changes gives such reads",
			len(r.keys), pgCopyReadMax)
	case r.late:
		return ""
	case ctx.Err() == nil:
		return "its large values could not be read from its copy: " + r.err.Error()
	}
	return ""
}

// pgCopyValues is src.CopyValues with a panic turned into an error: the
// caller holds a lock it must take again, and the process is the one that
// answers statements.
func pgCopyValues(ctx context.Context, src pgCopySource, t views.BaselineTable, keyCols, keys, cols []string, maxBytes int64) (rows map[string]map[string]*string, err error) {
	defer func() {
		if p := recover(); p != nil {
			slog.Error("Engine Routing: reading a table's large values from its copy stopped on a panic", "table", t.Schema+"."+t.Table,
				"error", fmt.Sprint(p), "stack", string(debug.Stack()))
			rows, err = nil, fmt.Errorf("panic: %v", p)
		}
	}()
	return src.CopyValues(ctx, t, keyCols, keys, cols, maxBytes)
}

// pgCopyTextBytes is how much text rows hold.
func pgCopyTextBytes(rows map[string]map[string]*string) int64 {
	var n int64
	for _, row := range rows {
		for _, v := range row {
			if v != nil {
				n += int64(len(*v))
			}
		}
	}
	return n
}

// CopyValues reads the rows of keys from the cut of t: its file and the
// table delta beside it, the delta first (reconstruct.PGCutValues).
func (x *pgTailSource) CopyValues(ctx context.Context, t views.BaselineTable, keyCols, keys, cols []string, maxBytes int64) (map[string]map[string]*string, error) {
	return reconstruct.PGCutValues(ctx, t.Path, keyCols, keys, cols, pgCopyTuning(maxBytes), maxBytes)
}

// pgCopyTuning is the engine's budget for one read of a cut that may bring
// in maxBytes of text (0: no bound): two threads, and as much memory as the
// text it may read, no less than pgCopyEngineMin (what the engine needs to
// join the keys) and no more than the 4GB every read of this process had.
// The read runs in the process that holds the tails, so what it may use is
// what the tails' budget leaves, not a fixed 4GB beside it.
func pgCopyTuning(maxBytes int64) duckdbutil.Tuning {
	t := duckdbutil.DefaultTuning()
	if maxBytes <= 0 {
		return t
	}
	mib := min(max(maxBytes>>20, pgCopyEngineMin>>20), pgCopyEngineMax>>20)
	t.MemoryLimit = fmt.Sprintf("%dMiB", mib)
	return t
}

const (
	pgCopyEngineMin = 256 << 20
	pgCopyEngineMax = 4 << 30
)

// pgStarted is the tail Start built for a table's cut, kept until the Fill
// that follows it: Fill takes it when it begins, and when it has read the
// window reads from the copy what the window left waiting, in the build's
// goroutine. Kept no longer: a build that ends in Fill must not leave its
// tail held here.
type pgStarted struct {
	path string
	tail *reconstruct.PGTail
}

// noteStarted remembers the tail Start built for t.
func (x *pgTailSource) noteStarted(t views.BaselineTable, tail *reconstruct.PGTail) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.started == nil {
		x.started = map[string]pgStarted{}
	}
	x.started[tailKey(t.Schema, t.Table)] = pgStarted{path: t.Path, tail: tail}
}

// takeStarted is the tail Start built for t, forgotten here; nil when the
// last one started for the table was for another cut (a build no longer
// wanted, which the keeper never installs).
func (x *pgTailSource) takeStarted(t views.BaselineTable) *reconstruct.PGTail {
	x.mu.Lock()
	defer x.mu.Unlock()
	key := tailKey(t.Schema, t.Table)
	s, ok := x.started[key]
	if !ok || s.path != t.Path {
		return nil
	}
	delete(x.started, key)
	return s.tail
}

// fillStartedFromCopy reads from t's cut the large values tail waits for,
// once Fill has handed it its whole window; tail is the one Start built for
// t (takeStarted), nil for none. A refusal when the cut cannot give one or
// cannot be read; an error for a read cut short by the build's deadline,
// which is tried again with the build.
func (x *pgTailSource) fillStartedFromCopy(ctx context.Context, t views.BaselineTable, tail *reconstruct.PGTail) (string, error) {
	if tail == nil {
		return "", nil
	}
	keys, cols := tail.CopyWants()
	if len(keys) == 0 {
		return "", nil
	}
	name := t.Schema + "." + t.Table
	began := time.Now()
	// Checked against the snapshot's record here, where a whole read of the
	// file holds no other table back; an update reads the file as a
	// statement does.
	if err := reconstruct.PGCutCheck(t.Path); err != nil {
		return fmt.Sprintf("the copy of %s does not pass its snapshot's record: %v", name, err), nil
	}
	rows, err := pgCopyValues(ctx, x, t, tail.PKColumns(), keys, cols, x.copyBytes)
	switch {
	case errors.Is(err, pgdelta.ErrBaseValuesTooLarge):
		return fmt.Sprintf("the large values to read from the copy of %s for %d key(s) are more than the tails' memory budget (%d MiB, BINTRAIL_SQL_TAIL_MAX_BYTES)",
			name, len(keys), x.copyBytes>>20), nil
	case err != nil && ctx.Err() != nil:
		return "", fmt.Errorf("read the large values of %s from its copy: %w", name, err)
	case err != nil:
		return fmt.Sprintf("the large values of %s could not be read from its copy: %v", name, err), nil
	}
	given, why := tail.GiveCopyValues(keys, cols, rows)
	if why != "" {
		return why, nil
	}
	slog.Debug("Engine Routing: a table's large values were read from its copy while its tail was built", "table", name,
		"keys", len(keys), "values", given, "took", time.Since(began))
	return "", nil
}

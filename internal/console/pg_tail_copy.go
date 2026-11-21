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
//     tails' memory budget leaves (tailBudget.Bytes). Past it the table is
//     refused, as a tail over its budget in a build is; no row is written
//     with part of its values.
//   - A tail is written whole or not at all: while a key waits, the last
//     version stays (reconstruct.ErrPGTailCopyPending), with its own older
//     instant.
//   - An update waits for its reads, so they have a time of their own
//     (pgCopyReadMax), all tables together: the table whose read is running
//     when it is up loses its tail, by name, and the others go on.
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
	PKColumns() []string
}

var _ pgCopyTail = (*reconstruct.PGTail)(nil)

// pgCopyReadMax is how long the reads of ONE update may take, all tables
// together: an update of the tails waits for them, so past it every table's
// answers would be that much older at every update. The read that is running
// when the time is up ends its table's tail (the table reads as of its
// snapshot until the next one, and the others go on); reads not yet started
// wait for the next update. A read cut short by the update's own deadline or
// a closed keeper is tried again.
var pgCopyReadMax = 10 * time.Second

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
		reads = append(reads, &pgCopyRead{key: key, entry: e, tail: e.tail, table: e.table, cut: e.cut, pk: ct.PKColumns(), keys: keys, cols: cols})
	}
	if len(reads) == 0 {
		return
	}
	slices.SortFunc(reads, func(a, b *pgCopyRead) int { return cmp.Compare(a.key, b.key) })
	// What the tails may still hold, all together: the values read count
	// against it as they come in.
	bounded := k.budget.Bytes > 0
	var left int64
	if bounded {
		left = k.budget.Bytes - k.heldBytesLocked("")
	}
	k.mu.Unlock()
	func() {
		defer k.mu.Lock()
		until := time.Now().Add(pgCopyReadMax)
		for _, r := range reads {
			began := time.Now()
			switch {
			case ctx.Err() != nil:
				r.err = ctx.Err()
			case !began.Before(until):
				// Not started: another table's read took the time.
				r.err = errPGCopyReadLater
			case bounded && left <= 0:
				r.err = pgdelta.ErrBaseValuesTooLarge
			default:
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
			} else {
				slog.Debug("Engine Routing: a table's large values could not be read from its copy in time; its last version stays",
					"table", name, "keys", len(r.keys), "took", r.took, "error", r.err)
			}
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

// errPGCopyReadLater marks a read that was not started in this update.
var errPGCopyReadLater = errors.New("left for the next update: the reads before it took the time")

// pgCopyReadRefusal says why a read of the copy that failed ends the
// table's tail, "" when it is to be tried again at the next update: a read
// not started, and one cut short by the update's own deadline or a closed
// keeper.
func (k *tailKeeper) pgCopyReadRefusal(ctx context.Context, r *pgCopyRead) string {
	switch {
	case errors.Is(r.err, errPGCopyReadLater):
		return ""
	case errors.Is(r.err, pgdelta.ErrBaseValuesTooLarge):
		return fmt.Sprintf("the large values to read from its copy for %d key(s) would take the tails past their memory budget (%d MiB, BINTRAIL_SQL_TAIL_MAX_BYTES)",
			len(r.keys), k.budget.Bytes>>20)
	case r.late:
		return fmt.Sprintf("reading the large values of %d key(s) from its copy did not end in the %s an update of the recent changes gives such reads",
			len(r.keys), pgCopyReadMax)
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
	return reconstruct.PGCutValues(ctx, t.Path, keyCols, keys, cols, duckdbutil.DefaultTuning(), maxBytes)
}

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

package console

import (
	"errors"
	"log/slog"
	"slices"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

// A PostgreSQL tail keeps its files across a prune (#3081).
//
// The keeper's general rule starts a new chain after a prune, because an
// older file may hold a dropped key's older row. That costs a write of the
// whole tail per table and snapshot update, on the clock of the first
// statement that reads it. Here the pending changes are written FIRST: then
// the newest row the chain holds of any key is that key's last change through
// the cursor, and for a key the prune drops that is the row the newer cut
// holds. So the chain is right over the newer cut, and only its leading files
// wholly below the cut are retired.
//
// Declared by the PostgreSQL source and tail alone (pgChainSource,
// pgChainTail): for a MySQL source every function here returns at its first
// line and the keeper does what it always did.

// pgChainSource is a tailSource whose tails may keep their files across a
// prune to a cut it accepts.
type pgChainSource interface {
	tailSource
	KeepsChainOver(old, t views.BaselineTable) bool
}

// pgChainTail is a tail that can be pruned with its files kept.
type pgChainTail interface {
	tailTable
	PruneKeepingChain(anchor query.BinlogPos, afterID uint64) (int, error)
	WrittenHigh() uint64
	WriteSince(path string, afterID uint64) (int64, error)
	LastEventID() uint64
}

// pgSegment is what the keeper remembers of one file of a PostgreSQL chain.
type pgSegment struct {
	high uint64
	rows int64
	// lastID is the tail's last event id when the file was written.
	lastID uint64
}

// pgNoteSegment records the file just written at the end of e's chain. No
// step for a tail that is not PostgreSQL's.
func (k *tailKeeper) pgNoteSegment(e *tailEntry, path string, rows int64) {
	tail, ok := e.tail.(pgChainTail)
	if !ok {
		return
	}
	kept := make(map[string]pgSegment, len(e.segs))
	for _, p := range e.segs {
		if s, ok := e.segHigh[p]; ok {
			kept[p] = s
		}
	}
	high := tail.WrittenHigh()
	if rows == 0 {
		// A file with no rows holds nothing a cut could need: the lowest known
		// position, so it does not stop the retiring of the files before it
		// (0 reads as "not known").
		high = 1
	}
	kept[path] = pgSegment{high: high, rows: rows, lastID: tail.LastEventID()}
	e.segHigh = kept
	if len(e.segs) == 1 {
		// A whole file: nothing is left from before a prune.
		e.pgPruned = 0
	}
}

// pgPruneKeepingChain moves e to t's newer cut at to with its files kept, and
// says whether it did. When it did not, e is over its old cut with the same
// chain, or with one more file holding what was pending, and the caller takes
// the general path, which starts a new chain.
func (k *tailKeeper) pgPruneKeepingChain(e *tailEntry, t views.BaselineTable, cut string, to tailFloor) bool {
	src, ok := k.src.(pgChainSource)
	if !ok {
		return false
	}
	tail, ok := e.tail.(pgChainTail)
	if !ok || to.lastEventID != 0 || len(e.segs) == 0 || e.fileCut != e.cut || !src.KeepsChainOver(e.table, t) {
		return false
	}
	if e.dirty {
		if err := k.write(e.table, e); err != nil {
			// An open transaction or a value still to be read from the copy
			// is a wait, and the general path writes the tail when it can.
			// Anything else (a full disk, a file that cannot be written) an
			// operator must be able to see.
			// A refusal from the writer is not a failure to write: the general
			// path meets it again at the next view, which ends the tail
			// (k.refuse) and says so there.
			var r *reconstruct.TailRefusal
			if errors.As(err, &r) {
				slog.Debug("Engine Routing: a table's tail was refused by its writer before its prune", "table", t.Schema+"."+t.Table, "reason", r.Reason)
				return false
			}
			log := slog.Warn
			if errors.Is(err, reconstruct.ErrPGTailTxnOpen) || errors.Is(err, reconstruct.ErrPGTailCopyPending) {
				log = slog.Debug
			}
			log("Engine Routing: a table's tail could not be written before its prune; its chain starts again", "table", t.Schema+"."+t.Table, "error", err)
			return false
		}
		if len(e.segs) == 0 {
			return false
		}
	}
	n, err := tail.PruneKeepingChain(to.anchor, to.lastEventID)
	if err != nil {
		slog.Warn("Engine Routing: a table's tail could not keep its chain across a prune; it starts again", "table", t.Schema+"."+t.Table, "error", err)
		return false
	}
	highs := make([]uint64, len(e.segs))
	for i, p := range e.segs {
		highs[i] = e.segHigh[p].high
	}
	gone := reconstruct.PGChainFilesToRetire(highs, to.anchor.Pos)
	for _, p := range e.segs[:gone] {
		e.old = append(e.old, tailOldFile{p, k.now()})
		e.segRows -= e.segHigh[p].rows
		delete(e.segHigh, p)
	}
	// A new slice: the version a statement was handed keeps its own.
	e.segs = append([]string(nil), e.segs[gone:]...)
	e.pgPruned = len(e.segs)
	e.table, e.cut, e.from, e.fileCut, e.dirty = t, cut, &to, cut, false
	slog.Debug("Engine Routing: a table's tail was pruned to its newer snapshot with its files kept", "table", t.Schema+"."+t.Table,
		"dropped", n, "keys", e.tail.Keys(), "files", len(e.segs), "files_retired", gone)
	return true
}

// pgMergeNewest is the write of a chain at tailMaxSegments files: instead of
// the whole tail, one file replaces the newest files that are cheapest to
// merge (pgCheapestMerge), so a key is not written again every eight writes. It
// says whether it took the write (with the write's error, nothing changed on
// one). Not when the chain is not this cut's, when its files are mostly
// overridden rows (the whole write is what shrinks them), or when fewer than
// two files were written since the last prune: a merge never takes in a file
// from before a prune, whose dropped keys no later write holds.
func (k *tailKeeper) pgMergeNewest(e *tailEntry, path string) (bool, error) {
	if _, ok := k.src.(pgChainSource); !ok {
		return false, nil
	}
	tail, ok := e.tail.(pgChainTail)
	if !ok || e.fileCut != e.cut || len(e.segs) < tailMaxSegments ||
		e.segRows > int64(tailMergeRatio)*int64(e.tail.Keys()) || len(e.segs)-e.pgPruned < 2 {
		return false, nil
	}
	for _, p := range e.segs[e.pgPruned:] {
		if _, known := e.segHigh[p]; !known {
			return false, nil
		}
	}
	sizes := make([]int64, len(e.segs)-e.pgPruned)
	for i, p := range e.segs[e.pgPruned:] {
		sizes[i] = e.segHigh[p].rows
	}
	first, rows := pgCheapestMerge(sizes)
	from := e.pgPruned + first
	var after uint64
	if from > 0 {
		s, known := e.segHigh[e.segs[from-1]]
		if !known {
			return false, nil
		}
		after = s.lastID
	}
	n, err := tail.WriteSince(path, after)
	if err != nil {
		return true, err
	}
	k.rowsWritten += n
	for _, p := range e.segs[from:] {
		e.old = append(e.old, tailOldFile{p, k.now()})
		delete(e.segHigh, p)
	}
	// A new slice: the version a statement was handed keeps its own.
	e.segs = append(slices.Clone(e.segs[:from]), path)
	e.segRows += n - rows
	e.dirty = false
	k.pgNoteSegment(e, path, n)
	return true, nil
}

// pgCheapestMerge picks which newest files to merge, given the rows of each,
// oldest first (at least two): the run ending at the newest file that costs
// the fewest rows per file it takes off the chain, and those rows. A big old
// file is then rewritten only once the small ones after it have grown to its
// size, which is what makes a key's writes grow with the logarithm of the
// files and not with their number. On a tie, the shorter run.
func pgCheapestMerge(sizes []int64) (from int, rows int64) {
	n := len(sizes)
	from, rows = n-2, sizes[n-2]+sizes[n-1]
	sum := rows
	for i := n - 3; i >= 0; i-- {
		sum += sizes[i]
		// sum/(n-i-1) < rows/(n-from-1), without dividing.
		if sum*int64(n-from-1) < rows*int64(n-i-1) {
			from, rows = i, sum
		}
	}
	return from, rows
}

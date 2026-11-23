package console

import (
	"errors"
	"log/slog"

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
}

// pgSegment is what the keeper remembers of one file of a PostgreSQL chain.
type pgSegment struct {
	high uint64
	rows int64
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
	kept[path] = pgSegment{high: high, rows: rows}
	e.segHigh = kept
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
	e.table, e.cut, e.from, e.fileCut, e.dirty = t, cut, &to, cut, false
	slog.Debug("Engine Routing: a table's tail was pruned to its newer snapshot with its files kept", "table", t.Schema+"."+t.Table,
		"dropped", n, "keys", e.tail.Keys(), "files", len(e.segs), "files_retired", gone)
	return true
}

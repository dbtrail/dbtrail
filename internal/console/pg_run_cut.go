package console

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #3084, the PostgreSQL counterpart of copy_run_cut.go (#2592).
//
// A snapshot update carries a table with no change in its window forward with
// its previous file, footer and all, so the file's own floor stays where an
// earlier update left it, and for a table nobody writes that is its last full
// read. Read from the footer alone, the new snapshot's cut is the old one, the
// keeper does not take a tail read from the old cut as standing for it
// (standsFor needs a later cut), and the tail is built again from nothing at
// every update while statements read the snapshot alone. And a cut a
// PostgreSQL footer records carries no event id, so even a file the update
// wrote is seen reached only once a later change is captured: on a quiet
// source, never.
//
// The update records both in the folder it publishes
// (reconstruct.PGRunCutFile): where it ended, the newest event id it held
// every change through, and the tables it brought to its end. Cut reads it
// for the table: a table the update carried starts at the update's end, and
// a table it brought there carries that id.
//
// Only Cut moves. cut (and with it Fill, Broken and its memo) keeps the
// table's own floor and bound: the read of the window starts where it always
// did, holding nothing more, since the update found nothing in between, and
// the guards that ask what happened since the rows were last read from the
// source (a loss of capture, a TRUNCATE, a changed shape) ask over the same
// span as before.

// pgRunCutMemo remembers each folder's run cut. A published folder is never
// written again, so a found cut stands, and so does "none" unless the folder
// is still marked incomplete; the memo starts over past copyCutMemoMax
// folders. A record that cannot be read is not remembered.
type pgRunCutMemo struct {
	mu   sync.Mutex
	dirs map[string]pgRunCutEntry
}

type pgRunCutEntry struct {
	cut reconstruct.PGRunCut
	ok  bool
}

// of is dir's run cut. warn hears why a record could not be read.
func (m *pgRunCutMemo) of(dir string, warn func(err error)) (reconstruct.PGRunCut, bool) {
	m.mu.Lock()
	e, ok := m.dirs[dir]
	m.mu.Unlock()
	if ok {
		return e.cut, e.ok
	}
	// A folder a run failed or is still writing says so: the record is
	// written before _SUCCESS, and a run that fails after it (its manifest,
	// say) leaves the folder incomplete with the record in it. Looked at
	// before the record, and never kept: a run still writing may finish
	// between the two, and "no record" kept for it would last.
	if _, err := os.Stat(filepath.Join(dir, baseline.IncompleteMarker)); err == nil {
		return reconstruct.PGRunCut{}, false
	}
	cut, found, err := reconstruct.ReadPGRunCut(dir)
	if err != nil {
		warn(err)
		return reconstruct.PGRunCut{}, false
	}
	m.mu.Lock()
	if m.dirs == nil || len(m.dirs) >= copyCutMemoMax {
		m.dirs = map[string]pgRunCutEntry{}
	}
	m.dirs[dir] = pgRunCutEntry{cut: cut, ok: found}
	m.mu.Unlock()
	return cut, found
}

// runCutFloor is the floor Cut hands the keeper for t, whose own cut is c:
// the table's own, moved to the run cut of the folder its file is in when the
// update that published the folder brought the table to its end.
//
//   - Own floor before the run's end: the table was carried, and starts at
//     the run's end, with the run's id.
//   - Own floor at the run's end: the update wrote the table there; the
//     floor stays, and takes the run's id.
//   - Own floor after the run's end (a newer full read the run held): as it
//     is.
//
// Never for a file a full read wrote whose rows are past its floor
// (PGAnchorLSN: anchorReached decides when a tail may go over it), never
// through the pointer to the newest snapshot (it moves) or a link, and never
// from a record written for another folder than the one it is in.
func (x *pgTailSource) runCutFloor(t views.BaselineTable, c pgCut) tailFloor {
	f := tailFloor{anchor: reconstruct.PGTailPos(c.floor)}
	if c.md.PGAnchorLSN != 0 || c.floor == 0 {
		return f
	}
	dir, ok := copySnapshotDir(t)
	if !ok {
		return f
	}
	clean := filepath.Clean(dir)
	if fi, err := os.Lstat(clean); err != nil || fi.Mode()&os.ModeSymlink != 0 {
		return f
	}
	// The pointer to the newest snapshot is refused here too: its name is
	// not a time.
	dirAt, ok := snapshotdir.ParseTime(filepath.Base(clean))
	if !ok {
		return f
	}
	run, ok := x.runCuts.of(dir, func(err error) {
		x.s.copyFaults.warn(dir, "Engine Routing: a snapshot folder's record of where its update ended could not be read; its PostgreSQL tables' tails are built again at each update until a newer update publishes a readable one",
			"dir", dir, "error", err)
	})
	if !ok || !run.At.Equal(dirAt) || !run.Holds(t.Schema, t.Table) {
		return f
	}
	switch {
	case c.floor < run.LSN:
		f.anchor, f.reachID = reconstruct.PGTailPos(run.LSN), run.ReachID
	case c.floor == run.LSN:
		f.reachID = run.ReachID
	}
	return f
}

// pgSameFloorStands says whether e's tail, read from the same floor as t's
// newer cut, may stand for it: only for a PostgreSQL source, a cut an update
// brought to its end with every change through a known event
// (tailFloor.reachID, from runCutFloor), and two files that descend from the
// same read of the source (KeepsChainOver). Both are then that read with the
// same recorded changes over it, through the same point: the same rows. A
// source with no change between two updates ends both at the same point, and
// without this the tail was built again at the second.
func (k *tailKeeper) pgSameFloorStands(e *tailEntry, t views.BaselineTable, to tailFloor) bool {
	src, ok := k.src.(pgChainSource)
	if !ok || to.reachID == 0 {
		return false
	}
	return src.KeepsChainOver(e.table, t)
}

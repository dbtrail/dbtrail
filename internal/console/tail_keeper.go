package console

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

// The tails of one server's tables (#2369): what keeps each table's tail
// (reconstruct.Tail) current with the index, so a statement on the copy reads
// what capture has indexed, not only what the last refresh published.
//
// One cursor for the whole server: every refresh first reads the newest event
// the index holds, then folds every event up to it and none past it. That
// order is what makes the instant reported for a tail ("through") the time of
// an event every tail has folded, so the number can only err old. And the
// cursor is an event id, never a time: a transaction that commits late
// carries an early time and a late id, and a time floor that moves forward
// would skip it.
//
// A tail only advances. When a refresh publishes a newer cut of a table, its
// tail is pruned to that cut, to shrink it (#2483): an advancing tail holds
// every change after its older cut up to the cursor, so dropping the keys
// whose last change the newer cut already holds leaves exactly what a rebuild
// from the newer cut would read, with no read of the index and no wait. That
// holds only while the cursor stands at or past the newer cut, which is not a
// given: a dump's cut is where the source stood when it was read, and capture
// may still be behind it; a refresh's cut ends after its last event, which is
// past where that event starts (its last event id says it then). So pruneTo
// checks it (reached), with every other condition the argument needs, and the
// tail is rebuilt from the cut when one fails; a tail built over a cut the
// cursor has not reached folds, as it advances, only the changes after that
// cut (tailFloor.admits), as its first read did. A rebuild that fails keeps
// the old tail while it is still advancing and the cursor has reached the
// newer cut: it then holds every change after an older cut up to the cursor,
// and a change it folds after that is newer than what the cut holds. A tail
// that grows past its
// budget stops advancing (frozen) and goes on serving what it holds, with its
// own older instant, until the next published cut rebuilds it; a frozen tail
// is never laid over a newer cut, whose rows may be newer than its own.
//
// Events are read and folded a page at a time, so the memory a refresh takes
// is a page and the tails, never the backlog since the last statement.

// tailMark is the newest event of an index: its id, where it sits in the
// source's binary log, and when the source ran it.
type tailMark struct {
	ID  uint64
	Pos query.BinlogPos
	At  time.Time
}

// tailSource is what the tails of one server read: its index, and the copy's
// files.
type tailSource interface {
	// Newest is the newest event the index holds; ID 0 when it holds none.
	Newest(ctx context.Context) (tailMark, error)
	// Holds says whether the index still holds m's event where m says: an
	// index rebuilt since gives its ids to other events.
	Holds(ctx context.Context, m tailMark) (bool, error)
	// Start builds t's empty tail, or says why t has none.
	Start(ctx context.Context, t views.BaselineTable) (tailTable, string, error)
	// Fill hands page t's events after its published cut with an id up to
	// upTo, a page at a time, or says why they cannot be read completely. An
	// error from page stops it and is returned.
	Fill(ctx context.Context, t views.BaselineTable, upTo uint64, page func([]query.ResultRow) error) (string, error)
	// Since hands page the events of tables with from < id <= upTo, a page
	// at a time, in id order.
	Since(ctx context.Context, tables []views.BaselineTable, from, upTo uint64, page func([]query.ResultRow) error) error
	// Cut is where the read of t's window starts, as Fill reads it: the
	// position of its published cut and the last event id it records (0:
	// none), or why that cannot be read.
	Cut(t views.BaselineTable) (tailFloor, string)
	// Broken says, by tailKey, why a table's tail cannot go on, asked at
	// every refresh (its definition changed after its cut, capture lost
	// events since). Tables it leaves out can.
	Broken(ctx context.Context, tables []views.BaselineTable) (map[string]string, error)
}

// tailFloor is where the read of a table's changes after its published cut
// starts: events whose start position is at or after anchor and, when
// lastEventID is not 0, whose id is above it (query.Options.SincePos and
// SinceEventID).
type tailFloor struct {
	anchor      query.BinlogPos
	lastEventID uint64
}

// tailTable is a reconstruct.Tail as the keeper drives it.
type tailTable interface {
	Apply(page []query.ResultRow) error
	PruneThrough(anchor query.BinlogPos, afterID uint64) (int, error)
	Keys() int
	WriteFile(path string) (int64, error)
	PKColumns() []string
}

var _ tailTable = (*reconstruct.Tail)(nil)

// tailView is what a statement reads of one table's tail: the file to lay
// over its published state ("" when nothing changed since its cut), its key,
// and the instant both together are indexed through.
type tailView struct {
	Path    string
	PK      []string
	Through time.Time
}

// tailKey names a table among a server's tails. Exact: on a source with
// lower_case_table_names=0, Orders and orders are two tables, and the index
// and the copy both keep a name as the source stores it.
func tailKey(schema, table string) string {
	return schema + "\x00" + table
}

// tailCutOf identifies the published files a table's view reads: a newer cut
// is a new file at the end of them.
func tailCutOf(t views.BaselineTable) string {
	cut := t.Path
	if n := len(t.DeltaFiles); n > 0 {
		cut += "\x00" + t.DeltaFiles[n-1].Upserts
	}
	return cut
}

// tailEntry is one table's tail and what the keeper knows of it.
type tailEntry struct {
	table views.BaselineTable
	cut   string
	// from is where the read the tail holds started (nil: not on record, and
	// the tail is never pruned). Set when the tail is built or pruned, never
	// when it is only kept standing for a newer cut: it still holds what it
	// read from its older one.
	from    *tailFloor
	tail    tailTable
	refused string
	frozen  bool
	dirty   bool
	used    time.Time
	// file is the newest version written ("" before the first, or while the
	// tail holds nothing), through the instant it is indexed through.
	file    string
	through time.Time
	// fileCut is the published cut file was written over: a version is
	// laid only over the copy it was written for.
	fileCut string
	// old are the earlier versions, kept while a statement may still read
	// them.
	old []tailOldFile
}

type tailOldFile struct {
	path string
	at   time.Time
}

// errTailBudget stops a Fill whose tail went over its budget.
var errTailBudget = errors.New("over the tail budget")

// errTailRenumbered stops a read that met an event before the one read just
// before it in the binary log.
var errTailRenumbered = errors.New("the source's binary log started again from another numbering")

// tailKeeper keeps one server's tails.
//
// One lock over a whole refresh: the tails are one cursor's, and a refresh is
// bounded by the caller's deadline, the time a statement is allowed to wait
// for its tails.
type tailKeeper struct {
	mu  sync.Mutex
	src tailSource
	// minBuild is the least time a table's first read is started with: one
	// left less by the others of its statement waits for the next, rather
	// than be refused as too slow on a remainder.
	minBuild time.Duration
	// noteMu guards failing, the reason no statement reads tails now.
	noteMu  sync.Mutex
	failing string
	// parent is where this keeper makes its own directory (which must
	// exist), at its first write: never one an earlier process wrote, so no
	// name repeats.
	parent string
	dir    string
	// maxKeys is the budget of one tail; keepOld how long a replaced
	// version stays for a statement that may still read it; idle how long
	// a tail no statement asked for is kept.
	maxKeys int
	keepOld time.Duration
	idle    time.Duration
	now     func() time.Time

	mark tailMark
	// through is the latest time of a mark folded, not the newest mark's
	// own: a transaction that commits late is the newest event with an
	// early time, and everything folded before it is still folded.
	through time.Time
	tables  map[string]*tailEntry
	written int
	// orphans are the files of tails replaced or forgotten, kept like old
	// versions.
	orphans []tailOldFile
}

func newTailKeeper(src tailSource, parent string, maxKeys int, keepOld, idle time.Duration) *tailKeeper {
	return &tailKeeper{src: src, parent: parent, maxKeys: maxKeys, keepOld: keepOld, idle: idle, now: time.Now, minBuild: tailMinBuild, tables: map[string]*tailEntry{}}
}

// refresh brings the tails of tables up to what the index holds now and
// returns, by tailKey, what a statement over them reads. A table it leaves
// out has no tail: the statement reads its published copy, as before tails.
// An error is the index's: no table has a tail for this statement.
func (k *tailKeeper) refresh(ctx context.Context, tables []views.BaselineTable) (map[string]tailView, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.refreshLocked(ctx, tables)
}

// refreshWithin is refresh with d to do it in, counted from when the keeper
// is this statement's: not spent waiting on another statement's refresh.
func (k *tailKeeper) refreshWithin(ctx context.Context, d time.Duration, tables []views.BaselineTable) (map[string]tailView, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	return k.refreshLocked(ctx, tables)
}

func (k *tailKeeper) refreshLocked(ctx context.Context, tables []views.BaselineTable) (map[string]tailView, error) {
	// The newest event FIRST: every read below is bounded by it, so the
	// instant reported is of an event every tail has folded.
	newest, err := k.src.Newest(ctx)
	if err != nil {
		return nil, err
	}
	if newest.ID == 0 {
		return nil, nil
	}
	why := tailStartOver(k.mark, newest)
	if why == "" && k.mark.ID != 0 {
		// Ids grow on a rebuilt index too, past the old mark in time: the
		// mark's own event says whether this is the index it was read from.
		held, err := k.src.Holds(ctx, k.mark)
		if err != nil {
			return nil, err
		}
		if !held {
			why = "the index no longer holds the last event read (it was rebuilt)"
		}
	}
	if why == "" && newest.ID > k.mark.ID && k.mark.ID != 0 {
		err := k.advance(ctx, newest.ID)
		switch {
		case errors.Is(err, errTailRenumbered):
			why = err.Error()
		case err != nil:
			return nil, err
		}
	}
	if why != "" {
		slog.Info("Engine Routing: the copy's tails are rebuilt", "reason", why)
		k.dropAll()
	}
	k.mark = newest
	if newest.At.After(k.through) {
		k.through = newest.At
	}
	changed, err := k.src.Broken(ctx, tables)
	if err != nil {
		return nil, err
	}
	out := map[string]tailView{}
	for _, t := range tables {
		key := tailKey(t.Schema, t.Table)
		e := k.entryFor(ctx, key, t, changed[key])
		if e == nil || e.refused != "" {
			continue
		}
		e.used = k.now()
		if v, ok := k.view(t, e); ok {
			out[key] = v
		}
	}
	k.forgetIdle()
	k.collect()
	return out, nil
}

// tailStartOver says why the tails folded so far cannot go on from mark to
// newest, or "" (an index rebuilt since is the mark's own check, Holds). A
// tail keeps a key's last change
// in the binary log by position, which only orders changes within one
// numbering of it: a source whose binary log started again (RESET MASTER, a
// failover, another log_bin base name) shows a newest event before the last
// one, or under another base name (an event with none included).
func tailStartOver(mark, newest tailMark) string {
	switch {
	case mark.ID == 0:
		return ""
	case reconstruct.BinlogBaseName(newest.Pos.File) != reconstruct.BinlogBaseName(mark.Pos.File) || !mark.Pos.AtOrBefore(newest.Pos):
		return "the source's binary log started again from another numbering"
	}
	return ""
}

// advance folds the events after the cursor up to upTo into the tails that
// are advancing. A table with no tail yet has its events read by its first
// Fill. The events must go on in the binary log from the cursor's: one
// before the last stops the read (errTailRenumbered), since a numbering that
// started again inside the window would rank its changes below older ones.
// Several rows of one statement share a position, so "before" is strict.
func (k *tailKeeper) advance(ctx context.Context, upTo uint64) error {
	var live []views.BaselineTable
	for _, e := range k.tables {
		if e.refused == "" && !e.frozen {
			live = append(live, e.table)
		}
	}
	if len(live) == 0 {
		return nil
	}
	last := k.mark.Pos
	return k.src.Since(ctx, live, k.mark.ID, upTo, func(page []query.ResultRow) error {
		byTable := map[string][]query.ResultRow{}
		folded := map[string]string{}
		for key, e := range k.tables {
			folded[strings.ToLower(e.table.Schema)+"\x00"+strings.ToLower(e.table.Table)] = key
		}
		for _, ev := range page {
			// The index compares names without case and the tails do not: an
			// event under another case of a tail's name is a table the copy
			// and the index spell apart, and that tail cannot tell it is
			// missing its changes.
			if key, ok := folded[strings.ToLower(ev.SchemaName)+"\x00"+strings.ToLower(ev.TableName)]; ok && key != tailKey(ev.SchemaName, ev.TableName) {
				if e := k.tables[key]; e.refused == "" {
					k.refuse(e, "the index names it in another case than its copy")
				}
				continue
			}
			at := query.BinlogPos{File: ev.BinlogFile, Pos: ev.StartPos}
			if at.File == "" || reconstruct.BinlogBaseName(at.File) != reconstruct.BinlogBaseName(last.File) || !last.AtOrBefore(at) {
				return errTailRenumbered
			}
			last = at
			key := tailKey(ev.SchemaName, ev.TableName)
			if e := k.tables[key]; e != nil && e.refused == "" && !e.frozen && e.from.admits(at, ev.EventID) {
				byTable[key] = append(byTable[key], ev)
			}
		}
		for key, evs := range byTable {
			e := k.tables[key]
			if err := e.tail.Apply(evs); err != nil {
				k.refuse(e, "its changes could not be folded: "+err.Error())
				continue
			}
			e.dirty = true
			if e.tail.Keys() > k.maxKeys {
				// Kept as it is, not dropped: dropping it would send the
				// next statement back to an older answer.
				e.frozen = true
				slog.Info("Engine Routing: a table's tail is over its budget; it stops advancing until the next snapshot update",
					"table", e.table.Schema+"."+e.table.Table, "keys", e.tail.Keys(), "budget", k.maxKeys)
			}
		}
		// The cursor moves with every page folded: a read that runs out of
		// time keeps what it did, and the next statement goes on from there
		// instead of reading the same backlog again.
		if n := len(page); n > 0 && page[n-1].EventID > k.mark.ID {
			ev := page[n-1]
			k.mark = tailMark{ID: ev.EventID, Pos: query.BinlogPos{File: ev.BinlogFile, Pos: ev.StartPos}, At: ev.EventTimestamp}
			if k.mark.At.After(k.through) {
				k.through = k.mark.At
			}
		}
		return nil
	})
}

// entryFor is key's tail over t's published cut, built when there is none
// or the cut moved; a refused one records why the table has none.
func (k *tailKeeper) entryFor(ctx context.Context, key string, t views.BaselineTable, changed string) *tailEntry {
	cut := tailCutOf(t)
	e := k.tables[key]
	if changed != "" {
		if e == nil || e.refused == "" {
			k.drop(e)
			e = &tailEntry{table: t, cut: cut, used: k.now()}
			k.tables[key] = e
			k.refuse(e, changed)
		}
		return nil
	}
	if e != nil && e.cut == cut {
		return e
	}
	// Before the statement's clock is looked at: a prune reads nothing.
	if e != nil && k.pruneTo(e, t, cut) {
		return e
	}
	if dl, ok := ctx.Deadline(); ctx.Err() != nil || ok && time.Until(dl) < k.minBuild {
		// The statement's time went on another table: this one is not
		// judged on what is left of it, and is built at the next statement.
		if e != nil && e.refused == "" && !e.frozen && k.reached(t) {
			return e
		}
		return nil
	}
	fresh, err := k.build(ctx, t, cut)
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		// Out of time: trying again at every statement would make each pay
		// the whole wait. A tail it already had stays, now standing for the
		// newer cut too (it is correct over it, see the top of this file);
		// else the table has none until its cut moves again.
		if e != nil && e.refused == "" && !e.frozen && k.reached(t) {
			e.cut = cut
			return e
		}
		fresh, err = &tailEntry{table: t, cut: cut, refused: "its changes since the snapshot could not be read in time"}, nil
	}
	if err != nil {
		// The index could not be read. A tail it already had stays while it
		// is advancing: it is correct over the newer copy too (see the top of
		// this file). Not a frozen one: it stopped before the cursor, maybe
		// before the newer cut, and laid over it would put older values over
		// newer ones.
		slog.Debug("Engine Routing: a table's tail could not be rebuilt over its newer snapshot", "table", t.Schema+"."+t.Table, "error", err)
		if e != nil && e.refused == "" && !e.frozen && k.reached(t) {
			return e
		}
		return nil
	}
	k.drop(e)
	fresh.used = k.now()
	k.tables[key] = fresh
	if why := fresh.refused; why != "" {
		fresh.refused = ""
		k.refuse(fresh, why)
	}
	return fresh
}

// pruneTo moves e's tail to t's newer cut by dropping what that cut holds,
// and says whether it did; when it did not, nothing changed and the tail is
// rebuilt from the cut. A tail is the newer cut's when, besides being live
// (not refused, not frozen: it holds every change after its own cut through
// the cursor):
//   - t reads the same base file: the tail's columns, key and the checks of
//     NewTail were made against it, and a copy written again (a new snapshot)
//     may differ;
//   - both cuts and the cursor are positions in one numbering of the binary
//     log, the only order the comparisons below mean anything in;
//   - the newer cut is after the older one: one that moved back or did not
//     move is another copy, not a later point of the same one;
//   - the newer cut's last event id is not below the older's: the tail did
//     not read the ids the newer window would;
//   - the cursor has folded everything the newer cut holds (reached): an
//     event between the cursor and the cut is in the cut and would be folded
//     later as a change after it.
//
// The event exactly at the newer cut's position is kept, as Fill keeps it:
// the position is where the next write begins.
func (k *tailKeeper) pruneTo(e *tailEntry, t views.BaselineTable, cut string) bool {
	if e.refused != "" || e.frozen || e.from == nil || e.tail == nil || t.Path != e.table.Path {
		return false
	}
	to, why := k.src.Cut(t)
	if why != "" {
		return false
	}
	from, mark := e.from.anchor, k.mark.Pos
	base := reconstruct.BinlogBaseName(to.anchor.File)
	switch {
	case to.anchor.File == "" || to.anchor.IsLSN() || from.IsLSN() || mark.IsLSN():
		return false
	case reconstruct.BinlogBaseName(from.File) != base || reconstruct.BinlogBaseName(mark.File) != base:
		return false
	case from == to.anchor || !from.AtOrBefore(to.anchor):
		return false
	case to.lastEventID < e.from.lastEventID:
		return false
	case !k.reachedFloor(to):
		return false
	}
	n, err := e.tail.PruneThrough(to.anchor, to.lastEventID)
	if err != nil {
		slog.Warn("Engine Routing: a table's tail could not be pruned to its newer snapshot; it is built again", "table", t.Schema+"."+t.Table, "error", err)
		return false
	}
	// Dirty even when nothing was dropped: the file written is laid only
	// over the cut it was written for (fileCut).
	e.table, e.cut, e.from, e.dirty = t, cut, &to, true
	slog.Debug("Engine Routing: a table's tail was pruned to its newer snapshot", "table", t.Schema+"."+t.Table, "dropped", n, "keys", e.tail.Keys())
	return true
}

// reached says whether the cursor has folded every change t's published cut
// holds, so that a change of t the keeper folds from now on is newer than the
// cut's: only then may a tail read from an older cut, or pruned to this one,
// be laid over it. The cursor's position is where its event STARTS, so a cut
// that ends exactly after that event is seen by its last event id instead (a
// refresh's cut records the last event of the table it folded; every event of
// the table it holds is at or below it, and indexed before the cursor was
// read). A dump's cut records no id, and the source may be ahead of capture:
// only the position says it then.
func (k *tailKeeper) reached(t views.BaselineTable) bool {
	to, why := k.src.Cut(t)
	return why == "" && k.reachedFloor(to)
}

func (k *tailKeeper) reachedFloor(to tailFloor) bool {
	mark := k.mark.Pos
	if to.anchor.File == "" || to.anchor.IsLSN() || mark.IsLSN() ||
		reconstruct.BinlogBaseName(to.anchor.File) != reconstruct.BinlogBaseName(mark.File) {
		return false
	}
	return to.anchor.AtOrBefore(mark) || to.lastEventID != 0 && to.lastEventID <= k.mark.ID
}

// admits says whether the read of a window from f would return an event at
// at with id: Fill's own floor, applied to the events advance folds after the
// cursor, so a tail built over a cut the cursor had not reached (a dump ahead
// of capture) does not fold the changes the cut already holds. A floor in
// another numbering than at, or none on record, admits it (the numbering is
// the cursor's check, errTailRenumbered).
func (f *tailFloor) admits(at query.BinlogPos, id uint64) bool {
	if f == nil || f.anchor.File == "" || reconstruct.BinlogBaseName(f.anchor.File) != reconstruct.BinlogBaseName(at.File) {
		return true
	}
	return f.anchor.AtOrBefore(at) && (f.lastEventID == 0 || id > f.lastEventID)
}

// refuse ends e's tail for why, said once: statements over the table read
// its copy as of its snapshot until its next one.
func (k *tailKeeper) refuse(e *tailEntry, why string) {
	if e.refused != "" {
		return
	}
	e.refused = why
	slog.Info("Engine Routing: a table has no tail; statements over it read the copy as of its snapshot",
		"table", e.table.Schema+"."+e.table.Table, "reason", why)
}

// note says, once per reason, that no statement reads tails now (the index
// or the keeper's directory cannot be read), and when they are read again:
// every statement then reads the copy as of its snapshots, which an
// operator must be able to find out without debug logs.
func (k *tailKeeper) note(err error) {
	k.noteMu.Lock()
	defer k.noteMu.Unlock()
	switch {
	case err == nil && k.failing != "":
		slog.Info("Engine Routing: the copy's tails are read again")
		k.failing = ""
	case err != nil && err.Error() != k.failing:
		slog.Warn("Engine Routing: no statement reads the copy's tails; they read the copy as of its snapshots", "error", err)
		k.failing = err.Error()
	case err != nil:
		slog.Debug("Engine Routing: the copy's tails could not be read", "error", err)
	}
}

// build starts t's tail over its published cut and folds its events up to
// the cursor.
func (k *tailKeeper) build(ctx context.Context, t views.BaselineTable, cut string) (*tailEntry, error) {
	refused := func(why string) (*tailEntry, error) { return &tailEntry{table: t, cut: cut, refused: why}, nil }
	// Where the read below starts, from the same footer Fill reads it from;
	// none on record (Fill refuses then) leaves a tail that is never pruned.
	var from *tailFloor
	if f, why := k.src.Cut(t); why == "" {
		from = &f
	}
	tail, why, err := k.src.Start(ctx, t)
	if err != nil {
		return nil, err
	}
	if why != "" {
		return refused(why)
	}
	var foldErr error
	why, err = k.src.Fill(ctx, t, k.mark.ID, func(page []query.ResultRow) error {
		if foldErr = tail.Apply(page); foldErr != nil {
			return foldErr
		}
		if tail.Keys() > k.maxKeys {
			return errTailBudget
		}
		return nil
	})
	switch {
	case errors.Is(err, errTailBudget):
		return refused(fmt.Sprintf("more keys changed since its snapshot than a tail holds (%d)", k.maxKeys))
	case foldErr != nil:
		return refused("its changes could not be folded: " + foldErr.Error())
	case err != nil:
		return nil, err
	case why != "":
		return refused(why)
	}
	return &tailEntry{table: t, cut: cut, from: from, tail: tail, dirty: true}, nil
}

// view is what a statement reads of e: its newest version, written first if
// the tail changed. A tail that holds nothing has no file: the published copy
// is all there is, current through the cursor. A frozen tail keeps the
// instant it stopped at. A version that cannot be written leaves the last
// one, with its own instant; a refusal from the writer ends the tail.
func (k *tailKeeper) view(t views.BaselineTable, e *tailEntry) (tailView, bool) {
	if !e.frozen && e.dirty {
		if err := k.write(t, e); err != nil {
			var r *reconstruct.TailRefusal
			if errors.As(err, &r) {
				k.refuse(e, r.Reason)
				return tailView{}, false
			}
			slog.Debug("Engine Routing: a table's tail could not be written; its last version stays", "table", t.Schema+"."+t.Table, "error", err)
			// Not a version written over an older copy: laid over a newer
			// one, its values would replace newer published ones.
			if e.through.IsZero() || e.fileCut != tailCutOf(t) {
				return tailView{}, false
			}
			return tailView{Path: e.file, PK: e.tail.PKColumns(), Through: e.through}, true
		}
	}
	if !e.frozen {
		e.through = k.through
	}
	if e.through.IsZero() {
		// Frozen before any version was written: nothing to read, and no
		// instant to say.
		return tailView{}, false
	}
	return tailView{Path: e.file, PK: e.tail.PKColumns(), Through: e.through}, true
}

// write publishes e's tail under a name never used before (a statement reads
// the file twice), or retires its file when the tail holds nothing.
func (k *tailKeeper) write(t views.BaselineTable, e *tailEntry) error {
	if e.tail.Keys() == 0 {
		k.retire(e)
		e.dirty = false
		return nil
	}
	if _, err := k.makeDir(); err != nil {
		return err
	}
	k.written++
	path := filepath.Join(k.dir, fmt.Sprintf("%d-%s.%s.parquet", k.written, url.PathEscape(t.Schema), url.PathEscape(t.Table)))
	if _, err := e.tail.WriteFile(path); err != nil {
		return err
	}
	k.retire(e)
	e.file, e.dirty, e.fileCut = path, false, e.cut
	return nil
}

// ensureDir is the keeper's own directory, made if it is not yet: a
// statement's sandbox is allowed to read it before the first version is
// written into it.
func (k *tailKeeper) ensureDir() (string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.makeDir()
}

func (k *tailKeeper) makeDir() (string, error) {
	if k.dir == "" {
		// Made here, by this process, under a name no one could have made
		// before (0700): a fixed name under a shared temporary directory is
		// one another user can make first, and then own.
		dir, err := os.MkdirTemp(k.parent, "bintrail-tails-")
		if err != nil {
			return "", err
		}
		k.dir = dir
	}
	return k.dir, nil
}

// retire moves e's current file among the old ones.
func (k *tailKeeper) retire(e *tailEntry) {
	if e.file != "" {
		e.old = append(e.old, tailOldFile{e.file, k.now()})
		e.file = ""
	}
}

// drop retires every file of e, which is being replaced or forgotten.
func (k *tailKeeper) drop(e *tailEntry) {
	if e == nil {
		return
	}
	k.retire(e)
	k.orphans = append(k.orphans, e.old...)
	e.old = nil
}

// dropAll forgets every tail, keeping their files until no statement reads
// them.
func (k *tailKeeper) dropAll() {
	for key, e := range k.tables {
		k.drop(e)
		delete(k.tables, key)
	}
	k.mark, k.through = tailMark{}, time.Time{}
}

// forgetIdle forgets the tails no statement asked for in a while: they are
// not folded for nothing, and the next statement over one builds it again.
func (k *tailKeeper) forgetIdle() {
	cutoff := k.now().Add(-k.idle)
	for key, e := range k.tables {
		if e.used.Before(cutoff) {
			k.drop(e)
			delete(k.tables, key)
		}
	}
}

// tidy forgets idle tails and removes old versions without waiting for a
// statement to refresh: a server nobody queries any more would otherwise
// keep its last tails for as long as the process runs (#2407).
func (k *tailKeeper) tidy() {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.forgetIdle()
	k.collect()
}

// collect removes the files replaced longer ago than a statement may still
// read them.
func (k *tailKeeper) collect() {
	cutoff := k.now().Add(-k.keepOld)
	keep := func(files []tailOldFile) []tailOldFile {
		out := files[:0]
		for _, f := range files {
			if f.at.After(cutoff) {
				out = append(out, f)
				continue
			}
			if err := os.Remove(f.path); err != nil && !os.IsNotExist(err) {
				slog.Debug("Engine Routing: an old version of a table's tail could not be removed", "path", f.path, "error", err)
			}
		}
		return out
	}
	for _, e := range k.tables {
		e.old = keep(e.old)
	}
	k.orphans = keep(k.orphans)
}

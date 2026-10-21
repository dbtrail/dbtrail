package console

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"runtime/debug"
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
// checks it (reachedFloor), with every other condition the argument needs, and the
// tail is rebuilt from the cut when one fails; a tail built over a cut the
// cursor has not reached folds, as it advances, only the changes after that
// cut (tailFloor.admits), as its first read did. A tail that grows past its
// budget stops advancing (frozen) and goes on serving what it holds, with its
// own older instant, until the next published cut rebuilds it; a frozen tail
// is never laid over a newer cut, whose rows may be newer than its own.
//
// A statement never waits for a tail to be built (#2483): a table with none
// (new, frozen, refused, or one whose newer cut could not be pruned to) is
// built in the background, one build per table at a time, and the statement
// goes on without it. Until the build is in, the old tail stands for the
// newer cut when it holds every change after it (standsFor: every condition
// a prune needs, short of the prune itself); else the table is read with no
// tail. A build reads the index without the keeper's lock, up to the
// cursor as it stood when it started; it then folds, the same way advance
// does, what the cursor passed since, and is installed under the lock, in
// the same hold that finds it caught up with the cursor, and only while it
// is still wanted:
// for the cut it was started for, with the tails it would join not forgotten
// since (an index rebuilt, a numbering started again) and the keeper not
// replaced (endBuild marks it dead in each of these). A build that
// fails reading the index or runs out of time is tried again later
// (retryMin, doubling to retryMax) rather than refusing the table until its
// next cut; at most maxBuilds read the index at once, which capture writes
// to.
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
// One lock over a whole refresh: the tails are one cursor's. Statements do
// not take it: they read what the follower published (pub, tail_follow.go),
// and the follower refreshes under it, bounded by its own deadline.
type tailKeeper struct {
	mu  sync.Mutex
	src tailSource
	pub tailPublish
	// life is the keeper's lifetime: every build runs under it, and stop
	// (close) ends them all when the keeper is replaced or forgotten.
	life context.Context
	stop context.CancelFunc
	// builds are the tails being built in the background, or waiting to be
	// tried again, by tailKey; wg counts the goroutines that build them.
	builds map[string]*tailBuild
	wg     sync.WaitGroup
	// buildTimeout bounds one build; retryMin and retryMax the wait before a
	// failed one is tried again, doubling from one to the other.
	buildTimeout       time.Duration
	retryMin, retryMax time.Duration
	// slots bounds the builds reading the index at once (maxBuilds): the
	// index is where capture writes, and a statement over many tables must
	// not start a read per table at the same time.
	slots chan struct{}
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
	life, stop := context.WithCancel(context.Background())
	return &tailKeeper{src: src, parent: parent, maxKeys: maxKeys, keepOld: keepOld, idle: idle, now: time.Now,
		life: life, stop: stop, builds: map[string]*tailBuild{},
		buildTimeout: tailBuildTimeout, retryMin: tailRetryMin, retryMax: tailRetryMax,
		slots:  make(chan struct{}, tailMaxBuilds),
		tables: map[string]*tailEntry{}, pub: newTailPublish()}
}

// tailBuild is one table's tail being built in the background, or the wait
// before a build that failed is tried again.
type tailBuild struct {
	// cut is the published cut it builds over; cancel ends its read. Set
	// before it starts, never after.
	cut    string
	cancel context.CancelFunc
	// running while its goroutine is; dead when what it builds is no longer
	// wanted (another cut, a definition that changed, every tail forgotten,
	// the keeper closed): it is then never installed, and its slot is
	// cleared when the goroutine ends.
	running, dead bool
	// failures in a row, and when the next try may start; asked when a
	// statement last wanted it; started when it started, and stuck that it
	// was said to run past its own deadline.
	failures int
	retryAt  time.Time
	asked    time.Time
	started  time.Time
	stuck    bool
}

// close ends every build of a keeper that was replaced or forgotten: none is
// installed after, none is started.
func (k *tailKeeper) close() {
	// Before the lock: an update holding it ends at its next read instead
	// of running out its deadline while the caller (a statement, when the
	// keeper is replaced) waits.
	k.stop()
	k.mu.Lock()
	defer k.mu.Unlock()
	k.dropBuilds()
}

// settle waits for every build started so far to end (tests).
func (k *tailKeeper) settle() { k.wg.Wait() }

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
	start, from, written := k.now(), k.mark.ID, k.written
	var read, folded time.Duration
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
	read = k.now().Sub(start)
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
	folded = k.now().Sub(start) - read
	changed, err := k.src.Broken(ctx, tables)
	if err != nil {
		return nil, err
	}
	out := map[string]tailView{}
	for _, t := range tables {
		key := tailKey(t.Schema, t.Table)
		e := k.entryFor(key, t, changed[key])
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
	slog.Debug("Engine Routing: the copy's tails were brought up to date", "tables", len(tables), "events_after", from, "events_through", k.mark.ID,
		"newest_read", read, "fold", folded, "files_written", k.written-written, "total", k.now().Sub(start))
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
// are advancing, moving the cursor with every page. A table with no tail yet
// has its events read by its first Fill.
func (k *tailKeeper) advance(ctx context.Context, upTo uint64) error {
	return k.fold(ctx, k.src, k.tables, k.mark, upTo, func(ev query.ResultRow) {
		// The cursor moves with every page folded: a read that runs out of
		// time keeps what it did, and the next statement goes on from there
		// instead of reading the same backlog again.
		if ev.EventID > k.mark.ID {
			k.mark = tailMark{ID: ev.EventID, Pos: query.BinlogPos{File: ev.BinlogFile, Pos: ev.StartPos}, At: ev.EventTimestamp}
			if k.mark.At.After(k.through) {
				k.through = k.mark.At
			}
		}
	})
}

// fold folds the events after from up to upTo into the entries that are
// advancing, read from src a page at a time; moved, when not nil, is handed
// the last event of each page. It touches nothing but entries, so a build
// catches its own tail up with it without the keeper's lock. The events must
// go on in the binary log from from's: one before the last stops the read
// (errTailRenumbered), since a numbering that started again inside the
// window would rank its changes below older ones. Several rows of one
// statement share a position, so "before" is strict.
func (k *tailKeeper) fold(ctx context.Context, src tailSource, entries map[string]*tailEntry, from tailMark, upTo uint64, moved func(query.ResultRow)) error {
	var live []views.BaselineTable
	for _, e := range entries {
		if e.refused == "" && !e.frozen {
			live = append(live, e.table)
		}
	}
	if len(live) == 0 {
		return nil
	}
	last := from.Pos
	return src.Since(ctx, live, from.ID, upTo, func(page []query.ResultRow) error {
		byTable := map[string][]query.ResultRow{}
		folded := map[string]string{}
		for key, e := range entries {
			folded[strings.ToLower(e.table.Schema)+"\x00"+strings.ToLower(e.table.Table)] = key
		}
		for _, ev := range page {
			// The index compares names without case and the tails do not: an
			// event under another case of a tail's name is a table the copy
			// and the index spell apart, and that tail cannot tell it is
			// missing its changes.
			if key, ok := folded[strings.ToLower(ev.SchemaName)+"\x00"+strings.ToLower(ev.TableName)]; ok && key != tailKey(ev.SchemaName, ev.TableName) {
				if e := entries[key]; e.refused == "" {
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
			if e := entries[key]; e != nil && e.refused == "" && !e.frozen && e.from.admits(at, ev.EventID) {
				byTable[key] = append(byTable[key], ev)
			}
		}
		for key, evs := range byTable {
			e := entries[key]
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
		if n := len(page); n > 0 && moved != nil {
			moved(page[n-1])
		}
		return nil
	})
}

// entryFor is key's tail over t's published cut; a refused one records why
// the table has none. A cut the tail cannot be pruned to has its tail built
// in the background (startBuild): until it is in, the tail it had stands for
// the newer cut when it may, else the table has none for this statement.
func (k *tailKeeper) entryFor(key string, t views.BaselineTable, changed string) *tailEntry {
	cut := tailCutOf(t)
	e := k.tables[key]
	if changed != "" {
		k.endBuild(key)
		if e == nil || e.refused == "" {
			k.drop(e)
			e = &tailEntry{table: t, cut: cut, used: k.now()}
			k.tables[key] = e
			k.refuse(e, changed)
		}
		return nil
	}
	if e != nil && e.cut == cut {
		if b := k.builds[key]; b != nil && b.cut != cut {
			k.endBuild(key) // for a cut no statement reads any more
		}
		return e
	}
	if e != nil && k.pruneTo(e, t, cut) {
		k.endBuild(key)
		return e
	}
	k.startBuild(key, t, cut)
	// A tail it already had stands for the newer cut when it holds every
	// change after that cut (standsFor: what a prune needs, short of the
	// prune itself, which can refuse on a key it cannot order). Not a frozen
	// one: it stopped before the cursor, maybe before the newer cut, and laid
	// over it would put older values over newer ones. Not one read from a cut
	// after the newer one (a copy taken further back) or from another file:
	// the changes between the two cuts are in neither. Its cut stays its own,
	// so the build that replaces it is installed, and it still prunes from
	// what it read.
	if e != nil {
		if _, ok := k.standsFor(e, t); ok {
			return e
		}
	}
	return nil
}

// startBuild builds key's tail over t's cut in the background, unless one
// is running (one per table: a build for an older cut is ended, and the
// newer one starts once it has), a failed one is waiting to be tried again,
// or the keeper is closed.
func (k *tailKeeper) startBuild(key string, t views.BaselineTable, cut string) {
	if k.life.Err() != nil {
		return
	}
	prev := k.builds[key]
	if prev != nil && prev.cut != cut {
		if prev.running {
			prev.dead = true
			prev.cancel()
			return
		}
		prev = nil
	}
	now := k.now()
	if prev != nil {
		prev.asked = now
		if prev.running && !prev.stuck && now.Sub(prev.started) > 2*k.buildTimeout {
			// Its read does not end on its deadline: nothing else would say
			// that this table gets no tail.
			prev.stuck = true
			slog.Warn("Engine Routing: building a table's tail runs far past its deadline; statements read it without one until it ends",
				"table", t.Schema+"."+t.Table, "running_for", now.Sub(prev.started))
		}
		if prev.running || now.Before(prev.retryAt) {
			return
		}
	}
	// The deadline starts once the build has a slot (runBuild): waiting for
	// one is not its read being slow.
	ctx, cancel := context.WithCancel(k.life)
	b := &tailBuild{cut: cut, cancel: cancel, running: true, asked: now, started: now}
	if prev != nil {
		b.failures = prev.failures
	}
	k.builds[key] = b
	src, from := k.src, k.mark
	k.wg.Add(1)
	go k.runBuild(ctx, b, key, t, src, from)
}

// endBuild ends key's build: what it reads is no longer wanted.
func (k *tailKeeper) endBuild(key string) {
	b := k.builds[key]
	switch {
	case b == nil:
	case b.running:
		b.dead = true
		b.cancel()
	default:
		delete(k.builds, key)
	}
}

// dropBuilds ends every build.
func (k *tailKeeper) dropBuilds() {
	for key := range k.builds {
		k.endBuild(key)
	}
}

// current says whether b is still wanted, under k.mu: every way a build
// stops being wanted (another cut, a definition that changed, every tail
// forgotten, the keeper closed) goes through endBuild, which marks it dead.
func (k *tailKeeper) current(key string, b *tailBuild) bool {
	return k.builds[key] == b && !b.dead
}

// runBuild is b's goroutine: it reads t's tail up to from without the lock,
// folds what the cursor passed since until it has caught up, and installs it
// under the lock, in the same hold as the check that it has caught up. A
// panic fails the build like an error, so its slot is not left running.
func (k *tailKeeper) runBuild(ctx context.Context, b *tailBuild, key string, t views.BaselineTable, src tailSource, from tailMark) {
	defer k.wg.Done()
	defer b.cancel()
	defer func() {
		if p := recover(); p != nil {
			k.failBuild(key, b, t, fmt.Errorf("panic: %v", p), string(debug.Stack()))
		}
	}()
	select {
	case k.slots <- struct{}{}:
		defer func() { <-k.slots }()
	case <-ctx.Done():
		k.failBuild(key, b, t, ctx.Err(), "")
		return
	}
	ctx, stop := context.WithTimeout(ctx, k.buildTimeout)
	defer stop()
	e, err := k.build(ctx, src, t, b.cut, from.ID)
	for err == nil {
		var to tailMark
		done := func() bool {
			k.mu.Lock()
			defer k.mu.Unlock()
			if !k.current(key, b) {
				if k.builds[key] == b {
					delete(k.builds, key)
				}
				return true
			}
			if e.refused != "" || e.frozen || from.ID >= k.mark.ID {
				if b.failures > 0 {
					slog.Info("Engine Routing: a table's tail was built after failing before", "table", t.Schema+"."+t.Table, "failures", b.failures)
				}
				k.install(key, e)
				return true
			}
			to = k.mark
			return false
		}()
		if done {
			return
		}
		if err = k.fold(ctx, src, map[string]*tailEntry{key: e}, from, to.ID, nil); err == nil {
			from = to
		}
	}
	k.failBuild(key, b, t, err, "")
}

// retryWait is the wait after n failures in a row: retryMin, doubling, up
// to retryMax.
func (k *tailKeeper) retryWait(n int) time.Duration {
	wait := k.retryMin
	for i := 1; i < n && wait < k.retryMax; i++ {
		wait *= 2
	}
	return min(wait, k.retryMax)
}

// install puts a finished build in, under k.mu: the tail it replaces is
// dropped, a refusal stands until the table's next cut.
func (k *tailKeeper) install(key string, e *tailEntry) {
	delete(k.builds, key)
	k.drop(k.tables[key])
	e.used = k.now()
	k.tables[key] = e
	why := e.refused
	if e.frozen {
		// Over the budget while it caught up: as over it at its first read.
		e.frozen, why = false, fmt.Sprintf("more keys changed since its snapshot than a tail holds (%d)", k.maxKeys)
	}
	if why != "" {
		e.refused = ""
		k.refuse(e, why)
	}
}

// failBuild records that b failed: the table goes on without the tail it
// was building (or with the one it had), and the build is tried again after
// a wait that doubles with each failure in a row. Said at the first, and
// again when the wait reaches its longest, which it then keeps: a table that
// never gets a tail keeps every statement over it off tails (statementTails),
// and an operator must be able to find out without debug logs. stack is a
// panic's, said with it.
func (k *tailKeeper) failBuild(key string, b *tailBuild, t views.BaselineTable, err error, stack string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.current(key, b) {
		if k.builds[key] == b {
			delete(k.builds, key)
		}
		return
	}
	b.running = false
	b.failures++
	wait := k.retryWait(b.failures)
	b.retryAt = k.now().Add(wait)
	log := slog.Debug
	if b.failures == 1 || wait == k.retryMax && k.retryWait(b.failures-1) < wait {
		log = slog.Warn
	}
	attrs := []any{"table", t.Schema + "." + t.Table, "failures", b.failures, "retry_in", wait, "error", err}
	if stack != "" {
		attrs = append(attrs, "stack", stack)
		if b.failures == 1 {
			log = slog.Error
		}
	}
	log("Engine Routing: a table's tail could not be built; statements read it without one (or with the one it had) until it is tried again", attrs...)
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
	to, ok := k.standsFor(e, t)
	if !ok {
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

// standsFor says whether e's tail holds every change after t's newer cut up
// to the cursor, so that it may be laid over that cut as it is (while the
// cut's own tail is built) or pruned to it, and returns where the cut's
// window starts. The conditions are pruneTo's, listed above it; none reads
// the index.
func (k *tailKeeper) standsFor(e *tailEntry, t views.BaselineTable) (tailFloor, bool) {
	if e.refused != "" || e.frozen || e.from == nil || e.tail == nil || t.Path != e.table.Path {
		return tailFloor{}, false
	}
	to, why := k.src.Cut(t)
	if why != "" {
		return tailFloor{}, false
	}
	from, mark := e.from.anchor, k.mark.Pos
	base := reconstruct.BinlogBaseName(to.anchor.File)
	switch {
	case to.anchor.File == "" || to.anchor.IsLSN() || from.IsLSN() || mark.IsLSN():
		return tailFloor{}, false
	case reconstruct.BinlogBaseName(from.File) != base || reconstruct.BinlogBaseName(mark.File) != base:
		return tailFloor{}, false
	case from == to.anchor || !from.AtOrBefore(to.anchor):
		return tailFloor{}, false
	case to.lastEventID < e.from.lastEventID:
		return tailFloor{}, false
	case !k.reachedFloor(to):
		return tailFloor{}, false
	}
	return to, true
}

// reachedFloor says whether the cursor has folded every change the published
// cut that starts at to holds, so that a change of t the keeper folds from now on is newer than the
// cut's: only then may a tail read from an older cut, or pruned to this one,
// be laid over it. The cursor's position is where its event STARTS, so a cut
// that ends exactly after that event is seen by its last event id instead (a
// refresh's cut records the last event of the table it folded; every event of
// the table it holds is at or below it, and indexed before the cursor was
// read). A dump's cut records no id, and the source may be ahead of capture:
// only the position says it then.
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
// upTo, read from src. It runs without the keeper's lock: it reads nothing of
// the keeper's but what never changes.
func (k *tailKeeper) build(ctx context.Context, src tailSource, t views.BaselineTable, cut string, upTo uint64) (*tailEntry, error) {
	refused := func(why string) (*tailEntry, error) { return &tailEntry{table: t, cut: cut, refused: why}, nil }
	// Where the read below starts, from the same footer Fill reads it from;
	// none on record (Fill refuses then) leaves a tail that is never pruned.
	var from *tailFloor
	if f, why := src.Cut(t); why == "" {
		from = &f
	}
	tail, why, err := src.Start(ctx, t)
	if err != nil {
		return nil, err
	}
	if why != "" {
		return refused(why)
	}
	var foldErr error
	why, err = src.Fill(ctx, t, upTo, func(page []query.ResultRow) error {
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
	k.dropBuilds()
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
	// And a failed build no statement asked for since: a running one is
	// installed, then forgotten like any tail.
	for key, b := range k.builds {
		if !b.running && b.asked.Before(cutoff) {
			delete(k.builds, key)
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

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
// tail is rebuilt from that cut, to shrink it; a rebuild that fails keeps the
// old one, which stays correct over the newer copy: it holds every change
// after an older cut up to the cursor, and the cursor is past anything a
// refresh can have published. A tail that grows past its budget stops
// advancing (frozen) and goes on serving what it holds, with its own older
// instant, until the next published cut rebuilds it.

// tailSource is what the tails of one server read: its index, and the copy's
// files.
type tailSource interface {
	// Newest is the newest event the index holds: its id and when the source
	// ran it. id 0: the index holds none.
	Newest(ctx context.Context) (id uint64, at time.Time, err error)
	// Start builds t's empty tail, or says why t has none.
	Start(ctx context.Context, t views.BaselineTable) (tailTable, string, error)
	// Fill is t's events after its published cut with an id up to upTo, or
	// why they cannot be read completely.
	Fill(ctx context.Context, t views.BaselineTable, upTo uint64) ([]query.ResultRow, string, error)
	// Since is every event, of any table, with from < id <= upTo.
	Since(ctx context.Context, from, upTo uint64) ([]query.ResultRow, error)
	// Changed says, by tailKey, why a table's definition may have changed
	// after its published cut. Tables it leaves out are unchanged.
	Changed(ctx context.Context, tables []views.BaselineTable) (map[string]string, error)
}

// tailTable is a reconstruct.Tail as the keeper drives it.
type tailTable interface {
	Apply(page []query.ResultRow) error
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

// tailKey names a table among a server's tails, as MySQL compares names.
func tailKey(schema, table string) string {
	return strings.ToLower(schema) + "\x00" + strings.ToLower(table)
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
	cut     string
	tail    tailTable
	refused string
	frozen  bool
	dirty   bool
	// file is the newest version written ("" before the first, or while the
	// tail holds nothing), through the instant it is indexed through.
	file    string
	through time.Time
	// old are the earlier versions, kept while a statement may still read
	// them.
	old []tailOldFile
}

type tailOldFile struct {
	path string
	at   time.Time
}

// tailKeeper keeps one server's tails.
type tailKeeper struct {
	mu  sync.Mutex
	src tailSource
	// dir is where the versions are written, a directory of this keeper's
	// own; maxKeys the budget of one tail; keepOld how long a replaced
	// version stays for a statement that may still read it.
	dir     string
	maxKeys int
	keepOld time.Duration
	now     func() time.Time

	cursor  uint64
	through time.Time
	tables  map[string]*tailEntry
	written int
	// orphans are the files of tails replaced or forgotten, kept like old
	// versions.
	orphans []tailOldFile
}

func newTailKeeper(src tailSource, dir string, maxKeys int, keepOld time.Duration) *tailKeeper {
	return &tailKeeper{src: src, dir: dir, maxKeys: maxKeys, keepOld: keepOld, now: time.Now, tables: map[string]*tailEntry{}}
}

// refresh brings the tails of tables up to what the index holds now and
// returns, by tailKey, what a statement over them reads. A table it leaves
// out has no tail: the statement reads its published copy, as before tails.
// An error is the index's: no table has a tail for this statement.
func (k *tailKeeper) refresh(ctx context.Context, tables []views.BaselineTable) (map[string]tailView, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	// The newest event FIRST: every read below is bounded by it, so the
	// instant reported is of an event every tail has folded.
	upTo, at, err := k.src.Newest(ctx)
	if err != nil {
		return nil, err
	}
	switch {
	case upTo == 0:
		return nil, nil
	case upTo < k.cursor:
		// The index holds less than it did (a capture restarted from an
		// earlier point deletes the rows past it): what the tails folded may
		// no longer be there. Start over.
		slog.Info("Engine Routing: the index holds fewer events than the copy's tails had read; they are rebuilt", "had", k.cursor, "now", upTo)
		k.dropAll()
	}
	if k.cursor == 0 {
		k.cursor, k.through = upTo, at
	} else if upTo > k.cursor {
		events, err := k.src.Since(ctx, k.cursor, upTo)
		if err != nil {
			return nil, err
		}
		k.apply(events)
		k.cursor, k.through = upTo, at
	}
	changed, err := k.src.Changed(ctx, tables)
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
		if err := k.write(key, t, e); err != nil {
			continue
		}
		out[key] = tailView{Path: e.file, PK: e.tail.PKColumns(), Through: e.through}
	}
	k.collect()
	return out, nil
}

// apply folds the events since the last refresh into the tails they belong
// to. A table with no tail yet has its events read by its first Fill.
func (k *tailKeeper) apply(events []query.ResultRow) {
	byTable := map[string][]query.ResultRow{}
	for _, ev := range events {
		key := tailKey(ev.SchemaName, ev.TableName)
		if e := k.tables[key]; e != nil && e.refused == "" && !e.frozen {
			byTable[key] = append(byTable[key], ev)
		}
	}
	for key, page := range byTable {
		e := k.tables[key]
		if err := e.tail.Apply(page); err != nil {
			e.refused = "its changes could not be folded: " + err.Error()
			continue
		}
		e.dirty = true
		if e.tail.Keys() > k.maxKeys {
			// Kept as it is, not dropped: dropping it would send the next
			// statement back to an older answer.
			e.frozen = true
			slog.Info("Engine Routing: a table's tail is over its budget; it stops advancing until the next snapshot update",
				"table", key, "keys", e.tail.Keys(), "budget", k.maxKeys)
		}
	}
}

// entryFor is key's tail over t's published cut, built when there is none
// or the cut moved; refused records why the table has none.
func (k *tailKeeper) entryFor(ctx context.Context, key string, t views.BaselineTable, changed string) *tailEntry {
	cut := tailCutOf(t)
	e := k.tables[key]
	if changed != "" {
		if e == nil || e.refused == "" {
			k.drop(e)
			k.tables[key] = &tailEntry{cut: cut, refused: changed}
		}
		return nil
	}
	if e != nil && e.cut == cut {
		return e
	}
	fresh, err := k.build(ctx, t, cut)
	if err != nil {
		// The index could not be read. A tail it already had stays: it is
		// correct over the newer copy too (see the top of this file).
		slog.Debug("Engine Routing: a table's tail could not be rebuilt over its newer snapshot; the previous one stays", "table", key, "error", err)
		if e != nil && e.refused == "" {
			return e
		}
		return nil
	}
	k.drop(e)
	k.tables[key] = fresh
	return fresh
}

// build starts t's tail over its published cut and folds its events up to
// the cursor.
func (k *tailKeeper) build(ctx context.Context, t views.BaselineTable, cut string) (*tailEntry, error) {
	tail, why, err := k.src.Start(ctx, t)
	if err != nil {
		return nil, err
	}
	if why != "" {
		return &tailEntry{cut: cut, refused: why}, nil
	}
	events, why, err := k.src.Fill(ctx, t, k.cursor)
	if err != nil {
		return nil, err
	}
	if why != "" {
		return &tailEntry{cut: cut, refused: why}, nil
	}
	if err := tail.Apply(events); err != nil {
		return &tailEntry{cut: cut, refused: "its changes could not be folded: " + err.Error()}, nil
	}
	if tail.Keys() > k.maxKeys {
		return &tailEntry{cut: cut, refused: fmt.Sprintf("%d keys changed since its snapshot, more than a tail holds (%d)", tail.Keys(), k.maxKeys)}, nil
	}
	return &tailEntry{cut: cut, tail: tail, dirty: true}, nil
}

// write publishes e's newest version when it changed: a new file under a name
// never used before, since a statement reads it twice. A tail that holds
// nothing has no file: the published copy is all there is, current through
// the cursor. A frozen tail keeps the instant it stopped at.
func (k *tailKeeper) write(key string, t views.BaselineTable, e *tailEntry) error {
	if e.frozen {
		return nil
	}
	defer func() { e.through = k.through }()
	if !e.dirty {
		return nil
	}
	if e.tail.Keys() == 0 {
		k.retire(e)
		e.dirty = false
		return nil
	}
	if err := os.MkdirAll(k.dir, 0o700); err != nil {
		e.refused = "its tail could not be written: " + err.Error()
		return err
	}
	k.written++
	path := filepath.Join(k.dir, fmt.Sprintf("%d-%s.%s.parquet", k.written, url.PathEscape(t.Schema), url.PathEscape(t.Table)))
	if _, err := e.tail.WriteFile(path); err != nil {
		var r *reconstruct.TailRefusal
		if errors.As(err, &r) {
			e.refused = r.Reason
		} else {
			e.refused = "its tail could not be written: " + err.Error()
		}
		return err
	}
	k.retire(e)
	e.file, e.dirty = path, false
	return nil
}

// retire moves e's current file among the old ones.
func (k *tailKeeper) retire(e *tailEntry) {
	if e.file != "" {
		e.old = append(e.old, tailOldFile{e.file, k.now()})
		e.file = ""
	}
}

// drop retires every file of e, which is being replaced.
func (k *tailKeeper) drop(e *tailEntry) {
	if e == nil {
		return
	}
	k.retire(e)
	k.orphans = append(k.orphans, e.old...)
}

// dropAll forgets every tail, keeping their files until no statement reads
// them.
func (k *tailKeeper) dropAll() {
	for key, e := range k.tables {
		k.drop(e)
		delete(k.tables, key)
	}
	k.cursor, k.through = 0, time.Time{}
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

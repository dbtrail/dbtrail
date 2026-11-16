package console

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/views"
)

// The SQL port reads each table with its tail laid over its published state
// (#2369): what capture has indexed since the snapshot, so a statement on the
// copy is as current as the index, not only as the last refresh. Only the
// port's statements (SQLOnCopy.Run) do; the console's own SQL card reads
// the published copy. Tails are the fresh mode, the default: each server's
// switch decides (#2546, tailsOnFor), fresh when it saved none, unless
// BINTRAIL_SQL_TAILS decides for every server (on: tailsOff is false; off:
// tailsForcedOff).

// Tail budgets: keys one tail may hold and memory all of them may (#2494,
// tailBudgetFor), how long a replaced version stays for a statement that may
// still read it, how long a tail no statement asked for is kept.
const (
	// tailDefaultMaxKeys is BINTRAIL_SQL_TAIL_MAX_KEYS when it is not set: a
	// TPC-C source at about 900 rows changed a second passes 200,000 keys on
	// its busiest tables inside a 5-minute refresh interval.
	tailDefaultMaxKeys = 500_000
	// tailBytesShare is the share of the memory this process may use that
	// every tail of ONE SERVER together may hold when
	// BINTRAIL_SQL_TAIL_MAX_BYTES is not set: one fifth. A router that
	// serves several servers gives each its fifth. The rest is the statements' (--sql-memory each), the
	// views' footers and Go's own heap.
	tailBytesShare = 5
	tailKeepOld    = 30 * time.Minute
	tailIdle       = 30 * time.Minute
	// tailBuildTimeout bounds one background build of a table's tail; a
	// failed one is tried again after tailRetryMin, doubling to tailRetryMax.
	tailBuildTimeout = 2 * time.Minute
	tailRetryMin     = 5 * time.Second
	tailRetryMax     = 2 * time.Minute
	// tailMaxBuilds is how many builds of one server's tails read its index
	// at once.
	tailMaxBuilds = 2
	// tailMaxSegments bounds the files of a table's tail chain, and
	// tailMergeRatio the rows they hold against the tail's keys: past either,
	// the next write is the whole tail (#2495). A statement reads every file
	// of the chain.
	tailMaxSegments = 8
	tailMergeRatio  = 2
)

// tailKeepers are the keepers of one process, by server id.
type tailKeepers struct {
	mu sync.Mutex
	by map[string]*tailKeeper
}

type tailsCtxKey struct{}

// withTailsFor marks ctx as a statement of the port on server id, which reads
// that server's tails.
func withTailsFor(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, tailsCtxKey{}, id)
}

type seenFloorKey struct{}

// withSeenFloor marks ctx with the highest index event id an earlier answer
// on the copy of the same connection held every event through.
func withSeenFloor(ctx context.Context, id uint64) context.Context {
	if id == 0 {
		return ctx
	}
	return context.WithValue(ctx, seenFloorKey{}, id)
}

func seenFloor(ctx context.Context) uint64 {
	id, _ := ctx.Value(seenFloorKey{}).(uint64)
	return id
}

func tailsFor(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(tailsCtxKey{}).(string)
	return id, ok && id != ""
}

// The setting that decides for every server, BINTRAIL_SQL_TAILS, as
// SQLModeSetting reads it.
const (
	SQLModeForcedFresh = "fresh"
	SQLModeForcedFast  = "fast"
)

// SQLModeSetting reads BINTRAIL_SQL_TAILS: forced is SQLModeForcedFresh for
// on, true, 1, yes or fresh, SQLModeForcedFast for off, false, 0, no or fast
// (in any case), and "" when it is unset or says anything else: then each
// server's own choice applies, fresh when it saved none. read is the value,
// trimmed, so the router can show one it did not understand.
func SQLModeSetting() (forced, read string) {
	read = strings.TrimSpace(os.Getenv("BINTRAIL_SQL_TAILS"))
	switch strings.ToLower(read) {
	case "on", "true", "1", "yes", "fresh":
		return SQLModeForcedFresh, read
	case "off", "false", "0", "no", "fast":
		return SQLModeForcedFast, read
	}
	return "", read
}

// tailsOff says the setting does NOT put every server in fresh mode: each
// server's switch decides then (tailsOnFor), unless the setting puts every
// server in fast mode (tailsForcedOff).
func tailsOff() bool {
	on, _ := TailsSetting()
	return !on
}

// tailsForcedOff says the setting puts every server in fast mode, whatever
// its entry says.
func tailsForcedOff() bool {
	forced, _ := SQLModeSetting()
	return forced == SQLModeForcedFast
}

// TailsSetting is whether the setting puts every server in fresh mode, and
// the BINTRAIL_SQL_TAILS value read, trimmed ("" when unset).
func TailsSetting() (on bool, read string) {
	forced, read := SQLModeSetting()
	return forced == SQLModeForcedFresh, read
}

// tailKeeperFor is server id's keeper over b, nil when the server can have no
// tails (which tables its capture records is not known here: no index read
// is paid for nothing). A keeper over another bundle (the server's index
// connection was opened again) is replaced; its files are removed once no
// statement can still read them.
func (s *Server) tailKeeperFor(id string, b *bundle) *tailKeeper {
	if s.pgTailsFor(id, b) {
		return s.tailKeeperForPG(id, b)
	}
	captures, capturesKey, known := s.tailCapturesKeyed(id)
	if !known {
		return nil
	}
	tailSweep.Do(func() { sweepTailDirs(os.TempDir(), time.Now().Add(-tailKeepOld)) })
	s.tails.mu.Lock()
	// Asked again under the keepers' lock (#2546): a statement that passed
	// the gate just before its server turned fast must not make the keeper
	// applySQLFresh dropped. It turns the server off BEFORE it takes this
	// lock to drop the keeper, so either the keeper made here is dropped
	// after, or the server is seen fast here and none is made.
	if !s.tailsOnFor(id) {
		s.tails.mu.Unlock()
		return nil
	}
	old := s.tails.by[id]
	if old != nil {
		// The same index connection, not the same bundle: an edit of the
		// server that leaves its index alone (its application login, a
		// setting) makes a new bundle over the same connection
		// (rebuildDerived), and a keeper replaced for it started with no
		// tails, so every statement read the snapshot alone, minutes old,
		// until they were built again. What capture records is part of it:
		// a tail of a table capture no longer records would claim changes
		// it cannot see.
		if src, ok := old.src.(*indexTailSource); ok && src.b.db == b.db && src.capturesKey == capturesKey {
			s.tails.mu.Unlock()
			return old
		}
	}
	src := newIndexTailSource(s, b).capturing(captures)
	src.capturesKey = capturesKey
	k := newTailKeeper(src, os.TempDir(), tailBudgetFromEnv(), tailKeepOld, tailIdle)
	k.pub.auto = true
	if s.tails.by == nil {
		s.tails.by = map[string]*tailKeeper{}
	}
	s.tails.by[id] = k
	s.tails.mu.Unlock()
	if old != nil {
		// Outside the keepers' lock: the old one may be mid-refresh. Its
		// builds end now, and none installs into it after.
		old.close()
		old.removeDirAfter(tailKeepOld)
	}
	return k
}

// tailCaptures is which tables capture records for server id, as the watch
// daemon decides it for a registry server (ServerEntry.Schemas; empty: every
// table). The boot server's filters are the daemon's flags, which this
// process does not know: no tails for it.
func (s *Server) tailCaptures(id string) (func(schema, table string) bool, bool) {
	f, _, ok := s.tailCapturesKeyed(id)
	return f, ok
}

// tailCapturesKeyed is tailCaptures with the setting it decided from (the
// entry's Schemas), both of one read of the registry, for telling one
// decision from another.
func (s *Server) tailCapturesKeyed(id string) (func(schema, table string) bool, string, bool) {
	if s.tailCapturesHook != nil {
		f, ok := s.tailCapturesHook(id)
		return f, "", ok
	}
	if id == bootServerID || s.cm == nil || s.cm.reg == nil {
		return nil, "", false
	}
	e, ok := s.cm.reg.Get(id)
	if !ok {
		return nil, "", false
	}
	if e.Schemas == "" {
		return nil, "", true
	}
	filters := cliutil.BuildIndexFilters(e.Schemas, "")
	return filters.Matches, e.Schemas, true
}

// statementTails returns tables with the tails of read, the tables a
// statement reads, set as the follower last published them, and the instant
// they are indexed through. It reads the index, within the time a statement
// waits, only for a table the follower has not brought up to date over its
// cut (tailKeeper.latest). All or nothing: a statement reads every table at one
// moment, so unless every table it reads has a tail at the same instant,
// none is used and it reads the published copy, which is one moment too. A
// join over a table read now and one read as of its snapshot would pair rows
// that never coexisted. ok false: tables, unchanged.
func (s *Server) statementTails(ctx context.Context, k *tailKeeper, tables, read []views.BaselineTable) (out []views.BaselineTable, through time.Time, ok bool) {
	out, through, _, ok = s.statementTailsMark(ctx, k, tables, read)
	return out, through, ok
}

// statementTailsMark is statementTails with throughID, the least id of the
// index's event the tails it read hold every event through (#2556), 0 when
// any is unknown.
func (s *Server) statementTailsMark(ctx context.Context, k *tailKeeper, tables, read []views.BaselineTable) (out []views.BaselineTable, through time.Time, throughID uint64, ok bool) {
	if len(read) == 0 {
		return tables, time.Time{}, 0, false
	}
	got := k.latest(ctx, copyUnchangedTimeout, read)
	ids := make([]uint64, 0, len(read))
	for i, t := range read {
		v, has := got[tailKey(t.Schema, t.Table)]
		if !has || (i > 0 && !v.Through.Equal(through)) {
			return tables, time.Time{}, 0, false
		}
		through = v.Through
		ids = append(ids, v.ThroughID)
	}
	throughID = leastEventID(ids...)
	out = make([]views.BaselineTable, len(tables))
	copy(out, tables)
	for i := range out {
		if v, ok := got[tailKey(out[i].Schema, out[i].Table)]; ok && len(v.Paths) > 0 {
			out[i].Tail = &views.TableTail{Paths: v.Paths, PK: v.PK}
		}
	}
	return out, through, throughID, true
}

// freshTailsWait is how long a fresh-mode statement on the port waits for
// a table's tail that is on its way, counted from the statement's first
// look. Past it the statement does not read the snapshot alone as if it
// were fresh: see statementTailsFresh.
const freshTailsWait = 3 * time.Second

// statementTailsFresh is statementTailsMark for fresh mode, which tells a
// table whose tail is on its way from one that cannot have one now.
//
// On its way: the keeper is building it over the cut the statement reads
// (the first statement over a table, the first after a newer snapshot), or
// no update has said yet (none ran over this cut, or the last one failed).
// The statement waits for it, the follower woken once, up to wait; past it,
// missing says why for a client: the caller sends the statement to MySQL
// (routing) or answers from the snapshot with it as a warning. An answer
// from the snapshot alone there would read as fresh and be minutes old.
//
// Cannot have one now (its key is not made of integers, its definition
// changed, capture does not record it, the index holds no captured change,
// tails at two instants after one stopped over its budget): no wait brings
// it, and the statement reads the snapshot as without fresh mode, ok false
// and missing "", its age in the answer's lag as always.
func (s *Server) statementTailsFresh(ctx context.Context, k *tailKeeper, tables, read []views.BaselineTable, wait time.Duration) (out []views.BaselineTable, through time.Time, throughID uint64, ok bool, missing string) {
	deadline := time.Now().Add(wait)
	woke := false
	for {
		ch := k.nextPublish()
		out, through, throughID, ok = s.statementTailsMark(ctx, k, tables, read)
		if ok || len(read) == 0 {
			return out, through, throughID, ok, ""
		}
		pub, _ := k.published(read)
		pending := ""
		for _, t := range read {
			if _, has := pub[tailKey(t.Schema, t.Table)]; has {
				continue
			}
			switch k.stateOf(t) {
			case tailNone:
				// All or nothing: one table that cannot have a tail now
				// keeps the statement from tails, however soon the others
				// come.
				return tables, time.Time{}, 0, false, ""
			default:
				if pending == "" {
					pending = t.Schema + "." + t.Table
				}
			}
		}
		if pending == "" {
			// Every table has a view, of one update, at two instants: a tail
			// over its budget stopped advancing, until the next snapshot.
			return tables, time.Time{}, 0, false, ""
		}
		left := time.Until(deadline)
		if left <= 0 || ctx.Err() != nil || k.life.Err() != nil {
			return tables, time.Time{}, 0, false, "the newest captured changes of " + pending + " are not ready yet"
		}
		if !woke {
			// Once: the follower then goes on at its own pace, so statements
			// waiting together do not keep it from pausing.
			k.wakeFollower()
			woke = true
		}
		t := time.NewTimer(left)
		select {
		case <-ch:
		case <-t.C:
		case <-ctx.Done():
		case <-k.life.Done():
		}
		t.Stop()
	}
}

// freshSnapshotNote is the warning of an answer that fresh mode had to give
// from the snapshot alone, on a connection that does not route.
func freshSnapshotNote(missing string, snapshot time.Time) string {
	at := "its last snapshot"
	if !snapshot.IsZero() {
		at = "its snapshot of " + snapshot.UTC().Format("2006-01-02 15:04:05 UTC")
	}
	return "Answered from " + at + " without the newest captured changes: " + missing + "."
}

// tailsSayTheInstant is whether a statement whose tables all read tails
// (tailed) is answered as of the tails' instant. Not one over the change
// log, read as it is, not at that moment: its number is the copy's, as
// without tails.
func tailsSayTheInstant(statement string, tailed bool) bool {
	return tailed && !sqlMentionsEvents(statement)
}

var tailSweep sync.Once

// errTailDirHeld is lockTailDir's answer for a directory a live process holds.
var errTailDirHeld = errors.New("another process holds the tails' directory")

// sweepTailDirs removes the tails' directories an earlier process left under
// parent (it stopped before removing them; nothing reads them again): this
// user's, last written before cutoff, and held by no process. Not written
// for a while is no proof it is unused (#2819): a keeper writes only when a
// tail changed, so a live process whose server had no change for longer
// still reads its files, and the lock it holds on its directory (makeDir)
// is what keeps it. The age stays a rule for the instant between a keeper
// making its directory and locking it, and for a directory of a version from
// before the lock, whose process may still run.
func sweepTailDirs(parent string, cutoff time.Time) {
	dirs, _ := filepath.Glob(filepath.Join(parent, "bintrail-tails-*"))
	for _, dir := range dirs {
		// Lstat: a link by that name is not a directory, and is left alone.
		fi, err := os.Lstat(dir)
		if err != nil || !fi.IsDir() || !fi.ModTime().Before(cutoff) {
			continue
		}
		if st, ok := fi.Sys().(*syscall.Stat_t); ok && int(st.Uid) != os.Getuid() {
			continue // another user's
		}
		lock, err := lockTailDir(dir)
		if err != nil {
			// Held: a live process reads it. Any other failure cannot tell
			// it is unused, so it stays too.
			if !errors.Is(err, errTailDirHeld) {
				slog.Debug("Engine Routing: an earlier process's tails were kept: whether a process still uses them is not known", "dir", dir, "error", err)
			}
			continue
		}
		// Removed while held, so no keeper takes it in between.
		if err := os.RemoveAll(dir); err != nil {
			slog.Debug("Engine Routing: an earlier process's tails could not be removed", "dir", dir, "error", err)
		}
		lock.Close()
	}
}

// TidyTails is the tails' housekeeping, run by the router every so often
// (#2407): a server no longer in the registry loses its keeper, whose files
// are removed once no statement can still read them, and every other keeper
// forgets its idle tails and old versions.
func (s *Server) TidyTails() {
	if s.cm == nil || s.cm.reg == nil {
		return
	}
	s.tails.mu.Lock()
	var gone, live []*tailKeeper
	for id, k := range s.tails.by {
		if _, ok := s.cm.reg.Get(id); ok {
			live = append(live, k)
			continue
		}
		gone = append(gone, k)
		delete(s.tails.by, id)
		s.copyAges.forget(id)
	}
	s.tails.mu.Unlock()
	for _, k := range gone {
		retireTailKeeper(k)
	}
	for _, k := range live {
		k.tidy()
	}
}

// tailBudgetFromEnv is the process's tail budget, read once from
// BINTRAIL_SQL_TAIL_MAX_KEYS and BINTRAIL_SQL_TAIL_MAX_BYTES and the memory
// the process plans with (MemoryBudgetBytes: the host, or its share, #2513);
// a value that cannot be read is warned once and the default stands.
var tailBudgetFromEnv = sync.OnceValue(func() tailBudget {
	b, warnings := tailBudgetFor(os.Getenv("BINTRAIL_SQL_TAIL_MAX_KEYS"), os.Getenv("BINTRAIL_SQL_TAIL_MAX_BYTES"), memoryBudget())
	for _, w := range warnings {
		slog.Warn("Engine Routing: " + w)
	}
	slog.Info("Engine Routing: the budget of the copy's tails", "max_keys_per_table", b.Keys, "max_bytes_all_tables", b.Bytes)
	return b
})

// tailBudgetFor reads the two settings over host, the bytes this process
// may use (0 unknown). Keys: a whole number above 0, else the default.
// Bytes: a size (1GiB, 512MB, ...), 0 or "off" for no bound, else one
// tailBytesShare-th of host, which is no bound when host is unknown.
func tailBudgetFor(keysRaw, bytesRaw string, host uint64) (b tailBudget, warnings []string) {
	b.Keys = tailDefaultMaxKeys
	if raw := strings.TrimSpace(keysRaw); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			b.Keys = n
		} else {
			warnings = append(warnings, fmt.Sprintf("BINTRAIL_SQL_TAIL_MAX_KEYS=%q is not a whole number above 0; the default of %d keys per table stands", raw, tailDefaultMaxKeys))
		}
	}
	b.Bytes = int64(host / tailBytesShare)
	if raw := strings.TrimSpace(bytesRaw); raw != "" {
		switch {
		case raw == "0" || strings.EqualFold(raw, "off"):
			b.Bytes = 0
		default:
			if n, err := cliutil.ParseByteSize(raw); err == nil && n > 0 {
				b.Bytes = n
			} else {
				warnings = append(warnings, fmt.Sprintf("BINTRAIL_SQL_TAIL_MAX_BYTES=%q is not a size (write it like 1GiB or 512MB, or 0 for no bound); the default of %d MiB for all tails together stands", raw, b.Bytes>>20))
			}
		}
	}
	return b, warnings
}

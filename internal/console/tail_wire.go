package console

import (
	"context"
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
// the published copy. Tails are the opt-in fresh mode: BINTRAIL_SQL_TAILS=on
// for every server (tailsOff), or one server's switch (#2546, tailsOnFor).

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

func tailsFor(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(tailsCtxKey{}).(string)
	return id, ok && id != ""
}

// tailsOff says the port answers from each table's snapshot alone, which is
// the default: fast answers first. Tails are opt-in, the "fresh
// mode": BINTRAIL_SQL_TAILS=on (or true, 1, yes, fresh, in any case). Any
// other value, a typo included, keeps them off; TailsSetting says what was
// read so the router can show it.
func tailsOff() bool {
	on, _ := TailsSetting()
	return !on
}

// TailsSetting is whether tails are on and the BINTRAIL_SQL_TAILS value read,
// trimmed ("" when unset).
func TailsSetting() (on bool, read string) {
	read = strings.TrimSpace(os.Getenv("BINTRAIL_SQL_TAILS"))
	switch strings.ToLower(read) {
	case "on", "true", "1", "yes", "fresh":
		return true, read
	}
	return false, read
}

// tailKeeperFor is server id's keeper over b, nil when the server can have no
// tails (which tables its capture records is not known here: no index read
// is paid for nothing). A keeper over another bundle (the server's index
// connection was opened again) is replaced; its files are removed once no
// statement can still read them.
func (s *Server) tailKeeperFor(id string, b *bundle) *tailKeeper {
	captures, known := s.tailCaptures(id)
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
		if src, ok := old.src.(*indexTailSource); ok && src.b == b {
			s.tails.mu.Unlock()
			return old
		}
	}
	k := newTailKeeper(newIndexTailSource(s, b).capturing(captures), os.TempDir(), tailBudgetFromEnv(), tailKeepOld, tailIdle)
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
		time.AfterFunc(tailKeepOld, func() {
			old.mu.Lock()
			dir := old.dir
			old.mu.Unlock()
			if dir != "" {
				_ = os.RemoveAll(dir)
			}
		})
	}
	return k
}

// tailCaptures is which tables capture records for server id, as the watch
// daemon decides it for a registry server (ServerEntry.Schemas; empty: every
// table). The boot server's filters are the daemon's flags, which this
// process does not know: no tails for it.
func (s *Server) tailCaptures(id string) (func(schema, table string) bool, bool) {
	if s.tailCapturesHook != nil {
		return s.tailCapturesHook(id)
	}
	if id == bootServerID || s.cm.reg == nil {
		return nil, false
	}
	e, ok := s.cm.reg.Get(id)
	if !ok {
		return nil, false
	}
	if e.Schemas == "" {
		return nil, true
	}
	filters := cliutil.BuildIndexFilters(e.Schemas, "")
	return filters.Matches, true
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

// tailsSayTheInstant is whether a statement whose tables all read tails
// (tailed) is answered as of the tails' instant. Not one over the change
// log, read as it is, not at that moment: its number is the copy's, as
// without tails.
func tailsSayTheInstant(statement string, tailed bool) bool {
	return tailed && !sqlMentionsEvents(statement)
}

var tailSweep sync.Once

// sweepTailDirs removes the tails' directories an earlier process left under
// parent (it stopped before removing them; nothing reads them again), those
// last written before cutoff: another process's are written while it runs.
func sweepTailDirs(parent string, cutoff time.Time) {
	dirs, _ := filepath.Glob(filepath.Join(parent, "bintrail-tails-*"))
	for _, dir := range dirs {
		fi, err := os.Lstat(dir)
		if err != nil || !fi.IsDir() || !fi.ModTime().Before(cutoff) {
			continue
		}
		if st, ok := fi.Sys().(*syscall.Stat_t); ok && int(st.Uid) != os.Getuid() {
			continue // another user's
		}
		if err := os.RemoveAll(dir); err != nil {
			slog.Debug("Engine Routing: an earlier process's tails could not be removed", "dir", dir, "error", err)
		}
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

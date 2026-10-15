package console

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/views"
)

// The SQL port reads each table with its tail laid over its published state
// (#2369): what capture has indexed since the snapshot, so a statement on the
// copy is as current as the index, not only as the last refresh. Only the
// port's statements (SQLOnCopy.Run) do; the console's own SQL card reads
// the published copy. BINTRAIL_SQL_TAILS=off turns tails off.

// Tail budgets: keys one tail may hold, how long a replaced version stays for
// a statement that may still read it, how long a tail no statement asked for
// is kept.
const (
	tailMaxKeys = 200_000
	tailKeepOld = 30 * time.Minute
	tailIdle    = 30 * time.Minute
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

// tailsOff is BINTRAIL_SQL_TAILS=off.
func tailsOff() bool { return os.Getenv("BINTRAIL_SQL_TAILS") == "off" }

// tailKeeperFor is server id's keeper over b. A keeper over another bundle
// (the server's index connection was opened again) is replaced; its files are
// removed once no statement can still read them.
func (s *Server) tailKeeperFor(id string, b *bundle) *tailKeeper {
	s.tails.mu.Lock()
	defer s.tails.mu.Unlock()
	if k := s.tails.by[id]; k != nil {
		if src, ok := k.src.(*indexTailSource); ok && src.b == b {
			return k
		}
		k.mu.Lock()
		dir := k.dir
		k.mu.Unlock()
		if dir != "" {
			time.AfterFunc(tailKeepOld, func() { _ = os.RemoveAll(dir) })
		}
	}
	src := newIndexTailSource(s, b)
	if captures, known := s.tailCaptures(id); known {
		src.capturing(captures)
	}
	k := newTailKeeper(src, filepath.Join(os.TempDir(), "bintrail-tails"), tailMaxKeys, tailKeepOld, tailIdle)
	if s.tails.by == nil {
		s.tails.by = map[string]*tailKeeper{}
	}
	s.tails.by[id] = k
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

// statementTails refreshes the tails of tables for a statement and returns
// the tables with theirs set, and the oldest instant the tails are indexed
// through; all says every table had one. A table with none reads its
// published copy. tables is not changed.
func (s *Server) statementTails(ctx context.Context, k *tailKeeper, tables, read []views.BaselineTable) (out []views.BaselineTable, through time.Time, all bool) {
	ctx, cancel := context.WithTimeout(ctx, copyUnchangedTimeout)
	defer cancel()
	got, err := k.refresh(ctx, read)
	if err != nil {
		slog.Debug("Engine Routing: the copy's tails could not be read; the statement reads the published copy", "error", err)
		return tables, time.Time{}, false
	}
	all = len(read) > 0
	for _, t := range read {
		v, ok := got[tailKey(t.Schema, t.Table)]
		if !ok {
			all = false
			continue
		}
		if through.IsZero() || v.Through.Before(through) {
			through = v.Through
		}
	}
	out = make([]views.BaselineTable, len(tables))
	copy(out, tables)
	for i := range out {
		if v, ok := got[tailKey(out[i].Schema, out[i].Table)]; ok && v.Path != "" {
			out[i].Tail = &views.TableTail{Path: v.Path, PK: v.PK}
		}
	}
	return out, through, all
}

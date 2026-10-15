package console

import (
	"context"
	"log/slog"
	"os"
	"strings"
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

// tailsOff is BINTRAIL_SQL_TAILS=off (or false, or 0, in any case).
func tailsOff() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("BINTRAIL_SQL_TAILS"))) {
	case "off", "false", "0", "no":
		return true
	}
	return false
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
	s.tails.mu.Lock()
	old := s.tails.by[id]
	if old != nil {
		if src, ok := old.src.(*indexTailSource); ok && src.b == b {
			s.tails.mu.Unlock()
			return old
		}
	}
	k := newTailKeeper(newIndexTailSource(s, b).capturing(captures), os.TempDir(), tailMaxKeys, tailKeepOld, tailIdle)
	if s.tails.by == nil {
		s.tails.by = map[string]*tailKeeper{}
	}
	s.tails.by[id] = k
	s.tails.mu.Unlock()
	if old != nil {
		// Outside the keepers' lock: the old one may be mid-refresh.
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

// statementTails refreshes the tails of read, the tables a statement reads,
// and returns tables with theirs set and the instant they are indexed
// through. All or nothing: a statement reads every table at one moment, so
// unless every table it reads has a tail at the same instant, none is used
// and it reads the published copy, which is one moment too. A join over a
// table read now and one read as of its snapshot would pair rows that never
// coexisted. ok false: tables, unchanged.
func (s *Server) statementTails(ctx context.Context, k *tailKeeper, tables, read []views.BaselineTable) (out []views.BaselineTable, through time.Time, ok bool) {
	if len(read) == 0 {
		return tables, time.Time{}, false
	}
	ctx, cancel := context.WithTimeout(ctx, copyUnchangedTimeout)
	defer cancel()
	got, err := k.refresh(ctx, read)
	if err != nil {
		slog.Debug("Engine Routing: the copy's tails could not be read; the statement reads the published copy", "error", err)
		return tables, time.Time{}, false
	}
	for i, t := range read {
		v, has := got[tailKey(t.Schema, t.Table)]
		if !has || (i > 0 && !v.Through.Equal(through)) {
			return tables, time.Time{}, false
		}
		through = v.Through
	}
	out = make([]views.BaselineTable, len(tables))
	copy(out, tables)
	for i := range out {
		if v, ok := got[tailKey(out[i].Schema, out[i].Table)]; ok && v.Path != "" {
			out[i].Tail = &views.TableTail{Path: v.Path, PK: v.PK}
		}
	}
	return out, through, true
}

// tailsSayTheInstant is whether a statement whose tables all read tails
// (tailed) is answered as of the tails' instant. Not one over the change
// log, read as it is, not at that moment: its number is the copy's, as
// without tails.
func tailsSayTheInstant(statement string, tailed bool) bool {
	return tailed && !sqlMentionsEvents(statement)
}

package console

import (
	"os"
	"slices"
	"sync/atomic"
	"time"
)

// pgTailsOn says this process gives PostgreSQL servers tails (#2756). Off
// unless the process asks (EnablePostgresTails): see there for who may.
var pgTailsOn atomic.Bool

// EnablePostgresTails turns the tails of PostgreSQL servers on or off for
// this process: with them, a statement on the copy of a PostgreSQL server
// reads the changes captured since its last snapshot (fresh mode), where the
// server's own switch says fresh.
//
// Only a process whose every statement on a PostgreSQL server's copy runs by
// PostgreSQL's rules (sqlsandbox.Session.PostgresRules) may turn it on. A
// PostgreSQL table's key is text, and a tail is laid over the copy by
// comparing key columns: exact where text compares by its bytes, as it does
// under those rules, and wrong where it compares without case, as it does in
// a MySQL session, which would take the row of another key away. The
// PostgreSQL router is such a process; the MySQL router is not.
func EnablePostgresTails(on bool) { pgTailsOn.Store(on) }

// pgTailsFor reports whether server id's tails are a PostgreSQL server's,
// built by tailKeeperForPG: the process asked for them and the server is a
// PostgreSQL one. Any other server, and every server of a process that did
// not ask, goes on through tailKeeperFor as before.
func (s *Server) pgTailsFor(id string, b *bundle) bool {
	if !pgTailsOn.Load() || b == nil {
		return false
	}
	var get func(string) (ServerEntry, bool)
	if s.cm != nil && s.cm.reg != nil {
		get = s.cm.reg.Get
	}
	return sourceIsPostgres(id, b.pgSource, get)
}

// tailKeeperForPG is tailKeeperFor for a PostgreSQL server: the same keeper,
// over a pgTailSource. Step for step what tailKeeperFor does, with the one
// type changed: which tables capture records, the sweep of directories
// earlier processes left, the server's switch asked again under the keepers'
// lock, a keeper kept for a new bundle over the same index connection, and
// the old one closed outside the lock.
func (s *Server) tailKeeperForPG(id string, b *bundle) *tailKeeper {
	captures, capturesKey, known := s.tailCapturesKeyed(id)
	if !known {
		return nil
	}
	tailSweep.Do(func() { sweepTailDirs(os.TempDir(), time.Now().Add(-tailKeepOld)) })
	s.tails.mu.Lock()
	if !s.tailsOnFor(id) {
		s.tails.mu.Unlock()
		return nil
	}
	old := s.tails.by[id]
	if old != nil {
		if src, ok := old.src.(*pgTailSource); ok && src.b.db == b.db && src.capturesKey == capturesKey {
			s.tails.mu.Unlock()
			return old
		}
	}
	src := newPGTailSource(s, b).capturing(captures)
	src.capturesKey = capturesKey
	k := newTailKeeper(src, os.TempDir(), tailBudgetFromEnv(), tailKeepOld, tailIdle)
	k.pub.auto = true
	if s.tails.by == nil {
		s.tails.by = map[string]*tailKeeper{}
	}
	s.tails.by[id] = k
	s.tails.mu.Unlock()
	if old != nil {
		old.close()
		old.removeDirAfter(tailKeepOld)
	}
	return k
}

// PostgresServerIDs is the registry ids of the PostgreSQL servers, sorted:
// what the PostgreSQL router's status says the mode of, each one whether or
// not a statement reached it yet, since that it is fresh or fast is what an
// operator who switched it looks for.
func (s *Server) PostgresServerIDs() []string {
	if s == nil || s.cm == nil || s.cm.reg == nil {
		return nil
	}
	var out []string
	for _, e := range s.cm.reg.List() {
		if s.SourceIsPostgres(e.ID) {
			out = append(out, e.ID)
		}
	}
	slices.Sort(out)
	return out
}

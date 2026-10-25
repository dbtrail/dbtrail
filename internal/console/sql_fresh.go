package console

import (
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"
)

// Fresh mode per server (#2546). A server's statements on the SQL port read
// tails when BINTRAIL_SQL_TAILS=on (every server, the switches deciding
// nothing) or when its registry entry says so (ServerEntry.SQLFresh). The
// entry is applied by applySQLFresh, which the process serving the port runs
// every time it reads the registry again, so a switch saved elsewhere takes
// effect within one follow interval, never for a statement before the pool
// has room for it.

// Where a server's mode comes from (SQLModeOf).
const (
	SQLModeFromEnvironment = "environment"
	SQLModeFromSaved       = "saved"
	SQLModeFromDefault     = "default"
)

// slotSetter is what applySQLFresh resizes: the SQL runner.
type slotSetter interface{ SetMaxInFlight(n int) error }

// sqlFreshState is which servers read tails because of their switch, and the
// two slot counts the router planned. The tails' memory is one budget for
// the process, set aside while at least one server is fresh: the pool runs
// freshWorkers then and fastWorkers otherwise. Zero counts, a nil pool or
// equal counts: the pool is never moved (the capture process, or a router
// whose cores or --concurrency decide the count either way).
type sqlFreshState struct {
	// apply serializes applySQLFresh; mu guards on, which every statement
	// reads.
	apply sync.Mutex
	mu    sync.Mutex
	on    map[string]bool

	pool                      slotSetter
	fastWorkers, freshWorkers int
	lowered                   bool
}

func (f *sqlFreshState) resizes() bool {
	return f.pool != nil && f.fastWorkers > 0 && f.freshWorkers > 0 && f.fastWorkers != f.freshWorkers
}

func (f *sqlFreshState) isOn(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.on[id]
}

// ids are the servers in fresh mode because of their switch, sorted.
func (f *sqlFreshState) ids() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.on))
	for id := range f.on {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// FreshServerNames are the names of the servers in fresh mode because of
// their switch, sorted, for the router's startup lines.
func (s *Server) FreshServerNames() []string {
	var out []string
	for _, id := range s.fresh.ids() {
		out = append(out, s.serverName(id))
	}
	slices.Sort(out)
	return out
}

// tailsOnFor reports whether server id's statements on the port read tails.
func (s *Server) tailsOnFor(id string) bool {
	if !tailsOff() {
		return true
	}
	return s.fresh.isOn(id)
}

// SQLModeOf is server id's mode as its statements run it now, "fresh" or
// "fast", and where it comes from (SQLModeFrom*).
func (s *Server) SQLModeOf(id string) (mode, from string) {
	switch {
	case !tailsOff():
		return "fresh", SQLModeFromEnvironment
	case s.fresh.isOn(id):
		return "fresh", SQLModeFromSaved
	}
	return "fast", SQLModeFromDefault
}

// applySQLFresh brings the servers that read tails to what the registry
// says. The order is what keeps statements and tails inside the process's
// memory: the pool is lowered BEFORE the first server reads tails, and
// raised only AFTER the last one stopped and its keepers' builds ended (up to
// tailSettleBound). A pool that cannot be lowered (the
// runner is closing) leaves the servers fast. A server turned fast, or
// deleted, has its keeper dropped, so nothing follows its index any more.
// With BINTRAIL_SQL_TAILS=on there is nothing to do: every server reads
// tails and the pool was planned for it at startup.
func (s *Server) applySQLFresh() {
	f := &s.fresh
	f.apply.Lock()
	defer f.apply.Unlock()
	if !tailsOff() {
		return
	}
	want := map[string]bool{}
	if s.cm != nil && s.cm.reg != nil {
		for _, e := range s.cm.reg.List() {
			if e.SQLFresh {
				want[e.ID] = true
			}
		}
	}
	f.mu.Lock()
	var on, off []string
	for id := range want {
		if !f.on[id] {
			on = append(on, id)
		}
	}
	for id := range f.on {
		if !want[id] {
			off = append(off, id)
		}
	}
	none := len(f.on) == 0
	f.mu.Unlock()

	if len(on) > 0 && none && f.resizes() && !f.lowered {
		if err := f.pool.SetMaxInFlight(f.freshWorkers); err != nil {
			slog.Warn("Engine Routing: fresh mode was saved for a server, but the statements that run at once could not be lowered to make room for its tails, so it stays in fast mode", "error", err)
			on = nil
		} else {
			f.lowered = true
			slog.Info("Engine Routing: a server turned to fresh mode; fewer statements run at once, to leave memory for the copy's tails",
				"statements_at_once", f.freshWorkers, "was", f.fastWorkers)
		}
	}
	f.mu.Lock()
	if f.on == nil {
		f.on = map[string]bool{}
	}
	for _, id := range on {
		f.on[id] = true
	}
	for _, id := range off {
		delete(f.on, id)
	}
	left := len(f.on)
	f.mu.Unlock()
	for _, id := range on {
		slog.Info("Engine Routing: fresh mode on: this server's statements read its newest captured changes over its snapshot", "server", s.serverName(id))
	}
	var dropped []*tailKeeper
	for _, id := range off {
		if k := s.dropTailKeeper(id); k != nil {
			dropped = append(dropped, k)
		}
		slog.Info("Engine Routing: fresh mode off: this server's statements read its snapshot alone", "server", s.serverName(id))
	}
	if left == 0 && f.lowered {
		// The dropped keepers' builds were told to end; what they read
		// still holds memory until they do. Waited for, within a bound: a
		// read the index does not give back is not a reason to keep the
		// pool small for minutes.
		settleWithin(dropped, tailSettleBound)
		if err := f.pool.SetMaxInFlight(f.fastWorkers); err != nil {
			slog.Warn("Engine Routing: no server is in fresh mode any more, but the statements that run at once could not be raised back", "error", err)
			return
		}
		f.lowered = false
		slog.Info("Engine Routing: no server is in fresh mode; the statements that run at once are back to their count",
			"statements_at_once", f.fastWorkers)
	}
}

// serverName is the registry name of id for a log line, id itself when the
// entry is gone.
func (s *Server) serverName(id string) string {
	if s.cm != nil && s.cm.reg != nil {
		if e, ok := s.cm.reg.Get(id); ok {
			return e.Name
		}
	}
	return id
}

// dropTailKeeper forgets server id's keeper and returns it (nil when it had
// none): its builds end now, and its files are removed once no statement can
// still read them.
func (s *Server) dropTailKeeper(id string) *tailKeeper {
	s.tails.mu.Lock()
	k := s.tails.by[id]
	delete(s.tails.by, id)
	s.tails.mu.Unlock()
	if k != nil {
		retireTailKeeper(k)
	}
	return k
}

// tailSettleBound is how long applySQLFresh waits for the builds of the
// keepers it dropped before it raises the pool.
const tailSettleBound = 5 * time.Second

// settleWithin waits for every build of ks to end, up to d, and says so when
// they did not.
func settleWithin(ks []*tailKeeper, d time.Duration) {
	if len(ks) == 0 {
		return
	}
	done := make(chan struct{})
	go func() {
		for _, k := range ks {
			k.settle()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		slog.Warn("Engine Routing: a tail build of a server turned to fast mode had not ended when more statements were let run at once; the memory it holds is given back when its read of the index ends", "waited", d)
	}
}

// retireTailKeeper closes k and removes its directory after tailKeepOld.
func retireTailKeeper(k *tailKeeper) {
	k.close()
	k.mu.Lock()
	dir := k.dir
	k.mu.Unlock()
	if dir != "" {
		time.AfterFunc(tailKeepOld, func() { _ = os.RemoveAll(dir) })
	}
}

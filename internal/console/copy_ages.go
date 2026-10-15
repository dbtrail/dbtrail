package console

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"
)

// copyAges is, per server, how far behind capture the data of the copy's
// answers was (#2369): the last answer's instant and age, and the worst age
// since this process started. It is what the router's status document says
// under each server, for the panel; a statement pays nothing for it beyond a
// map write.
//
// An answer whose tables all read tails knows its instant at once. One that
// read published snapshots knows it only from the index (the event the
// snapshot was cut at), which a statement never asks: it waits as pending
// until resolve, which the status document and a 30-second loop call. A
// snapshot's instant never changes, so each one is asked once.
type copyAges struct {
	mu sync.Mutex
	by map[string]*serverCopyAges
}

type serverCopyAges struct {
	// last is the newest answer; lastThrough its instant, zero when unknown;
	// lastPending says it waits on resolve (lastKey).
	last        copyAnswer
	lastThrough time.Time
	lastPending bool
	lastKey     string
	// worst is the largest age of an answer whose instant is known.
	worst      time.Duration
	worstKnown bool
	// pending are the answers whose snapshots' instant is not known yet, by
	// the snapshots read (copyAgesKey).
	pending map[string]*pendingCopyAge
	// cut is the instant of each set of snapshots already resolved. Bounded:
	// a router reads few snapshots at once, and one dropped is asked again.
	cut map[string]time.Time
}

// pendingCopyAge is what an unresolved set of snapshots must give the worst
// age: the latest answer over it (the oldest data relative to its run), and
// the worst age its tails gave, for an answer that read both.
type pendingCopyAge struct {
	files     map[string][]string
	latest    time.Time
	tailWorst time.Duration
}

// copyAgesCutKeep is how many resolved sets of snapshots a server keeps.
const copyAgesCutKeep = 16

// copyAgesKey names the snapshot directories files reads, in order.
func copyAgesKey(files map[string][]string) string {
	dirs := make([]string, 0, len(files))
	for d := range files {
		dirs = append(dirs, d)
	}
	slices.Sort(dirs)
	return strings.Join(dirs, "\x00")
}

// copyAgeOf is ranAt minus through, never below zero (the source's clock may
// run ahead of the router's).
func copyAgeOf(ranAt, through time.Time) time.Duration {
	return max(ranAt.Sub(through), 0)
}

func (c *copyAges) server(id string) *serverCopyAges {
	if c.by == nil {
		c.by = map[string]*serverCopyAges{}
	}
	s := c.by[id]
	if s == nil {
		s = &serverCopyAges{pending: map[string]*pendingCopyAge{}, cut: map[string]time.Time{}}
		c.by[id] = s
	}
	return s
}

func (s *serverCopyAges) worse(age time.Duration) {
	if !s.worstKnown || age > s.worst {
		s.worst, s.worstKnown = age, true
	}
}

// note records a, an answer the copy gave for server id.
func (c *copyAges) note(id string, a copyAnswer) {
	if a.side != "copy" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.server(id)
	s.last, s.lastThrough, s.lastPending, s.lastKey = a, time.Time{}, false, ""
	switch {
	case a.allTailed:
		s.lastThrough = a.tailThrough
		s.worse(copyAgeOf(a.ranAt, a.tailThrough))
	case !a.ok || len(a.files) == 0:
		// Which snapshots it read is not known: unknown, and not a worst.
	default:
		key := copyAgesKey(a.files)
		if cut, ok := s.cut[key]; ok {
			s.lastThrough = olderOf(cut, a.tailThrough)
			s.worse(copyAgeOf(a.ranAt, s.lastThrough))
			return
		}
		s.lastPending, s.lastKey = true, key
		p := s.pending[key]
		if p == nil {
			if len(s.pending) >= copyAgesCutKeep {
				// Nothing resolves them (no status read, no loop): this
				// answer stays unknown rather than the map growing.
				s.lastPending, s.lastKey = false, ""
				return
			}
			p = &pendingCopyAge{files: a.files}
			s.pending[key] = p
		}
		if a.ranAt.After(p.latest) {
			p.latest = a.ranAt
		}
		if !a.tailThrough.IsZero() {
			p.tailWorst = max(p.tailWorst, copyAgeOf(a.ranAt, a.tailThrough))
		}
	}
}

// olderOf is the older of a snapshot's instant and a tail's, a zero tail
// being none: an answer over both is as current as the older.
func olderOf(cut, tail time.Time) time.Time {
	if !tail.IsZero() && tail.Before(cut) {
		return tail
	}
	return cut
}

// pendingFor is what resolve must ask the index for, by server.
func (c *copyAges) pendingFor() map[string]map[string]map[string][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]map[string]map[string][]string{}
	for id, s := range c.by {
		for key, p := range s.pending {
			if out[id] == nil {
				out[id] = map[string]map[string][]string{}
			}
			out[id][key] = p.files
		}
	}
	return out
}

// settle records what the index said of server id's snapshots key: their
// instant, or why it is not known (then the answers over them count as
// unknown, and toward no worst).
func (c *copyAges) settle(id, key string, cut time.Time, why string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.by[id]
	if s == nil {
		return
	}
	p := s.pending[key]
	if p == nil {
		return
	}
	delete(s.pending, key)
	if why == "" {
		s.worse(max(copyAgeOf(p.latest, cut), p.tailWorst))
		if len(s.cut) >= copyAgesCutKeep {
			clear(s.cut)
		}
		s.cut[key] = cut
	}
	if s.lastPending && s.lastKey == key {
		s.lastPending = false
		if why == "" {
			s.lastThrough = olderOf(cut, s.last.tailThrough)
		}
	}
}

// copyAgesNow is one server's numbers for the status document: zero values
// and false when no answer is known.
type copyAgesNow struct {
	asOf          time.Time
	age, worst    time.Duration
	known, anyMax bool
}

func (c *copyAges) now(id string) copyAgesNow {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.by[id]
	if s == nil {
		return copyAgesNow{}
	}
	out := copyAgesNow{worst: s.worst, anyMax: s.worstKnown}
	if !s.lastPending && !s.lastThrough.IsZero() {
		out.asOf, out.age, out.known = s.lastThrough, copyAgeOf(s.last.ranAt, s.lastThrough), true
	}
	return out
}

// ResolveCopyAges asks the index the instants of the snapshots the copy's
// pending answers read (copyAges). Called by the router's status document
// and its 30-second loop; at most one index read per set of snapshots.
func (s *Server) ResolveCopyAges(ctx context.Context) {
	for id, sets := range s.copyAges.pendingFor() {
		b, err := s.cm.Resolve(ctx, id)
		if err != nil || b == nil || b.db == nil {
			why := "the server's index could not be reached"
			if err != nil {
				slog.Debug("router status: a server could not be resolved, so its copy answers' age is not known", "server", id, "error", err)
			}
			for key := range sets {
				s.copyAges.settle(id, key, time.Time{}, why)
			}
			continue
		}
		for key, files := range sets {
			rctx, cancel := context.WithTimeout(ctx, copyUnchangedTimeout)
			cut, why := s.copyIndexedThrough(rctx, b.db, files)
			cancel()
			if why != "" {
				slog.Debug("router status: how far a copy answer's data was indexed is not known", "server", id, "reason", why)
			}
			s.copyAges.settle(id, key, cut, why)
		}
	}
}

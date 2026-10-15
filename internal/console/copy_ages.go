package console

import (
	"context"
	"path/filepath"
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
	// resolving keeps one ResolveCopyAges at a time: the loop and the
	// status document would otherwise ask the index the same thing twice.
	resolving sync.Mutex
	// faults logs why an answer's lag is not known, at Warn now and then.
	faults copyFaultLog
}

type serverCopyAges struct {
	// last is the newest answer; lastThrough its instant, zero when unknown;
	// lastPending says it waits on resolve (lastKey).
	last        copyAnswer
	lastThrough time.Time
	lastPending bool
	lastKey     string
	// worst is the largest age of an answer whose instant is known;
	// unknown counts the answers whose instant is not, which the worst
	// cannot account for (it is then "at least"), and why the last of them.
	worst      time.Duration
	worstKnown bool
	unknown    uint64
	// lastWhy is why the last answer's lag is unknown, "" when it is known
	// or still waits.
	lastWhy string
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
	// n is how many answers wait on it.
	n uint64
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

// The reasons the status document gives for an unknown lag. A fixed few on
// purpose: the document is served to whoever reaches it, with no password,
// so it never carries an error's text (a driver error can name an account
// or a host) or a path. The detail goes to the log.
const (
	copyAgeWhyNoFiles   = "which snapshot the answer read could not be told"
	copyAgeWhyBacklog   = "too many snapshots wait to be placed in the index"
	copyAgeWhyNoIndex   = "the server's index could not be reached"
	copyAgeWhyNotPlaced = "the snapshot it read could not be placed in the index"
	copyAgeWhyMoving    = "it read the snapshot through the current link, which moves"
)

// unknownFor counts an answer whose lag cannot be told: public is one of
// the copyAgeWhy reasons, detail what the log says.
func (c *copyAges) unknownFor(id string, s *serverCopyAges, n uint64, public, detail string) {
	s.unknown += n
	c.faults.warn(id, "router: how far behind a copy answer was is not known", "server", id, "reason", public, "detail", detail)
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
	s.last, s.lastThrough, s.lastPending, s.lastKey, s.lastWhy = a, time.Time{}, false, "", ""
	switch {
	case a.allTailed:
		s.lastThrough = a.tailThrough
		s.worse(copyAgeOf(a.ranAt, a.tailThrough))
	case !a.ok || len(a.files) == 0:
		// Which snapshots it read is not known.
		s.lastWhy = copyAgeWhyNoFiles
		c.unknownFor(id, s, 1, copyAgeWhyNoFiles, "")
	default:
		key := copyAgesKey(a.files)
		if !copyAgesCacheable(key) {
			// Which snapshot the link named when the statement ran is not
			// known by the time the index is asked: never a number.
			s.lastWhy = copyAgeWhyMoving
			c.unknownFor(id, s, 1, copyAgeWhyMoving, "")
			return
		}
		if cut, ok := s.cut[key]; ok {
			s.lastThrough = olderOf(cut, a.tailThrough)
			s.worse(copyAgeOf(a.ranAt, s.lastThrough))
			return
		}
		s.lastPending, s.lastKey = true, key
		p := s.pending[key]
		if p == nil {
			if len(s.pending) >= copyAgesCutKeep {
				// The index is not keeping up: this answer stays unknown
				// rather than the map growing.
				s.lastPending, s.lastKey, s.lastWhy = false, "", copyAgeWhyBacklog
				c.unknownFor(id, s, 1, copyAgeWhyBacklog, "")
				return
			}
			p = &pendingCopyAge{files: a.files}
			s.pending[key] = p
		}
		p.n++
		if a.ranAt.After(p.latest) {
			p.latest = a.ranAt
		}
		if !a.tailThrough.IsZero() {
			p.tailWorst = max(p.tailWorst, copyAgeOf(a.ranAt, a.tailThrough))
		}
	}
}

// copyAgesCurrentLink is baseline.CurrentLinkName (a test pins them equal).
const copyAgesCurrentLink = "current"

// copyAgesCacheable says a set of snapshot directories always holds the
// same data: not one reached through the moving "current" link.
func copyAgesCacheable(key string) bool {
	for _, d := range strings.Split(key, "\x00") {
		if filepath.Base(filepath.Clean(d)) == copyAgesCurrentLink {
			return false
		}
	}
	return true
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
	public := copyAgeWhyNotPlaced
	if strings.HasPrefix(why, copyAgeWhyNoIndex) {
		public = copyAgeWhyNoIndex
	}
	if why != "" {
		c.unknownFor(id, s, p.n, public, why)
	}
	if s.lastPending && s.lastKey == key {
		s.lastPending = false
		if why == "" {
			s.lastThrough = olderOf(cut, s.last.tailThrough)
		} else {
			s.lastWhy = public
		}
	}
}

// copyAgesNow is one server's numbers for the status document: zero values
// and false when no answer is known.
type copyAgesNow struct {
	asOf          time.Time
	age, worst    time.Duration
	known, anyMax bool
	// unknown is how many answers' lag could not be told; pending says the
	// last answer's still waits on the index, and unknownWhy is why the
	// last answer's is not known (empty when it waits).
	unknown    uint64
	pending    bool
	unknownWhy string
}

func (c *copyAges) now(id string) copyAgesNow {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.by[id]
	if s == nil {
		return copyAgesNow{}
	}
	out := copyAgesNow{worst: s.worst, anyMax: s.worstKnown, unknown: s.unknown, pending: s.lastPending, unknownWhy: s.lastWhy}
	if !s.lastPending && !s.lastThrough.IsZero() {
		out.asOf, out.age, out.known = s.lastThrough, copyAgeOf(s.last.ranAt, s.lastThrough), true
	}
	return out
}

// ResolveCopyAges asks the index the instants of the snapshots the copy's
// pending answers read (copyAges), within ctx. Called by the router's
// 30-second loop and, briefly, its status document; one at a time (a call
// while another runs returns at once), at most one index read per set of
// snapshots.
func (s *Server) ResolveCopyAges(ctx context.Context) {
	if !s.copyAges.resolving.TryLock() {
		return
	}
	defer s.copyAges.resolving.Unlock()
	for id, sets := range s.copyAges.pendingFor() {
		if ctx.Err() != nil {
			return // left pending, for the next call
		}
		b, err := s.cm.Resolve(ctx, id)
		if err != nil && ctx.Err() != nil {
			return
		}
		if err != nil || b == nil || b.db == nil {
			why := "the server's index could not be reached"
			if err != nil {
				why += ": " + err.Error()
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
			if why != "" && ctx.Err() != nil {
				return // out of the caller's time, not the index's answer
			}
			s.copyAges.settle(id, key, cut, why)
		}
	}
}

package console

import (
	"context"
	"math"
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
	last copyAnswer
	// lastTailedRan is when the newest answer that read every table it
	// asked for with its tail ran: the end of fresh mode's warm-up
	// (Server.SQLModeWarming).
	lastTailedRan time.Time
	lastThrough   time.Time
	lastPending   bool
	lastKey       string
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
	// shapes is the same record per statement of the per-statement table
	// (#2360), for the panel's detail of one statement. Bounded like the
	// table: a statement first answered past StatementTableLimit has none.
	shapes map[string]*shapeAges
}

// shapeAges is how far behind capture the data of the copy's answers to one
// statement was. Every age is one answer's own, so the least is a real
// answer's and never fresher than any was.
type shapeAges struct {
	// last is the newest answer: its run, its instant when known, whether
	// it waits on the index (lastKey), or why it is unknown.
	lastRan, lastThrough time.Time
	lastTail             time.Time
	lastPending          bool
	lastKey, lastWhy     string
	least, most          time.Duration
	// known counts the answers whose age is known; unknown those whose age
	// cannot be told; tailed those that read every table up to date
	// (allTailed); snapshot those that read some table as of its snapshot.
	known, unknown, tailed, snapshot uint64
}

func (a *shapeAges) record(age time.Duration) {
	if a.known == 0 || age < a.least {
		a.least = age
	}
	if a.known == 0 || age > a.most {
		a.most = age
	}
	a.known++
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
	// shapes is, per statement, the answers that wait on it, each with its
	// own run and tail, so each one's age is told when the index answers.
	// At most freshPendingPerShape a statement: past it they are unknown.
	shapes map[string][]pendingShapeAnswer
}

type pendingShapeAnswer struct{ ranAt, tail time.Time }

// freshPendingPerShape is how many of one statement's answers wait on one
// set of snapshots with their own times; the index is asked every 30 seconds,
// so more than this is a statement run several times a second.
const freshPendingPerShape = 64

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
		s = &serverCopyAges{pending: map[string]*pendingCopyAge{}, cut: map[string]time.Time{}, shapes: map[string]*shapeAges{}}
		c.by[id] = s
	}
	return s
}

// shape is s's record of one statement, nil when it has none and no room.
func (s *serverCopyAges) shape(shape string) *shapeAges {
	if shape == "" {
		return nil
	}
	a := s.shapes[shape]
	if a == nil {
		if len(s.shapes) >= StatementTableLimit {
			return nil
		}
		a = &shapeAges{}
		s.shapes[shape] = a
	}
	return a
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

// forget drops server id's record: it left the registry.
func (c *copyAges) forget(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.by, id)
}

// lastTailedRan is when the copy's newest answer for server id that read
// every table it asked for with its tail ran; zero when none did.
func (c *copyAges) lastTailedRan(id string) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.by[id]; s != nil {
		return s.lastTailedRan
	}
	return time.Time{}
}

// answered are the servers the copy answered for, sorted.
func (c *copyAges) answered() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.by))
	for id, s := range c.by {
		if s.last.side == "copy" {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// note records a, an answer the copy gave for server id, for the server and
// for its statement (a.shape) on one path, so the panel's lag and the
// detail's never disagree.
func (c *copyAges) note(id string, a copyAnswer) {
	if a.side != "copy" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.server(id)
	s.last, s.lastThrough, s.lastPending, s.lastKey, s.lastWhy = a, time.Time{}, false, "", ""
	sh := s.shape(a.shape)
	if sh != nil {
		sh.lastRan, sh.lastTail, sh.lastThrough, sh.lastPending, sh.lastKey, sh.lastWhy = a.ranAt, a.tailThrough, time.Time{}, false, "", ""
	}
	unknown := func(why string) {
		s.lastWhy = why
		c.unknownFor(id, s, 1, why, "")
		if sh != nil {
			sh.unknown++
			sh.lastWhy = why
		}
	}
	known := func(through time.Time) {
		s.lastThrough = through
		age := copyAgeOf(a.ranAt, through)
		s.worse(age)
		if sh != nil {
			sh.lastThrough = through
			sh.record(age)
		}
	}
	switch {
	case a.allTailed:
		s.lastTailedRan = a.ranAt
		if sh != nil {
			sh.tailed++
		}
		known(a.tailThrough)
	case !a.ok || len(a.files) == 0:
		// Which snapshots it read is not known.
		unknown(copyAgeWhyNoFiles)
	default:
		// snapshot counts an answer only once the snapshot it read is one
		// that can be placed: an answer through the moving link, or past
		// the backlog, is unknown and nothing more.
		key := copyAgesKey(a.files)
		if !copyAgesCacheable(key) {
			// Which snapshot the link named when the statement ran is not
			// known by the time the index is asked: never a number.
			unknown(copyAgeWhyMoving)
			return
		}
		if cut, ok := s.cut[key]; ok {
			if sh != nil {
				sh.snapshot++
			}
			known(olderOf(cut, a.tailThrough))
			return
		}
		p := s.pending[key]
		if p == nil {
			if len(s.pending) >= copyAgesCutKeep {
				// The index is not keeping up: this answer stays unknown
				// rather than the map growing.
				unknown(copyAgeWhyBacklog)
				return
			}
			p = &pendingCopyAge{files: a.files}
			s.pending[key] = p
		}
		s.lastPending, s.lastKey = true, key
		p.n++
		if a.ranAt.After(p.latest) {
			p.latest = a.ranAt
		}
		if !a.tailThrough.IsZero() {
			p.tailWorst = max(p.tailWorst, copyAgeOf(a.ranAt, a.tailThrough))
		}
		if sh != nil {
			sh.snapshot++
			if p.shapes == nil {
				p.shapes = map[string][]pendingShapeAnswer{}
			}
			if len(p.shapes[a.shape]) < freshPendingPerShape {
				p.shapes[a.shape] = append(p.shapes[a.shape], pendingShapeAnswer{a.ranAt, a.tailThrough})
				sh.lastPending, sh.lastKey = true, key
			} else {
				// Past the cap its own times are not kept: unknown, never
				// a guess.
				sh.unknown++
				sh.lastWhy = copyAgeWhyBacklog
			}
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
	for shape, answers := range p.shapes {
		sh := s.shapes[shape]
		if sh == nil {
			continue
		}
		for _, ans := range answers {
			if why == "" {
				sh.record(copyAgeOf(ans.ranAt, olderOf(cut, ans.tail)))
			} else {
				sh.unknown++
			}
		}
		if sh.lastPending && sh.lastKey == key {
			sh.lastPending = false
			if why == "" {
				sh.lastThrough = olderOf(cut, sh.lastTail)
			} else {
				sh.lastWhy = public
			}
		}
	}
}

// shapeNow is one statement's lag for the per-statement table, nil when the
// copy answered it nothing this process recorded.
func (c *copyAges) shapeNow(id, shape string) *RouterStatementFresh {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.by[id]
	if s == nil {
		return nil
	}
	a := s.shapes[shape]
	if a == nil {
		return nil
	}
	// Rounded up: a lag is never said smaller than it was.
	secs := func(d time.Duration) *int64 { v := int64(math.Ceil(d.Seconds())); return &v }
	out := &RouterStatementFresh{Known: a.known, Unknown: a.unknown, Tailed: a.tailed, Snapshot: a.snapshot, Pending: a.lastPending}
	if !a.lastPending && !a.lastThrough.IsZero() {
		out.LastAsOf, out.LastAgeSeconds = a.lastThrough.UTC().Format(time.RFC3339), secs(copyAgeOf(a.lastRan, a.lastThrough))
	}
	if !a.lastPending && a.lastThrough.IsZero() {
		out.UnknownReason = a.lastWhy
	}
	if a.known > 0 {
		out.MinAgeSeconds, out.MaxAgeSeconds = secs(a.least), secs(a.most)
	}
	return out
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

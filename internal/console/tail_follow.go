package console

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// A statement does not bring the tails up to date itself: it reads the
// version the keeper's follower last published. Bringing them up to date
// reads the index (its newest event, the events after the cursor, which
// tables changed definition) and writes every changed tail again, whole; on
// a table under a steady write load that is every statement, and on the
// statement's clock it cost more than laying the tail over the copy did. The
// follower does it in the background, about once a second while statements
// ask for tails, so an answer can be that much older than the index.
const (
	// tailFollowEvery is the pause between the end of one update and the
	// start of the next.
	tailFollowEvery = time.Second
	// tailFollowIdle is how long the follower goes on after the last
	// statement that asked for a table's tail: a table no statement asked for
	// in that long is left out, and with none left the follower stops.
	//
	// Minutes, not seconds: a client's statements can sit on MySQL for
	// minutes (a report the copy refuses, a long forward) and ask for no
	// tail all that while. Had the follower stopped, the client's next
	// statement on the copy would have to bring minutes of changes up to date
	// within its own small budget (copyUnchangedTimeout), which under a
	// steady write load it cannot, and would read the published copy, as
	// old as the last snapshot. Measured on a 100-warehouse CH-benCHmark at
	// 500 tx/s: with one minute, answers were 200 to 330 s behind the source.
	// An update with no new events is one read of the index's newest event;
	// the keeper forgets an unused tail on its own, longer clock (tailIdle).
	tailFollowIdle = 15 * time.Minute
	// tailFollowTimeout bounds one update; one that runs out keeps what it
	// folded (the cursor moves with every page) and publishes nothing.
	tailFollowTimeout = 30 * time.Second
)

// tailPublish is what the follower last published, and what statements asked
// for since. Its own lock, never held across an update: a statement takes it
// while an update holds the keeper's.
type tailPublish struct {
	mu sync.Mutex
	// asked are the tables statements read, by tailKey, as the last one to
	// ask saw them (its cut), and when.
	asked map[string]tailAsk
	// views are the last update's, by tailKey; nil when there is none to
	// read (no update yet, the last one failed, the follower stopped).
	views map[string]tailView
	// cuts are the cuts the last update brought the tails up to date over,
	// by tailKey, views or not: a view is read only over its own cut, and a
	// table the update was asked for that has no view (refused, building)
	// does not wake the follower again.
	cuts map[string]string
	// running while the follower's goroutine is.
	running bool
	// gen numbers the updates as they start, pubGen is the one published;
	// updating, since started, while the last one to start runs, and stuck
	// once that was said to run past its deadline.
	gen, pubGen     uint64
	updating, stuck bool
	started         time.Time
	// panics in a row ended the follower; none starts before restartAt.
	panics    int
	restartAt time.Time
	// auto starts the follower on demand. Off in tests, which step it
	// (followOnce).
	auto                  bool
	every, idle, deadline time.Duration
	// refused are, by tailKey, the tables the last update said have no tail
	// a wait would bring (over its cut, like views).
	refused map[string]string
	// wake cuts the follower's pause short: one pending wake, however many
	// statements asked. pubCh is closed at every publish and replaced, so a
	// statement can wait for the next one.
	wake  chan struct{}
	pubCh chan struct{}
	// okAt is when an update last published without an error: views kept
	// over failures are kept only while their files are (tailKeepOld).
	okAt time.Time
}

type tailAsk struct {
	table views.BaselineTable
	at    time.Time
}

func newTailPublish() tailPublish {
	return tailPublish{every: tailFollowEvery, idle: tailFollowIdle, deadline: tailFollowTimeout,
		wake: make(chan struct{}, 1), pubCh: make(chan struct{})}
}

// wakeFollower ends the follower's pause now, so its next update starts as
// soon as the running one (if any) ends. Many calls before it wakes are one
// wake.
func (k *tailKeeper) wakeFollower() {
	select {
	case k.pub.wake <- struct{}{}:
	default:
	}
}

// nextPublish is closed at the next publish (or when the keeper closes).
func (k *tailKeeper) nextPublish() <-chan struct{} {
	k.pub.mu.Lock()
	defer k.pub.mu.Unlock()
	return k.pub.pubCh
}

// refusedOf is why the last update said read's tables have no tail a wait
// would bring, by tailKey, over the cuts read reads.
func (k *tailKeeper) refusedOf(read []views.BaselineTable) map[string]string {
	p := &k.pub
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[string]string{}
	for _, t := range read {
		key := tailKey(t.Schema, t.Table)
		if why, ok := p.refused[key]; ok && p.cuts[key] == tailCutOf(t) {
			out[key] = why
		}
	}
	return out
}

// markPublishedLocked wakes the statements waiting for a publish (p.mu held).
func (p *tailPublish) markPublishedLocked() {
	close(p.pubCh)
	p.pubCh = make(chan struct{})
}

// latest is what a statement over read reads of the tails: the follower's
// last version of each, when it was brought up to date over the cut the
// statement reads. A table the last update was not asked for, or asked for
// over another cut (a refresh published since), has none: the statement then
// brings them up to date itself, within d, as every statement did before the
// follower (a statement after a minute with none, the first after a newer
// snapshot), so a client that asks less often than the follower lives still
// reads tails; unless an update is running, which publishes them for the
// statements after. Every other statement reads nothing of the index and never
// waits. The follower is started if it is not running.
func (k *tailKeeper) latest(ctx context.Context, d time.Duration, read []views.BaselineTable) map[string]tailView {
	out, wake := k.published(read)
	if !wake {
		return out
	}
	p := &k.pub
	p.mu.Lock()
	running := p.updating
	p.mu.Unlock()
	if running {
		// Not behind it: the update that runs publishes them, and the
		// statement does not wait out a read the follower may take its whole
		// deadline over.
		return out
	}
	k.followOnce(ctx, d)
	out, _ = k.published(read)
	return out
}

// published is latest without the update: the views over read's cuts, and
// whether a table of read was not in the last update over its cut.
func (k *tailKeeper) published(read []views.BaselineTable) (map[string]tailView, bool) {
	p := &k.pub
	p.mu.Lock()
	defer p.mu.Unlock()
	if k.life.Err() != nil {
		return nil, false
	}
	now := k.now()
	if p.asked == nil {
		p.asked = map[string]tailAsk{}
	}
	out := map[string]tailView{}
	wake := false
	for _, t := range read {
		key, cut := tailKey(t.Schema, t.Table), tailCutOf(t)
		p.asked[key] = tailAsk{table: t, at: now}
		if c, ok := p.cuts[key]; !ok || c != cut {
			wake = true
			continue
		}
		if v, ok := p.views[key]; ok {
			out[key] = v
		}
	}
	switch {
	case p.updating && !p.stuck && now.Sub(p.started) > 2*p.deadline:
		// Its read does not end on its deadline (a write takes no context):
		// statements keep the last version, older at every one, and nothing
		// else would say so.
		p.stuck = true
		slog.Warn("Engine Routing: bringing the copy's tails up to date runs far past its deadline; statements read the last version until it ends",
			"running_for", now.Sub(p.started))
	case p.auto && !p.running && !now.Before(p.restartAt):
		p.running = true
		go k.runFollower()
	}
	return out, wake
}

// followOnce brings the tails statements asked for in the last idle up to
// date, within d, and publishes them, and says whether there were any. With
// none it publishes nothing and, in the same hold of the lock, marks the
// follower stopped, so a statement that asks after starts it again. Of two
// updates that overlap (a statement's and the follower's), the one that
// started last is published.
func (k *tailKeeper) followOnce(ctx context.Context, d time.Duration) bool {
	p := &k.pub
	tables, cuts, gen := k.toFollow()
	if tables == nil {
		return false
	}
	got, refused, dropped, err := k.refreshWithinRefused(ctx, d, tables)
	if k.life.Err() == nil {
		// Not a keeper closed under its update: nothing reads it any more.
		k.note(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if gen == p.gen {
		p.updating, p.stuck = false, false
	}
	if gen < p.pubGen {
		return true
	}
	p.pubGen = gen
	defer p.markPublishedLocked()
	if err != nil {
		// The last views published stay, where their cut is still the one
		// statements read: older than the index by the time the update took,
		// and saying how old (their through), which a statement reading the
		// snapshot alone in their place would not. Dropping them sent every
		// statement back minutes, to the snapshot, until an update succeeded,
		// with nothing in the answer to say so (the update that failed may be
		// a statement's own, run in its 2 s). The cuts move: statements do
		// not wake the follower for tables it just failed on, which it tries
		// again in its own time.
		kept := map[string]tailView{}
		if dropped || k.now().Sub(p.okAt) > k.keepOld/2 {
			// dropped: the update started every tail over (the index was
			// rebuilt, the directory removed) before it failed, and the last
			// views are of what it threw away. Past half of keepOld: replaced
			// versions are removed after it, and the files these views read
			// may be the next to go.
			p.views = nil
		}
		for key, v := range p.views {
			if c, ok := p.cuts[key]; ok && c == cuts[key] {
				kept[key] = v
			}
		}
		if len(kept) == 0 {
			kept = nil
		}
		// A refusal stands only over the cut it was made for.
		for key := range p.refused {
			if p.cuts[key] != cuts[key] {
				delete(p.refused, key)
			}
		}
		p.views, p.cuts = kept, cuts
		return true
	}
	p.views, p.cuts, p.refused, p.okAt = got, cuts, refused, k.now()
	return true
}

// toFollow is the tables of the next update with their cuts and the update's
// number, nil when no statement asked in idle (the follower is then marked
// stopped).
func (k *tailKeeper) toFollow() ([]views.BaselineTable, map[string]string, uint64) {
	p := &k.pub
	p.mu.Lock()
	defer p.mu.Unlock()
	cutoff := k.now().Add(-p.idle)
	var tables []views.BaselineTable
	cuts := map[string]string{}
	for key, a := range p.asked {
		if a.at.Before(cutoff) {
			delete(p.asked, key)
			continue
		}
		tables = append(tables, a.table)
		cuts[key] = tailCutOf(a.table)
	}
	if len(tables) == 0 {
		p.views, p.cuts, p.running = nil, nil, false
		return nil, nil, 0
	}
	p.gen++
	p.updating, p.stuck, p.started = true, false, k.now()
	return tables, cuts, p.gen
}

// runFollower is the follower's goroutine: an update, a pause of every (or
// less, when a statement wakes it), until no statement has asked in idle or
// the keeper is closed. Its last act is to publish nothing, so a statement
// never reads a version no follower keeps up to date. A panic ends it like
// that too: left marked running, no statement would start another and every
// statement would read the copy without tails until the process restarts.
// The next one starts after a wait that doubles with each panic in a row, and
// only the first of a run is said with its stack: one that recurs would
// otherwise log a stack per statement.
func (k *tailKeeper) runFollower() {
	p := &k.pub
	stopped := func() {
		p.mu.Lock()
		p.views, p.cuts, p.running = nil, nil, false
		p.markPublishedLocked()
		p.mu.Unlock()
	}
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		p.mu.Lock()
		p.views, p.cuts, p.running, p.updating = nil, nil, false, false
		p.markPublishedLocked()
		p.panics++
		wait := k.retryWait(p.panics)
		p.restartAt = k.now().Add(wait)
		n := p.panics
		p.mu.Unlock()
		k.note(fmt.Errorf("keeping them up to date stopped on a panic: %v", r))
		attrs := []any{"error", fmt.Sprint(r), "panics", n, "restart_in", wait}
		if n == 1 {
			attrs = append(attrs, "stack", string(debug.Stack()))
		}
		slog.Error("Engine Routing: keeping the copy's tails up to date stopped on a panic; a statement starts it again", attrs...)
	}()
	for {
		if k.life.Err() != nil {
			stopped()
			return
		}
		if !k.followOnce(k.life, p.deadline) {
			return
		}
		p.mu.Lock()
		if p.pubGen > 0 && p.views != nil {
			p.panics = 0
		}
		p.mu.Unlock()
		select {
		case <-k.life.Done():
			stopped()
			return
		case <-time.After(p.every):
		case <-p.wake:
		}
	}
}

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
	tailFollowIdle = time.Minute
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
}

type tailAsk struct {
	table views.BaselineTable
	at    time.Time
}

func newTailPublish() tailPublish {
	return tailPublish{every: tailFollowEvery, idle: tailFollowIdle, deadline: tailFollowTimeout}
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
	got, err := k.refreshWithin(ctx, d, tables)
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
	if err != nil {
		// As a statement that met the error itself did: no tails until an
		// update succeeds, not the last ones kept as if current. The cuts
		// stay: statements do not wake the follower for tables it just failed
		// on, which it tries again in its own time.
		p.views, p.cuts = nil, cuts
		return true
	}
	p.views, p.cuts = got, cuts
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
		p.mu.Unlock()
	}
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		p.mu.Lock()
		p.views, p.cuts, p.running, p.updating = nil, nil, false, false
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
		}
	}
}

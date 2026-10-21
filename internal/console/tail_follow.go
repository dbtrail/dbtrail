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
// while the follower holds the keeper's for a whole update.
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
	// running while the follower's goroutine is; kick wakes it early.
	running bool
	kick    chan struct{}
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
	return tailPublish{kick: make(chan struct{}, 1), every: tailFollowEvery, idle: tailFollowIdle, deadline: tailFollowTimeout}
}

// latest is what a statement over read reads of the tails: the follower's
// last version of each, when it was brought up to date over the cut the
// statement reads. It reads nothing of the index and never waits for an
// update. A table the last update was not asked for, or asked for over
// another cut (a refresh published since), has none, and wakes the follower,
// which is started if it is not running.
func (k *tailKeeper) latest(read []views.BaselineTable) map[string]tailView {
	p := &k.pub
	p.mu.Lock()
	defer p.mu.Unlock()
	if k.life.Err() != nil {
		return nil
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
	if p.auto && !p.running {
		p.running = true
		go k.runFollower()
		return out
	}
	if wake {
		select {
		case p.kick <- struct{}{}:
		default:
		}
	}
	return out
}

// followOnce brings the tails statements asked for in the last idle up to
// date and publishes them, and says whether there were any. With none it
// publishes nothing and, in the same hold of the lock, marks the follower
// stopped, so a statement that asks after starts it again.
func (k *tailKeeper) followOnce(ctx context.Context) bool {
	p := &k.pub
	p.mu.Lock()
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
		p.mu.Unlock()
		return false
	}
	p.mu.Unlock()

	got, err := k.refreshWithin(ctx, p.deadline, tables)
	k.note(err)

	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		// As a statement that met the error itself did: no tails until an
		// update succeeds, not the last ones kept as if current.
		p.views, p.cuts = nil, nil
		return true
	}
	p.views, p.cuts = got, cuts
	return true
}

// runFollower is the follower's goroutine: an update, a pause of every (or
// less, when a statement wakes it), until no statement has asked in idle or
// the keeper is closed. Its last act is to publish nothing, so a statement
// never reads a version no follower keeps up to date. A panic ends it like
// that too, said with its stack: left marked running, no statement would
// start another and every statement would read the copy without tails until
// the process restarts.
func (k *tailKeeper) runFollower() {
	p := &k.pub
	stopped := func() {
		p.mu.Lock()
		p.views, p.cuts, p.running = nil, nil, false
		p.mu.Unlock()
	}
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Engine Routing: keeping the copy's tails up to date stopped on a panic; the next statement starts it again",
				"error", fmt.Sprint(r), "stack", string(debug.Stack()))
			stopped()
		}
	}()
	for {
		if k.life.Err() != nil {
			stopped()
			return
		}
		if !k.followOnce(k.life) {
			return
		}
		select {
		case <-k.life.Done():
			stopped()
			return
		case <-time.After(p.every):
		case <-p.kick:
		}
	}
}

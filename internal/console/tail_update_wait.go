package console

import (
	"context"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// waitUpdateOver waits for the update of the tails running now, when a table
// of read has no version to read yet and one may be on its way. A statement
// over tables that have one does not wait for an update (ensureDir). One
// over a table with none has nothing to read until an update brings it, and
// its own wait for a tail (statementTailsFresh, freshTailsWait) starts after
// this one, as it did when every statement waited for the update.
//
// Only the update running when it is asked, and for at most limit: this runs
// inside the statement's own time on the copy, so a long update must leave it
// time to run. Past limit the statement goes on the normal way: MySQL or the
// source answers it, or the copy does, saying it read the snapshot alone.
// limit 0 or less is one update's own deadline.
func (k *tailKeeper) waitUpdateOver(ctx context.Context, read []views.BaselineTable, limit time.Duration) {
	if len(read) == 0 {
		return
	}
	p := &k.pub
	p.mu.Lock()
	running, target := p.updating, p.gen
	waitFor := p.deadline
	p.mu.Unlock()
	// Once, and after reading what runs: it records what the statement reads
	// for the next update, and may start the follower, whose update is not
	// one this statement is to wait for.
	k.published(read)
	if !running {
		return
	}
	if limit > 0 {
		waitFor = limit
	}
	if waitFor <= 0 {
		waitFor = tailFollowTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, waitFor)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		ch := k.nextPublish()
		p.mu.Lock()
		over := !p.updating || p.pubGen >= target
		pending := p.tailsPendingLocked(read)
		p.mu.Unlock()
		if over || !pending {
			return
		}
		// The lock is what an update holds: free, none is running, whatever
		// was last said of one.
		if k.mu.TryLock() {
			k.mu.Unlock()
			return
		}
		select {
		case <-ch:
		case <-tick.C:
		case <-ctx.Done():
			return
		case <-k.life.Done():
			return
		}
	}
}

// tailsPendingLocked says a table of read has no version over its cut and
// one may come, as statementTailsFresh tells them apart. False when every
// table has one, and when any cannot have one now (the last update covered
// its cut and is not building it): a statement reads its tables' tails all
// or none, so waiting for the others would bring it nothing. p.mu held.
func (p *tailPublish) tailsPendingLocked(read []views.BaselineTable) bool {
	pending := false
	for _, t := range read {
		key := tailKey(t.Schema, t.Table)
		cut, covered := p.cuts[key]
		covered = covered && cut == tailCutOf(t)
		if covered {
			if _, has := p.views[key]; has {
				continue
			}
			if p.building != nil && !p.building[key] {
				return false
			}
		}
		pending = true
	}
	return pending
}

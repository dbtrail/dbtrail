package notify

import (
	"sync"
	"time"
)

// DefaultRepeatEvery is how long an Edge stays quiet about a still-active
// condition before re-notifying — long enough that a persistent problem
// produces one reminder a day, not one per polling cycle.
const DefaultRepeatEvery = 24 * time.Hour

// Edge tracks per-key condition state so callers notify on the *transition*
// into a bad state, re-notify while it persists only after repeatEvery, and
// notify exactly once on recovery. Keys are caller-defined, e.g.
// "verify:<severity>:<server-id>".
//
// Key identity is the caller's contract: if a key's identity churns (e.g. a
// DSN-derived key after a credential edit), the old entry stays active until
// resolved and the condition re-fires under the new key — a duplicate alert,
// never a lost one.
type Edge struct {
	repeatEvery time.Duration
	now         func() time.Time // injectable for tests

	mu     sync.Mutex
	active map[string]edgeState
}

// edgeState is one active condition: when it was last notified, and the
// detail identifying it.
type edgeState struct {
	last   time.Time
	detail string
}

// NewEdge builds an Edge; repeatEvery <= 0 uses DefaultRepeatEvery.
func NewEdge(repeatEvery time.Duration) *Edge {
	if repeatEvery <= 0 {
		repeatEvery = DefaultRepeatEvery
	}
	return &Edge{repeatEvery: repeatEvery, now: time.Now, active: make(map[string]edgeState)}
}

// NewEdgeWithClock is NewEdge reading the time from now, so a caller's tests
// can move through the repeat window without sleeping through it.
func NewEdgeWithClock(repeatEvery time.Duration, now func() time.Time) *Edge {
	e := NewEdge(repeatEvery)
	e.now = now
	return e
}

// Fire reports whether the caller should notify for key being in a bad state
// now: true on the transition into it, and again each repeatEvery while it
// persists. detail (optional) carries the condition's identity — a CHANGED
// non-empty detail is a NEW condition under the same key, so it fires
// immediately, bypassing (and refreshing) the repeat window.
func (e *Edge) Fire(key, detail string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now()
	cur, ok := e.active[key]
	changed := ok && detail != "" && cur.detail != "" && detail != cur.detail
	if ok && !changed && now.Sub(cur.last) < e.repeatEvery {
		return false
	}
	e.active[key] = edgeState{last: now, detail: detail}
	return true
}

// Active reports whether key has fired and not yet resolved — for callers
// that must treat "cannot re-evaluate" differently while an alert is standing
// (e.g. a baseline source that now lists nothing).
func (e *Edge) Active(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.active[key]
	return ok
}

// Resolve reports whether the caller should send a recovery notification:
// true only if key was active (fired at least once and not yet resolved).
func (e *Edge) Resolve(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.active[key]; !ok {
		return false
	}
	delete(e.active, key)
	return true
}

// Package stmtclock is a statement's time cap that can be held while the
// statement waits its turn.
//
// A MySQL-protocol port bounds each statement with a deadline, so one that
// runs away is stopped. A statement that finds every worker of the copy busy
// waits in a line first, and that wait is not the statement running: counted
// against the cap, a statement that waited long was stopped for "running too
// long" without having run, or was handed a worker with seconds left. The
// line holds the clock while a statement waits in it (Hold) and lets it run
// again when the statement gets its turn.
//
// A connection can have more than one clock at a time (a routed statement
// has one for the decision and one for the copy), and the wait is the same
// wait for all of them, so they are held together: the clocks of one Group.
package stmtclock

import (
	"context"
	"sync"
	"time"
)

// Group is the clocks of one connection. The zero value is ready to use.
type Group struct {
	mu     sync.Mutex
	clocks map[*clock]struct{}
	holds  int
}

type groupKey struct{}

// WithTimeout is context.WithTimeout with a clock Hold can stop: the context
// ends with context.DeadlineExceeded once it has run for d, not counting the
// time it was held, and with the parent's error when the parent ends first.
// The cancel function must be called, as with context.WithTimeout.
func (g *Group) WithTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	c := &clock{g: g, parent: parent, done: make(chan struct{}), left: d}
	g.mu.Lock()
	if g.clocks == nil {
		g.clocks = map[*clock]struct{}{}
	}
	g.clocks[c] = struct{}{}
	if g.holds == 0 {
		c.startLocked()
	}
	g.mu.Unlock()
	// A parent that has already ended ends it here, before it is handed
	// out; one that ends later does on the callback's goroutine.
	if err := parent.Err(); err != nil {
		c.end(err)
		return c, func() {}
	}
	stop := context.AfterFunc(parent, func() { c.end(parent.Err()) })
	g.mu.Lock()
	c.stopParent = stop
	over := c.err != nil
	g.mu.Unlock()
	if over {
		stop() // it ended before stop was there for end to call
	}
	return c, func() { c.end(context.Canceled) }
}

// Hold stops the clocks of ctx's connection until the function it returns is
// called. Calls nest: the clocks run again when every hold was released. A
// context with no such clock is left alone, and releasing twice is the same
// as once.
func Hold(ctx context.Context) (release func()) {
	g, _ := ctx.Value(groupKey{}).(*Group)
	if g == nil {
		return func() {}
	}
	g.mu.Lock()
	if g.holds++; g.holds == 1 {
		for c := range g.clocks {
			c.stopLocked()
		}
	}
	g.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			if g.holds--; g.holds == 0 {
				for c := range g.clocks {
					c.startLocked()
				}
			}
			g.mu.Unlock()
		})
	}
}

// clock is one statement's context. Its fields are guarded by g.mu.
type clock struct {
	g          *Group
	parent     context.Context
	stopParent func() bool
	done       chan struct{}
	err        error
	// left is how long the clock still has to run; deadline is when it
	// runs out, set only while it runs (timer != nil).
	left     time.Duration
	deadline time.Time
	timer    *time.Timer
	// run numbers each start, so a timer that fired as it was being
	// stopped finds it is not the current one and does nothing.
	run int
}

func (c *clock) startLocked() {
	if c.err != nil || c.timer != nil {
		return
	}
	c.run++
	run := c.run
	c.deadline = time.Now().Add(c.left)
	c.timer = time.AfterFunc(c.left, func() { c.expire(run) })
}

func (c *clock) stopLocked() {
	if c.timer == nil {
		return
	}
	c.timer.Stop()
	c.timer = nil
	c.run++
	c.left = max(time.Until(c.deadline), 0)
}

func (c *clock) expire(run int) {
	c.g.mu.Lock()
	current := c.run == run && c.timer != nil
	c.g.mu.Unlock()
	if current {
		c.end(context.DeadlineExceeded)
	}
}

func (c *clock) end(err error) {
	c.g.mu.Lock()
	if c.err != nil {
		c.g.mu.Unlock()
		return
	}
	c.err = err
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	delete(c.g.clocks, c)
	close(c.done)
	stop := c.stopParent
	c.g.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// Deadline reports when the clock runs out. While it is held there is no
// such time, and none is reported.
func (c *clock) Deadline() (time.Time, bool) {
	c.g.mu.Lock()
	defer c.g.mu.Unlock()
	if c.timer == nil {
		return time.Time{}, false
	}
	return c.deadline, true
}

func (c *clock) Done() <-chan struct{} { return c.done }

func (c *clock) Err() error {
	// A parent that has ended has ended this one, as of now: the callback
	// that says so runs on its own goroutine and may not have yet.
	if err := c.parent.Err(); err != nil {
		c.end(err)
	}
	c.g.mu.Lock()
	defer c.g.mu.Unlock()
	return c.err
}

func (c *clock) Value(key any) any {
	if _, ok := key.(groupKey); ok {
		return c.g
	}
	return c.parent.Value(key)
}

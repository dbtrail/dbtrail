package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// heavyLane names who is asking heavyGate for room. Snapshot work comes
// before checks: the copy is what a recovery reads, the check only grades it.
type heavyLane int

const (
	// laneSnapshot is the DuckDB part of the work that writes snapshots: the
	// fold of a refresh or a restore, a compaction, the resolved pairs, the
	// fold of an SQL export. One at a time in the process. Only that part:
	// a full read (whose start time is the schedule's promise), reading the
	// source, uploading, are outside the lane.
	laneSnapshot heavyLane = iota
	// laneCheck is one table of a check, or one Explain. One at a time, and
	// never while snapshot work runs or waits.
	laneCheck
)

// heavyGate keeps the process's heavy DuckDB work from piling up (#2385).
// Each of these jobs opens DuckDB sessions with a memory cap of their own, and
// before the gate nothing stopped a check, a refresh and a compaction (each
// for any server) from running together in the process that is also capture.
// On an 8 GB host that got the process, or the index next to it, killed.
//
// Two lanes, not one lock, because snapshot work must never wait behind a
// check: an Explain is one long table that cannot be cut short. So:
//
//   - snapshot work waits only for other snapshot work;
//   - check work waits for other check work, and for snapshot work that is
//     running OR waiting, so a stream of check tables cannot starve a
//     refresh, and a check started while a refresh runs gives way between
//     tables, not in the middle of one.
//
// At most one of each runs at a time. It is not reentrant: a job takes its
// lane once, at one level, and anything it starts on another goroutine queues.
type heavyGate struct {
	mu          sync.Mutex
	held        [2]bool
	snapWaiting int
	changed     chan struct{} // closed and replaced whenever the state above changes
	// unbounded lets everything in at once. Tests only: one whose subject
	// is two folds overlapping cannot otherwise stage it.
	unbounded bool
}

func newHeavyGate() *heavyGate { return &heavyGate{changed: make(chan struct{})} }

// heavyWork is the gate every supervisor in this process shares: it is about
// the process's memory, so one per process.
var heavyWork = newHeavyGate()

// acquire waits for room in lane and returns the function that gives it back.
// onWait, when not nil, is called once, before waiting, only if the caller
// has to wait: what a job says while it stands in line. A canceled ctx ends
// the wait with its error and nothing held. Release is safe to call twice,
// and must be deferred so a job that panics still gives its lane back.
func (g *heavyGate) acquire(ctx context.Context, lane heavyLane, onWait func()) (release func(), err error) {
	if g.unbounded {
		return func() {}, nil
	}
	g.mu.Lock()
	waited := false
	for !g.freeLocked(lane) {
		if !waited {
			waited = true
			if lane == laneSnapshot {
				g.snapWaiting++
			}
			if onWait != nil {
				g.mu.Unlock()
				onWait()
				g.mu.Lock()
				continue
			}
		}
		ch := g.changed
		g.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			g.mu.Lock()
			if lane == laneSnapshot {
				g.snapWaiting--
				g.broadcastLocked()
			}
			g.mu.Unlock()
			return nil, ctx.Err()
		}
		g.mu.Lock()
	}
	if waited && lane == laneSnapshot {
		g.snapWaiting--
	}
	g.held[lane] = true
	g.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			g.held[lane] = false
			g.broadcastLocked()
			g.mu.Unlock()
		})
	}, nil
}

func (g *heavyGate) freeLocked(lane heavyLane) bool {
	if lane == laneSnapshot {
		return !g.held[laneSnapshot]
	}
	return !g.held[laneCheck] && !g.held[laneSnapshot] && g.snapWaiting == 0
}

func (g *heavyGate) broadcastLocked() {
	close(g.changed)
	g.changed = make(chan struct{})
}

func (s *baselineSupervisor) heavy() *heavyGate {
	if s.gate != nil {
		return s.gate
	}
	return heavyWork
}

func (s *verifySupervisor) heavy() *heavyGate {
	if s.gate != nil {
		return s.gate
	}
	return heavyWork
}

// errStoppedBeforeStart is the end of a snapshot job that was still waiting
// for the snapshot lane when DBTrail shut down.
var errStoppedBeforeStart = errors.New("stopped because DBTrail shut down before it could start")

// takeSnapshotLane waits for the snapshot lane on the daemon's context and
// returns how long it waited. The wait is logged, so a job that sits in line
// does not read as a slow one, and a caller that times its work subtracts it.
func (s *baselineSupervisor) takeSnapshotLane(server, job string) (func(), time.Duration, error) {
	var waitedSince time.Time
	release, err := s.heavy().acquire(s.ctx, laneSnapshot, func() {
		waitedSince = time.Now()
		slog.Info("snapshot work waiting: another server's snapshot work is running in this process; they run one at a time",
			"server", server, "job", job)
	})
	var waited time.Duration
	if !waitedSince.IsZero() {
		waited = time.Since(waitedSince)
		if err == nil {
			slog.Info("snapshot work starting after waiting its turn", "server", server, "job", job,
				"waited", waited.Round(time.Second).String())
		}
	}
	return release, waited, err
}

// stoppedBeforeStart ends a job slot that was claimed "running" but never got
// the snapshot lane, so no exit leaves "running" standing (the busyLocked
// single-flight would refuse that server's jobs until a restart).
func (s *baselineSupervisor) stoppedBeforeStart(slots map[string]*console.BaselineStatus, serverID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st := slots[serverID]; st != nil && st.State == "running" {
		st.State, st.LastError, st.FinishedAt = "failed", errStoppedBeforeStart.Error(), nowStamp()
	}
}

// checkLaneWaitNote is what a check says while one of its tables waits.
const checkLaneWaitNote = "Waiting for a snapshot update or another check to finish. Checks run one table at a time and give way to snapshot updates."

// inCheckLane runs one table of a check in the check lane: it waits for
// snapshot work and for any other check's table, saying so in the run's note
// while it waits, and clears that note once its turn comes. f gets ctx with
// the lane's DuckDB budget on it for a job of one session at a time
// (soloLaneCtx): a check table runs one.
func (s *verifySupervisor) inCheckLane(ctx context.Context, serverID string, f func(context.Context)) error {
	release, err := s.heavy().acquire(ctx, laneCheck, func() { s.setNote(serverID, checkLaneWaitNote) })
	s.clearNote(serverID, checkLaneWaitNote) // on both paths: a stopped run must not keep it
	if err != nil {
		return fmt.Errorf("verify interrupted: %w", err)
	}
	defer release()
	f(soloLaneCtx(ctx, laneCheck))
	return nil
}

// clearNote removes note from the run's status if it is still the one shown.
func (s *verifySupervisor) clearNote(serverID, note string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j, ok := s.jobs[serverID]; ok && j.status.Note == note {
		j.status.Note = ""
	}
}

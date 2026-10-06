package consoleapp

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// A restart decided in the background must never undo an operator's Stop or
// Start that landed first: start with an expected job refuses once the slot
// holds anything else, and leaves the slot as it was.
func TestStartRefusesWhenTheExpectedJobIsGone(t *testing.T) {
	m := newMonitorSupervisor(context.Background(), "", nil, 0)
	e := console.ServerEntry{ID: "s2105", Name: "shop", SourceDSN: "root@tcp(127.0.0.1:1)/", DSN: "root@tcp(127.0.0.1:1)/bintrail_idx_s2105"}
	lost := &monitorJob{cancel: func() {}, done: make(chan struct{})}

	// Stopped meanwhile: the slot is empty.
	if err := m.start(context.Background(), e, lost, nil); !errors.Is(err, errNotCurrent) {
		t.Fatalf("slot empty: err=%v, want errNotCurrent", err)
	}
	if len(m.jobs) != 0 {
		t.Fatalf("refused start reserved a slot: %v", m.jobs)
	}
	// Started again by the operator meanwhile: another job is there.
	other := &monitorJob{cancel: func() {}, done: make(chan struct{})}
	other.set("running", "")
	m.jobs[e.ID] = other
	if err := m.start(context.Background(), e, lost, nil); !errors.Is(err, errNotCurrent) {
		t.Fatalf("slot taken: err=%v, want errNotCurrent", err)
	}
	if m.jobs[e.ID] != other {
		t.Fatal("refused start replaced the operator's job")
	}
}

// The commonest cause of a lost lock is an unreachable index, and then the
// restart fails too. It must stay a retrying failure, never one that waits
// for someone to press Start; and Stop must end the retries.
func TestLockLostRestartThatFailsKeepsRetrying(t *testing.T) {
	base := lockLostBackoffBase
	lockLostBackoffBase = 50 * time.Millisecond
	t.Cleanup(func() { lockLostBackoffBase = base })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := newMonitorSupervisor(ctx, "", nil, 0)
	// An index DSN start cannot parse: start reserves the slot and fails.
	e := console.ServerEntry{ID: "s2105b", Name: "shop", SourceDSN: "root@tcp(127.0.0.1:1)/", DSN: "::not a dsn::"}
	lost := &monitorJob{cancel: func() {}, done: make(chan struct{})}
	m.jobs[e.ID] = lost
	m.wg.Add(1) // run's own count, which lockLost relies on
	m.lockLost(lost, e)
	close(lost.done)
	m.wg.Done()

	deadline := time.Now().Add(5 * time.Second)
	var st console.MonitorStatus
	for time.Now().Before(deadline) {
		m.mu.Lock()
		j := m.jobs[e.ID]
		m.mu.Unlock()
		if j != nil && j != lost {
			if st = j.snapshot(); st.State == "failed" && st.Retrying {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if st.State != "failed" || !st.Retrying {
		t.Fatalf("after a failed restart: %+v, want failed and retrying", st)
	}
	// Stop alone, the daemon still running, must end the retries.
	if err := m.Stop(context.Background(), e.ID); err != nil {
		t.Fatal(err)
	}
	waited := make(chan struct{})
	go func() { m.wg.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("the retry loop outlived Stop")
	}
	if _, ok := m.jobs[e.ID]; ok {
		t.Fatal("the retry loop started the server again after Stop")
	}
}

// The restart reads the entry again: a server the operator stopped
// (monitor_desired off) or removed since is not started behind their back.
func TestLockLostRestartRespectsTheRegistry(t *testing.T) {
	path := t.TempDir() + "/servers.yaml"
	if err := os.WriteFile(path, []byte("version: 1\nservers:\n  - id: s2105c\n    name: shop\n    index_dsn: \"::not a dsn::\"\n    source_dsn: root@tcp(127.0.0.1:1)/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := console.LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := newMonitorSupervisor(ctx, "", reg, 0)
	e := console.ServerEntry{ID: "s2105c", Name: "shop", SourceDSN: "root@tcp(127.0.0.1:1)/", DSN: "::not a dsn::", MonitorDesired: true}
	lost := &monitorJob{cancel: func() {}, done: make(chan struct{})}
	m.jobs[e.ID] = lost
	m.wg.Add(1)
	m.lockLost(lost, e) // e says desired; the registry says not
	close(lost.done)
	m.wg.Done()
	time.Sleep(500 * time.Millisecond)
	m.mu.Lock()
	j := m.jobs[e.ID]
	m.mu.Unlock()
	if j != lost {
		t.Fatalf("restarted a server the registry no longer wants captured: %+v", j.snapshot())
	}
}

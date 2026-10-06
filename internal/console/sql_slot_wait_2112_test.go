package console

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// slotWaitMetrics reads bintrail_sql_slot_wait_seconds (sample count and sum
// of seconds per outcome) and bintrail_sql_slot_waiting.
func slotWaitMetrics(t *testing.T) (counts map[string]uint64, sums map[string]float64, waiting float64) {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	counts, sums = map[string]uint64{}, map[string]float64{}
	for _, mf := range mfs {
		switch mf.GetName() {
		case "bintrail_sql_slot_wait_seconds":
			for _, m := range mf.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "outcome" {
						counts[l.GetValue()] = m.GetHistogram().GetSampleCount()
						sums[l.GetValue()] = m.GetHistogram().GetSampleSum()
					}
				}
			}
		case "bintrail_sql_slot_waiting":
			waiting = mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	return counts, sums, waiting
}

// #2112: the wait for one of the copy's slots is visible. One slot, one place
// in the line: the first statement takes the slot, the second waits for it
// and is counted as waiting meanwhile, the third finds the line full and is
// refused at once, and the second gets the slot when the first lets go, with
// the time it waited in the histogram. The SQL card and the port both take
// their slot through this one function.
func TestSQLSlotWait_isObserved(t *testing.T) {
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{MaxInFlight: 1, MaxWaiters: 1, MaxWait: 10 * time.Second})}
	ctx := context.Background()
	before, sumBefore, waitingBefore := slotWaitMetrics(t)

	first, _, err := reserveSQLSlot(ctx, runner, "server:a")
	if err != nil {
		t.Fatal(err)
	}
	type got struct {
		slot   sqlSlot
		waited time.Duration
		err    error
	}
	second := make(chan got, 1)
	go func() {
		slot, waited, err := reserveSQLSlot(ctx, runner, "server:b")
		second <- got{slot, waited, err}
	}()
	// The second statement is in the line once the gauge says so.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, _, waiting := slotWaitMetrics(t); waiting == waitingBefore+1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("bintrail_sql_slot_waiting never showed the waiting statement")
		}
		time.Sleep(time.Millisecond)
	}
	// The gauge moves before Reserve is called; the line itself is full a
	// moment later. Ask until the runner says so, which it does at once.
	var full error
	for full == nil && time.Now().Before(deadline) {
		var slot sqlSlot
		slot, _, full = reserveSQLSlotNoWaitForTest(runner, "server:c")
		if slot != nil {
			t.Fatal("a third statement got a slot while the only one was held")
		}
	}
	if !errors.Is(full, sqlsandbox.ErrBusy) {
		t.Fatalf("third statement: %v, want ErrBusy (the line is full)", full)
	}
	const held = 60 * time.Millisecond
	time.Sleep(held)
	first.Release()
	g := <-second
	if g.err != nil {
		t.Fatalf("the waiting statement: %v", g.err)
	}
	g.slot.Release()

	after, sumAfter, waitingAfter := slotWaitMetrics(t)
	if after["slot"] != before["slot"]+2 {
		t.Errorf(`outcome "slot": count %d→%d, want +2 (the one that did not wait and the one that did)`, before["slot"], after["slot"])
	}
	if waitedSum := sumAfter["slot"] - sumBefore["slot"]; waitedSum < held.Seconds() || g.waited < held {
		t.Errorf("the wait: histogram +%.3f s, returned %s; want both at least %s", waitedSum, g.waited, held)
	}
	if after["queue_full"] != before["queue_full"]+1 {
		t.Errorf(`outcome "queue_full": count %d→%d, want +1`, before["queue_full"], after["queue_full"])
	}
	if sumAfter["queue_full"]-sumBefore["queue_full"] > 0.05 {
		t.Errorf("a statement refused by a full line waited %.3f s; it does not wait", sumAfter["queue_full"]-sumBefore["queue_full"])
	}
	if waitingAfter != waitingBefore {
		t.Errorf("bintrail_sql_slot_waiting = %v after everyone left, want %v", waitingAfter, waitingBefore)
	}
}

// reserveSQLSlotNoWaitForTest is reserveSQLSlot for the statement the test
// expects to be refused at once: its caller has already left, so a line that
// is not full yet hands it back (nil error here) instead of parking it.
func reserveSQLSlotNoWaitForTest(runner sqlRunner, user string) (sqlSlot, time.Duration, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	slot, waited, err := reserveSQLSlot(ctx, runner, user)
	if errors.Is(err, context.Canceled) {
		return nil, waited, nil
	}
	return slot, waited, err
}

// The two other ways a wait ends without a slot: the whole wait passes, or
// the caller leaves (a client that disconnected, a connection's time cap).
func TestSQLSlotWait_timeoutAndCancelAreTheirOwnOutcomes(t *testing.T) {
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{MaxInFlight: 1, MaxWait: 20 * time.Millisecond})}
	held, _, err := reserveSQLSlot(context.Background(), runner, "")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	before, _, waitingBefore := slotWaitMetrics(t)

	var busy *sqlsandbox.BusyError
	if _, _, err := reserveSQLSlot(context.Background(), runner, ""); !errors.As(err, &busy) {
		t.Fatalf("after the whole wait: %v, want a *BusyError", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := reserveSQLSlot(ctx, runner, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("a caller that left: %v, want context.Canceled", err)
	}

	after, _, waitingAfter := slotWaitMetrics(t)
	for _, outcome := range []string{"timeout", "cancelled"} {
		if after[outcome] != before[outcome]+1 {
			t.Errorf("outcome %q: count %d→%d, want +1", outcome, before[outcome], after[outcome])
		}
	}
	if after["slot"] != before["slot"] || after["queue_full"] != before["queue_full"] {
		t.Errorf("slot %d→%d, queue_full %d→%d: neither statement got a slot nor found the line full",
			before["slot"], after["slot"], before["queue_full"], after["queue_full"])
	}
	if waitingAfter != waitingBefore {
		t.Errorf("bintrail_sql_slot_waiting = %v after both left, want %v", waitingAfter, waitingBefore)
	}
}

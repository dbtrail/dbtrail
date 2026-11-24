//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// The schedule choosing the read of some tables, end to end (#2640, part 4):
// the real mydumper, the supervisor's real capture, the real fold, driven by
// the schedule's own ticks. Needs a mydumper on PATH, like the rig it uses.
//
// A TRUNCATE refuses one table. The scheduled update is refused for it, the
// schedule reads that table alone (never the whole server), and after one
// more update the copy equals the source row for row. With the choice off the
// slot takes the full read, and this test fails on that.
func TestIntegrationScheduleReread_aTruncateReadsOnlyThatTable_2640(t *testing.T) {
	r := newRereadRig(t)
	h, err := console.OpenBaselineHistory(t.TempDir() + "/h.json")
	if err != nil {
		t.Fatal(err)
	}
	r.sup.history = h
	r.sup.rereadRefused = true
	firstRead := r.sup.Status(r.req.ServerID).Since

	e, ok := r.reg.Get(r.req.ServerID)
	if !ok {
		t.Fatal("the rig's server is not in its registry")
	}
	e.BackupSchedule = &console.BackupSchedule{Every: "1h", At: "00:00"}
	if err := r.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b := newBackupScheduler(r.sup, r.reg, true, true)
	b.window = nil
	t0 := time.Now().UTC().Truncate(time.Hour)
	b.tick(context.Background(), t0.Add(5*time.Second)) // observed; the next slot fires

	truncateOrdersCaptured(t, r)

	// Writes that never pause, on every table, the read one included: the read
	// is taken under them, and capture has something to pass its point with.
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 3000; !stop.Load(); i++ {
			for _, table := range rereadRigTables {
				_, _ = r.src.Exec(fmt.Sprintf("INSERT INTO %s VALUES (%d, 'during', 2)", table, i))
				_, _ = r.src.Exec(fmt.Sprintf("UPDATE %s SET n = n + 1 WHERE id = %d", table, i-1))
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	st := scheduleSlot(t, b, e.ID, t0.Add(time.Hour+5*time.Second))
	stop.Store(true)
	wg.Wait()

	if st.LastMethod != console.BackupMethodRefresh || st.Last == nil || st.Last.State != "succeeded" || !st.Last.Published {
		t.Fatalf("the slot ended with %+v; want the update that read the table alone, published", st)
	}
	if got := r.sup.Status(e.ID).Since; got != firstRead {
		t.Fatalf("a full read ran in the slot (the dump slot moved from %s to %s)", firstRead, got)
	}
	for _, rec := range h.List(e.ID) {
		if rec.Kind == console.BaselineRunDump && rec.SkipReason == "" {
			t.Fatalf("the history holds a full read: %+v", rec)
		}
	}
	if st.LastFallbackAt != "" || h.EmergencyStarted(e.ID) != "" {
		t.Fatalf("a full read is on record: %+v", st)
	}
	if want := rereadUpdateWhy(1); st.LastUpdateWhy != want {
		t.Fatalf("LastUpdateWhy = %q, want %q", st.LastUpdateWhy, want)
	}
	r.wantWorkEmpty(t)

	// One more update, the writes stopped and captured: the copy is the source.
	r.captured(t)
	st = scheduleSlot(t, b, e.ID, t0.Add(2*time.Hour+5*time.Second))
	if st.LastMethod != console.BackupMethodRefresh || st.Last == nil || st.Last.State != "succeeded" {
		t.Fatalf("the update after it: %+v", st)
	}
	r.wantSource(t)
}

// truncateOrdersCaptured empties orders the way the issue's case does, with
// rows written after it, and waits for capture to hold them.
func truncateOrdersCaptured(t *testing.T, r *rereadRig) {
	t.Helper()
	if _, err := r.src.Exec("TRUNCATE TABLE orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.src.Exec("INSERT INTO orders VALUES (1000, 'after-truncate', 1), (1001, 'after-truncate', 1)"); err != nil {
		t.Fatal(err)
	}
	r.captured(t)
}

// scheduleSlot ticks at slot and waits for every job the slot started.
func scheduleSlot(t *testing.T, b *backupScheduler, id string, slot time.Time) console.BackupScheduleState {
	t.Helper()
	nextSecond()
	b.tick(context.Background(), slot)
	waitTerminal(t, b, id)
	b.watchers.Wait()
	for deadline := time.Now().Add(10 * time.Minute); b.ScheduleState(id).Running; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the slot's jobs did not end")
		}
	}
	b.watchers.Wait()
	// A run reports its end before its deferred removal of the read runs.
	b.sup.postRefresh.Wait()
	return b.ScheduleState(id)
}

package console

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// A read that never ends, for the fake copy.
func pgCopyNeverEnds(ctx context.Context, _ views.BaselineTable) error {
	<-ctx.Done()
	return ctx.Err()
}

// pgCopyDelayBeforeReads puts a delay right before each read takes its clock.
func pgCopyDelayBeforeReads(t *testing.T, d time.Duration) {
	t.Helper()
	pgCopyBeforeRead = func() { time.Sleep(d) }
	t.Cleanup(func() { pgCopyBeforeRead = nil })
}

const pgCopyWholeTimeRefusal = "from its copy did not end in the"

// Only a read whose deadline was the whole time from its own start loses its
// tail, whatever the process did just before the read or after its deadline.
func TestPGTailCopy_wholeTimeIsDecidedFromTheDeadline(t *testing.T) {
	ctx := context.Background()

	t.Run("a delay just before the read does not take from its time: refused", func(t *testing.T) {
		pgCopyTimes(t, 20*time.Millisecond, 0, 0)
		pgCopyDelayBeforeReads(t, 5*time.Millisecond)
		r := newPGCopyRig(t, tailBudget{})
		settled(t, ctx, r.k, r.cut1)
		r.f.update("orders", 0x610, "1", pgCopyUnsent, "n")
		r.f.hook(pgCopyNeverEnds)
		if _, err := r.k.refresh(ctx, []views.BaselineTable{r.cut1}); err != nil {
			t.Fatal(err)
		}
		if e := r.k.tableEntry(r.key); e == nil || !strings.Contains(e.refused, pgCopyWholeTimeRefusal) {
			t.Fatalf("the read that had the whole time was not refused: %+v", e)
		}
	})

	t.Run("a read not started takes no time from the next: that one is refused", func(t *testing.T) {
		pgCopyTimes(t, 20*time.Millisecond, 0, 0)
		r := newPGCopyRig(t, tailBudget{Bytes: 1 << 20})
		items := pgCopyCut(t, filepath.Join(r.root, "snap1"), "items", 0x500, [3]string{"1", "i", "n"})
		r.f.floors[items.Path] = 0x500
		both := []views.BaselineTable{r.cut1, items}
		settled(t, ctx, r.k, both...)
		r.f.update("items", 0x610, "1", pgCopyUnsent, "n")
		r.f.update("orders", 0x620, "1", pgCopyUnsent, "n")
		// First update: items goes first and is told it did not fit, so the
		// next update skips it without reading.
		r.f.hook(func(context.Context, views.BaselineTable) error { return nil })
		ki := tailKey("public", "items")
		r.k.mu.Lock()
		tail, ok := r.k.tables[ki].tail.(pgCopyTail)
		r.k.mu.Unlock()
		if !ok {
			t.Fatal("items has no tail")
		}
		tail.DeferCopy(1 << 40)
		pgCopyDelayBeforeReads(t, 5*time.Millisecond)
		r.f.hooks = nil
		r.f.hook(pgCopyNeverEnds)
		if _, err := r.k.refresh(ctx, both); err != nil {
			t.Fatal(err)
		}
		if e := r.k.tableEntry(ki); e == nil || e.refused != "" {
			t.Fatalf("the read not started was refused: %+v", e)
		}
		if e := r.k.tableEntry(r.key); e == nil || !strings.Contains(e.refused, pgCopyWholeTimeRefusal) {
			t.Fatalf("the read after one not started did not have the whole time: %+v", e)
		}
		if got := r.f.copied(); len(got) == 0 || got[len(got)-1] != r.cut1.Path {
			t.Fatalf("reads %v, want the last one on orders", got)
		}
	})

	t.Run("a read that starts after another did not have the whole time: it waits", func(t *testing.T) {
		pgCopyTimes(t, 40*time.Millisecond, 0, 0)
		r := newPGCopyRig(t, tailBudget{})
		items := pgCopyCut(t, filepath.Join(r.root, "snap1"), "items", 0x500, [3]string{"1", "i", "n"})
		r.f.floors[items.Path] = 0x500
		both := []views.BaselineTable{r.cut1, items}
		settled(t, ctx, r.k, both...)
		r.f.update("items", 0x610, "1", pgCopyUnsent, "n")
		r.f.update("orders", 0x620, "1", pgCopyUnsent, "n")
		r.f.hook(func(context.Context, views.BaselineTable) error {
			time.Sleep(5 * time.Millisecond)
			return nil
		})
		r.f.hook(pgCopyNeverEnds)
		if _, err := r.k.refresh(ctx, both); err != nil {
			t.Fatal(err)
		}
		if e := r.k.tableEntry(r.key); e == nil || e.refused != "" {
			t.Fatalf("a read that started late was refused: %+v", e)
		}
	})

	t.Run("a read the update's own deadline cut short: it waits", func(t *testing.T) {
		pgCopyTimes(t, 10*time.Second, 4900*time.Millisecond, 0)
		r := newPGCopyRig(t, tailBudget{})
		settled(t, ctx, r.k, r.cut1)
		r.f.update("orders", 0x610, "1", pgCopyUnsent, "n")
		r.f.hook(pgCopyNeverEnds)
		short, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := r.k.refresh(short, []views.BaselineTable{r.cut1}); err != nil {
			t.Fatal(err)
		}
		if e := r.k.tableEntry(r.key); e == nil || e.refused != "" {
			t.Fatalf("a read cut short by the update's deadline was refused: %+v", e)
		}
	})
}

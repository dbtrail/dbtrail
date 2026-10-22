package console

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/views"
)

// raceIndex hands pages OUTSIDE the fake's mutex, as the MySQL source does
// (indexTailSource.pages materializes each page before the callback).
type raceIndex struct {
	*fakeIndex
	gate    chan struct{}
	entered chan struct{}
}

func (f *raceIndex) collect(key string, from, upTo uint64) []query.ResultRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	var evs []query.ResultRow
	for _, ev := range f.events {
		if (key == "" || tailKey(ev.SchemaName, ev.TableName) == key) && ev.EventID > from && ev.EventID <= upTo {
			evs = append(evs, ev)
		}
	}
	return evs
}

func (f *raceIndex) Fill(_ context.Context, t views.BaselineTable, upTo uint64, page func([]query.ResultRow) error) (string, error) {
	evs := f.collect(tailKey(t.Schema, t.Table), 0, upTo)
	if t.Table == "orders" {
		f.entered <- struct{}{}
		<-f.gate
	}
	return "", f.hand(evs, page)
}

func (f *raceIndex) Since(_ context.Context, tables []views.BaselineTable, from, upTo uint64, page func([]query.ResultRow) error) error {
	evs := f.collect("", from, upTo)
	time.Sleep(30 * time.Millisecond)
	return f.hand(evs, page)
}

// TestTailKeeper_buildCatchesUpWithoutTheLock_2494: a build's catch-up fold
// and its Fill run without k.mu while statements refresh other tables; the
// memory judgement must read nothing of the keeper's there (a map iterated
// while another goroutine writes it ends the process). Meaningful under
// -race, which CI runs.
func TestTailKeeper_buildCatchesUpWithoutTheLock_2494(t *testing.T) {
	ctx := context.Background()
	orders := keeperTable("orders")
	f := &fakeIndex{}
	f.add("orders", "1")
	src := &raceIndex{fakeIndex: f, gate: make(chan struct{}), entered: make(chan struct{}, 8)}
	k := newTailKeeper(src, t.TempDir(), tailBudget{Keys: 1000, Bytes: 1 << 30}, time.Minute, time.Hour)
	k.retryMin, k.retryMax = 0, 0
	if _, err := k.refresh(ctx, []views.BaselineTable{orders}); err != nil {
		t.Fatal(err)
	}
	<-src.entered
	f.add("orders", "2")
	f.add("orders", "3")
	if _, err := k.refresh(ctx, []views.BaselineTable{orders}); err != nil {
		t.Fatal(err)
	}
	if k.mark.ID != 3 {
		t.Fatalf("cursor %d", k.mark.ID)
	}
	close(src.gate)
	deadline := time.Now().Add(300 * time.Millisecond)
	for i := 0; time.Now().Before(deadline); i++ {
		tbl := keeperTable(fmt.Sprintf("t%d", i))
		if _, err := k.refresh(ctx, []views.BaselineTable{tbl}); err != nil {
			t.Fatal(err)
		}
		if i%50 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	k.settle()
	k.mu.Lock()
	e := k.tables[tailKey("shop", "orders")]
	k.mu.Unlock()
	if e == nil || e.tail == nil || e.tail.Keys() != 3 {
		t.Fatalf("orders tail after catch-up: %+v", e)
	}
}

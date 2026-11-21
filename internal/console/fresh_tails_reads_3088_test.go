package console

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// #3088: statementTailsFresh reads the tails, then what was published, then
// each table's state, each in a hold of its own. An update that publishes a
// table's tail between two of those reads made them disagree: the tails
// said "none yet", what was published said "here it is" (or the state said
// "no tail now", the new update having no build left to name), and the
// statement read the snapshot alone with no reason given. Each case below
// publishes the tail at one of those two instants, from the statement's own
// goroutine, so the interleaving is the same at every run.
func TestStatementTailsFreshAPublishBetweenItsReads_3088(t *testing.T) {
	for _, step := range []string{"marked", "published"} {
		t.Run("the tail published after the step "+step, func(t *testing.T) {
			ctx := context.Background()
			f := &fakeIndex{}
			f.add("orders", "1")
			k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
			defer func() { k.close(); k.settle() }()
			s := &Server{}
			orders := []views.BaselineTable{keeperTable("orders")}
			// The tail is built and installed, and the last update published
			// said it was being built: the next update publishes it.
			s.statementTails(ctx, k, orders, orders)
			k.settle()
			if v, _ := k.published(orders); len(v) != 0 || k.stateOf(orders[0]) != tailBuilding {
				t.Fatalf("setup: published %v, state %v; want no view, being built", v, k.stateOf(orders[0]))
			}
			var once sync.Once
			s.tailsFreshHook = func(at string) {
				if at == step {
					once.Do(func() { k.followOnce(ctx, time.Minute) })
				}
			}
			start := time.Now()
			_, _, _, ok, missing := s.statementTailsFresh(ctx, k, orders, orders, 10*time.Second)
			if !ok || missing != "" {
				t.Fatalf("ok %v, missing %q: the statement read the snapshot alone, with no reason, while its tail was published", ok, missing)
			}
			if took := time.Since(start); took > 2*time.Second {
				t.Fatalf("waited %v for a tail already published", took)
			}
		})
	}
}

package consoleapp

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// #3086: what the port asks of a statement's tails reaches the copy's run.
func TestPGRunContext_3086(t *testing.T) {
	type key struct{}
	base := context.WithValue(context.Background(), key{}, "kept")
	if got := pgRunContext(base, pgshim.RunOptions{Probe: true}); got != base {
		t.Error("a probe runs under another context")
	}
	// #3116: a statement answered to the client is marked so, a probe not.
	if got := pgRunContext(base, pgshim.RunOptions{}); !console.PGPortStatement(got) || console.PGNeedsTails(got) || console.PGNoOlderTails(got) {
		t.Error("an unmarked answer to the client is not marked as the port's statement alone")
	}
	got := pgRunContext(base, pgshim.RunOptions{NeedTails: true})
	if !console.PGNeedsTails(got) {
		t.Error("a statement that needs its tails is not marked so")
	}
	if got.Value(key{}) != "kept" {
		t.Error("the statement's own context was lost")
	}
	if console.PGNoOlderTails(got) {
		t.Error("a statement marked only for the snapshot alone is marked for older changes too")
	}
	// #3116: the two refusals are marked apart, so a heavy statement can be
	// held to the first and not to the second.
	older := pgRunContext(base, pgshim.RunOptions{NoOlderTails: true})
	if !console.PGNoOlderTails(older) || console.PGNeedsTails(older) {
		t.Errorf("NoOlderTails alone: marked older=%v snapshot=%v, want true and false", console.PGNoOlderTails(older), console.PGNeedsTails(older))
	}
}

// #3116: a heavy statement the copy answered with older data is counted in
// /status by kind, apart from the reasons.
func TestPGRouteStatsCountsHeavyOlder_3116(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame, HeavyOlder: console.PGOlderTailsNotReady})
	r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame, HeavyOlder: console.PGOlderThanEarlier})
	r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame, HeavyOlder: console.PGOlderThanEarlier})
	r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame})
	n := r.doc("").Servers["s1"]
	if n.HeavyOlderOnCopy[console.PGOlderTailsNotReady] != 1 || n.HeavyOlderOnCopy[console.PGOlderThanEarlier] != 2 || len(n.HeavyOlderOnCopy) != 2 {
		t.Errorf("heavy_older_on_copy %v", n.HeavyOlderOnCopy)
	}
	if n.Reasons[pgshim.ReasonAnswersTheSame] != 4 {
		t.Errorf("reasons %v: the count apart must not change them", n.Reasons)
	}
	if got := r.doc("").Servers["s2"].HeavyOlderOnCopy; got != nil {
		t.Errorf("a server with none: %v", got)
	}
}

package consoleapp

import (
	"maps"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// What the copy's join check did is counted on /status by outcome, and only
// when there is one to count.
func TestPGRouteStats_countsJoinChecks(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	for _, jc := range []string{pgshim.JoinCheckPlanned, pgshim.JoinCheckPlanned, pgshim.JoinCheckWrittenOrder, pgshim.JoinCheckNoStatistics, pgshim.JoinCheckCouldNotRun, ""} {
		r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame, JoinCheck: jc})
	}
	r.note("s2", pgshim.RouteNote{Side: pgshim.RouteSource, Reason: pgshim.ReasonJoinTooBig})
	d := r.counts("")
	want := map[string]uint64{pgshim.JoinCheckPlanned: 2, pgshim.JoinCheckWrittenOrder: 1, pgshim.JoinCheckNoStatistics: 1, pgshim.JoinCheckCouldNotRun: 1}
	if got := d.Servers["s1"].JoinChecks; !maps.Equal(got, want) {
		t.Errorf("s1: %v, want %v", got, want)
	}
	if got := d.Servers["s2"]; got.JoinChecks != nil || got.Reasons[pgshim.ReasonJoinTooBig] != 1 {
		t.Errorf("s2: %+v", got)
	}
}

// One JoinStats per server and forwarding account: a changed account reads
// its own.
func TestPGJoinStats_perServerAndAccount(t *testing.T) {
	var p pgJoinStats
	a := p.of("s1", "postgres://a@h/db", nil)
	if p.of("s1", "postgres://a@h/db", nil) != a {
		t.Error("the same server and account: another JoinStats")
	}
	if p.of("s1", "postgres://b@h/db", nil) == a || p.of("s2", "postgres://a@h/db", nil) == a {
		t.Error("another account or server shares the counts")
	}
	if a.Read == nil || a.Server != "s1" {
		t.Errorf("%+v", a)
	}
}

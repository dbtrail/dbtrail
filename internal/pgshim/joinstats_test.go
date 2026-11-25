package pgshim

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

func TestParseJoinStats(t *testing.T) {
	for _, tc := range []struct {
		cell string
		want map[string]float64
		err  bool
	}{
		{`{"c_w_id": 200, "o_id": 3000.5}`, map[string]float64{"c_w_id": 200, "o_id": 3000.5}, false},
		// The query lowercases; a name it did not is lowercased here too.
		{`{"C_W_ID": 200}`, map[string]float64{"c_w_id": 200}, false},
		// Nothing a count of distinct values can be: dropped.
		{`{"a": 0, "b": -3, "c": 7}`, map[string]float64{"c": 7}, false},
		// No statistics at all: no check.
		{`{}`, nil, false},
		{`{"a": 0}`, nil, false},
		{``, nil, true},
		{`42`, nil, true},
		{`{"a": "many"}`, nil, true},
		{`[1, 2]`, nil, true},
	} {
		got, err := parseJoinStats([]byte(tc.cell))
		if (err != nil) != tc.err || !maps.Equal(got, tc.want) {
			t.Errorf("%q: %v %v, want %v (error %v)", tc.cell, got, err, tc.want, tc.err)
		}
	}
}

func TestJoinStats_readAgainAfterAWhile(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	js := &JoinStats{now: func() time.Time { return now }}
	if js.current() != nil {
		t.Fatal("statistics before any read")
	}
	if !js.begin() {
		t.Fatal("nothing read yet, and not due")
	}
	// One reader at a time: another connection does not read them too.
	if js.begin() {
		t.Fatal("two reads at once")
	}
	ndv := map[string]float64{"a": 5}
	js.done(ndv, nil)
	if !maps.Equal(js.current(), ndv) || js.begin() {
		t.Fatalf("right after a read: %v", js.current())
	}
	now = now.Add(joinStatsFresh + time.Second)
	// Due again, and still used until a newer read replaces them.
	if !maps.Equal(js.current(), ndv) {
		t.Fatalf("past fresh: %v", js.current())
	}
	if !js.begin() {
		t.Fatal("past fresh, not due")
	}
	// A failed read keeps the last good ones, and is not tried again at
	// once.
	js.done(nil, errTest)
	if !maps.Equal(js.current(), ndv) || js.begin() {
		t.Fatalf("after a failed read: %v", js.current())
	}
	now = now.Add(joinStatsRetry + time.Second)
	if !js.begin() {
		t.Fatal("not tried again after the retry interval")
	}
	js.done(nil, errTest)
	// Too old to use: no check rather than one by stale counts.
	now = now.Add(joinStatsKept)
	if js.current() != nil {
		t.Fatalf("an hour old: %v", js.current())
	}
	// A read that found no statistics: no check.
	if !js.begin() {
		t.Fatal("not due")
	}
	js.done(nil, nil)
	if js.current() != nil {
		t.Fatalf("no statistics: %v", js.current())
	}
}

var errTest = &Error{Code: "42501", Message: "permission denied"}

func TestJoinStats_notForTheCaptureAccountNorWithoutASource(t *testing.T) {
	js := &JoinStats{}
	for name, b := range map[string]*Binding{
		"no forwarding":       {},
		"the capture account": {Forward: &Forward{CaptureAccount: true, JoinStats: js}},
		"no statistics kept":  {Forward: &Forward{}},
	} {
		s := &session{bound: b}
		if s.joinStats() != nil {
			t.Errorf("%s: statistics read", name)
		}
	}
	s := &session{bound: &Binding{Forward: &Forward{JoinStats: js}}}
	if s.joinStats() != js {
		t.Error("a forwarding account: no statistics")
	}
}

// Through the port: the statistics are read once on the connection to the
// source, while it is open for the plan, and every statement the copy runs
// on a connection that can fall back is given them.
func TestRoute_joinStatisticsReachTheCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := startFakeSourceWith(t, &fakeSource{plans: costPlans, ndvCell: `{"c_w_id": 200, "s_w_id": 200}`})
	js := &JoinStats{}
	l := &costLogin{src: src, joinStats: js}
	c := costPortConn(t, ctx, l)
	for range 2 {
		if got := costSimple(ctx, c, l, "SELECT sum(n) FROM big"); got != (costOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}) {
			t.Fatalf("a report: %+v", got)
		}
	}
	want := map[string]float64{"c_w_id": 200, "s_w_id": 200}
	for i, got := range l.joinNDVs() {
		if !maps.Equal(got, want) {
			t.Errorf("statement %d: %v", i, got)
		}
	}
	// A second connection of the same server: read already.
	c2 := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c2, l, "SELECT sum(n) FROM big"); got.side != RouteCopy {
		t.Fatalf("the second connection: %+v", got)
	}
	if n := src.statsRead(); n != 1 {
		t.Errorf("the statistics were read %d times, want once", n)
	}
	// A statement the source answers does not read them.
	if got := costSimple(ctx, c, l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource || src.statsRead() != 1 {
		t.Errorf("a cheap one: %+v, %d reads", got, src.statsRead())
	}
}

func TestRoute_joinStatisticsNotKept(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := startFakeSourceWith(t, &fakeSource{plans: costPlans, ndvCell: `{"c_w_id": 200}`})
	l := &costLogin{src: src}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, "SELECT sum(n) FROM big"); got.side != RouteCopy {
		t.Fatalf("%+v", got)
	}
	if ndv := l.joinNDVs(); len(ndv) != 1 || ndv[0] != nil || src.statsRead() != 0 {
		t.Errorf("no statistics kept for the server: %v, %d reads", ndv, src.statsRead())
	}
}

// Statistics the source will not give: the statement runs on the copy as it
// did before the check, and the read is not tried on every statement.
func TestRoute_joinStatisticsRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, cell := range []string{"error 42501", "not json"} {
		src := startFakeSourceWith(t, &fakeSource{plans: costPlans, ndvCell: cell})
		l := &costLogin{src: src, joinStats: &JoinStats{}}
		c := costPortConn(t, ctx, l)
		for range 2 {
			if got := costSimple(ctx, c, l, "SELECT sum(n) FROM big"); got != (costOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}) {
				t.Fatalf("%s: %+v", cell, got)
			}
		}
		for i, ndv := range l.joinNDVs() {
			if ndv != nil {
				t.Errorf("%s, statement %d: %v", cell, i, ndv)
			}
		}
		if n := src.statsRead(); n != 1 {
			t.Errorf("%s: read %d times", cell, n)
		}
	}
}

// The copy refuses a statement whose joins would explode in either order:
// the source answers it, counted under its own reason.
func TestRoute_joinTooBigGoesToTheSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := startFakeSourceWith(t, &fakeSource{plans: costPlans, ndvCell: `{"c_w_id": 200}`})
	l := &costLogin{src: src, joinStats: &JoinStats{}, copyErr: func(string) error {
		return &sqlsandbox.JoinTooBigError{Reason: "a join of the statement on the copy is estimated far past its inputs"}
	}}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, "SELECT sum(n) FROM big"); got != (costOutcome{"42", "", RouteSource, ReasonJoinTooBig}) {
		t.Fatalf("%+v", got)
	}
}

// Where nothing can go to the source the copy's error is the client's.
func TestFreeError_joinTooBig(t *testing.T) {
	perr := freeError(&sqlsandbox.JoinTooBigError{Reason: "a join of the statement on the copy is estimated far past its inputs"}, discardLogger())
	if perr.code != "54000" || perr.msg != "a join of the statement on the copy is estimated far past its inputs" {
		t.Errorf("%+v", perr)
	}
}

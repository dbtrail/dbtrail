package pgshim

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

func TestParseJoinStats(t *testing.T) {
	for _, tc := range []struct {
		cell string
		want JoinTables
		err  bool
	}{
		{`{"public.stock": {"s_w_id": 200, "s_i_id": 100000.5}}`, JoinTables{"public.stock": {"s_w_id": 200, "s_i_id": 100000.5}}, false},
		// The query lowercases; a name it did not is lowercased here too.
		{`{"Public.Stock": {"S_W_ID": 200}}`, JoinTables{"public.stock": {"s_w_id": 200}}, false},
		// Nothing a count of distinct values can be: dropped, and a table
		// left with none too.
		{`{"public.a": {"x": 0, "y": -3, "z": 7}, "public.b": {"x": 0}}`, JoinTables{"public.a": {"z": 7}}, false},
		// No statistics at all: no check.
		{`{}`, nil, false},
		{``, nil, true},
		{`42`, nil, true},
		{`{"public.a": {"x": "many"}}`, nil, true},
		{`{"c_w_id": 200}`, nil, true},
	} {
		got, err := ParseJoinStats([]byte(tc.cell))
		if (err != nil) != tc.err || !maps.EqualFunc(got, tc.want, maps.Equal) {
			t.Errorf("%q: %v %v, want %v (error %v)", tc.cell, got, err, tc.want, tc.err)
		}
	}
}

// fakeReads is a JoinStats.Read a test drives: each read answers the next
// of answers (the last one again past them), and counts.
type fakeReads struct {
	mu      sync.Mutex
	answers []fakeRead
	n       int
}

type fakeRead struct {
	tables JoinTables
	err    error
}

func (f *fakeReads) read(context.Context) (JoinTables, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := f.answers[min(f.n, len(f.answers)-1)]
	f.n++
	return a.tables, a.err
}

func (f *fakeReads) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

// syncJoinStats is a JoinStats whose reads run at once, in the caller, on a
// clock the test moves, logging to log.
func syncJoinStats(reads *fakeReads, now *time.Time, log *bytes.Buffer) *JoinStats {
	return &JoinStats{Read: reads.read, Server: "orders", Logger: slog.New(slog.NewTextHandler(log, nil)),
		now: func() time.Time { return *now }, spawn: func(f func()) { f() }}
}

var stock = JoinTables{"public.stock": {"s_w_id": 200}}

func TestJoinStats_readInTheBackgroundAndAgainAfterAWhile(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	reads := &fakeReads{answers: []fakeRead{{tables: stock}}}
	var log bytes.Buffer
	js := syncJoinStats(reads, &now, &log)
	// The first statement starts the read and is not given counts: the read
	// is not its to wait for.
	if js.current() != nil || reads.count() != 1 {
		t.Fatalf("the first statement: %d reads", reads.count())
	}
	if !maps.EqualFunc(js.current(), stock, maps.Equal) || reads.count() != 1 {
		t.Fatalf("the next one: %d reads", reads.count())
	}
	now = now.Add(joinStatsFresh + time.Second)
	if !maps.EqualFunc(js.current(), stock, maps.Equal) || reads.count() != 2 {
		t.Fatalf("past fresh: %d reads", reads.count())
	}
	if log.Len() != 0 {
		t.Errorf("logged: %s", log.String())
	}
}

// A read that fails or finds nothing is said once, tried later and later,
// and the last good counts are kept until they are too old, which is said
// too.
func TestJoinStats_failedReadsBackOffAndSayIt(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	reads := &fakeReads{answers: []fakeRead{{tables: stock}, {err: errors.New("password authentication failed")}}}
	var log bytes.Buffer
	js := syncJoinStats(reads, &now, &log)
	js.current()
	now = now.Add(joinStatsFresh + time.Second)
	if !maps.EqualFunc(js.current(), stock, maps.Equal) || reads.count() != 2 {
		t.Fatalf("a failed read keeps the counts: %d reads", reads.count())
	}
	// Waits 1, 2, 4, 8, then 10 minutes between tries.
	for i, wait := range []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 10 * time.Minute, 10 * time.Minute} {
		before := reads.count()
		now = now.Add(wait - time.Second)
		js.current()
		if reads.count() != before {
			t.Fatalf("retry %d: tried before %s", i, wait)
		}
		now = now.Add(time.Second)
		js.current()
		if reads.count() != before+1 {
			t.Fatalf("retry %d: not tried after %s", i, wait)
		}
	}
	if n := strings.Count(log.String(), "cause=read_failed"); n != 1 {
		t.Errorf("the failure said %d times: %s", n, log.String())
	}
	// Past an hour since the good read: no counts, said once.
	now = now.Add(joinStatsKept)
	if js.current() != nil || js.current() != nil {
		t.Error("counts past joinStatsKept")
	}
	if n := strings.Count(log.String(), "cause=too_old"); n != 1 {
		t.Errorf("too old said %d times: %s", n, log.String())
	}
	// A good read again: counts, said, and a later failure is said again.
	reads.mu.Lock()
	reads.answers = []fakeRead{{tables: stock}}
	reads.n = 0
	reads.mu.Unlock()
	now = now.Add(joinStatsRetryMax)
	js.current()
	if !maps.EqualFunc(js.current(), stock, maps.Equal) || !strings.Contains(log.String(), "join check is on") {
		t.Fatalf("after recovering: %s", log.String())
	}
}

func TestJoinStats_noStatisticsIsSaid(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	reads := &fakeReads{answers: []fakeRead{{tables: nil}}}
	var log bytes.Buffer
	js := syncJoinStats(reads, &now, &log)
	js.current()
	if js.current() != nil || reads.count() != 1 {
		t.Fatalf("no statistics: %d reads", reads.count())
	}
	if !strings.Contains(log.String(), "cause=none") {
		t.Errorf("not said: %s", log.String())
	}
	now = now.Add(joinStatsRetry)
	js.current()
	if reads.count() != 2 {
		t.Errorf("not tried again after a minute: %d", reads.count())
	}
}

// Only the tables a statement reads, and a column two of them hold left out.
func TestJoinTables_ofTheStatementsTables(t *testing.T) {
	tables := JoinTables{
		"public.stock":    {"s_w_id": 200, "w_id": 200},
		"public.customer": {"c_w_id": 200, "w_id": 300},
		"other.stock":     {"s_w_id": 5},
		"public.item":     {"i_id": 100000},
	}
	for _, tc := range []struct {
		name  string
		names [][2]string
		all   bool
		want  map[string]float64
	}{
		// stock with no schema is public.stock and other.stock: s_w_id is
		// in both, w_id in public.stock and public.customer.
		{"two tables, a shared name left out", [][2]string{{"", "stock"}, {"public", "customer"}}, false, map[string]float64{"c_w_id": 200}},
		{"one schema", [][2]string{{"public", "stock"}, {"public", "customer"}}, false, map[string]float64{"s_w_id": 200, "c_w_id": 200}},
		{"a table joined to itself", [][2]string{{"public", "item"}, {"public", "item"}}, false, map[string]float64{"i_id": 100000}},
		{"a table the source has no counts for", [][2]string{{"public", "nope"}, {"public", "item"}}, false, map[string]float64{"i_id": 100000}},
		{"nothing known", [][2]string{{"public", "nope"}, {"public", "nada"}}, false, nil},
		{"a statement not read: every table", nil, true, map[string]float64{"c_w_id": 200, "i_id": 100000}},
	} {
		got := tables.joinNDVOf(tc.names, tc.all)
		if !maps.Equal(got, tc.want) {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

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

// joinPlans are the source's plans of the join statements: expensive, so the
// copy answers them.
var joinPlans = func() map[string]string {
	p := maps.Clone(costPlans)
	p["FROM big a JOIN big b"] = plan(`{"Node Type": "Aggregate", "Total Cost": 98367, "Plan Rows": 1, "Plans": [{"Node Type": "Hash Join", "Total Cost": 98000, "Plan Rows": 2400000, "Plans": [
		{"Node Type": "Seq Scan", "Total Cost": 40000, "Plan Rows": 2400000},
		{"Node Type": "Hash", "Total Cost": 40000, "Plan Rows": 2400000, "Plans": [{"Node Type": "Seq Scan", "Total Cost": 40000, "Plan Rows": 2400000}]}]}]}`)
	return p
}()

const bigJoin = "SELECT count(*) FROM big a JOIN big b ON a.n = b.n"

var bigStats = JoinTables{"public.big": {"n": 5}}

// Through the port: the counts are read on the host's own connection, the
// statement's own tables are given to the copy, a statement that reads one
// table asks no plan, and each outcome is counted.
func TestRoute_joinStatisticsReachTheCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := startFakeSource(t, joinPlans)
	reads := &fakeReads{answers: []fakeRead{{tables: bigStats}}}
	js := &JoinStats{Read: reads.read, spawn: func(f func()) { f() }}
	l := &costLogin{src: src, joinStats: js}
	c := costPortConn(t, ctx, l)
	for i, want := range []string{JoinCheckNoStatistics, JoinCheckPlanned} {
		if got := costSimple(ctx, c, l, bigJoin); got != (costOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}) {
			t.Fatalf("the join %d: %+v", i, got)
		}
		if got := l.last().JoinCheck; got != want {
			t.Errorf("the join %d: counted %q, want %q", i, got, want)
		}
	}
	runs := l.joinRuns()
	if runs[0].ndv != nil || !maps.Equal(runs[1].ndv, map[string]float64{"n": 5}) {
		t.Errorf("given to the copy: %v", runs)
	}
	// One table: no check, nothing counted, nothing read.
	if got := costSimple(ctx, c, l, "SELECT sum(n) FROM big"); got.side != RouteCopy || l.last().JoinCheck != "" {
		t.Errorf("one table: %+v %q", got, l.last().JoinCheck)
	}
	if runs := l.joinRuns(); runs[len(runs)-1].ndv != nil {
		t.Errorf("one table given counts: %v", runs[len(runs)-1].ndv)
	}
	// Read once, on the host's own connection: nothing of it on the
	// client's connection to the source.
	if reads.count() != 1 {
		t.Errorf("%d reads", reads.count())
	}
	for _, q := range src.ran() {
		if strings.Contains(q, "pg_stats") {
			t.Errorf("read on the client's connection: %s", q)
		}
	}
}

// With routing by plan off the check is on all the same: the counts do not
// come from the plan's path.
func TestRoute_joinCheckWithoutRoutingByPlan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := startFakeSource(t, joinPlans)
	reads := &fakeReads{answers: []fakeRead{{tables: bigStats}}}
	l := &costLogin{src: src, cost: &CostPolicy{}, joinStats: &JoinStats{Read: reads.read, spawn: func(f func()) { f() }}}
	c := costPortConn(t, ctx, l)
	costSimple(ctx, c, l, bigJoin)
	costSimple(ctx, c, l, bigJoin)
	if runs := l.joinRuns(); len(runs) != 2 || !maps.Equal(runs[1].ndv, map[string]float64{"n": 5}) || len(src.explained()) != 0 {
		t.Errorf("runs %v, %d plans asked", runs, len(src.explained()))
	}
}

// The copy refuses a statement whose joins would explode in either order:
// the source answers it, counted under its own reason.
func TestRoute_joinTooBigGoesToTheSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := startFakeSource(t, joinPlans)
	l := &costLogin{src: src, joinStats: &JoinStats{Read: (&fakeReads{answers: []fakeRead{{tables: bigStats}}}).read, spawn: func(f func()) { f() }},
		copyErr: func(string) error {
			return &sqlsandbox.JoinTooBigError{Reason: "a join of the statement on the copy is estimated far past its inputs"}
		}}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, bigJoin); got != (costOutcome{"42", "", RouteSource, ReasonJoinTooBig}) {
		t.Fatalf("%+v", got)
	}
}

// A prepared statement: its Describe (NULL arguments, LIMIT 0) is not
// checked, so it neither fails nor keeps the statement on the source; each
// Execute is checked with its values, and one the copy refuses goes to the
// source, the next one tried on the copy again.
func TestRoute_joinCheckOnAPreparedStatement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := startFakeSource(t, joinPlans)
	l := &costLogin{src: src, joinStats: &JoinStats{Read: (&fakeReads{answers: []fakeRead{{tables: bigStats}}}).read, spawn: func(f func()) { f() }}}
	l.joinStats.current()
	l.copyErr = func(st string) error {
		if strings.Contains(st, "dbtrail_describe") {
			return nil
		}
		return &sqlsandbox.JoinTooBigError{Reason: "estimated far past its inputs"}
	}
	c := costPortConn(t, ctx, l)
	if _, err := c.Prepare(ctx, "j", bigJoin+" WHERE a.n = $1", nil); err != nil {
		t.Fatalf("the Describe: %v", err)
	}
	for i := range 2 {
		before := l.runs.Load()
		res := c.ExecPrepared(ctx, "j", [][]byte{[]byte("0")}, nil, nil).Read()
		if res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "42" {
			t.Fatalf("execute %d: %v %v", i, res.Err, res.Rows)
		}
		if l.runs.Load() == before {
			t.Errorf("execute %d: not tried on the copy: %+v, plans asked %q", i, l.last(), src.explained())
		}
	}
	for _, r := range l.joinRuns() {
		if strings.Contains(r.statement, "dbtrail_describe") && r.ndv != nil {
			t.Errorf("a Describe given counts: %s", r.statement)
		}
	}
}

// Where nothing can go to the source the copy's error is the client's.
func TestFreeError_joinTooBig(t *testing.T) {
	perr := freeError(&sqlsandbox.JoinTooBigError{Reason: "a join of the statement on the copy is estimated far past its inputs"}, discardLogger())
	if perr.code != "54000" || perr.msg != "a join of the statement on the copy is estimated far past its inputs" {
		t.Errorf("%+v", perr)
	}
}

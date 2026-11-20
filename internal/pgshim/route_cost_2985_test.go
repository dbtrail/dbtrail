package pgshim

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// #2985: what a load of 50 clients found in routing by plan. Every statement
// the copy can answer now asks the source for its plan, so every one needs a
// connection to the source: past the port's limit they were refused (53300)
// instead of waiting for one. And a statement answered by the source, then
// one of the same shape by the copy a second or two behind it, read a
// counter go back.

func costDSN(addr string) string {
	return fmt.Sprintf("host=%s port=%s user=orders password=%s sslmode=disable", hostOf(addr), portOf(addr), portPassword)
}

// 50 clients over 16 connections to the source: each statement waits for one
// to be given back, and none is refused.
func TestRouteByCost_2985_manyClientsWaitForASourceSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const clients, slots, each = 50, 16, 20
	src := startFakeSourceSlow(t, costPlans, 2*time.Millisecond)
	l := &costLogin{src: src, slots: make(chan struct{}, slots)}
	addr := serveAddrCfg(t, Config{Login: l.login})
	var wg sync.WaitGroup
	var failed atomic.Int32
	errs := make(chan string, 10)
	report := func(msg string) {
		failed.Add(1)
		select {
		case errs <- msg:
		default:
		}
	}
	for i := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := pgconn.Connect(ctx, costDSN(addr))
			if err != nil {
				report("connect: " + err.Error())
				return
			}
			defer c.Close(context.Background())
			for j := range each {
				res, err := c.Exec(ctx, fmt.Sprintf("SELECT n FROM t WHERE id = %d", i*each+j)).ReadAll()
				var pe *pgconn.PgError
				switch {
				case errors.As(err, &pe):
					report(pe.Code + ": " + pe.Message)
				case err != nil:
					report(err.Error())
				case len(res) != 1 || len(res[0].Rows) != 1 || string(res[0].Rows[0][0]) != "42":
					report(fmt.Sprintf("answer %v, want the source's 42", res))
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if n := failed.Load(); n != 0 {
		t.Errorf("%d of %d statements failed", n, clients*each)
	}
	if m := l.maxHeld.Load(); m > slots {
		t.Errorf("%d connections to the source were held at once, over the limit of %d", m, slots)
	}
	if m := l.maxHeld.Load(); m < slots {
		t.Logf("at most %d connections were held at once: the clients did not contend for all %d", m, slots)
	}
}

// A statement that finds every connection taken waits for one as long as its
// statement_timeout lets it, then gets 53300 with the limit's name.
func TestRouteByCost_2985_aWaitThatRunsOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	src := startFakeSource(t, costPlans)
	l := &costLogin{src: src, slots: make(chan struct{}, 1)}
	addr := serveAddrCfg(t, Config{Login: l.login})
	a, err := pgconn.Connect(ctx, costDSN(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	b, err := pgconn.Connect(ctx, costDSN(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(context.Background())
	// a's block holds the one connection to the source.
	if _, err := a.Exec(ctx, "BEGIN").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Exec(ctx, "SET statement_timeout = '300ms'").ReadAll(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = b.Exec(ctx, "SELECT n FROM t WHERE id = 1").ReadAll()
	took := time.Since(start)
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "53300" || !strings.Contains(pe.Message, "--max-source-connections") || !strings.Contains(pe.Message, "waited") {
		t.Fatalf("with every connection taken: %v, want 53300 naming the limit and the wait", err)
	}
	if took < 250*time.Millisecond {
		t.Errorf("refused after %v: it did not wait for its statement_timeout", took)
	}
	// Once a's block ends, b's statement gets the connection.
	if _, err := a.Exec(ctx, "COMMIT").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if res, err := b.Exec(ctx, "SELECT n FROM t WHERE id = 1").ReadAll(); err != nil || len(res) != 1 || string(res[0].Rows[0][0]) != "42" {
		t.Errorf("after the block ended: %v %v", res, err)
	}
}

// copyAgeKnob is a copy whose RunAge a test sets: noTable for a run that read
// no table, snapshotAlone for one that read a snapshot without its changes,
// otherwise the instant its data reaches.
type copyAgeKnob struct{ v atomic.Int64 }

const (
	noTable       = -1
	snapshotAlone = 0
)

func (k *copyAgeKnob) at(t time.Time) { k.v.Store(t.UnixNano()) }
func (k *copyAgeKnob) set(v int64)    { k.v.Store(v) }
func (k *copyAgeKnob) age() CopyAge {
	switch v := k.v.Load(); v {
	case noTable:
		return CopyAge{}
	case snapshotAlone:
		return CopyAge{Read: true}
	default:
		return CopyAge{Read: true, Through: time.Unix(0, v)}
	}
}

// Once the source answered a connection, the copy does not answer it with
// data older than that answer: the statement goes to the source
// (copy_too_old), and comes back to the copy once the copy has caught up.
func TestRouteByCost_2985_noReadGoesBackAfterTheSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	knob := &copyAgeKnob{}
	src := startFakeSource(t, costPlans)
	l := &costLogin{src: src, age: knob.age}
	c := costPortConn(t, ctx, l)
	const report = "SELECT sum(n) FROM big"
	onCopy := costOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}
	tooOld := costOutcome{"42", "", RouteSource, ReasonCopyTooOld}

	// Before the source answered anything, the copy answers, however old.
	knob.at(time.Now().Add(-time.Hour))
	if got := costSimple(ctx, c, l, report); got != onCopy {
		t.Errorf("before any answer of the source: %+v, want %+v", got, onCopy)
	}
	if got := costSimple(ctx, c, l, "SELECT n FROM t WHERE id = 1"); got != (costOutcome{"42", "", RouteSource, ReasonCheapPlan}) {
		t.Fatalf("the read by key: %+v", got)
	}
	for _, tc := range []struct {
		name string
		set  func()
		want costOutcome
	}{
		{"the copy a second behind the source's answer", func() { knob.at(time.Now().Add(-time.Second)) }, tooOld},
		{"a snapshot alone, whose reach is not known", func() { knob.set(snapshotAlone) }, tooOld},
		{"a run that read no table", func() { knob.set(noTable) }, onCopy},
		{"the copy caught up", func() { knob.at(time.Now().Add(time.Second)) }, onCopy},
	} {
		tc.set()
		if got := costSimple(ctx, c, l, report); got != tc.want {
			t.Errorf("%s: %+v, want %+v", tc.name, got, tc.want)
		}
	}
	// The source's answer that the copy was too old for is one more answer
	// of the source: the copy must reach past it too.
	sent := time.Now()
	if got := costSimple(ctx, c, l, "SELECT n FROM t WHERE id = 2"); got.side != RouteSource {
		t.Fatalf("the second read by key: %+v", got)
	}
	// Through the instant it was sent: its answer, which ended later, may
	// hold a change committed after that.
	knob.at(sent)
	if got := costSimple(ctx, c, l, report); got != tooOld {
		t.Errorf("the copy just behind the last answer: %+v, want %+v", got, tooOld)
	}

	// Prepared: an execution the copy is too old for goes to the source, and
	// the statement is not kept there: once the copy caught up it answers.
	if _, err := c.Prepare(ctx, "rep", report, nil); err != nil {
		t.Fatal(err)
	}
	knob.at(time.Now().Add(-time.Second))
	if res := c.ExecPrepared(ctx, "rep", nil, nil, nil).Read(); res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "42" {
		t.Errorf("prepared, the copy behind: %v %q, want the source's 42", res.Err, res.Rows)
	}
	if n := l.last(); n.Side != RouteSource || n.Reason != ReasonCopyTooOld {
		t.Errorf("prepared, the copy behind: noted %s/%s", n.Side, n.Reason)
	}
	knob.at(time.Now().Add(time.Hour))
	if res := c.ExecPrepared(ctx, "rep", nil, nil, nil).Read(); res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "7" {
		t.Errorf("prepared, the copy caught up: %v %q, want the copy's 7", res.Err, res.Rows)
	}
	if c.TxStatus() != 'I' {
		t.Errorf("status %c", c.TxStatus())
	}

	// A connection the source never answered: the copy answers, whatever its
	// age.
	d := costPortConn(t, ctx, l)
	knob.set(snapshotAlone)
	if got := costSimple(ctx, d, l, report); got != onCopy {
		t.Errorf("another connection: %+v, want %+v", got, onCopy)
	}
	// Nor one the source only answered with an error: the client was given
	// no value to go back from.
	if got := costSimple(ctx, d, l, "SELECT count(*) FROM nosuch"); got.code != "42P01" || got.side != RouteSource {
		t.Fatalf("a table neither side has: %+v", got)
	}
	if got := costSimple(ctx, d, l, report); got != onCopy {
		t.Errorf("after the source's error: %+v, want %+v", got, onCopy)
	}
}

// The verdict on a statement shape is remembered for a while, only when it
// was read from a plan far from the thresholds over a statement whose values
// are compared with =.
func TestRouteByCost_2985_remembersAShape(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	plans := maps.Clone(costPlans)
	plans["FROM huge"] = plan(`{"Node Type": "Aggregate", "Total Cost": 13351258, "Plan Rows": 1, "Plans": [{"Node Type": "Seq Scan", "Total Cost": 13000000, "Plan Rows": 61000000}]}`)
	src := startFakeSource(t, plans)
	l := &costLogin{src: src}
	c := costPortConn(t, ctx, l)
	asked := func(name string, want int, run func()) {
		t.Helper()
		before := len(src.explained())
		run()
		if got := len(src.explained()) - before; got != want {
			t.Errorf("%s: asked the source %d times, want %d", name, got, want)
		}
	}
	runs := func(want costOutcome, stmts ...string) func() {
		return func() {
			for _, s := range stmts {
				if got := costSimple(ctx, c, l, s); got != want {
					t.Errorf("%s: %+v, want %+v", s, got, want)
				}
			}
		}
	}
	cheap := costOutcome{"42", "", RouteSource, ReasonCheapPlan}
	onCopy := costOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}
	asked("the same read by key, five keys", 1, runs(cheap,
		"SELECT n FROM t WHERE id = 1", "SELECT n FROM t WHERE id = 2", "select n from t where id = '3'", "SELECT n FROM t WHERE id = 4", "SELECT n FROM t WHERE id=5"))
	asked("a value not compared with =", 2, runs(cheap, "SELECT n FROM t WHERE id = 1 LIMIT 5", "SELECT n FROM t WHERE id = 1 LIMIT 5"))
	asked("a plan expensive by a margin under ten", 2, runs(onCopy, "SELECT sum(n) FROM big", "SELECT sum(n) FROM big"))
	asked("a plan ten times over", 1, runs(onCopy, "SELECT sum(n) FROM huge", "SELECT sum(n) FROM huge"))
	asked("a plan refused for the moment", 2, runs(costOutcome{"42", "", RouteSource, ReasonExplainFailed}, "SELECT count(*) FROM locked", "SELECT count(*) FROM locked"))
	asked("a statement the source refuses", 2, runs(costOutcome{"", "42P01", RouteSource, ReasonExplainFailed}, "SELECT count(*) FROM nosuch", "SELECT count(*) FROM nosuch"))
	asked("the remembered shape again", 0, runs(cheap, "SELECT n FROM t WHERE id = 9"))

	// Each verdict for its own policy: kept for Remember, or not at all.
	for _, tc := range []struct {
		name     string
		remember time.Duration
		want     int
	}{
		{"a verdict past Remember", 50 * time.Millisecond, 2},
		{"Remember negative", -1, 3},
	} {
		pol := DefaultCostPolicy()
		pol.Remember = tc.remember
		k := &costLogin{src: startFakeSource(t, costPlans), cost: &pol}
		e := costPortConn(t, ctx, k)
		for i := range 3 {
			if i == 2 {
				time.Sleep(80 * time.Millisecond)
			}
			if got := costSimple(ctx, e, k, fmt.Sprintf("SELECT n FROM t WHERE id = %d", i)); got != cheap {
				t.Errorf("%s, run %d: %+v", tc.name, i, got)
			}
		}
		if got := len(k.src.explained()); got != tc.want {
			t.Errorf("%s: asked %d times over three reads by key, want %d", tc.name, got, tc.want)
		}
	}
}

func TestValuesOnlyEqualed(t *testing.T) {
	for q, want := range map[string]bool{
		"SELECT n FROM t WHERE id = 1":                   true,
		"SELECT n FROM t WHERE id = $1 AND k = 'x'":      true,
		"SELECT n FROM t":                                true,
		"SELECT n FROM t WHERE id > 1":                   false,
		"SELECT n FROM t WHERE id >= 1":                  false,
		"SELECT n FROM t WHERE id BETWEEN 1 AND 9":       false,
		"SELECT n FROM t WHERE id IN (1, 2)":             false,
		"SELECT n FROM t WHERE id = 1 LIMIT 10":          false,
		"SELECT n FROM t WHERE id = -1":                  false,
		"SELECT 1 FROM t WHERE id = 1":                   false,
		"SELECT n FROM t WHERE k LIKE 'a%'":              false,
		"SELECT n FROM t WHERE k = $$x$$":                false,
		"SELECT n FROM t WHERE id = 1 OFFSET 5":          false,
		"SELECT n FROM t WHERE id = 1 AND d = now() - 1": false,
	} {
		if got := valuesOnlyEqualed(q); got != want {
			t.Errorf("valuesOnlyEqualed(%q) = %v, want %v", q, got, want)
		}
	}
}

func TestCostPolicyClear(t *testing.T) {
	pol := DefaultCostPolicy()
	for _, tc := range []struct {
		pl   pgPlan
		want bool
	}{
		{pgPlan{cost: 12}, true},
		{pgPlan{cost: 4999}, true},
		{pgPlan{cost: 5000}, false},
		{pgPlan{cost: 15059}, false},
		{pgPlan{cost: 12, maxSeqScanRows: 99999}, true},
		{pgPlan{cost: 12, maxSeqScanRows: 100000}, false},
		{pgPlan{cost: 98367}, false},
		{pgPlan{cost: 499999}, false},
		{pgPlan{cost: 500000}, true},
		{pgPlan{cost: 30000, maxSeqScanRows: 2400000}, false},
		{pgPlan{cost: 30000, maxSeqScanRows: 10000000}, true},
	} {
		if got := pol.clear(tc.pl); got != tc.want {
			t.Errorf("clear(%+v) = %v, want %v", tc.pl, got, tc.want)
		}
	}
	// A rule turned off does not hold a verdict back.
	if !(CostPolicy{ScanRows: 1000}).clear(pgPlan{cost: 1e9, maxSeqScanRows: 10}) {
		t.Error("with the cost rule off, a cheap scan is not kept")
	}
}

// copyWaitKnob is a CopyWait a test steers: reach says whether the copy
// reaches the position (block: it waits until its context ends), and it
// records the positions and deadlines it was given.
type copyWaitKnob struct {
	mu       sync.Mutex
	reach    bool
	block    bool
	none     bool // nothing to wait on
	lsns     []uint64
	deadline []time.Duration
}

func (k *copyWaitKnob) wait(ctx context.Context, lsn uint64) (CopyWaitOutcome, string) {
	k.mu.Lock()
	k.lsns = append(k.lsns, lsn)
	if d, ok := ctx.Deadline(); ok {
		k.deadline = append(k.deadline, time.Until(d))
	}
	reach, block, none := k.reach, k.block, k.none
	k.mu.Unlock()
	if none {
		return CopyNoWait, "no tails yet"
	}
	if block {
		<-ctx.Done()
		return CopyNotReached, "ran out"
	}
	if !reach {
		return CopyNotReached, "capture is behind"
	}
	return CopyReached, ""
}

func (k *copyWaitKnob) set(reach, block bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.reach, k.block = reach, block
}

func (k *copyWaitKnob) asked() (int, uint64, time.Duration) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.lsns) == 0 {
		return 0, 0, 0
	}
	return len(k.lsns), k.lsns[len(k.lsns)-1], k.deadline[len(k.deadline)-1]
}

// On a connection the source answered, the copy waits for the source's
// position before it runs: reached, it answers whatever its tails' instant
// says (a quiet source's stays at the last commit); not reached, the source
// answers and the copy runs nothing.
func TestRouteByCost_2985_theCopyWaits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	knob := &copyAgeKnob{}
	w := &copyWaitKnob{reach: true}
	src := startFakeSource(t, costPlans)
	l := &costLogin{src: src, age: knob.age, wait: w.wait}
	c := costPortConn(t, ctx, l)
	const report = "SELECT sum(n) FROM big"
	onCopy := costOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}
	tooOld := costOutcome{"42", "", RouteSource, ReasonCopyTooOld}
	wantLSN, _ := parseLSN(fakeWALPosition)

	// Before the source answered: no wait at all.
	knob.at(time.Now().Add(-time.Hour))
	if got := costSimple(ctx, c, l, report); got != onCopy {
		t.Fatalf("before the source answered: %+v", got)
	}
	if n, _, _ := w.asked(); n != 0 || src.positionsRead() != 0 {
		t.Fatalf("waited %d times and read %d positions before the source answered", n, src.positionsRead())
	}
	if got := costSimple(ctx, c, l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource {
		t.Fatalf("the read by key: %+v", got)
	}

	// Reached: the copy answers, its tails' instant an hour old (a quiet
	// source), and the position was read on the connection its plan was.
	opened := l.opened.Load()
	if got := costSimple(ctx, c, l, report); got != onCopy {
		t.Errorf("the copy reached the position: %+v, want %+v", got, onCopy)
	}
	n, lsn, deadline := w.asked()
	if n != 1 || lsn != wantLSN {
		t.Errorf("waited %d times for %x, want once for %x", n, lsn, wantLSN)
	}
	if deadline <= 0 || deadline > copyWaitLimit {
		t.Errorf("the wait's deadline was %v away, want within %v", deadline, copyWaitLimit)
	}
	if d := l.opened.Load() - opened; d != 1 {
		t.Errorf("%d connections to the source for one report, want 1: its position is read where its plan was", d)
	}

	// Reached, but the run read a snapshot alone: still the source.
	knob.set(snapshotAlone)
	if got := costSimple(ctx, c, l, report); got != tooOld {
		t.Errorf("reached, a snapshot alone: %+v, want %+v", got, tooOld)
	}

	// Not reached: the source answers, and the copy ran nothing.
	knob.at(time.Now().Add(-time.Hour))
	w.set(false, false)
	runs := l.runs.Load()
	if got := costSimple(ctx, c, l, report); got != tooOld {
		t.Errorf("not reached: %+v, want %+v", got, tooOld)
	}
	if d := l.runs.Load() - runs; d != 0 {
		t.Errorf("the copy ran %d statements it could not answer", d)
	}

	// Nothing to wait on (the server keeps no recent changes yet): the copy
	// runs, and its answer's age decides, as without the wait.
	w.mu.Lock()
	w.none = true
	w.mu.Unlock()
	knob.at(time.Now().Add(-time.Hour))
	runs = l.runs.Load()
	if got := costSimple(ctx, c, l, report); got != tooOld || l.runs.Load() != runs+1 {
		t.Errorf("nothing to wait on, the copy behind: %+v after %d runs, want %+v after one", got, l.runs.Load()-runs, tooOld)
	}
	knob.at(time.Now().Add(time.Hour))
	if got := costSimple(ctx, c, l, report); got != onCopy {
		t.Errorf("nothing to wait on, the copy past it: %+v, want %+v", got, onCopy)
	}
	w.mu.Lock()
	w.none = false
	w.mu.Unlock()

	// A command of the copy's own is not waited for: the source would not
	// take it.
	knob.set(noTable)
	waits, _, _ := w.asked()
	if got := costSimple(ctx, c, l, "SHOW TABLES"); got.answer != "7" || got.side != RouteCopy {
		t.Errorf("SHOW TABLES: %+v, want the copy's", got)
	}
	if n, _, _ := w.asked(); n != waits {
		t.Errorf("SHOW TABLES waited")
	}
	knob.at(time.Now().Add(-time.Hour))

	// A prepared statement waits at each execution: the first with the
	// position read where its plan was, the next with one read on its own.
	w.set(true, false)
	if _, err := c.Prepare(ctx, "rep2", report, nil); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		before, _, _ := w.asked()
		positions := src.positionsRead()
		if res := c.ExecPrepared(ctx, "rep2", nil, nil, nil).Read(); res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "7" {
			t.Errorf("prepared, run %d, reached: %v %q, want the copy's 7", i, res.Err, res.Rows)
		}
		if after, _, _ := w.asked(); after != before+1 {
			t.Errorf("prepared, run %d: waited %d times, want once", i, after-before)
		}
		if i == 1 && src.positionsRead() != positions+1 {
			t.Errorf("prepared, run %d: read the position %d times, want once", i, src.positionsRead()-positions)
		}
	}

	// The statement's own timeout ends the wait: 57014, and it runs nowhere.
	w.set(false, true)
	if _, err := c.Exec(ctx, "SET statement_timeout = '150ms'").ReadAll(); err != nil {
		t.Fatal(err)
	}
	ran := len(src.ran())
	runs = l.runs.Load()
	start := time.Now()
	got := costSimple(ctx, c, l, report)
	if got.code != "57014" {
		t.Errorf("the statement's timeout while it waited: %+v, want 57014", got)
	}
	if took := time.Since(start); took > copyWaitLimit {
		t.Errorf("waited %v, past the statement's 150ms", took)
	}
	if len(src.ran()) != ran || l.runs.Load() != runs {
		t.Errorf("a statement stopped while it waited ran on the source (%d) or the copy (%d)", len(src.ran())-ran, l.runs.Load()-runs)
	}
	if c.TxStatus() != 'I' {
		t.Errorf("status %c", c.TxStatus())
	}
}

func TestParseLSN(t *testing.T) {
	for in, want := range map[string]uint64{"16/B374D848": 0x16B374D848, "0/0": 0, "FFFFFFFF/FFFFFFFF": 1<<64 - 1} {
		if got, ok := parseLSN(in); !ok || got != want {
			t.Errorf("parseLSN(%q) = %x %v, want %x", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "42", "/1", "1/", "G/1", "100000000/0", "1/2/3"} {
		if _, ok := parseLSN(in); ok {
			t.Errorf("parseLSN(%q) read a position", in)
		}
	}
}

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
		"SELECT n FROM t WHERE id = 1":                    true,
		"SELECT n FROM t WHERE id = $1 AND k = 'x'":       true,
		"SELECT n FROM t":                                 true,
		"SELECT n FROM t WHERE id > 1":                    false,
		"SELECT n FROM t WHERE id >= 1":                   false,
		"SELECT n FROM t WHERE id BETWEEN 1 AND 9":        false,
		"SELECT n FROM t WHERE id IN (1, 2)":              false,
		"SELECT n FROM t WHERE id = 1 LIMIT 10":           false,
		"SELECT n FROM t WHERE id = -1":                   false,
		"SELECT 1 FROM t WHERE id = 1":                    false,
		"SELECT n FROM t WHERE k LIKE 'a%'":               false,
		"SELECT n FROM t WHERE k = $$x$$":                 false,
		"SELECT n FROM t WHERE id = 1 OFFSET 5":           false,
		"SELECT n FROM t WHERE id = 1 AND d = now() - 1":  false,
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

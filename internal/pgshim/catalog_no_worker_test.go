package pgshim

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// heldCatalog is a copy whose one worker is taken: reading its list of
// columns does not come back until release is closed, and reads counts the
// attempts.
type heldCatalog struct {
	reads   atomic.Int32
	release chan struct{}
}

func holdCatalog(t *testing.T, l *costLogin) *heldCatalog {
	t.Helper()
	h := &heldCatalog{release: make(chan struct{})}
	t.Cleanup(func() { close(h.release) })
	l.catalog = func() []CatalogTable { return catalogOf("t", "big") }
	l.catalogErr = func() error {
		h.reads.Add(1)
		<-h.release
		return nil
	}
	return h
}

// answersAtOnce runs f, which must come back well before any wait for a
// worker would end.
func answersAtOnce(t *testing.T, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); f() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("%s waited for the copy's worker", what)
	}
}

// A statement the source answers does not read the copy's list of columns:
// simple, prepared and with parameters.
func TestCatalogNoWorker_aCheapStatementDoesNotAskTheCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	l := &costLogin{src: startFakeSource(t, costPlans), keep: &keptSources{}}
	h := holdCatalog(t, l)
	c := costPortConn(t, ctx, l)
	answersAtOnce(t, "a simple read by key", func() {
		if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
			t.Errorf("%+v", got)
		}
	})
	answersAtOnce(t, "a prepared read by key", func() {
		if _, err := c.Prepare(ctx, "bykey", "SELECT n FROM t WHERE id = $1", nil); err != nil {
			t.Error(err)
			return
		}
		res := c.ExecPrepared(ctx, "bykey", [][]byte{[]byte("1")}, nil, nil).Read()
		if res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "42" {
			t.Errorf("%v %q", res.Err, res.Rows)
		}
	})
	answersAtOnce(t, "a read by key with parameters", func() {
		res := c.ExecParams(ctx, "SELECT n FROM t WHERE id = $1", [][]byte{[]byte("1")}, nil, nil, nil).Read()
		if res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "42" {
			t.Errorf("%v %q", res.Err, res.Rows)
		}
	})
	if n := h.reads.Load(); n != 0 {
		t.Errorf("the copy's list was read %d times for statements the source answers", n)
	}
}

// The capture account's gate asks for the copy's table names on every
// statement: they come without a worker.
func TestCatalogNoWorker_theCaptureGateDoesNotAskAWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	p.l.captureAccount = true
	p.l.names = func() [][2]string { return [][2]string{{"public", "t"}, {"public", "big"}} }
	h := holdCatalog(t, p.l)
	c := costPortConn(t, ctx, p.l)
	answersAtOnce(t, "a read by key on the capture account", func() {
		if got := costSimple(ctx, c, p.l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource || got.answer != "42" {
			t.Errorf("%+v", got)
		}
	})
	if n := h.reads.Load(); n != 0 {
		t.Errorf("the copy's list was read %d times", n)
	}
}

// The gate still refuses what the names do not hold.
func TestCatalogNoWorker_theCaptureGateStillRefuses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	p.l.captureAccount = true
	p.l.names = func() [][2]string { return [][2]string{{"public", "big"}} }
	c := costPortConn(t, ctx, p.l)
	got := costSimple(ctx, c, p.l, "SELECT n FROM t WHERE id = 1")
	if got.side != RouteCopy || got.reason != ReasonCaptureGate {
		t.Errorf("%+v, want the copy under %s: the source was asked about a table the copy does not hold", got, ReasonCaptureGate)
	}
}

// What depends on a column's type is still decided before the copy runs, on
// every path that reaches it.
func TestCatalogNoWorker_aTypedVetoStillKeepsItFromTheCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const typed = "SELECT label FROM big ORDER BY label"
	cat := func() []CatalogTable {
		return []CatalogTable{{Schema: "public", Name: "big", Columns: []CatalogColumn{{Name: "label", Type: "VARCHAR"}, {Name: "n", Type: "INTEGER"}}}}
	}
	plans := map[string]string{"FROM big": costPlans["FROM big"]}
	src := startFakeSource(t, plans)
	if screenForCopy(mustLex(t, typed), screenTypes(cat(), tablesRead(typed, cat()))) == "" {
		t.Fatal("the statement is not one the typed screen vetoes")
	}
	if screenUntyped(mustLex(t, typed)) != "" {
		t.Fatal("the statement is vetoed without its types")
	}
	off := CostPolicy{}
	for _, tc := range []struct {
		name string
		l    *costLogin
	}{
		{"by its plan", &costLogin{src: src, catalog: cat}},
		{"routing by plan off", &costLogin{src: src, catalog: cat, cost: &off}},
		{"the source's connections all taken", &costLogin{src: src, catalog: cat, refuse: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := costPortConn(t, ctx, tc.l)
			got := costSimple(ctx, c, tc.l, typed)
			if n := tc.l.runs.Load(); n != 0 {
				t.Errorf("the copy ran it: %+v", got)
			}
			if got.side != RouteSource || got.reason != ReasonVeto {
				t.Errorf("%+v, want the source as %s", got, ReasonVeto)
			}
			// The same through the extended protocol, with its own decision.
			res := c.ExecParams(ctx, typed, nil, nil, nil, nil).Read()
			if !tc.l.refuse && (res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "42") {
				t.Errorf("with the extended protocol: %v %q, want the source's 42", res.Err, res.Rows)
			}
			if n := tc.l.runs.Load(); n != 0 {
				t.Errorf("with the extended protocol the copy ran it")
			}
			if n := tc.l.last(); n.Side != RouteSource || n.Reason != ReasonVeto {
				t.Errorf("with the extended protocol: noted %s/%s, want the source as %s", n.Side, n.Reason, ReasonVeto)
			}
		})
	}
}

// A statement the plan sends to the copy, with the copy too busy to list its
// columns: refused as busy, not sent to the source (#3109).
func TestCatalogNoWorker_anExpensiveStatementOnABusyCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	l := &costLogin{src: startFakeSource(t, costPlans), catalog: func() []CatalogTable { return catalogOf("big") },
		catalogErr: func() error { return sqlsandbox.ErrBusy }}
	c := costPortConn(t, ctx, l)
	got := costSimple(ctx, c, l, "SELECT sum(n) FROM big")
	if got.code != busyCode || got.side != RouteRefused {
		t.Errorf("%+v, want refused as busy", got)
	}
	if n := l.runs.Load(); n != 0 {
		t.Errorf("the copy ran %d", n)
	}
	// One that names no table: nothing else on the way to the copy asks for
	// the list, so only this refusal keeps it from running there unscreened.
	const noTable = "SELECT 40 + 2"
	m := &costLogin{src: startFakeSource(t, map[string]string{"40 + 2": costPlans["FROM big"]}),
		catalog: func() []CatalogTable { return catalogOf("big") }, catalogErr: func() error { return sqlsandbox.ErrBusy }}
	d := costPortConn(t, ctx, m)
	if got := costSimple(ctx, d, m, noTable); got.code != busyCode || got.side != RouteRefused {
		t.Errorf("a statement that names no table: %+v, want refused as busy", got)
	}
	if n := m.runs.Load(); n != 0 {
		t.Errorf("a statement that names no table: the copy ran %d", n)
	}
}

func mustLex(t *testing.T, q string) []rtToken {
	t.Helper()
	toks, why := lexSQL(RewriteForCopy(q))
	if why != "" {
		t.Fatal(why)
	}
	return toks
}

// A heavy report grouped by a date_trunc of a timestamp column is the copy's:
// its first screen, which knows no types, does not send it to the source.
func TestCatalogNoWorker_dateTruncReachesThePlan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const report = "SELECT date_trunc('month', placed_at), count(*) FROM big GROUP BY 1"
	if why := screenUntyped(mustLex(t, report)); why != "" {
		t.Fatalf("vetoed without its types: %s", why)
	}
	col := func(typ string) func() []CatalogTable {
		return func() []CatalogTable {
			return []CatalogTable{{Schema: "public", Name: "big", Columns: []CatalogColumn{{Name: "placed_at", Type: typ}}}}
		}
	}
	src := startFakeSource(t, map[string]string{"FROM big": costPlans["FROM big"]})
	l := &costLogin{src: src, catalog: col("TIMESTAMP")}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, report); got.side != RouteCopy {
		t.Errorf("of a timestamp: %+v, want the copy", got)
	}
	// Of a date the typed screen still keeps it from the copy.
	d := &costLogin{src: src, catalog: col("DATE")}
	e := costPortConn(t, ctx, d)
	if got := costSimple(ctx, e, d, report); got.side != RouteSource || got.reason != ReasonVeto {
		t.Errorf("of a date: %+v, want the source as %s", got, ReasonVeto)
	}
	// With the extended protocol too, whose decision is its own: nothing
	// later on the way to the copy would catch this one.
	res := e.ExecParams(ctx, report, nil, nil, nil, nil).Read()
	if res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "42" {
		t.Errorf("of a date, with parameters: %v %q, want the source's 42", res.Err, res.Rows)
	}
	if n := d.runs.Load(); n != 0 {
		t.Errorf("of a date: the copy ran it %d times", n)
	}
}

// Told "answered by the copy" only when the copy does answer.
func TestCatalogNoWorker_theUnreachableNoticeIsTrue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cat := func() []CatalogTable {
		return []CatalogTable{{Schema: "public", Name: "big", Columns: []CatalogColumn{{Name: "label", Type: "VARCHAR"}, {Name: "n", Type: "INTEGER"}}}}
	}
	l := &costLogin{src: startFakeSource(t, costPlans), catalog: cat, refuse: true}
	addr := serveAddrCfg(t, Config{Login: l.login})
	var notices []string
	cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=%s port=%s user=orders password=%s sslmode=disable", hostOf(addr), portOf(addr), portPassword))
	if err != nil {
		t.Fatal(err)
	}
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) { notices = append(notices, n.Message) }
	c, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	_, _ = c.Exec(ctx, "SELECT label FROM big ORDER BY label").ReadAll()
	for _, n := range notices {
		if strings.Contains(n, "Answered by the copy") {
			t.Errorf("told %q of a statement the copy did not answer", n)
		}
	}
	notices = nil
	if _, err := c.Exec(ctx, "SELECT sum(n) FROM big").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if len(notices) != 1 || !strings.Contains(notices[0], "Answered by the copy") {
		t.Errorf("the copy answered, told %q", notices)
	}
}

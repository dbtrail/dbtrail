package pgshim

import (
	"context"
	"fmt"
	"maps"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// What the host's copy says in the two fresh-mode cases of #3116 when the
// recent changes a statement would read are older than an earlier answer of
// the copy on the same connection: its refusal (console sql_api.go) and its
// note.
const (
	olderThanSeenReason = "the copy's newest version is older than an earlier answer of the copy on this connection"
	olderThanSeenNote   = "Answered from captured changes older than an earlier answer of the copy on this connection: the copy's newest version is behind it."
)

// strictOlderKind is the host's Binding.OlderKind for the fake copy's note.
func strictOlderKind(note string) string {
	if strings.HasPrefix(note, "Answered from captured changes older than an earlier answer") {
		return "older_than_earlier"
	}
	return ""
}

// strictConn is a connection to l's port that keeps the notices it is sent.
func strictConn(t *testing.T, ctx context.Context, l *costLogin) (*pgconn.PgConn, func() []string) {
	t.Helper()
	addr := serveAddrCfg(t, Config{Login: l.login})
	cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=%s port=%s user=orders password=%s sslmode=disable", hostOf(addr), portOf(addr), portPassword))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var notices []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		mu.Lock()
		notices = append(notices, n.Message)
		mu.Unlock()
	}
	c, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(context.Background()) })
	return c, func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := notices
		notices = nil
		return out
	}
}

// strictRun runs stmt over one of the three ways a client sends it: the
// simple protocol, the extended one with an unnamed statement, and a named
// prepared statement.
func strictRun(ctx context.Context, c *pgconn.PgConn, l *costLogin, how, stmt string) costOutcome {
	var rows [][][]byte
	var err error
	switch how {
	case "simple":
		return costSimple(ctx, c, l, stmt)
	case "extended":
		res := c.ExecParams(ctx, stmt, nil, nil, nil, nil).Read()
		rows, err = res.Rows, res.Err
	case "prepared":
		name := fmt.Sprintf("s%d", time.Now().UnixNano())
		if _, err = c.Prepare(ctx, name, stmt, nil); err == nil {
			res := c.ExecPrepared(ctx, name, nil, nil, nil).Read()
			rows, err = res.Rows, res.Err
		}
	}
	o := costOutcome{}
	if err != nil {
		o.code = err.Error()
	}
	if len(rows) > 0 {
		o.answer = string(rows[0][0])
	}
	n := l.last()
	o.side, o.reason = n.Side, n.Reason
	return o
}

var strictProtocols = []string{"simple", "extended", "prepared"}

// strictPorts are the two kinds of statement the copy runs on a connection
// that can reach its source: a heavy one, whose plan the cost routing found
// expensive (the default policy, a sum over big), and a light one, which the
// copy runs with routing by plan off (a light statement with routing on goes
// to the source by its plan and never reaches the copy).
func strictPorts(t *testing.T) []struct {
	name  string
	heavy bool
	p     *tablesPort
} {
	heavy := newTablesPort(t)
	light := newTablesPort(t)
	light.l.cost = &CostPolicy{}
	for _, p := range []*tablesPort{heavy, light} {
		p.l.olderKind = strictOlderKind
	}
	return []struct {
		name  string
		heavy bool
		p     *tablesPort
	}{{"heavy", true, heavy}, {"light", false, light}}
}

// #3116, case 1: the recent changes a statement would read are older than
// an earlier answer of the copy on this connection. A light statement is
// refused before the copy runs it and the source answers it under
// copy_too_old, as the MySQL router sends it to MySQL. A heavy one stays on
// the copy (product decision, 2026-11-24), answered with the copy's notice.
// Over every protocol. Once the copy is not behind any more, the light one
// is answered by the copy again: the refusal is the moment's, and a
// prepared statement is not kept on the source for it.
func TestStrict_3116_olderThanAnEarlierAnswer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, port := range strictPorts(t) {
		p := port.p
		p.knob.at(time.Now().Add(time.Hour))
		for _, how := range strictProtocols {
			t.Run(port.name+"/"+how, func(t *testing.T) {
				c, notices := strictConn(t, ctx, p.l)
				p.l.olderThanSeen.Store(true)
				runs := p.l.runs.Load()
				got := strictRun(ctx, c, p.l, how, "SELECT sum(n) FROM big")
				p.l.olderThanSeen.Store(false)
				if p.l.lastNoOlderTails() == port.heavy {
					t.Errorf("the copy was asked NoOlderTails=%v", !port.heavy)
				}
				n := notices()
				if want := map[bool]string{true: "older_than_earlier", false: ""}[port.heavy]; p.l.last().HeavyOlder != want {
					t.Errorf("counted as heavy and older %q, want %q", p.l.last().HeavyOlder, want)
				}
				if port.heavy {
					if got != tablesOnCopy {
						t.Errorf("%+v, want the copy's %+v", got, tablesOnCopy)
					}
					if len(n) != 1 || !strings.Contains(n[0], "older than an earlier answer of the copy on this connection") {
						t.Errorf("notices %q, want the copy's note", n)
					}
					return
				}
				if got != tablesTooOld {
					t.Errorf("%+v, want %+v", got, tablesTooOld)
				}
				// Over the extended protocol the copy also describes the
				// statement first, which counts as a run: only the simple
				// one counts the statement's own.
				if d := p.l.runs.Load() - runs; how == "simple" && d != 0 {
					t.Errorf("the copy ran %d statements, want none", d)
				}
				if len(n) != 0 {
					t.Errorf("notices %q with the source's answer", n)
				}
				if got := strictRun(ctx, c, p.l, how, "SELECT sum(n) FROM big"); got != tablesOnCopy {
					t.Errorf("no longer behind: %+v, want %+v", got, tablesOnCopy)
				}
			})
		}
	}
}

// #3116, case 2: a statement whose tables' recent changes are not ready
// after fresh mode's wait, on a connection the source never answered. A
// light one used to be answered from the snapshot alone with only a notice;
// the source answers it now, as the MySQL router does. A heavy one stays on
// the copy, as before #3116. Over every protocol.
func TestStrict_3116_snapshotAlone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, port := range strictPorts(t) {
		p := port.p
		p.l.tailsPending.Store(true)
		for _, how := range strictProtocols {
			t.Run(port.name+"/"+how, func(t *testing.T) {
				c, _ := strictConn(t, ctx, p.l)
				runs := p.l.runs.Load()
				got := strictRun(ctx, c, p.l, how, "SELECT sum(n) FROM big")
				if p.l.lastNeedTails() == port.heavy {
					t.Errorf("the copy was asked NeedTails=%v", !port.heavy)
				}
				want := tablesTooOld
				if port.heavy {
					want = tablesOnCopy
				}
				if got != want {
					t.Errorf("%+v, want %+v", got, want)
				}
				if d := p.l.runs.Load() - runs; how == "simple" && !port.heavy && d != 0 {
					t.Errorf("the copy ran %d statements, want none", d)
				}
			})
		}
	}
}

// A shape the cost routing holds on the copy within the band (CostSides,
// #3045) is heavy too: its plan just under the threshold, after one over it
// sent the shape to the copy, still answers there with older data. The
// verdicts a connection remembers (costVerdictKey) are only ever the
// source's, so they never send a statement to the copy at all.
func TestStrict_3116_aShapeHeldOnTheCopyIsHeavy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	plans := maps.Clone(costPlans)
	for _, c := range []int{48000, 52000} {
		plans[fmt.Sprintf("k = %d", c)] = plan(fmt.Sprintf(`{"Node Type": "Index Scan", "Total Cost": %d, "Plan Rows": 10}`, c))
	}
	at := func(c int) string { return fmt.Sprintf("SELECT n FROM t WHERE k = %d", c) }
	l := &costLogin{src: startFakeSource(t, plans), sides: &CostSides{}}
	c, notices := strictConn(t, ctx, l)
	if got := costSimple(ctx, c, l, at(52000)); got.side != RouteCopy {
		t.Fatalf("over the threshold: %+v", got)
	}
	l.olderThanSeen.Store(true)
	got := costSimple(ctx, c, l, at(48000))
	if got.side != RouteCopy || got.answer != "7" {
		t.Errorf("held on the copy within the band, behind an earlier answer: %+v, want the copy's 7", got)
	}
	if l.lastNoOlderTails() {
		t.Error("a shape held on the copy was asked not to read older changes")
	}
	if n := notices(); len(n) != 1 {
		t.Errorf("notices %q, want the copy's note", n)
	}
}

// The copy's own commands are not told to have their recent changes: refused
// for them, they would go to a source that does not take them.
func TestStrict_3116_theCopysOwnCommandsAreNotMarked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	p.l.tailsPending.Store(true)
	p.l.olderThanSeen.Store(true)
	c := costPortConn(t, ctx, p.l)
	for _, stmt := range []string{"SHOW TABLES", "DESCRIBE t"} {
		got := costSimple(ctx, c, p.l, stmt)
		if got.side != RouteCopy || got.code != "" || got.answer != "7" {
			t.Errorf("%s: %+v, want the copy's 7", stmt, got)
		}
		if p.l.lastNeedTails() || p.l.lastNoOlderTails() {
			t.Errorf("%s: the copy was told to refuse it for its recent changes", stmt)
		}
	}
}

// Inside a transaction block on a connection that can reach its source, the
// source answers every statement (in_transaction): the copy is not asked,
// whatever its recent changes, as the MySQL router answers a statement in a
// transaction from MySQL.
func TestStrict_3116_aTransactionBlockIsTheSources(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	p.l.tailsPending.Store(true)
	p.l.olderThanSeen.Store(true)
	c := costPortConn(t, ctx, p.l)
	if _, err := c.Exec(ctx, "BEGIN").ReadAll(); err != nil {
		t.Fatal(err)
	}
	p.l.mu.Lock()
	asked := len(p.l.needTails)
	p.l.mu.Unlock()
	want := costOutcome{"42", "", RouteSource, ReasonInTransaction}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != want {
		t.Errorf("%+v, want %+v", got, want)
	}
	p.l.mu.Lock()
	n := len(p.l.needTails) - asked
	p.l.mu.Unlock()
	if n != 0 {
		t.Errorf("the copy was asked %d times inside the block", n)
	}
	if _, err := c.Exec(ctx, "COMMIT").ReadAll(); err != nil {
		t.Fatal(err)
	}
}

// A connection with nowhere else to send the statement (its server hides
// tables or columns, or it has no forwarding account) is answered by the
// copy in both cases, with the copy's note as a notice: what the MySQL port
// does on a connection that does not route. Inside a transaction block too,
// which lives on the copy there.
func TestStrict_3116_aConnectionThatCannotForwardIsAnsweredWithANotice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	barred := newTablesPort(t)
	barred.l.barred = func() string { return "hidden rules" }
	noAccount := &costLogin{catalog: func() []CatalogTable { return catalogOf("t", "big") }}
	for _, tc := range []struct {
		name string
		l    *costLogin
	}{
		{"hidden rules", barred.l},
		{"no forwarding account", noAccount},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, block := range []bool{false, true} {
				for _, how := range strictProtocols {
					c, notices := strictConn(t, ctx, tc.l)
					if block {
						if _, err := c.Exec(ctx, "BEGIN").ReadAll(); err != nil {
							t.Fatal(err)
						}
					}
					tc.l.olderThanSeen.Store(true)
					got := strictRun(ctx, c, tc.l, how, "SELECT sum(n) FROM big")
					tc.l.olderThanSeen.Store(false)
					if got.answer != "7" || got.code != "" {
						t.Errorf("block=%v %s, behind an earlier answer: %+v, want the copy's 7", block, how, got)
					}
					if tc.l.lastNeedTails() {
						t.Errorf("block=%v %s: the copy was told to refuse where nothing else can answer", block, how)
					}
					if n := notices(); len(n) != 1 || !strings.Contains(n[0], "older than an earlier answer of the copy on this connection") {
						t.Errorf("block=%v %s: notices %q, want the copy's note", block, how, n)
					}
					tc.l.tailsPending.Store(true)
					got = strictRun(ctx, c, tc.l, how, "SELECT sum(n) FROM big")
					tc.l.tailsPending.Store(false)
					if got.answer != "7" || got.code != "" {
						t.Errorf("block=%v %s, changes not ready: %+v, want the copy's 7", block, how, got)
					}
					if tc.l.lastNeedTails() {
						t.Errorf("block=%v %s, changes not ready: the copy was told to refuse", block, how)
					}
				}
			}
		})
	}
}

// The port's own rule against the source's earlier answer (#2985,
// copyOlderThanSource) still holds with the changes ready: an answer read
// from the snapshot alone after the source answered about the table is held
// back after it ran, and the source answers.
func TestStrict_3116_theSourcesEarlierAnswerStillHoldsBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	c := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, c, p.l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource {
		t.Fatalf("the read by key: %+v", got)
	}
	runs := p.l.runs.Load()
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big b JOIN t ON t.id = b.id"); got != tablesTooOld {
		t.Errorf("%+v, want %+v", got, tablesTooOld)
	}
	if d := p.l.runs.Load() - runs; d != 1 {
		t.Errorf("the copy ran %d statements, want 1 (held back after it ran)", d)
	}
}

// A light statement the copy answers because nothing can be sent to the
// source for it stays on the copy with its notice, in both cases: refused
// for its recent changes, it would reach a source that refuses it or is
// down, and the client would get an error where the copy answers (#3116
// review). The capture account's gate keeps a statement over a table the
// copy does not list from the source (capture_gate); a source that cannot
// be reached leaves the copy alone (source_unreachable).
func TestStrict_3116_aStatementThatCannotReachTheSourceStaysOnTheCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	gate := newTablesPort(t)
	gate.l.captureAccount = true
	gate.l.names = func() [][2]string { return [][2]string{{"public", "big"}} }
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	down := &costLogin{src: &fakeSource{addr: ln.Addr().String()}}
	for _, tc := range []struct {
		name, stmt, reason string
		l                  *costLogin
		protocols          []string
	}{
		{"capture gate", "SELECT n FROM t WHERE id = 1", ReasonCaptureGate, gate.l, strictProtocols},
		{"source unreachable", "SELECT sum(n) FROM big", ReasonSourceUnreachable, down, []string{"simple"}},
	} {
		for _, how := range tc.protocols {
			for _, knob := range []string{"older", "not ready"} {
				t.Run(tc.name+"/"+how+"/"+knob, func(t *testing.T) {
					c, notices := strictConn(t, ctx, tc.l)
					set := &tc.l.olderThanSeen
					if knob == "not ready" {
						set = &tc.l.tailsPending
					}
					set.Store(true)
					got := strictRun(ctx, c, tc.l, how, tc.stmt)
					set.Store(false)
					if got.answer != "7" || got.code != "" || got.side != RouteCopy || got.reason != tc.reason {
						t.Errorf("%+v, want the copy's 7 under %s", got, tc.reason)
					}
					if tc.l.lastNeedTails() || tc.l.lastNoOlderTails() {
						t.Error("the copy was told to refuse a statement nothing else can answer")
					}
					if n := notices(); knob == "older" && !slicesContain(n, "older than an earlier answer of the copy on this connection") {
						t.Errorf("notices %q, want the copy's note", n)
					}
				})
			}
		}
	}
}

func slicesContain(notices []string, part string) bool {
	for _, n := range notices {
		if strings.Contains(n, part) {
			return true
		}
	}
	return false
}

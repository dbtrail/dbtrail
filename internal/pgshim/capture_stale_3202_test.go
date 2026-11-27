package pgshim

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// captureKnob is a host's Binding.CaptureState: what it says now, or an
// error, and how many times it was asked.
type captureKnob struct {
	mu    sync.Mutex
	st    CaptureState
	err   error
	asked atomic.Int32
}

func (k *captureKnob) set(st CaptureState, err error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.st, k.err = st, err
}

func (k *captureKnob) state(context.Context) (CaptureState, error) {
	k.asked.Add(1)
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.st, k.err
}

var (
	captureStale = CaptureState{Kind: CaptureStale, Why: "capture has not saved its state for 95 s while your PostgreSQL wrote past the last position it saved"}
	staleOnSrc   = costOutcome{"42", "", RouteSource, ReasonCaptureStateStale}
)

// withCaptureKnob gives p's host a capture state.
func withCaptureKnob(p *tablesPort) *captureKnob {
	k := &captureKnob{}
	p.l.captureState = k.state
	return k
}

// The defect (#3202): capture stopped saving its state while the source
// kept writing, so its record of the changes it dropped is not current, and
// a table it drops reads as one it keeps whole. Every statement that reads a
// table of the copy goes to the source, heavy or light, over every protocol,
// before the copy runs anything. Once capture saves again, the same
// statements, the prepared one included, are the copy's again.
func TestCaptureStale_3202_aStatementOnTheCopyGoesToTheSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, port := range strictPorts(t) {
		k := withCaptureKnob(port.p)
		for _, how := range strictProtocols {
			t.Run(port.name+"/"+how, func(t *testing.T) {
				k.set(captureStale, nil)
				c, notices := strictConn(t, ctx, port.p.l)
				stmt := "SELECT sum(n) FROM big"
				if !port.heavy {
					stmt = "SELECT n FROM t WHERE id = 1"
				}
				runs := port.p.l.runs.Load()
				if got := strictRun(ctx, c, port.p.l, how, stmt); got != staleOnSrc {
					t.Errorf("stale: %+v, want %+v", got, staleOnSrc)
				}
				if d := port.p.l.runs.Load() - runs; d != 0 && how != "prepared" {
					// A prepared statement is described on the copy
					// (dbtrail_describe) when it is made: its columns, no row.
					t.Errorf("the copy ran %d statements, want none", d)
				}
				if n := notices(); len(n) != 0 {
					t.Errorf("notices %q for a statement the source answered", n)
				}
				if n := port.p.l.last(); n.CaptureState != "" {
					t.Errorf("a statement the source answered is counted as %q on the copy", n.CaptureState)
				}
				// Once capture saves again, a new connection is the copy's.
				// (This one stays held back on the tables the source
				// answered it about until the copy reaches that answer,
				// #2985, as for any statement the source answered.)
				k.set(CaptureState{}, nil)
				d, _ := strictConn(t, ctx, port.p.l)
				if got := strictRun(ctx, d, port.p.l, how, stmt); got.answer != "7" || got.side != RouteCopy || got.code != "" {
					t.Errorf("capture saves again, a new connection: %+v, want the copy's 7", got)
				}
			})
		}
	}
	if !transientFallback(ReasonCaptureStateStale) {
		t.Error("capture_state_stale must not keep a prepared statement on the source for good")
	}
}

// A prepared statement made while capture was stale, executed after it saves
// again, is the copy's: nothing keeps it on the source.
func TestCaptureStale_3202_aPreparedStatementComesBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	k := withCaptureKnob(p)
	k.set(captureStale, nil)
	c := costPortConn(t, ctx, p.l)
	if _, err := c.Prepare(ctx, "report", "SELECT sum(n) FROM big", nil); err != nil {
		t.Fatal(err)
	}
	exec := func() (string, RouteNote) {
		t.Helper()
		res := c.ExecPrepared(ctx, "report", nil, nil, nil).Read()
		if res.Err != nil || len(res.Rows) != 1 {
			t.Fatalf("%v %q", res.Err, res.Rows)
		}
		return string(res.Rows[0][0]), p.l.last()
	}
	for i := range 2 {
		if got, n := exec(); got != "42" || n.Side != RouteSource || n.Reason != ReasonCaptureStateStale {
			t.Errorf("run %d while stale: %s %s/%s, want the source's 42", i, got, n.Side, n.Reason)
		}
	}
	// Nothing keeps it on the source: on this connection it is held back by
	// #2985's rule (the source answered it about big), as any statement the
	// source answered; on a new one it is the copy's.
	k.set(CaptureState{}, nil)
	if got, n := exec(); got != "42" || n.Side != RouteSource || n.Reason != ReasonCopyTooOld {
		t.Errorf("after capture saved again, the same connection: %s %s/%s, want copy_too_old", got, n.Side, n.Reason)
	}
	d := costPortConn(t, ctx, p.l)
	if _, err := d.Prepare(ctx, "report", "SELECT sum(n) FROM big", nil); err != nil {
		t.Fatal(err)
	}
	if res := d.ExecPrepared(ctx, "report", nil, nil, nil).Read(); res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "7" {
		t.Errorf("after capture saved again, a new connection: %v %q, want the copy's 7", res.Err, res.Rows)
	}
}

// A capture whose record is current (it saved lately, or the source wrote
// nothing since its last save) leaves every statement where it was. A
// statement that reads no table of the copy is not asked about at all, and
// neither are the copy's own commands that read no rows.
func TestCaptureStale_3202_currentAndNoTableAreNotHeldBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	k := withCaptureKnob(p)
	c := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != tablesOnCopy {
		t.Errorf("current: %+v, want %+v", got, tablesOnCopy)
	}
	if k.asked.Load() != 1 {
		t.Errorf("asked %d times, want once", k.asked.Load())
	}
	k.set(captureStale, nil)
	k.asked.Store(0)
	for _, stmt := range []string{"SHOW TABLES", "DESCRIBE big"} {
		if got := costSimple(ctx, c, p.l, stmt); got.code != "" || got.side != RouteCopy || got.reason != ReasonCopyCommand {
			t.Errorf("%s: %+v, want the copy's", stmt, got)
		}
	}
	if n := k.asked.Load(); n != 0 {
		t.Errorf("the copy's own commands asked the host %d times", n)
	}
}

// Which statements read a table of the copy: more, never fewer.
func TestCaptureStale_3202_readsATable(t *testing.T) {
	for _, tc := range []struct {
		stmt string
		want bool
	}{
		{"SELECT 1", false},
		{"SELECT 1 + 2 AS x", false},
		{"SELECT sum(n) FROM big", true},
		{"SELECT sum(n) FROM public.big", true},
		{`SELECT n FROM "Mixed"`, true},
		{"WITH x AS (SELECT 1 AS n) SELECT n FROM x", true}, // a WITH counts as a table: in doubt, the source
		{"SELECT sum(n) FROM wide WHERE id IN (SELECT id FROM big)", true},
		{"SELECT sum(n) FROM wide, LATERAL (SELECT 1) x", true}, // not read whole: any table
		{"", true},
	} {
		if got := readsATable(tc.stmt); got != tc.want {
			t.Errorf("%q: %v, want %v", tc.stmt, got, tc.want)
		}
	}
}

// Where the statement cannot go to the source, the copy answers it, with a
// NOTICE that says capture's record is not current and why the copy's
// answer is the one given: a server that hides tables (nothing is sent to
// its source), a source that could not be reached for the statement's plan,
// a statement the capture account's gate keeps from the source, and
// SUMMARIZE, a command of the copy's own. Counted apart, as stale on the
// copy.
func TestCaptureStale_3202_nowhereElseTheCopyAnswersWithANotice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	hidden := newTablesPort(t)
	hidden.l.barred = func() string { return "the server hides tables or columns" }
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
	down := &costLogin{src: &fakeSource{addr: ln.Addr().String()}, catalog: func() []CatalogTable { return catalogOf("big", "wide") }}
	summarize := newTablesPort(t)
	for _, tc := range []struct {
		name, stmt, reason, because string
		l                           *costLogin
	}{
		{"hidden rules", "SELECT sum(n) FROM big", ReasonHiddenRules, "sends nothing to your PostgreSQL", hidden.l},
		{"capture gate", "SELECT n FROM t WHERE id = 1", ReasonCaptureGate, "capture account", gate.l},
		{"source unreachable", "SELECT sum(n) FROM big", ReasonSourceUnreachable, "could not be reached", down},
		{"summarize", "SUMMARIZE big", ReasonCopyCommand, "SUMMARIZE is a command of the copy's own", summarize.l},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := &captureKnob{}
			k.set(captureStale, nil)
			tc.l.captureState = k.state
			c, notices := strictConn(t, ctx, tc.l)
			got := costSimple(ctx, c, tc.l, tc.stmt)
			if got.answer != "7" || got.code != "" || got.side != RouteCopy || got.reason != tc.reason {
				t.Errorf("%+v, want the copy's 7 under %s", got, tc.reason)
			}
			n := notices()
			said := 0
			for _, m := range n {
				if strings.Contains(m, captureStale.Why) && strings.Contains(m, tc.because) && strings.Contains(m, "may be missing changes") {
					said++
				}
			}
			if said != 1 {
				t.Errorf("notices %q, want one that says why capture's record is not current and %q", n, tc.because)
			}
			if note := tc.l.last(); note.CaptureState != CaptureStale {
				t.Errorf("counted as %q, want %q", note.CaptureState, CaptureStale)
			}
		})
	}
}

// When the host could not tell (the source could not be asked whether it
// wrote since capture's last save, or the host failed), the copy answers as
// it does without the check, with a NOTICE, on every protocol: the source is
// most likely away, and the copy is the way to read meanwhile. The host's
// error never reaches the client.
func TestCaptureStale_3202_uncheckedTheCopyAnswersWithANotice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	k := withCaptureKnob(p)
	for _, tc := range []struct {
		name string
		st   CaptureState
		err  error
		want string
	}{
		{"source not asked", CaptureState{Kind: CaptureUnchecked, Why: "your PostgreSQL did not answer within 2s"}, nil, "your PostgreSQL did not answer within 2s"},
		{"host error", CaptureState{}, errors.New("dial tcp 10.0.3.7:3306: connect: connection refused"), "DBTrail's log has the details"},
	} {
		for _, how := range strictProtocols {
			t.Run(tc.name+"/"+how, func(t *testing.T) {
				k.set(tc.st, tc.err)
				c, notices := strictConn(t, ctx, p.l)
				got := strictRun(ctx, c, p.l, how, "SELECT sum(n) FROM big")
				if got.answer != "7" || got.code != "" || got.side != RouteCopy {
					t.Errorf("%+v, want the copy's 7", got)
				}
				n := strings.Join(notices(), "|")
				if !strings.Contains(n, "could not check") || !strings.Contains(n, tc.want) || strings.Contains(n, "10.0.3.7") {
					t.Errorf("notices %q, want one that says it could not check, and why (%q), without the host's error", n, tc.want)
				}
				if note := p.l.last(); note.CaptureState != CaptureUnchecked {
					t.Errorf("counted as %q, want %q", note.CaptureState, CaptureUnchecked)
				}
			})
		}
	}
}

// A statement stopped while the host was asked is not sent anywhere.
func TestCaptureStale_3202_stoppedWhileAsked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	p.l.captureState = func(ctx context.Context) (CaptureState, error) {
		<-ctx.Done()
		return CaptureState{}, ctx.Err()
	}
	c := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, c, p.l, "SET statement_timeout = 200"); got.code != "" {
		t.Fatalf("SET: %+v", got)
	}
	runs := p.l.runs.Load()
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got.code != "57014" || got.answer != "" {
		t.Errorf("%+v, want 57014 and no answer from either side", got)
	}
	if d := p.l.runs.Load() - runs; d != 0 {
		t.Errorf("the copy ran %d statements", d)
	}
}

// The NOTICE goes out only with the copy's answer: a statement the copy
// fails and the source answers gets none, over every protocol. (Here the
// state could not be checked, so the copy runs it on a connection that
// forwards.)
func TestCaptureStale_3202_theNoticeWaitsForTheCopysAnswer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	k := withCaptureKnob(p)
	k.set(CaptureState{Kind: CaptureUnchecked, Why: "your PostgreSQL did not answer within 2s"}, nil)
	var fail atomic.Bool
	p.l.copyErr = func(st string) error {
		if fail.Load() && !strings.Contains(st, "dbtrail_describe") {
			return errors.New("Catalog Error: Scalar Function with name nope does not exist")
		}
		return nil
	}
	for _, how := range strictProtocols {
		t.Run(how, func(t *testing.T) {
			fail.Store(true)
			c, notices := strictConn(t, ctx, p.l)
			got := strictRun(ctx, c, p.l, how, "SELECT sum(n) FROM big")
			if got.answer != "42" || got.side != RouteSource || got.reason != ReasonCopyRefused {
				t.Errorf("the copy failed: %+v, want the source's 42", got)
			}
			if n := strings.Join(notices(), "|"); strings.Contains(n, "could not check") {
				t.Errorf("notices %q for a statement the source answered", n)
			}
			if note := p.l.last(); note.CaptureState != "" {
				t.Errorf("counted as %q on the copy, though the source answered", note.CaptureState)
			}
			fail.Store(false)
			d, notices := strictConn(t, ctx, p.l)
			if got := strictRun(ctx, d, p.l, how, "SELECT sum(n) FROM big"); got.answer != "7" || got.side != RouteCopy {
				t.Errorf("the copy answered: %+v", got)
			}
			if n := strings.Join(notices(), "|"); strings.Count(n, "could not check") != 1 {
				t.Errorf("notices %q, want the one notice with the copy's answer", n)
			}
		})
	}
}

package shim

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// #2611: a routed statement that reads the copy's clock goes to the copy
// only when the source's clock is the time and this host's agrees, asked of
// every such statement, prepared or not; one that does not call the four
// never asks. A probe that fails or answers nothing to read keeps it on
// MySQL, counted as a session that differs.
func TestRouter_nowAsksTheSourcesClock_2611(t *testing.T) {
	const now = "SELECT count(*) FROM t WHERE ts >= NOW() - INTERVAL 1 DAY"
	answer := func(rows [][]any, err error) func() ([][]any, error) {
		return func() ([][]any, error) { return rows, err }
	}
	for _, c := range []struct {
		name, stmt string
		router     fakeRouter
		wantSide   string
		wantProbes int
	}{
		{"a live clock", now, fakeRouter{toCopy: true}, "copy", 1},
		{"a frozen clock", now, fakeRouter{toCopy: true, clockFrozenAt: 1700000000}, "mysql", 1},
		{"a clock frozen a moment ago", now, fakeRouter{toCopy: true, clockFrozenAt: float64(time.Now().Unix() - 3)}, "mysql", 1},
		{"two clocks a minute apart", now, fakeRouter{toCopy: true, clockOff: 60}, "mysql", 1},
		{"a probe that fails", now, fakeRouter{toCopy: true, clockAnswer: answer(nil, errors.New("boom"))}, "mysql", 1},
		{"a probe with no row", now, fakeRouter{toCopy: true, clockAnswer: answer(nil, nil)}, "mysql", 1},
		{"a probe that answers NULL", now, fakeRouter{toCopy: true, clockAnswer: answer([][]any{{nil, nil}}, nil)}, "mysql", 1},
		{"no NOW(): never asked", "SELECT count(*) FROM t", fakeRouter{toCopy: true, clockFrozenAt: 1700000000}, "copy", 0},
	} {
		for _, prepared := range []bool{false, true} {
			name := c.name
			if prepared {
				name += " (prepared)"
			}
			t.Run(name, func(t *testing.T) {
				r := c.router
				f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
				h := NewHandler(nil, nil)
				h.BindFreeSQL(f)
				var sides []RouteSide
				var reasons []RouteReason
				h.BindRouter(&r, RouterConfig{MaxCopyAge: time.Minute, Observe: func(side RouteSide, reason RouteReason) {
					sides, reasons = append(sides, side), append(reasons, reason)
				}})
				res, err := runOnce(h, c.stmt, prepared)
				if err != nil {
					t.Fatalf("run: %v", err)
				}
				if !prepared {
					if got := firstCell(t, res); got != c.wantSide {
						t.Errorf("answered by %s, want %s", got, c.wantSide)
					}
				}
				wantSide := RouteMySQL
				if c.wantSide == "copy" {
					wantSide = RouteCopy
				}
				if len(sides) != 1 || sides[0] != wantSide {
					t.Errorf("routed %v, want one %v", sides, wantSide)
				}
				if c.wantSide == "copy" && !f.gotSess.MySQLClock {
					t.Error("the copy ran without MySQL's clock functions")
				}
				if c.wantSide == "mysql" && (len(reasons) != 1 || reasons[0] != RouteReasonSessionDiffers) {
					t.Errorf("reasons %v, want one %s", reasons, RouteReasonSessionDiffers)
				}
				if r.clockProbes != c.wantProbes {
					t.Errorf("clock asked %d times, want %d", r.clockProbes, c.wantProbes)
				}
			})
		}
	}
}

// The clock is asked again for every statement on one connection: a session
// whose clock stops after its first statement is MySQL's from the next one,
// with no SET the port would see.
func TestRouter_nowAsksEveryStatement_2611(t *testing.T) {
	const now = "SELECT count(*) FROM t WHERE ts >= NOW() - INTERVAL 1 DAY"
	r := &fakeRouter{toCopy: true}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Minute)
	for i, want := range []string{"copy", "mysql"} {
		res, err := h.HandleQuery(now)
		if err != nil {
			t.Fatalf("statement %d: %v", i+1, err)
		}
		if got := firstCell(t, res); got != want {
			t.Errorf("statement %d answered by %s, want %s", i+1, got, want)
		}
		r.clockFrozenAt = float64(time.Now().Unix() - 3)
	}
	if r.clockProbes != 2 {
		t.Errorf("clock asked %d times, want 2", r.clockProbes)
	}
}

// runOnce runs a statement as a query, or prepared and executed.
func runOnce(h *Handler, stmt string, prepared bool) (*mysql.Result, error) {
	if !prepared {
		return h.HandleQuery(stmt)
	}
	_, _, ctx, err := h.HandleStmtPrepare(stmt)
	if err != nil {
		return nil, err
	}
	return h.HandleStmtExecute(ctx, "", nil)
}

func TestClockVerdict_2611(t *testing.T) {
	now := time.Unix(1_780_000_000, 0)
	for _, c := range []struct {
		lag, ts, want string
	}{
		{"0.000012", "1780000000.000100", ""},
		{"-0.4", "1780000000.4", ""},
		{"-80000000.5", "1700000000", "SET timestamp"},
		{"0", "1700000000", "from this host's"},
		{"0", "1780000002", "from this host's"},
		{"1.5", "1780000001.5", "SET timestamp"},
		{"x", "1", "could not be read"},
	} {
		got := clockVerdict(c.lag, c.ts, now)
		if (got == "") != (c.want == "") || !strings.Contains(got, c.want) {
			t.Errorf("clockVerdict(%s, %s) = %q, want one holding %q", c.lag, c.ts, got, c.want)
		}
	}
}

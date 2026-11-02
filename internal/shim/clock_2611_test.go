package shim

import (
	"strings"
	"testing"
	"time"
)

// #2611: a routed statement that reads the copy's clock goes to the copy
// only when the source's clock is the time and this host's agrees, asked of
// every such statement; one that does not call the four never asks.
func TestRouter_nowAsksTheSourcesClock_2611(t *testing.T) {
	const now = "SELECT count(*) FROM t WHERE ts >= NOW() - INTERVAL 1 DAY"
	fresh := time.Now()
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
		{"no NOW(): never asked", "SELECT count(*) FROM t", fakeRouter{toCopy: true, clockFrozenAt: 1700000000}, "copy", 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := c.router
			f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: fresh}
			h := routingHandler(t, &r, f, time.Minute)
			res, err := h.HandleQuery(c.stmt)
			if err != nil {
				t.Fatalf("HandleQuery: %v", err)
			}
			if got := firstCell(t, res); got != c.wantSide {
				t.Errorf("answered by %s, want %s", got, c.wantSide)
			}
			if c.wantSide == "copy" && !f.gotSess.MySQLClock {
				t.Error("the copy ran without MySQL's clock functions")
			}
			if r.clockProbes != c.wantProbes {
				t.Errorf("clock asked %d times, want %d", r.clockProbes, c.wantProbes)
			}
		})
	}
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

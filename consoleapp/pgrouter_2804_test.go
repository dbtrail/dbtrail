package consoleapp

import (
	"strings"
	"testing"
)

// The PostgreSQL router's result budget (#2804) when nobody set one: the
// MySQL port's size, and said at start-up.
func TestPGRouter_defaultResultBudgetIsTheMySQLPorts(t *testing.T) {
	b := newPGPortResultBudget()
	if b == nil {
		t.Fatal("the port has no result budget by default")
	}
	mysqlPort := flashbackConfig{}.withDefaults().ResultBudget
	if b.Max() != mysqlPort.Max() || b.Max() != 256<<20 {
		t.Fatalf("the PostgreSQL port's budget is %d bytes, the MySQL port's %d; want both 256 MiB", b.Max(), mysqlPort.Max())
	}
	out := pgRouterStartupLines(pgRouterSettings{Listen: "127.0.0.1:5433", Workers: 2, Threads: 2, MaxConns: 16}, "2GB", "the default", nil, 1, true)
	if !strings.Contains(out, "up to 256 MB in all") || !strings.Contains(out, "53000") {
		t.Errorf("the start-up lines do not say how much the port holds, or the error past it:\n%s", out)
	}
}

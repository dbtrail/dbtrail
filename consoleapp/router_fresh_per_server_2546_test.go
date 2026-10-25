package consoleapp

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// #2546: with no BINTRAIL_SQL_TAILS the router plans its pool twice, fast
// and with the tails set aside, so a server switched to fresh mode can make
// room; with it on, the pool is planned for tails and never moves.
func TestRouterPool_plansBothModesUnlessTheEnvironmentDecides_2546(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	p := routerPool(routerSettings{}, "2GB")
	if p.Workers < 1 || p.FreshWorkers < 1 || p.FreshWorkers > p.Workers {
		t.Fatalf("no environment: workers %d, fresh workers %d; want both set, fresh not above fast", p.Workers, p.FreshWorkers)
	}
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	if p := routerPool(routerSettings{}, "2GB"); p.FreshWorkers != 0 {
		t.Fatalf("BINTRAIL_SQL_TAILS=on: fresh workers %d, want 0 (planned for tails, never moved)", p.FreshWorkers)
	}
	// --concurrency is taken as said in both modes.
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	if p := routerPool(routerSettings{Concurrency: 3}, "2GB"); p.Workers != 3 || p.FreshWorkers != 3 {
		t.Fatalf("--concurrency 3: workers %d, fresh workers %d, want 3 and 3", p.Workers, p.FreshWorkers)
	}
}

func TestRouterFreshServersLine_2546(t *testing.T) {
	if got := routerFreshServersLine(nil, 4, 3); got != "" {
		t.Fatalf("no fresh server: %q, want nothing", got)
	}
	got := routerFreshServersLine([]string{"shop", "billing"}, 4, 3)
	for _, want := range []string{"2 servers are", "their own switch: billing, shop.", "3 statements run at once instead of 4"} {
		if !strings.Contains(got, want) {
			t.Errorf("%q does not say %q", got, want)
		}
	}
	if !strings.HasSuffix(got, ".\n") {
		t.Errorf("%q does not end the line", got)
	}
	one := routerFreshServersLine([]string{"shop"}, 2, 2)
	if !strings.Contains(one, "1 server is in fresh mode by its own switch: shop. Its statements") {
		t.Errorf("one server: %q", one)
	}
	if lone := routerFreshServersLine([]string{"shop"}, 4, 1); !strings.Contains(lone, "1 statement runs at once instead of 4") {
		t.Errorf("one slot left: %q", lone)
	}
	if strings.Contains(one, "at once") {
		t.Errorf("the pool does not move (2 and 2), yet the line says it does: %q", one)
	}
}

// Every server in the status document says its mode and where it comes
// from, the ones with no statements yet included.
func TestRouterStatus_saysEachServersMode_2546(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	numbers := console.RouterNumbers{Servers: map[string]console.RouterServerNumbers{
		"a": {SQLMode: "fresh", SQLModeFrom: console.SQLModeFromSaved},
	}}
	last := &routerCopySnapshots{}
	last.set(map[string]time.Time{"b": time.Now()}, time.Now())
	doc := routerStatus(routerSettings{}, numbers, last)
	if n := doc.Servers["a"]; n.SQLMode != "fresh" || n.SQLModeFrom != console.SQLModeFromSaved {
		t.Errorf("a: %q from %q, want fresh as saved", n.SQLMode, n.SQLModeFrom)
	}
	if n := doc.Servers["b"]; n.SQLMode != "fast" || n.SQLModeFrom != console.SQLModeFromDefault {
		t.Errorf("b, only a copy: %q from %q, want fast by default", n.SQLMode, n.SQLModeFrom)
	}
	if !doc.PerServerMode {
		t.Error("the router does not say it reads each server's switch")
	}
	if doc.Fresh == nil || *doc.Fresh {
		t.Errorf("top-level fresh %v: an older console reads it as the whole router; only the environment sets it", doc.Fresh)
	}
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	doc = routerStatus(routerSettings{}, console.RouterNumbers{}, last)
	if n := doc.Servers["b"]; n.SQLMode != "fresh" || n.SQLModeFrom != console.SQLModeFromEnvironment || !n.SQLModeWarming {
		t.Errorf("b with the environment on: %q from %q", n.SQLMode, n.SQLModeFrom)
	}
}

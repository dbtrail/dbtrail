package consoleapp

import (
	"os"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2531: the router takes every core only when it runs at a lower CPU
// priority than capture. The compose file and the systemd unit are what give
// it that priority, and a pool that still halves says so at startup.

func TestRouterStartupLines_sayTheCoresNote(t *testing.T) {
	pool := console.SQLPool{Workers: 1, Threads: 2, Why: "half of the machine's 4"}
	if got := routerStartupLines(routerStartup{MaxWait: sqlsandbox.NoMaxWait, Pool: pool}); strings.Contains(got, "Router: NOTE:") {
		t.Errorf("no note, yet the startup says one:\n%s", got)
	}
	pool.CoresNote = "statements use 2 of this machine's 4 cores and leave the rest to capture"
	got := routerStartupLines(routerStartup{MaxWait: sqlsandbox.NoMaxWait, Pool: pool})
	if !strings.Contains(got, "Router: NOTE: statements use 2 of this machine's 4 cores and leave the rest to capture.\n") {
		t.Errorf("the startup does not say the note:\n%s", got)
	}
}

func TestCompose_onlyTheRoutersRunAtALowerCPUPriority(t *testing.T) {
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Services map[string]map[string]any `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if got := doc.Services["router"]["cpu_shares"]; got != "${ROUTER_CPU_SHARES:-256}" {
		t.Errorf("router cpu_shares = %v, want ${ROUTER_CPU_SHARES:-256}", got)
	}
	// Lowering capture too would take the router's advantage away: the
	// scheduler weighs them against each other. The PostgreSQL router is the
	// other service that answers statements on the copy, and runs below
	// capture too (pgrouter_cpu_priority_test.go).
	for name, svc := range doc.Services {
		if _, ok := svc["cpu_shares"]; ok && name != "router" && name != "pg-router" {
			t.Errorf("service %s sets cpu_shares; only the two routers may", name)
		}
	}
}

func TestRouterUnit_runsAtALowerCPUPriority(t *testing.T) {
	data, err := os.ReadFile("../build/packaging/bintrail-console/bintrail-router.service")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "\nCPUWeight=25\n") {
		t.Error("bintrail-router.service lost CPUWeight=25: the router would halve its cores again")
	}
}

package consoleapp

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"
)

// The PostgreSQL router answers statements on the copy beside capture, as
// the MySQL router does, and is given the same standing against it: a lower
// CPU priority, from the compose file and from the packaged unit.

// composeServiceKeys reads the shipped compose file's services, each as its
// keys.
func composeServiceKeys(t *testing.T) map[string]map[string]any {
	t.Helper()
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
	return doc.Services
}

func TestCompose_thePostgreSQLRouterRunsAtALowerCPUPriority(t *testing.T) {
	services := composeServiceKeys(t)
	pg, ok := services["pg-router"]
	if !ok {
		t.Fatalf("no pg-router service in %s", composePath)
	}
	// Its own variable, so one router can be changed without the other, and
	// `:-`, so an empty PG_ROUTER_CPU_SHARES= in .env is the default too: an
	// empty cpu_shares is a file Docker Compose refuses.
	if got := pg["cpu_shares"]; got != "${PG_ROUTER_CPU_SHARES:-256}" {
		t.Errorf("pg-router cpu_shares = %v, want ${PG_ROUTER_CPU_SHARES:-256}", got)
	}
	// The same priority as the MySQL router: an installation serves one
	// family, and neither router should outrank capture.
	number := regexp.MustCompile(`:-([0-9]+)\}$`)
	own := number.FindStringSubmatch(asString(pg["cpu_shares"]))
	twin := number.FindStringSubmatch(asString(services["router"]["cpu_shares"]))
	if own == nil || twin == nil || own[1] != twin[1] {
		t.Errorf("pg-router cpu_shares is %v and the router's %v; the two defaults must be the same number", pg["cpu_shares"], services["router"]["cpu_shares"])
	}
	// What it is lowered against stays at the default: capture and the index
	// it writes to.
	for _, name := range []string{composeService, "index-mysql"} {
		svc, ok := services[name]
		if !ok {
			t.Fatalf("no %s service in %s", name, composePath)
		}
		if _, set := svc["cpu_shares"]; set {
			t.Errorf("service %s sets cpu_shares; lowered too, the routers are no longer below it", name)
		}
	}
	// The ceiling stays its own setting beside the priority.
	if got := pg["cpus"]; got != "${PG_ROUTER_CPUS:-0}" {
		t.Errorf("pg-router cpus = %v, want ${PG_ROUTER_CPUS:-0}", got)
	}
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

// The two packaged units carry the same weight: the PostgreSQL router's is
// read from the MySQL router's, so the two cannot drift apart.
func TestPGRouterUnit_runsAtTheRoutersCPUPriority(t *testing.T) {
	weight := regexp.MustCompile(`(?m)^CPUWeight=([0-9]+)$`)
	read := func(unit string) string {
		t.Helper()
		data, err := os.ReadFile("../build/packaging/bintrail-console/" + unit)
		if err != nil {
			t.Fatal(err)
		}
		all := weight.FindAllStringSubmatch(string(data), -1)
		if len(all) != 1 {
			t.Fatalf("%s has %d CPUWeight= lines, want exactly one", unit, len(all))
		}
		// Under [Service]: systemd ignores it anywhere else.
		at := strings.Index(string(data), "\nCPUWeight=")
		service, install := strings.Index(string(data), "\n[Service]\n"), strings.Index(string(data), "\n[Install]\n")
		if service < 0 || install < 0 || at < service || at > install {
			t.Fatalf("%s: CPUWeight= is not in the [Service] section", unit)
		}
		return all[0][1]
	}
	router, pg := read("bintrail-router.service"), read("bintrail-pg-router.service")
	// Well below the default of 100, as cpuPriorityOf counts it for the
	// MySQL router: half or less.
	if n, err := strconv.Atoi(pg); err != nil || n < 1 || n > 50 {
		t.Errorf("bintrail-pg-router.service has CPUWeight=%s, want at most half the default of 100", pg)
	}
	if pg != router {
		t.Errorf("bintrail-pg-router.service has CPUWeight=%s and bintrail-router.service CPUWeight=%s; they must match", pg, router)
	}
	// The service that runs capture stays at the default, or the weight
	// protects nothing.
	console, err := os.ReadFile("../build/packaging/bintrail-console/bintrail-console.service")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(console), "\n") {
		if strings.HasPrefix(line, "CPUWeight=") || strings.HasPrefix(line, "Nice=") {
			t.Errorf("bintrail-console.service sets %q; capture must stay at the default priority", line)
		}
	}
}

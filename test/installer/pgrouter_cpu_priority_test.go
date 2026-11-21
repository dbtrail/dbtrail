package installer

import (
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"
)

// The compose file an install ends with, after the installer's own edits to
// it (the memory ceiling turned on, a taken port left out), still runs the
// PostgreSQL router below capture's CPU priority, as it runs the MySQL
// router, and leaves capture at the default.
func TestInstaller_thePostgreSQLRouterKeepsItsLowerCPUPriority(t *testing.T) {
	for name, env := range map[string][]string{
		"as shipped":            nil,
		"memory ceiling on":     {"STUB_IMAGES=" + consoleImage},
		"both ports taken":      {"BUSY_PORTS=3309 5433"},
		"ceiling on, 5433 busy": {"STUB_IMAGES=" + consoleImage, "BUSY_PORTS=5433"},
	} {
		r := install(t, env...)
		if r.failed {
			t.Fatalf("%s: install failed:\n%s", name, r.out)
		}
		c := r.compose(t)
		var doc struct {
			Services map[string]map[string]any `yaml:"services"`
		}
		if err := yaml.Unmarshal([]byte(c), &doc); err != nil {
			t.Fatalf("%s: the installed compose file is not YAML: %v", name, err)
		}
		for svc, want := range map[string]string{
			"pg-router": "${PG_ROUTER_CPU_SHARES:-256}",
			"router":    "${ROUTER_CPU_SHARES:-256}",
		} {
			if got := doc.Services[svc]["cpu_shares"]; got != want {
				t.Errorf("%s: %s cpu_shares = %v, want %s", name, svc, got, want)
			}
		}
		if _, set := doc.Services["bintrail"]["cpu_shares"]; set {
			t.Errorf("%s: the capture service sets cpu_shares; it must stay at the default priority", name)
		}
		// One line each, inside its own service: a second one would be a key
		// set twice, which Docker Compose refuses.
		if n := strings.Count(c, "\n    cpu_shares: "); n != 2 {
			t.Errorf("%s: %d cpu_shares lines in the installed file, want 2 (the two routers)", name, n)
		}
	}
}

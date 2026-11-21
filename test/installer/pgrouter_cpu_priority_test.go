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
	const ceilingOn = "\n    security_opt:\n      - writable-cgroups=true\n"
	for _, tc := range []struct {
		name string
		env  []string
		// edited says what the installer must have done to the file, so a
		// case cannot pass as a copy of "as shipped".
		edited func(c string) bool
	}{
		{"as shipped", nil, func(c string) bool {
			return !strings.Contains(c, ceilingOn) && strings.Contains(c, `"127.0.0.1:5433:5433"`)
		}},
		{"memory ceiling on", []string{"STUB_IMAGES=" + consoleImage}, func(c string) bool {
			return strings.Count(c, ceilingOn) == 3
		}},
		{"both ports taken", []string{"BUSY_PORTS=3309 5433"}, func(c string) bool {
			return !strings.Contains(c, `"127.0.0.1:5433:5433"`) && !strings.Contains(c, `"127.0.0.1:3309:3309"`)
		}},
		{"ceiling on, 5433 taken", []string{"STUB_IMAGES=" + consoleImage, "BUSY_PORTS=5433"}, func(c string) bool {
			return strings.Count(c, ceilingOn) == 3 && !strings.Contains(c, `"127.0.0.1:5433:5433"`)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := install(t, tc.env...)
			if r.failed {
				t.Fatalf("install failed:\n%s", r.out)
			}
			c := r.compose(t)
			if !tc.edited(c) {
				t.Fatalf("the installer did not leave the file this case is about:\n%s", r.out)
			}
			var doc struct {
				Services map[string]map[string]any `yaml:"services"`
			}
			if err := yaml.Unmarshal([]byte(c), &doc); err != nil {
				t.Fatalf("the installed compose file is not YAML: %v", err)
			}
			for svc, want := range map[string]string{
				"pg-router": "${PG_ROUTER_CPU_SHARES:-256}",
				"router":    "${ROUTER_CPU_SHARES:-256}",
			} {
				if got := doc.Services[svc]["cpu_shares"]; got != want {
					t.Errorf("%s cpu_shares = %v, want %s", svc, got, want)
				}
			}
			if _, set := doc.Services["bintrail"]["cpu_shares"]; set {
				t.Error("the capture service sets cpu_shares; it must stay at the default priority")
			}
			// One line each, inside its own service: a second one would be a
			// key set twice, which Docker Compose refuses.
			if n := strings.Count(c, "\n    cpu_shares: "); n != 2 {
				t.Errorf("%d cpu_shares lines in the installed file, want 2 (the two routers)", n)
			}
		})
	}
}

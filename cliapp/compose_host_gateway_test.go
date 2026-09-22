package cliapp

import (
	"os"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"
)

// The installer and the docs tell a user whose database runs on the same
// machine to connect to host.docker.internal. Docker Desktop defines that
// name on its own; Docker Engine on Linux does not, so on Linux the advice
// failed with "no such host" until the compose mapped it with host-gateway.
// Every service with a DSN variable (a source, or a brought-your-own index)
// must carry the mapping, including one added later.
func TestComposeSourceServicesMapHostDockerInternal(t *testing.T) {
	raw, err := os.ReadFile(icebergComposePath)
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Services map[string]struct {
			Environment map[string]string `yaml:"environment"`
			ExtraHosts  []string          `yaml:"extra_hosts"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	checked := 0
	for name, svc := range f.Services {
		// Any variable that holds a DSN can name host.docker.internal: the
		// source ones, and a brought-your-own INDEX_DSN.
		reaches := false
		for k := range svc.Environment {
			if strings.HasSuffix(k, "_DSN") {
				reaches = true
			}
		}
		if !reaches {
			continue
		}
		checked++
		if !slices.Contains(svc.ExtraHosts, "host.docker.internal:${HOST_GATEWAY:-host-gateway}") {
			t.Errorf("service %q connects to the user's database but does not map host.docker.internal; on Linux the name the installer recommends does not resolve", name)
		}
	}
	// bintrail, shim, baseline-dump and iceberg-export today. Fewer means the
	// guard stopped recognising the services it exists for.
	if checked < 4 {
		t.Fatalf("only %d services read a *_DSN variable; this guard covers less than it should", checked)
	}
}

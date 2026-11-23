package consoleapp

import (
	"os"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// composeReportingServices names the services of the bundled file that can
// report usage telemetry while the stack runs: the ones that stay up (a
// `restart:` policy) on one of DBTrail's own images. Every command of those
// images starts the telemetry client (the root command's hook), whatever it
// goes on to do, so the list is read off the file and a service added later
// is in it without anyone remembering this test (#3143: pg-router was not).
func composeReportingServices(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var doc struct {
		Services map[string]struct {
			Image   string `yaml:"image"`
			Restart string `yaml:"restart"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse %s: %v", composePath, err)
	}
	var names []string
	for name, svc := range doc.Services {
		if svc.Restart != "" && svc.Restart != "no" && strings.HasPrefix(svc.Image, "ghcr.io/dbtrail/") {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	// The rule above must keep finding the ones known to report: a renamed
	// image or a dropped restart policy would otherwise empty the list and
	// pass.
	for _, known := range []string{composeService, "router", "pg-router", "shim"} {
		if !slices.Contains(names, known) {
			t.Fatalf("the %q service is not among the long-lived DBTrail services read off %s (%v)", known, composePath, names)
		}
	}
	return names
}

// TestComposeKeepsTheTelemetryChoiceOnTheStateVolume: every long-lived
// service of the bundled file that runs the telemetry client has to read the
// SAME choice, from a folder that outlives the container. A service without
// the setting reads its own home directory, which a recreate empties, so it is
// back to the default (on) whatever the operator chose in the web interface.
//
// The folder also has to sit under a mount of the state volume, or the setting
// points at a path that is just as much part of the container as the home.
func TestComposeKeepsTheTelemetryChoiceOnTheStateVolume(t *testing.T) {
	doc := readComposeDriftFile(t)
	const stateVolume = "bintrail-state"

	var want string
	for _, name := range composeReportingServices(t) {
		svc := doc.Services[name]
		dir, _ := svc.Environment[telemetry.DirEnvVar].(string)
		if dir == "" {
			t.Errorf("the %q service does not set %s, so it does not follow the telemetry choice made in the web interface", name, telemetry.DirEnvVar)
			continue
		}
		if want == "" {
			want = dir
		} else if dir != want {
			t.Errorf("the %q service keeps the telemetry choice in %s and another keeps it in %s; they must read one folder", name, dir, want)
		}
		onVolume := false
		for _, v := range svc.Volumes {
			parts := strings.Split(v, ":")
			if len(parts) >= 2 && parts[0] == stateVolume && strings.HasPrefix(dir, strings.TrimSuffix(parts[1], "/")+"/") {
				onVolume = true
			}
		}
		if !onVolume {
			t.Errorf("the %q service keeps the telemetry choice in %s, which is not under its mount of the %s volume (%v)", name, dir, stateVolume, svc.Volumes)
		}
	}
}

package consoleapp

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

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
	for _, name := range []string{composeService, "router", "shim"} {
		svc, ok := doc.Services[name]
		if !ok {
			t.Fatalf("no %q service in %s", name, composePath)
		}
		dir, _ := svc.Environment[telemetry.DirEnvVar].(string)
		if dir == "" {
			t.Errorf("the %q service does not set %s, so it does not follow the telemetry choice made in the web interface", name, telemetry.DirEnvVar)
			continue
		}
		if want == "" {
			want = dir
		} else if dir != want {
			t.Errorf("the %q service keeps the telemetry choice in %s and %q keeps it in %s; they must read one folder", name, dir, composeService, want)
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

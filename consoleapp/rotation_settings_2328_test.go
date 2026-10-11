package consoleapp

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/rotation"
)

func withRotationGlobals(t *testing.T, cfg rotation.Settings, serversFile string) {
	t.Helper()
	prevCfg, prevFile := upRotationCfg, upConsoleServersFile
	t.Cleanup(func() { upRotationCfg, upConsoleServersFile = prevCfg, prevFile })
	upRotationCfg, upConsoleServersFile = cfg, serversFile
}

var flags48h = rotation.Settings{Enabled: true, Retain: 48 * time.Hour, RetainRaw: "48h", Interval: time.Hour, AddFuture: 3}

// #2328, the production wiring: the Checks report is handed the loop's own
// settings, read from the REAL registry each time, so a retention saved in
// the web interface is what it grades over, as an explicit choice.
func TestChecksRotationSettings_followTheSavedPolicy(t *testing.T) {
	reg, err := console.LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	withRotationGlobals(t, flags48h, "")
	settings := checksRotationSettings(reg)

	if s := settings(); !s.Enabled || s.Explicit || s.Retain != 48*time.Hour {
		t.Fatalf("nothing saved: %+v, want the flags' implicit 48h", s)
	}
	if err := reg.SetRotation(console.RotationConfig{Retain: "7d", Interval: "1h", AddFuture: 3}); err != nil {
		t.Fatal(err)
	}
	if s := settings(); !s.Enabled || !s.Explicit || s.Retain != 7*24*time.Hour {
		t.Fatalf("7d saved: %+v, want an explicit 7d on the next report", s)
	}
	// A saved policy the loop refuses (a hand edit) is one the report ignores.
	if err := reg.SetRotation(console.RotationConfig{Retain: "7d", Interval: "1x", AddFuture: 3}); err != nil {
		t.Fatal(err)
	}
	if s := settings(); s.Explicit || s.Retain != 48*time.Hour {
		t.Fatalf("invalid policy saved: %+v, want the flags' 48h the loop falls back to", s)
	}
}

// A daemon started with rotation off runs no loop: StartLoop decides once.
// A retention saved afterwards drops nothing until a restart, so the report
// must keep grading "off", not a 7-day projection nothing enforces.
func TestChecksRotationSettings_noLoopNoWindow(t *testing.T) {
	reg, err := console.LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	withRotationGlobals(t, rotation.Settings{}, "")
	settings := checksRotationSettings(reg)
	if err := reg.SetRotation(console.RotationConfig{Retain: "7d", Interval: "1h", AddFuture: 3}); err != nil {
		t.Fatal(err)
	}
	if s := settings(); s.Enabled {
		t.Fatalf("settings = %+v, want rotation off: no loop is running", s)
	}
	// After a restart the loop reads the saved policy at start, and runs.
	if s := checksRotationSettings(reg)(); !s.Enabled || s.Retain != 7*24*time.Hour {
		t.Fatalf("after a restart: %+v, want the saved 7d in force", s)
	}
}

// The boot preflight reads the saved policy BEFORE the daemon loads its
// registry, and must not write the file the daemon is about to own.
func TestPreflightRotationSettings_readsTheSavedPolicyWithoutWriting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-servers.yaml")

	withRotationGlobals(t, flags48h, path)
	if s := preflightRotationSettings(); s.Explicit || s.Retain != 48*time.Hour {
		t.Fatalf("no file yet (a first boot): %+v, want the flags' 48h", s)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("the preflight created the registry file (stat err = %v)", err)
	}

	reg, err := console.LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.SetRotation(console.RotationConfig{Retain: "7d", Interval: "1h", AddFuture: 3}); err != nil {
		t.Fatal(err)
	}
	// A saved server too: it is what the daemon's own load migrates (and
	// rewrites the file for), so a preflight that loaded the file as its
	// owner would be caught by the byte comparison below.
	if _, err := reg.Add(console.ServerEntry{Name: "s", DSN: "u:p@tcp(h:3306)/a"}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the registry did not save its file: %v", err)
	}
	if s := preflightRotationSettings(); !s.Explicit || s.Retain != 7*24*time.Hour {
		t.Fatalf("7d saved: %+v, want the explicit 7d the loop will start with", s)
	}
	// Started with rotation off, a saved policy still starts the loop.
	upRotationCfg = rotation.Settings{}
	if s := preflightRotationSettings(); !s.Enabled || s.Retain != 7*24*time.Hour {
		t.Fatalf("flags off, 7d saved: %+v, want the saved 7d", s)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("the preflight rewrote the registry file")
	}

	// A file that cannot be parsed answers the flags.
	upRotationCfg = flags48h
	if err := os.WriteFile(path, []byte("{{{ not yaml"), 0o600); err != nil {
		t.Fatal(err)
	}
	if s := preflightRotationSettings(); s.Explicit || s.Retain != 48*time.Hour {
		t.Fatalf("unreadable file: %+v, want the flags' 48h", s)
	}
}

package consoleapp

import (
	"context"
	"github.com/dbtrail/dbtrail/internal/config"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// The console half of the RDS global-lock refusal: the snapshot job's error
// must name the console's own setting, not the CLI flag.
func TestRunMydumperFTWRLDeniedNamesTheSetting(t *testing.T) {
	dir := t.TempDir()
	script := "#!/bin/bash\n" +
		"if [ \"$1\" = \"--version\" ]; then printf '%s\\n' '" + versionModern + "'; exit 0; fi\n" +
		"printf '%s\\n' \"** (mydumper:4242): CRITICAL **: 23:14:02.118: Couldn't acquire global lock, snapshots will not be consistent: Access denied for user 'admin'@'%' (using password: YES)\" >&2\n" +
		"exit 1\n"
	if err := os.WriteFile(filepath.Join(dir, "mydumper"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	stubPreflight(t, nil)

	err := runMydumper(context.Background(), "admin:p@tcp(127.0.0.1:1)/", config.SSL{Mode: "disabled"}, []string{"appdb"},
		filepath.Join(t.TempDir(), "out"), baseline.LockModeFTWRL, lockModeFromEnv)
	if err == nil {
		t.Fatal("runMydumper succeeded over a failed mydumper")
	}
	t.Logf("error: %v", err)
	for _, want := range []string{"RDS", "lock-all", "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=lock-all"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q: %v", want, err)
		}
	}
	// #1986: the snapshot settings have no lock control since #1846.
	if strings.Contains(err.Error(), "Lock while dumping") || strings.Contains(err.Error(), "snapshot settings") {
		t.Errorf("the console error sends the operator to a control that does not exist: %v", err)
	}
	if strings.Contains(err.Error(), "--lock-mode") {
		t.Errorf("the console error names the CLI flag: %v", err)
	}
}

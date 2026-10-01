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

// #1380: the full snapshot the web interface takes leaves in its dump the
// lock mode it gave mydumper, which baseline.Run puts in the snapshot. A
// build that was not given the mode leaves nothing.

func lockRecordAfterRun(t *testing.T, version string, mode baseline.LockMode) string {
	t.Helper()
	fakeConsoleMydumper(t, printsVersion(version))
	stubPreflight(t, nil)
	out := filepath.Join(t.TempDir(), "out")
	if err := os.Mkdir(out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", config.SSL{Mode: "disabled"}, []string{"appdb"}, out, mode, lockModeFromEnv); err != nil {
		t.Fatalf("runMydumper: %v", err)
	}
	if err := os.WriteFile(filepath.Join(out, "metadata"), []byte("Started dump at: 2026-06-10 12:00:00\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	md, err := baseline.ParseMetadata(out)
	if err != nil {
		t.Fatalf("ParseMetadata: %v", err)
	}
	return md.LockMode
}

func TestRunMydumper_recordsTheLockModeItSent(t *testing.T) {
	for _, mode := range baseline.LockModeValues {
		if got := lockRecordAfterRun(t, versionModern, mode); got != string(mode) {
			t.Errorf("mode %s: the dump records %q", mode, got)
		}
	}
}

func TestRunMydumper_recordsNothingWhenTheModeWasNotSent(t *testing.T) {
	if got := lockRecordAfterRun(t, versionDistro, baseline.LockModeFTWRL); got != "" {
		t.Fatalf("a mydumper that was not given a lock mode left the record %q", got)
	}
}

// A dump that failed leaves no record: there is no dump to describe.
func TestRunMydumper_recordsNothingForAFailedDump(t *testing.T) {
	dir := t.TempDir()
	script := "#!/bin/bash\nif [ \"$1\" = \"--version\" ]; then\n" + printsVersion(versionModern) + "\nfi\nexit 3\n"
	if err := os.WriteFile(filepath.Join(dir, "mydumper"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	stubPreflight(t, nil)
	out := filepath.Join(t.TempDir(), "out")
	if err := os.Mkdir(out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", config.SSL{Mode: "disabled"}, nil, out, baseline.LockModeNoLock, lockModeFromEnv); err == nil {
		t.Fatal("a mydumper that exits 3 was taken as a dump")
	}
	if _, err := os.Stat(filepath.Join(out, baseline.LockModeMarkerFile)); err == nil {
		t.Fatal("a failed dump left a lock mode record")
	}
}

// The compose pipeline runs mydumper from a shell and converts the dump with
// `bintrail baseline`, so the shell writes the record. It must write the file
// baseline.Run reads, from the variable the mode was chosen with, after
// mydumper ran.
func TestComposeBaselineDump_recordsTheLockMode(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	compose := string(raw)
	write := `printf '%s\n' "$${BASELINE_LOCK_MODE:-ftwrl}" > /dump/snapshot/` + baseline.LockModeMarkerFile
	ran := `mydumper "$$@"`
	at, ranAt := strings.Index(compose, write), strings.Index(compose, ran)
	if at < 0 {
		t.Fatalf("docker-compose.yml does not write %s into the dump", baseline.LockModeMarkerFile)
	}
	if ranAt < 0 || at < ranAt {
		t.Fatalf("the record is written before mydumper runs (record at %d, mydumper at %d)", at, ranAt)
	}
	// The four names the shell accepts are the four this program reads.
	for _, m := range baseline.LockModeValues {
		if !strings.Contains(compose, "\n          "+string(m)+")") {
			t.Errorf("the compose case statement does not name %s", m)
		}
	}
}

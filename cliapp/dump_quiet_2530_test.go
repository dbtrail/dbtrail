package cliapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// fakeMydumperAnchored is a mydumper that writes a dump with a binlog
// position, so runDump completes; it records that it ran.
func fakeMydumperAnchored(t *testing.T, dir string) (bin, ranFile string) {
	t.Helper()
	bin = filepath.Join(dir, "mydumper")
	ranFile = filepath.Join(dir, "ran")
	script := "#!/bin/bash\n" +
		"if [ \"$1\" = \"--version\" ]; then printf 'mydumper v0.18.1, built against MySQL 8.0.36 with SSL support\\n'; exit 0; fi\n" +
		"touch " + ranFile + "\n" +
		"out=\"\"; prev=\"\"; for a in \"$@\"; do if [ \"$prev\" = \"--outputdir\" ]; then out=\"$a\"; fi; prev=\"$a\"; done\n" +
		"mkdir -p \"$out\"\n" +
		"printf 'Started dump at: 2026-09-18 03:00:00\\nSHOW MASTER STATUS:\\n\\tLog: binlog.000001\\n\\tPos: 4\\n\\nFinished dump at: 2026-09-18 03:00:01\\n' > \"$out/metadata\"\n" +
		"exit 0\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, ranFile
}

func setupQuietDump(t *testing.T, waitFn func(baseline.LockMode) error) (out, ranFile, bin string) {
	t.Helper()
	dir := t.TempDir()
	bin, ranFile = fakeMydumperAnchored(t, dir)
	stubPingSource(t)
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = os.TempDir })
	checkMydumperPrivileges = func(context.Context, string, baseline.LockMode, mydumperlock.Remedy, []string) error { return nil }
	t.Cleanup(func() { checkMydumperPrivileges = mydumperlock.CheckPrivileges })
	prevWait := waitForQuietSource
	t.Cleanup(func() { waitForQuietSource = prevWait })
	waitForQuietSource = func(_ context.Context, _ string, mode baseline.LockMode) error { return waitFn(mode) }

	out = filepath.Join(dir, "out")
	dmpSourceDSN = "admin:p@tcp(127.0.0.1:1)/"
	dmpOutputDir = out
	dmpMydumperPath = bin
	dmpLockMode = "ftwrl"
	dmpFormat = "json"
	t.Cleanup(func() { dmpLockMode = "ftwrl"; dmpSourceDSN = ""; dmpOutputDir = ""; dmpFormat = "text" })
	return out, ranFile, bin
}

// #2530: the dump-start stamp that anchors the snapshot is taken AFTER the
// wait for a quiet source, not before it.
func TestRunDump_stampsTheStartAfterTheQuietWait(t *testing.T) {
	var waitEnded time.Time
	out, ranFile, bin := setupQuietDump(t, func(baseline.LockMode) error {
		time.Sleep(1100 * time.Millisecond) // the marker keeps whole seconds
		waitEnded = time.Now().UTC()
		return nil
	})
	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	if err := runDump(cmd, nil); err != nil {
		t.Fatalf("runDump: %v", err)
	}
	if _, err := os.Stat(ranFile); err != nil {
		t.Fatal("mydumper never ran")
	}
	b, err := os.ReadFile(filepath.Join(out, baseline.StartedAtMarkerFile))
	if err != nil {
		t.Fatal(err)
	}
	stamp, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(b)))
	if err != nil {
		t.Fatalf("marker %q: %v", b, err)
	}
	if stamp.Before(waitEnded.Truncate(time.Second)) {
		t.Fatalf("dump start stamped %v, before the wait ended at %v", stamp, waitEnded)
	}
}

// A source never quiet: mydumper never runs, and the refusal is the error.
func TestRunDump_aSourceNeverQuietIsRefused(t *testing.T) {
	_, ranFile, bin := setupQuietDump(t, func(baseline.LockMode) error {
		return &mydumperlock.QuietWaitError{Waited: 10 * time.Minute}
	})
	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	err := runDump(cmd, nil)
	var qe *mydumperlock.QuietWaitError
	if !errors.As(err, &qe) {
		t.Fatalf("err = %v, want the QuietWaitError", err)
	}
	if _, serr := os.Stat(ranFile); serr == nil {
		t.Error("mydumper ran after the source was never quiet")
	}
}

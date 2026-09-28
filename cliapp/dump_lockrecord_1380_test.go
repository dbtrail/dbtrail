package cliapp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #1380: `bintrail dump` leaves in the dump the lock mode it gave mydumper,
// for `bintrail baseline` to put in the snapshot. It leaves nothing when the
// mode was not given: what mydumper chose then is not on record.

// runDumpForLockRecord runs the real runDump over a fake mydumper and returns
// the dump's metadata as `bintrail baseline` reads it.
func runDumpForLockRecord(t *testing.T, versionLine, lockMode string) baseline.DumpMetadata {
	t.Helper()
	dir := t.TempDir()
	bin, record := fakeMydumperVersion(t, dir, versionLine)

	stubPingSource(t)
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = os.TempDir })

	out := filepath.Join(dir, "out")
	if err := os.Mkdir(out, 0o755); err != nil {
		t.Fatal(err)
	}
	dmpSourceDSN = "u:p@tcp(127.0.0.1:1)/"
	dmpOutputDir = out
	dmpMydumperPath = bin
	dmpFormat = "text"
	t.Cleanup(func() { dmpLockMode = "ftwrl"; dmpSourceDSN = ""; dmpOutputDir = "" })

	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	if lockMode != "" {
		if err := cmd.Flags().Set("lock-mode", lockMode); err != nil {
			t.Fatal(err)
		}
	}
	if err := runDump(cmd, nil); err != nil {
		t.Fatalf("runDump: %v", err)
	}
	if _, err := os.Stat(record); err != nil {
		t.Fatalf("mydumper never ran: %v", err)
	}
	// The fake writes no metadata; this is the file a real mydumper leaves.
	if err := os.WriteFile(filepath.Join(out, "metadata"), []byte("Started dump at: 2026-06-10 12:00:00\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	md, err := baseline.ParseMetadata(out)
	if err != nil {
		t.Fatalf("ParseMetadata: %v", err)
	}
	return md
}

func TestRunDump_recordsTheLockModeItGaveMydumper(t *testing.T) {
	const modern = "mydumper v0.18.1, built against MySQL 8.0.36 with SSL support"
	for _, c := range []struct {
		mode string
		want baseline.ReadConsistency
	}{
		{"no-lock", baseline.ReadTorn},
		{"safe-no-lock", baseline.ReadConsistent},
	} {
		md := runDumpForLockRecord(t, modern, c.mode)
		if md.LockMode != c.mode {
			t.Errorf("--lock-mode %s: the dump records %q", c.mode, md.LockMode)
		}
		if got := baseline.ReadConsistencyOf(md); got != c.want {
			t.Errorf("--lock-mode %s: the dump reads %s, want %s", c.mode, got, c.want)
		}
	}
}

// A build too old for --sync-thread-lock-mode takes its own mode. The default
// the operator did not change is ftwrl, and writing that down would call the
// dump locked on nobody's word.
func TestRunDump_recordsNothingWhenTheModeWasNotSent(t *testing.T) {
	md := runDumpForLockRecord(t, "mydumper 0.10.1 (built with foo)", "")
	if md.LockMode != "" {
		t.Fatalf("a mydumper that was not given a lock mode left the record %q", md.LockMode)
	}
	if got := baseline.ReadConsistencyOf(md); got != baseline.ReadUnknown {
		t.Fatalf("the dump reads %s, want unknown", got)
	}
}

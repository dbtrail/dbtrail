package cliapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// On RDS and Aurora even the master user, who holds RELOAD, cannot take the
// global read lock the default mode uses. The privilege check passes, mydumper
// runs and fails with "Access denied", which reads like a password problem.
// The error must say that the lock mode has to be lock-all there.
func TestRunDumpFTWRLDeniedNamesLockAll(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "mydumper")
	script := "#!/bin/bash\n" +
		"if [ \"$1\" = \"--version\" ]; then printf '%s\\n' 'mydumper v0.18.1, built against MySQL 8.0.36 with SSL support'; exit 0; fi\n" +
		"printf '%s\\n' \"** (mydumper:4242): CRITICAL **: 23:14:02.118: Couldn't acquire global lock, snapshots will not be consistent: Access denied for user 'admin'@'%' (using password: YES)\" >&2\n" +
		"exit 1\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	stubPingSource(t)
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = os.TempDir })
	checkMydumperPrivileges = func(context.Context, string, baseline.LockMode, mydumperlock.Remedy, []string) error { return nil }
	t.Cleanup(func() { checkMydumperPrivileges = mydumperlock.CheckPrivileges })

	dmpSourceDSN = "admin:p@tcp(127.0.0.1:1)/"
	dmpOutputDir = filepath.Join(dir, "out")
	dmpMydumperPath = bin
	dmpLockMode = "ftwrl"
	dmpFormat = "json"
	t.Cleanup(func() { dmpLockMode = "ftwrl"; dmpSourceDSN = ""; dmpOutputDir = ""; dmpFormat = "text" })

	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	err := runDump(cmd, nil)
	if err == nil {
		t.Fatal("runDump succeeded over a failed mydumper")
	}
	t.Logf("error: %v", err)
	for _, want := range []string{"RDS", "--lock-mode lock-all", "Access denied"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q: %v", want, err)
		}
	}
}

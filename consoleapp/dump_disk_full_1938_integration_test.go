//go:build integration

package consoleapp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// smallFSEnv names a directory on a filesystem too small for the dump below
// (about 64 MiB of random text): a loop-mounted image or a size-limited
// tmpfs. The test needs root to make one, so it does not make it itself.
const smallFSEnv = "BINTRAIL_TEST_SMALL_FS"

// #1938 with nothing faked: the real mydumper on PATH dumps a real table into
// a real filesystem that is too small, and the failure that comes back is the
// working folder's, with the folder's own free space, and the partial dump is
// gone. This is what proves the words runMydumper matches are the ones a real
// full disk produces, and that statfs agrees.
//
// It SKIPS without BINTRAIL_TEST_SMALL_FS, which CI does not set: the unit
// tests carry the captured output, this one is run by hand when mydumper's
// pinned version changes.
func TestIntegrationDumpFillsARealWorkingFolder_1938(t *testing.T) {
	small := os.Getenv(smallFSEnv)
	if small == "" {
		t.Skipf("%s is not set: no small filesystem to fill", smallFSEnv)
	}
	if _, err := exec.LookPath("mydumper"); err != nil {
		t.Fatalf("%s is set and mydumper is not on PATH", smallFSEnv)
	}
	db, name := testutil.CreateTestDB(t)
	if _, err := db.Exec("CREATE TABLE big (id BIGINT PRIMARY KEY AUTO_INCREMENT, pad VARCHAR(600) NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO big (pad) VALUES (HEX(RANDOM_BYTES(300)))"); err != nil {
		t.Fatal(err)
	}
	for range 17 { // 2^17 rows of about 600 bytes: a dump of about 80 MiB
		if _, err := db.Exec("INSERT INTO big (pad) SELECT HEX(RANDOM_BYTES(300)) FROM big"); err != nil {
			t.Fatal(err)
		}
	}
	stage, err := os.MkdirTemp(small, "work-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(stage) })
	freeBefore, ok, why := measureFree(stage)
	if !ok {
		t.Fatalf("free space at %s cannot be measured: %s", stage, why)
	}
	if freeBefore > 70<<20 {
		t.Fatalf("%s has %s free: too much for an 80 MiB dump to fill", stage, humanSize(int64(freeBefore)))
	}

	s := newBaselineSupervisor(context.Background(), stage, baseline.LockModeNoLock)
	req := console.BaselineRequest{ServerID: "s1", ServerName: "shop-db", SourceDSN: testutil.BaseDSN() + "/", Schemas: []string{name}}
	_, err = s.dumpAttempt(req, baseline.LockModeNoLock, lockModeFromEnv)
	t.Logf("error: %v", err)

	var full *workingFolderFullError
	if !errors.As(err, &full) || !foldDiskRefused(err) {
		t.Fatalf("err = %v, want the working folder's disk failure", err)
	}
	if full.folder != stage || full.free >= dumpFullFreeBelow || full.rmErr != nil {
		t.Fatalf("folder=%q free=%d rmErr=%v", full.folder, full.free, full.rmErr)
	}
	var noSpace *mydumperNoSpaceError
	if !errors.As(err, &noSpace) || !strings.Contains(err.Error(), mydumperNoSpaceText) {
		t.Fatalf("mydumper's own failure is not in the chain: %v", err)
	}
	if left, _ := filepath.Glob(filepath.Join(stage, "*")); len(left) != 0 {
		t.Fatalf("the working folder still holds %v", left)
	}
	// With the partial dump gone the folder has its room back, which is why
	// the measurement has to come before the removal.
	if after, _, _ := measureFree(stage); after < freeBefore/2 {
		t.Fatalf("free after cleanup = %d, before the dump = %d", after, freeBefore)
	}

	// The control: the same dump with room is a success, so the failure above
	// is the disk's and not this test's wiring.
	roomy := t.TempDir()
	s2 := newBaselineSupervisor(context.Background(), roomy, baseline.LockModeNoLock)
	att, err := s2.dumpAttempt(req, baseline.LockModeNoLock, lockModeFromEnv)
	if err != nil {
		t.Fatalf("the same dump with room failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(att.dir, "metadata")); err != nil {
		t.Fatalf("the roomy dump has no metadata: %v", err)
	}
}

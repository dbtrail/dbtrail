//go:build unix

package cliapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/icebergexport"
)

// TestRunExportIceberg_everyReleasesLockBetweenRuns: on the local (hadoop)
// catalog the single-writer lock is held per run, so between two runs of the
// loop another export can take it. The REAL export runs here: it takes the
// lock, fails on the unreachable index, and the loop carries on.
func TestRunExportIceberg_everyReleasesLockBetweenRuns(t *testing.T) {
	resetCatalogGlobals(t)
	everyValid(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sRun, sSleep := exportRunFn, exportSleepFn
	t.Cleanup(func() { exportRunFn, exportSleepFn = sRun, sSleep })
	exportRunFn = icebergexport.Run
	lockPath := filepath.Join(eiWarehouse, ".bintrail-export.lock")
	waits := 0
	exportSleepFn = func(ctx context.Context, _ time.Duration) error {
		waits++
		f, err := os.OpenFile(lockPath, os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("wait %d: the run did not create the lock file: %v", waits, err)
		}
		defer f.Close()
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			t.Fatalf("wait %d: the lock is still held between runs: %v", waits, err)
		}
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		if waits == 2 {
			cancel()
			return ctx.Err()
		}
		return nil
	}
	var errb strings.Builder
	exportIcebergCmd.SetContext(ctx)
	exportIcebergCmd.SetOut(&strings.Builder{})
	exportIcebergCmd.SetErr(&errb)
	t.Cleanup(func() { exportIcebergCmd.SetErr(nil) })
	if err := runExportIceberg(exportIcebergCmd, nil); err != nil {
		t.Fatalf("runExportIceberg = %v, want nil", err)
	}
	if waits != 2 || strings.Count(errb.String(), "connect to index") != 2 {
		t.Fatalf("waits = %d; want two failed runs logged:\n%s", waits, errb.String())
	}
}

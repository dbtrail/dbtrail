package consoleapp

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/console"
)

// #2517: one Info line per refresh cycle with the integrity checks it ran:
// files read whole (and their MB) and files skipped as verified earlier.

func TestReportIntegrityChecks_logsTheCycleDelta(t *testing.T) {
	baselineintegrity.ForgetVerified()
	t.Cleanup(baselineintegrity.ForgetVerified)
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))

	snap := t.TempDir()
	path := filepath.Join(snap, "db", "t.parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, 2*1024*1024), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := baselineintegrity.WriteManifest(snap); err != nil {
		t.Fatal(err)
	}
	baselineintegrity.ForgetVerified()
	// Aged past the memo's racy margin (a file modified seconds ago is never
	// remembered), so the second and third checks are skips.
	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	// A check before the cycle starts must not be counted in it.
	if err := baselineintegrity.ValidateLocalFileOnce(path); err != nil {
		t.Fatal(err)
	}
	baselineintegrity.ForgetVerified()

	before := snapshotIntegrityCounts()
	for range 3 {
		if err := baselineintegrity.ValidateLocalFileOnce(path); err != nil {
			t.Fatal(err)
		}
	}
	reportIntegrityChecks("srv", before)
	out := buf.String()
	if !strings.Contains(out, `msg="baseline refresh: integrity checks this cycle"`) {
		t.Fatalf("no summary line:\n%s", out)
	}
	for _, want := range []string{"server=srv", "read_files=1", "read_mb=2.0", "skipped_files=2"} {
		if !strings.Contains(out, want) {
			t.Errorf("the summary lacks %q:\n%s", want, out)
		}
	}
}

// The wiring: runRefresh reports the delta even on a cycle that published
// nothing (its reads happened all the same).
func TestRunRefresh_reportsIntegrityChecksEveryCycle(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: t.TempDir()},
		time.Now().UTC(), time.Nanosecond)

	out := buf.String()
	if !strings.Contains(out, "published nothing") {
		t.Fatalf("fixture: the cycle did not run: %q", out)
	}
	if !strings.Contains(out, `msg="baseline refresh: integrity checks this cycle"`) || !strings.Contains(out, "read_files=0") {
		t.Errorf("the cycle did not report its integrity checks:\n%s", out)
	}
}

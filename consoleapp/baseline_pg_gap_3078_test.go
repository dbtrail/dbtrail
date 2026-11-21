package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

func stubPGCaptureStart(t *testing.T, lsn uint64, ok bool) {
	t.Helper()
	prev := pgCaptureStartFunc
	t.Cleanup(func() { pgCaptureStartFunc = prev })
	pgCaptureStartFunc = func(console.BaselineRequest) (uint64, bool) { return lsn, ok }
}

// What the index knows of the capture's start against a snapshot's floor
// (#3078): only a start after the floor is a gap.
func TestPGRefuseKnownGap_3078(t *testing.T) {
	const floor = 0x2_64ED_0000
	for _, tc := range []struct {
		name    string
		start   uint64
		known   bool
		refused bool
	}{
		{"no start on record (capture has not saved a position)", 0, false, false},
		{"capture started before the snapshot (the usual case)", floor - 0x1000, true, false},
		{"capture starts at the slot the snapshot created", floor, true, false},
		{"capture started after the snapshot's floor", floor + 0x1E68, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubPGCaptureStart(t, tc.start, tc.known)
			err := pgRefuseKnownGap(console.BaselineRequest{ServerID: "pg1"}, pgbaseline.Stats{DeltaStartLSN: floor})
			if (err != nil) != tc.refused {
				t.Fatalf("err = %v, want refused = %v", err, tc.refused)
			}
			if err == nil {
				return
			}
			for _, want := range []string{"the snapshot is not published", "2/64ED1E68", "2/64ED0000", "Run the snapshot again"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not say %q:\n%v", want, err)
				}
			}
		})
	}
}

// With no index to ask there is nothing to compare.
func TestPGCaptureStartFunc_NoIndex_3078(t *testing.T) {
	if lsn, ok := pgCaptureStartFunc(console.BaselineRequest{}); ok || lsn != 0 {
		t.Fatalf("no index: got %d, %v", lsn, ok)
	}
}

// executePG hands the read its question before the publish, and a refused
// read is a failed read: its folder is removed and the error is the job's.
func TestExecutePG_KnownGapFailsTheRead_3078(t *testing.T) {
	at := time.Date(2026, 11, 21, 14, 38, 55, 0, time.UTC)
	name := reconstruct.SnapshotDirName(at)
	prevRun, prevW := pgBaselineRun, snapshotWriterIDFunc
	t.Cleanup(func() { pgBaselineRun, snapshotWriterIDFunc = prevRun, prevW })
	snapshotWriterIDFunc = func(string) (string, error) { return "w", nil }
	pgBaselineRun = func(_ context.Context, cfg pgbaseline.Config) (pgbaseline.Stats, error) {
		cfg.DirStarting(cfg.OutputDir, name)
		st := pgbaseline.Stats{TablesProcessed: 2, SnapshotTime: at, DeltaStartLSN: 100}
		if cfg.BeforePublish == nil {
			t.Error("executePG runs the read without its question before the publish")
		} else if err := cfg.BeforePublish(st); err != nil {
			writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, name), baseline.IncompleteMarker)
			return st, err
		}
		writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, name), baseline.SuccessMarker)
		return st, nil
	}

	t.Run("capture started after the floor", func(t *testing.T) {
		stubPGCaptureStart(t, 200, true)
		local := t.TempDir()
		out, err := pgSupervisor(t).executePG(pgFullReadRequest(local, ""))
		if err == nil || !strings.Contains(err.Error(), "the snapshot is not published") || !strings.Contains(err.Error(), "was removed") {
			t.Fatalf("err = %v, want the gap and that the folder was removed", err)
		}
		if out.snapDir != "" {
			t.Errorf("a refused read returned the snapshot %q to publish", out.snapDir)
		}
		if _, serr := os.Stat(filepath.Join(local, name)); !os.IsNotExist(serr) {
			t.Errorf("the refused read's folder is still there: %v", serr)
		}
	})
	t.Run("capture started at or before the floor", func(t *testing.T) {
		stubPGCaptureStart(t, 100, true)
		local := t.TempDir()
		out, err := pgSupervisor(t).executePG(pgFullReadRequest(local, ""))
		if err != nil || out.snapDir != filepath.Join(local, name) {
			t.Fatalf("out = %+v, err = %v", out, err)
		}
	})
}

// A slot that never got its position reaches the job as the read's refusal,
// with nothing to remove: no folder was started.
func TestExecutePG_SlotNotReadyIsTheJobsError_3078(t *testing.T) {
	prevRun, prevW := pgBaselineRun, snapshotWriterIDFunc
	t.Cleanup(func() { pgBaselineRun, snapshotWriterIDFunc = prevRun, prevW })
	snapshotWriterIDFunc = func(string) (string, error) { return "w", nil }
	refusal := errors.Join(errors.New(`pgcapture: no table was read: replication slot "s" is still being created after 5m0s`), pgcapture.ErrSlotNotReady)
	pgBaselineRun = func(context.Context, pgbaseline.Config) (pgbaseline.Stats, error) { return pgbaseline.Stats{}, refusal }
	local := t.TempDir()
	_, err := pgSupervisor(t).executePG(pgFullReadRequest(local, ""))
	if !errors.Is(err, pgcapture.ErrSlotNotReady) || !strings.Contains(err.Error(), "no table was read") || strings.Contains(err.Error(), "removed") {
		t.Fatalf("err = %v", err)
	}
	if entries, _ := os.ReadDir(local); len(entries) != 0 {
		t.Errorf("the refused read left %v", entries)
	}
}

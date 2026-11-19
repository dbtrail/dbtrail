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
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// recordingJournal is the run's journal, recorded.
type recordingJournal struct{ dirs []string }

func (j *recordingJournal) Created(root, name string) { j.dirs = append(j.dirs, filepath.Join(root, name)) }

// A PostgreSQL full read keeps the snapshot folder it creates the way a
// MySQL read does (#1938, #2180): journaled as it is created, removed when
// the read fails. Before, a read that failed on a full disk left one more
// _INCOMPLETE folder at every slot, which nothing removes.
func TestExecutePG_failedReadRemovesItsFolder(t *testing.T) {
	at := time.Date(2026, 11, 19, 20, 0, 0, 0, time.UTC)
	name := reconstruct.SnapshotDirName(at)
	stub := func(t *testing.T, beforeDir bool, fail error) {
		t.Helper()
		prevRun, prevW := pgBaselineRun, snapshotWriterIDFunc
		t.Cleanup(func() { pgBaselineRun, snapshotWriterIDFunc = prevRun, prevW })
		snapshotWriterIDFunc = func(string) (string, error) { return "w", nil }
		pgBaselineRun = func(_ context.Context, cfg pgbaseline.Config) (pgbaseline.Stats, error) {
			if beforeDir {
				// Refused before any folder: a connection, the slot.
				return pgbaseline.Stats{}, fail
			}
			if cfg.DirStarting != nil {
				cfg.DirStarting(cfg.OutputDir, name)
			}
			if fail != nil {
				writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, name), baseline.IncompleteMarker)
				return pgbaseline.Stats{SnapshotTime: at}, fail
			}
			writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, name), baseline.SuccessMarker)
			return pgbaseline.Stats{TablesProcessed: 1, SnapshotTime: at}, nil
		}
	}
	full := errors.New("write orders.parquet: no space left on device")

	t.Run("fails after creating its folder", func(t *testing.T) {
		stub(t, false, full)
		local := t.TempDir()
		j := &recordingJournal{}
		req := pgFullReadRequest(local, "")
		req.Journal = j
		_, err := pgSupervisor(t).executePG(req)
		if err == nil || !errors.Is(err, full) || !strings.Contains(err.Error(), "was removed") {
			t.Fatalf("err = %v, want the read's error and that its folder was removed", err)
		}
		if _, serr := os.Stat(filepath.Join(local, name)); !os.IsNotExist(serr) {
			t.Errorf("the failed read's folder is still there: %v", serr)
		}
		if len(j.dirs) != 1 || j.dirs[0] != filepath.Join(local, name) {
			t.Errorf("journaled %v, want the folder", j.dirs)
		}
	})
	t.Run("fails before any folder", func(t *testing.T) {
		stub(t, true, errors.New("connect: refused"))
		local := t.TempDir()
		j := &recordingJournal{}
		req := pgFullReadRequest(local, "")
		req.Journal = j
		_, err := pgSupervisor(t).executePG(req)
		if err == nil || strings.Contains(err.Error(), "removed") {
			t.Fatalf("err = %v", err)
		}
		if len(j.dirs) != 0 {
			t.Errorf("journaled %v for a read that created nothing", j.dirs)
		}
	})
	t.Run("a folder that already holds files stays", func(t *testing.T) {
		stub(t, false, full)
		local := t.TempDir()
		other := filepath.Join(local, name)
		writeSnapshotFiles(t, other)
		j := &recordingJournal{}
		req := pgFullReadRequest(local, "")
		req.Journal = j
		if _, err := pgSupervisor(t).executePG(req); err == nil {
			t.Fatal("no error")
		}
		if _, serr := os.Stat(filepath.Join(other, "shop", "orders.parquet")); serr != nil {
			t.Errorf("a folder that was not this run's alone was touched: %v", serr)
		}
		if len(j.dirs) != 0 {
			t.Errorf("journaled %v, a folder that was not vacant", j.dirs)
		}
	})
	t.Run("a read that finishes keeps its folder", func(t *testing.T) {
		stub(t, false, nil)
		local := t.TempDir()
		j := &recordingJournal{}
		req := pgFullReadRequest(local, "")
		req.Journal = j
		out, err := pgSupervisor(t).executePG(req)
		if err != nil {
			t.Fatal(err)
		}
		if out.snapDir != filepath.Join(local, name) {
			t.Errorf("snapDir %q", out.snapDir)
		}
		if _, serr := os.Stat(filepath.Join(local, name, baseline.SuccessMarker)); serr != nil {
			t.Errorf("the finished snapshot is gone: %v", serr)
		}
		if len(j.dirs) != 1 {
			t.Errorf("journaled %v", j.dirs)
		}
	})
}

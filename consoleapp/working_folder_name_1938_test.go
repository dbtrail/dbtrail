package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
)

// #1938: a full read whose working folder cannot be created fails with a
// message that names it the way the settings row does. It is the one failure
// where the reader has to find that row.
func TestExecute_anUncreatableWorkingFolderIsNamedAsOne(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "a-file")
	if err := os.WriteFile(blocker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(blocker, "work")
	calls := countMydumper(t)
	s := newBaselineSupervisor(context.Background(), stage, baseline.LockModeFTWRL)
	req := console.BaselineRequest{ServerID: "s1", SourceDSN: "src", S3: "s3://b/p"}

	_, err := s.execute(req)
	if err == nil || !strings.HasPrefix(err.Error(), "create the working folder "+stage+": ") {
		t.Fatalf("execute: err = %v, want it to name the working folder %s", err, stage)
	}
	if strings.Contains(err.Error(), "staging") {
		t.Fatalf("execute: the old name is still in the message: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("mydumper ran %d time(s) with no folder to write to", calls.Load())
	}

	_, _, err = s.executePG(req)
	if err == nil || !strings.HasPrefix(err.Error(), "create the working folder "+stage+": ") {
		t.Fatalf("executePG: err = %v, want it to name the working folder %s", err, stage)
	}
}

// #1938: the console's full read converts a dump that is its own, in a
// folder it removes when the run ends, so it has each table's dump data
// removed as soon as the table is converted, and it never retries from a
// dump (the two cannot be combined).
func TestDumpBaselineConfig_removesConvertedDataAndNeverRetries(t *testing.T) {
	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.LockModeFTWRL)
	cfg := s.dumpBaselineConfig(console.BaselineRequest{ServerID: "s1"}, "/dump", "/out", time.Now(), "", "")
	if !cfg.RemoveConvertedData || cfg.Retry {
		t.Fatalf("RemoveConvertedData = %v, Retry = %v", cfg.RemoveConvertedData, cfg.Retry)
	}
}

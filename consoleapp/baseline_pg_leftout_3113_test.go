package consoleapp

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// A table the PostgreSQL read left out reaches the job's stats, which is what
// the run's page and history show, and its key keeps the next update from
// calling it a new table and starting a full read that would leave it out
// again. The odd schemas are the case: their keys start with a dot.
func TestExecutePG_TablesLeftOutReachTheRun_3113(t *testing.T) {
	at := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	name := reconstruct.SnapshotDirName(at)
	left := []baseline.LeftOutTable{
		{Table: `"public"."a/b"`, Reason: "why", Schema: "public", Name: "a/b"},
		{Table: `".."."t"`, Reason: "why", Schema: "..", Name: "t"},
		{Table: `"."."t"`, Reason: "why", Schema: ".", Name: "t"},
	}
	prevRun, prevW := pgBaselineRun, snapshotWriterIDFunc
	t.Cleanup(func() { pgBaselineRun, snapshotWriterIDFunc = prevRun, prevW })
	snapshotWriterIDFunc = func(string) (string, error) { return "w", nil }
	pgBaselineRun = func(_ context.Context, cfg pgbaseline.Config) (pgbaseline.Stats, error) {
		cfg.DirStarting(cfg.OutputDir, name)
		writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, name), baseline.SuccessMarker)
		return pgbaseline.Stats{TablesProcessed: 1, SnapshotTime: at, TablesLeftOut: left}, nil
	}
	stubPGCaptureStart(t, 0, false)
	out, err := pgSupervisor(t).executePG(pgFullReadRequest(t.TempDir(), ""))
	if err != nil {
		t.Fatal(err)
	}
	if len(out.stats.TablesLeftOut) != len(left) {
		t.Fatalf("the run's stats carry %d left-out tables, want %d", len(out.stats.TablesLeftOut), len(left))
	}
	keys := console.LeftOutKeys(leftOutOf(out.stats))
	snapshot := []string{"public.keep"}
	source := []string{"public.keep", "public.a/b", "...t", "..t", "public.really_new"}
	got := withoutKnownLeftOut(tablesLeftOut(snapshot, source, false), snapshot, keys, false)
	if !slices.Equal(got, []string{"public.really_new"}) {
		t.Fatalf("new tables = %q, want only the really new one", got)
	}
}

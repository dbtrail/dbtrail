package consoleapp

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
)

// Where the readers of a PostgreSQL snapshot start is read from its files'
// footers whether table deltas are on or off (#2757): a table carried forward
// unchanged keeps an older start than the snapshot's time.
func TestPublishedReadsFromPostgres(t *testing.T) {
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	older := at.Add(-3 * time.Hour)
	prevPG, prev := snapshotReadsFromPG, snapshotReadsFrom
	t.Cleanup(func() { snapshotReadsFromPG, snapshotReadsFrom = prevPG, prev })
	var pgReads, reads int
	snapshotReadsFromPG = func(context.Context, string, time.Time) (time.Time, error) { pgReads++; return older, nil }
	snapshotReadsFrom = func(context.Context, string, time.Time) (time.Time, error) { reads++; return at, nil }

	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: t.TempDir()}

	if from, ok := sup.publishedReadsFrom(req, at, true); !ok || !from.Equal(older) || pgReads != 1 || reads != 0 {
		t.Fatalf("PostgreSQL, deltas off: from %v ok %v (pg reads %d, reads %d), want the files' start", from, ok, pgReads, reads)
	}
	if from, ok := sup.publishedReadsFrom(req, at, false); !ok || !from.Equal(at) || pgReads != 1 || reads != 0 {
		t.Fatalf("MySQL, deltas off: from %v ok %v (pg reads %d, reads %d), want the snapshot's time and no read", from, ok, pgReads, reads)
	}

	// The scheduler, with no memo of the snapshot (a daemon just started).
	pg := console.ServerEntry{ID: "p", DSN: "d", BaselineDir: t.TempDir(), Flavor: console.FlavorPostgres}
	if from, ok := sup.snapshotReadersStart(context.Background(), pg, at); !ok || !from.Equal(older) || pgReads != 2 || reads != 0 {
		t.Fatalf("scheduler, PostgreSQL: from %v ok %v (pg reads %d, reads %d)", from, ok, pgReads, reads)
	}
	my := console.ServerEntry{ID: "m", DSN: "d", BaselineDir: t.TempDir()}
	if from, ok := sup.snapshotReadersStart(context.Background(), my, at); !ok || !from.Equal(at) || pgReads != 2 || reads != 0 {
		t.Fatalf("scheduler, MySQL: from %v ok %v (pg reads %d, reads %d)", from, ok, pgReads, reads)
	}
}

// A PostgreSQL update that carries unchanged tables forward is handed the
// line that ends a carried file's life (#2757), with table deltas off too:
// without it nothing rewrites a quiet table before the index drops the
// changes its file starts from.
func TestRunRefresh_postgresCarryDrawsTheLineWithDeltasOff(t *testing.T) {
	cases := []struct {
		name      string
		pg, carry bool
		line      bool
	}{
		{name: "PostgreSQL carrying forward", pg: true, carry: true, line: true},
		{name: "PostgreSQL rewriting", pg: true},
		{name: "MySQL carrying forward", carry: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			folds := probeFolds(t)
			stubCoverage(t, true, true)
			stubLiveFloor(t, refreshAt.Add(-12*time.Hour), true)
			prev := snapshotReadsFromPG
			t.Cleanup(func() { snapshotReadsFromPG = prev })
			snapshotReadsFromPG = func(context.Context, string, time.Time) (time.Time, error) { return refreshAt, nil }
			sup, req, _ := deltasRig(t, false)
			req.SourcePostgres, req.CarryForwardUnchanged = tc.pg, tc.carry
			sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
			sup.runRefresh(req, refreshAt, 5*time.Minute)
			cfgs := folds.all()
			if len(cfgs) != 1 {
				t.Fatalf("folded %d time(s), want 1", len(cfgs))
			}
			if got := !cfgs[0].ChainStartFloor.IsZero(); got != tc.line {
				t.Fatalf("ChainStartFloor = %s, want a line: %v", cfgs[0].ChainStartFloor, tc.line)
			}
		})
	}
}

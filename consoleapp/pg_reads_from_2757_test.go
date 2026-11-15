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

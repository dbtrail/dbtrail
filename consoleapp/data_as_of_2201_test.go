package consoleapp

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2201: an update records the newest change the copy it publishes holds, so
// the Overview can say how old the DATA is instead of how long ago the copy
// was written. An update made while capture is an hour behind writes a copy
// whose data is an hour old, however fresh its files are.

func TestFoldDataAsOf(t *testing.T) {
	at := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	hourAgo := at.Add(-time.Hour)
	for _, c := range []struct {
		name          string
		ancestor      time.Time
		ancestorKnown bool
		newest        time.Time
		newestKnown   bool
		want          time.Time
		wantKnown     bool
	}{
		{name: "capture an hour behind: the newest change folded", newest: hourAgo, newestKnown: true, want: hourAgo, wantKnown: true},
		// The source clock ahead of this host: an event stamped after the
		// instant the fold targets is not folded, so it cannot date the copy.
		{name: "source clock ahead: never later than the fold's own instant", newest: at.Add(2 * time.Minute), newestKnown: true, want: at, wantKnown: true},
		// A full read is current as of its own instant even while capture is
		// behind; an update from it that folded nothing newer holds the same.
		{name: "full-read ancestor newer than the index: carried forward", ancestor: at.Add(-5 * time.Minute), ancestorKnown: true,
			newest: hourAgo, newestKnown: true, want: at.Add(-5 * time.Minute), wantKnown: true},
		{name: "ancestor older than what was folded: the folded change", ancestor: at.Add(-2 * time.Hour), ancestorKnown: true,
			newest: hourAgo, newestKnown: true, want: hourAgo, wantKnown: true},
		// The index did not answer: the ancestor still bounds the copy from
		// below, and only that is said.
		{name: "index silent, ancestor known: the ancestor", ancestor: hourAgo, ancestorKnown: true, want: hourAgo, wantKnown: true},
		{name: "index silent, ancestor unknown: unknown"},
		// Nothing indexed yet and no ancestor on record: nothing to date.
		{name: "empty index, ancestor unknown: unknown", newestKnown: true},
		{name: "empty index, ancestor known: the ancestor", newestKnown: true, ancestor: hourAgo, ancestorKnown: true, want: hourAgo, wantKnown: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, ok := foldDataAsOf(c.ancestor, c.ancestorKnown, c.newest, c.newestKnown, at)
			if ok != c.wantKnown || !got.Equal(c.want) {
				t.Fatalf("foldDataAsOf = %v, %v; want %v, %v", got, ok, c.want, c.wantKnown)
			}
		})
	}
}

// stubNewestIndexed replaces the read of the index's newest change.
func stubNewestIndexed(t *testing.T, newest *time.Time, known *bool) {
	t.Helper()
	prev := readNewestIndexed
	t.Cleanup(func() { readNewestIndexed = prev })
	readNewestIndexed = func(context.Context, string) (time.Time, bool) { return *newest, *known }
}

// TestRunRefresh_recordsDataAsOf drives the real refresh loop: the run record
// carries the newest change the published copy holds, the next update from a
// full read on record carries that read's instant forward, and an update
// whose index did not answer records nothing rather than a guess.
func TestRunRefresh_recordsDataAsOf(t *testing.T) {
	stubCoverage(t, true, true)
	local := t.TempDir()
	injectFold(t, 0, nil)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.history = openHistoryForTest(t)
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: local}
	full := refreshAt.Add(-5 * time.Minute)
	writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(full)), baseline.SuccessMarker)
	mark := indexMark{events: 1000, schemaChanges: 1}
	stubIndexMark(t, &mark, true)
	newest, known := refreshAt.Add(-time.Hour), true
	stubNewestIndexed(t, &newest, &known)

	// The ancestor is a snapshot nobody recorded (the CLI's): only what was
	// folded dates the copy.
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(req, refreshAt, time.Minute)
	runs := sup.history.List("s")
	if len(runs) != 1 || runs[0].DataAsOf != refreshAt.Add(-time.Hour).Format(time.RFC3339) {
		t.Fatalf("update an hour behind = %+v, want data as of an hour before its instant", runs)
	}

	// A full read this daemon recorded, newer than anything indexed: the
	// update from it holds the read's instant, not the index's older change.
	fullRead := refreshAt.Add(8 * time.Minute)
	writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(fullRead)), baseline.SuccessMarker)
	if err := sup.history.Append(console.BaselineRunRecord{ServerID: "s", Kind: console.BaselineRunDump,
		SnapshotTime: fullRead.Format(time.RFC3339), StartedAt: fullRead.Format(time.RFC3339), FinishedAt: fullRead.Format(time.RFC3339)}); err != nil {
		t.Fatal(err)
	}
	mark = indexMark{events: 2000, schemaChanges: 1}
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(req, refreshAt.Add(10*time.Minute), time.Minute)
	runs = sup.history.List("s")
	if got := runs[len(runs)-1]; got.Kind != console.BaselineRunRefresh || got.DataAsOf != fullRead.Format(time.RFC3339) {
		t.Fatalf("update after a full read = %+v, want the read's instant carried forward", got)
	}

	// The index did not answer the read of its newest change, and the
	// ancestor (the update just above) is on record: its value stands.
	known = false
	mark = indexMark{events: 3000, schemaChanges: 1}
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(req, refreshAt.Add(15*time.Minute), time.Minute)
	runs = sup.history.List("s")
	if got := runs[len(runs)-1]; got.DataAsOf != fullRead.Format(time.RFC3339) {
		t.Fatalf("update with the index silent = %+v, want the ancestor's value, never a guess", got)
	}
}

package consoleapp

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2675, point 8: the daemon-wide --baseline-refresh-interval names no
// destination and uploads nothing, so on a server with an S3 destination
// and a count its snapshots were taken for failed uploads by the cleanup,
// which sent them, up to three per cleanup. They are now marked as kept on
// this machine on purpose, and the count removes them like any held one.

func TestBaselineRefreshTargets_holdOnlyWhereTheCountPrunes_2675(t *testing.T) {
	entries := []console.ServerEntry{
		{ID: "a", Name: "s3-and-count", DSN: "d", BaselineDir: "/srv/a", BaselineS3: "s3://bucket/a/", LocalKeepNewest: 3},
		{ID: "b", Name: "s3-no-count", DSN: "d", BaselineDir: "/srv/b", BaselineS3: "s3://bucket/b/"},
		{ID: "c", Name: "count-no-s3", DSN: "d", BaselineDir: "/srv/c", LocalKeepNewest: 3},
		{ID: "d", Name: "shared-folder-1", DSN: "d", BaselineDir: "/srv/d", BaselineS3: "s3://bucket/d1/", LocalKeepNewest: 3},
		{ID: "e", Name: "shared-folder-2", DSN: "d", BaselineDir: "/srv/d", BaselineS3: "s3://bucket/d2/", LocalKeepNewest: 3},
		{ID: "f", Name: "the-daemons-folder", DSN: "d", BaselineDir: "/srv/global", BaselineS3: "s3://bucket/f/", LocalKeepNewest: 3},
		{ID: "g", Name: "blank-destination", DSN: "d", BaselineDir: "/srv/g", BaselineS3: "  ", LocalKeepNewest: 3},
	}
	reqs, _, _ := baselineRefreshTargets(entries, "dsn", "/srv/global")
	got := map[string]bool{}
	for _, r := range reqs {
		got[r.ServerName] = r.HoldFromS3
		if r.BaselineS3 != "" {
			t.Fatalf("%s: the daemon-wide loop got a destination %q", r.ServerName, r.BaselineS3)
		}
	}
	want := map[string]bool{"boot": false, "s3-and-count": true, "s3-no-count": false, "count-no-s3": false,
		"shared-folder-1": false, "the-daemons-folder": false, "blank-destination": false}
	for name, w := range want {
		if h, ok := got[name]; ok && h != w {
			t.Errorf("%s: HoldFromS3 = %v, want %v", name, h, w)
		}
	}
	if !got["s3-and-count"] {
		t.Fatal("the server the issue names is not held")
	}
}

func TestRunRefresh_theLoopsSnapshotIsHeldBesideACount_2675(t *testing.T) {
	for _, hold := range []bool{true, false} {
		stubGateReads(t)
		local := stageBaselineRoot(t)
		injectFold(t, 0, nil)
		out, st := runRefreshFor(t, refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: local, HoldFromS3: hold})
		if out != "" || !st.Published || st.State != "succeeded" {
			t.Fatalf("hold=%v: warned %q, status %+v", hold, out, st)
		}
		held, err := baseline.UploadHeld(local, reconstruct.SnapshotDirName(refreshAt))
		if err != nil || held != hold {
			t.Fatalf("hold=%v: the snapshot carries the held mark %v (%v)", hold, held, err)
		}
	}
}

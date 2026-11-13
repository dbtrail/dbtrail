package consoleapp

import (
	"context"
	"errors"
	"testing"
	"time"

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
	sched := &console.BackupSchedule{Every: "1h", At: "00:00", UploadEvery: "1d"}
	entries := []console.ServerEntry{
		{ID: "a", Name: "s3-count-schedule", DSN: "d", BaselineDir: "/srv/a", BaselineS3: "s3://bucket/a/", LocalKeepNewest: 3, BackupSchedule: sched},
		{ID: "a2", Name: "s3-count-loop-only", DSN: "d", BaselineDir: "/srv/a2", BaselineS3: "s3://bucket/a2/", LocalKeepNewest: 3},
		{ID: "a3", Name: "s3-count-schedule-unparsable", DSN: "d", BaselineDir: "/srv/a3", BaselineS3: "s3://bucket/a3/", LocalKeepNewest: 3,
			BackupSchedule: &console.BackupSchedule{Every: "soon"}},
		{ID: "b", Name: "s3-no-count", DSN: "d", BaselineDir: "/srv/b", BaselineS3: "s3://bucket/b/", BackupSchedule: sched},
		{ID: "c", Name: "count-no-s3", DSN: "d", BaselineDir: "/srv/c", LocalKeepNewest: 3, BackupSchedule: sched},
		{ID: "g", Name: "blank-destination", DSN: "d", BaselineDir: "/srv/g", BaselineS3: "  ", LocalKeepNewest: 3, BackupSchedule: sched},
		// The folder stopped being shared and still holds the other
		// server's snapshots: never pruned to a count (heldNow).
		{ID: "h", Name: "folder-still-holding-another", DSN: "d", BaselineDir: "/srv/h", LocalKeepHeldDir: "/srv/h",
			BaselineS3: "s3://bucket/h/", LocalKeepNewest: 3, BackupSchedule: sched},
		// Two servers, two folders, one S3 prefix: neither is pruned to a
		// count (LocalKeepSharedS3).
		{ID: "p1", Name: "same-prefix-1", DSN: "d", BaselineDir: "/srv/p1", BaselineS3: "s3://bucket/shared/", LocalKeepNewest: 3, BackupSchedule: sched},
		{ID: "p2", Name: "same-prefix-2", DSN: "d", BaselineDir: "/srv/p2", BaselineS3: "s3://bucket/shared", LocalKeepNewest: 3, BackupSchedule: sched},
	}
	reqs, _, _ := baselineRefreshTargets(entries, "dsn", "/srv/global")
	got := map[string]bool{}
	for _, r := range reqs {
		got[r.ServerName] = r.HoldFromS3
		if r.BaselineS3 != "" {
			t.Fatalf("%s: the daemon-wide loop got a destination %q", r.ServerName, r.BaselineS3)
		}
	}
	want := map[string]bool{"boot": false, "s3-count-schedule": true, "s3-count-loop-only": false, "s3-count-schedule-unparsable": false,
		"s3-no-count": false, "count-no-s3": false, "blank-destination": false, "folder-still-holding-another": false}
	// Two servers on one S3 prefix are refused as targets before the hold is
	// decided (console.SharedLocationRefusal), so the loop writes nothing
	// for either: required absent, not compared.
	for _, name := range []string{"same-prefix-1", "same-prefix-2"} {
		if _, ok := got[name]; ok {
			t.Errorf("%s: a server sharing its S3 prefix became a target of the loop", name)
		}
	}
	for name, w := range want {
		h, ok := got[name]
		if !ok {
			t.Errorf("%s: not a target of the loop, so the row tests nothing", name)
			continue
		}
		if h != w {
			t.Errorf("%s: HoldFromS3 = %v, want %v", name, h, w)
		}
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

// Review of #2727: a server with an S3 destination and a count that only
// the daemon-wide loop refreshes (no schedule of its own) must keep sending
// to the bucket, since nothing would ever send a held snapshot of it; one
// with its own schedule is held, and the schedule's due upload sends it.
func TestLoopSnapshot_reachesTheBucketWithOrWithoutASchedule_2675(t *testing.T) {
	for _, withSchedule := range []bool{false, true} {
		stubGateReads(t)
		local := stageBaselineRoot(t)
		injectFold(t, 0, nil)
		uploads, _ := stubS3Fold(t, nil, nil)
		e := console.ServerEntry{ID: "a", Name: "s3-and-count", DSN: "d", BaselineDir: local, BaselineS3: "s3://bucket/a/", LocalKeepNewest: 3}
		if withSchedule {
			e.BackupSchedule = &console.BackupSchedule{Every: "1h", At: "00:00", UploadEvery: "1d"}
		}
		reqs, _, _ := baselineRefreshTargets([]console.ServerEntry{e}, "", "")
		if len(reqs) != 1 || reqs[0].HoldFromS3 != withSchedule {
			t.Fatalf("schedule=%v: targets %+v", withSchedule, reqs)
		}
		if _, st := runRefreshFor(t, reqs[0]); !st.Published {
			t.Fatalf("schedule=%v: the loop's refresh did not publish: %+v", withSchedule, st)
		}
		name := reconstruct.SnapshotDirName(refreshAt)
		held, err := baseline.UploadHeld(local, name)
		if err != nil || held != withSchedule {
			t.Fatalf("schedule=%v: held=%v (%v)", withSchedule, held, err)
		}
		sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
		sent := func() bool {
			for _, u := range *uploads {
				if u.dest == "s3://bucket/a/"+name {
					return true
				}
			}
			return false
		}
		err = sup.resendLacking(context.Background(), local, "s3://bucket/a/", name)
		if !withSchedule {
			// The cleanup's resend: the newest snapshot reaches S3.
			if err != nil || !sent() {
				t.Fatalf("no schedule: the cleanup did not send the loop's snapshot (err %v, uploads %+v)", err, *uploads)
			}
			continue
		}
		if !errors.Is(err, errNotResent) || sent() {
			t.Fatalf("with a schedule: the cleanup sent a held snapshot (err %v)", err)
		}
		// The schedule's own request, as it is built for that server.
		b, _, _ := newScheduleFixture(t, true)
		p, perr := e.BackupSchedule.Parse()
		if perr != nil {
			t.Fatal(perr)
		}
		sup.sendOwedHeldUpload(b.rebuildRequest(e, p), refreshAt.Add(time.Hour))
		if !sent() {
			t.Fatalf("with a schedule: its due upload did not send the held snapshot (uploads %+v)", *uploads)
		}
	}
}

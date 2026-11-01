package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2596: a server that updates every 5 minutes and sends to S3 once per slot
// of its own upload grid.

// stubBucketNewest answers the bucket listing with one snapshot at newest (none
// when zero), or with err; a local directory is listed for real.
func stubBucketNewest(t *testing.T, newest time.Time, err error) {
	t.Helper()
	real := listBaselines
	t.Cleanup(func() { listBaselines = real })
	listBaselines = func(ctx context.Context, src string) ([]reconstruct.BaselineFile, error) {
		if !strings.HasPrefix(src, "s3://") {
			return real(ctx, src)
		}
		if err != nil {
			return nil, err
		}
		if newest.IsZero() {
			return nil, nil
		}
		return []reconstruct.BaselineFile{{Schema: "shop", Table: "orders", SnapshotTime: newest}}, nil
	}
}

func TestRunRefresh_uploadInterval(t *testing.T) {
	// refreshAt is 10:00. With the grid on :10 and 1h, its upload slot is 09:10.
	const every = time.Hour
	const gridAt = 10 * time.Minute
	cases := []struct {
		name       string
		uploadEvry time.Duration
		bucket     time.Time
		listErr    error
		wantUpload bool
		wantWarn   string
	}{
		{name: "the bucket has this slot: held", uploadEvry: every, bucket: refreshAt.Add(-30 * time.Minute)},
		{name: "the bucket is from before the slot: sent", uploadEvry: every, bucket: refreshAt.Add(-55 * time.Minute), wantUpload: true},
		{name: "the bucket is empty: sent", uploadEvry: every, wantUpload: true},
		{name: "the bucket cannot be listed: sent, and said", uploadEvry: every, listErr: errors.New("throttled"), wantUpload: true,
			wantWarn: "could not list the S3 destination to apply the upload interval"},
		{name: "no interval: sent as before", bucket: refreshAt.Add(-time.Minute), wantUpload: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stubGateReads(t)
			local := stageBaselineRoot(t)
			uploads, _ := stubS3Fold(t, []string{"shop.orders"}, nil)
			stubBucketNewest(t, c.bucket, c.listErr)
			injectFold(t, 0, nil)

			prev := slog.Default()
			t.Cleanup(func() { slog.SetDefault(prev) })
			var buf bytes.Buffer
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
			sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
			sup.history = openHistoryForTest(t)
			sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
			sup.runRefresh(refreshRequest{
				ServerID: "s", ServerName: "s", IndexDSN: "d",
				BaselineDir: local, BaselineS3: "s3://bucket/backups/",
				UploadEvery: c.uploadEvry, UploadAt: gridAt,
			}, refreshAt, 5*time.Minute)

			st := sup.RefreshStatus("s")
			if !st.Published || st.State != "succeeded" {
				t.Fatalf("status = %+v, want a published success either way", st)
			}
			if got := len(*uploads) == 1; got != c.wantUpload {
				t.Fatalf("uploads = %+v, want upload=%v", *uploads, c.wantUpload)
			}
			name := reconstruct.SnapshotDirName(refreshAt)
			held, err := baseline.UploadHeld(local, name)
			if err != nil || held == c.wantUpload {
				t.Fatalf("held mark = %v (err %v), want %v", held, err, !c.wantUpload)
			}
			runs := sup.history.List("s")
			if len(runs) != 1 || runs[0].UploadHeld == c.wantUpload || runs[0].Error != "" {
				t.Fatalf("history = %+v, want upload_held=%v and no error", runs, !c.wantUpload)
			}
			out := buf.String()
			if c.wantWarn == "" && out != "" {
				t.Fatalf("warned: %s", out)
			}
			if c.wantWarn != "" && !strings.Contains(out, c.wantWarn) {
				t.Fatalf("log %q lacks %q", out, c.wantWarn)
			}
		})
	}
}

// The schedule stamps its upload grid onto the request it dispatches. Without
// this line the setting is saved, shown, and never applied.
func TestRebuildRequest_stampsTheUploadGrid(t *testing.T) {
	b, reg, _ := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	e.BaselineS3 = "s3://bucket/backups/"
	p, err := console.BackupSchedule{Every: "5m", At: "00:10", UploadEvery: "1h"}.Parse()
	if err != nil {
		t.Fatal(err)
	}
	got := b.rebuildRequest(e, p)
	if got.UploadEvery != time.Hour || got.UploadAt != 10*time.Minute || got.BaselineS3 != e.BaselineS3 {
		t.Fatalf("dispatched %+v, want the 1h grid on 00:10 and the destination", got)
	}
}

// The full read's sweep sends a failed upload, never a held one.
func TestDump_sweepSkipsSnapshotsTheUploadIntervalHeld(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 9, 18, 11, 0, 0, 0, time.UTC)
	held, failed := at.Add(-2*time.Hour), at.Add(-time.Hour)
	for _, ts := range []time.Time{held, failed} {
		writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(ts)), baseline.SuccessMarker)
	}
	if err := baseline.MarkUploadHeld(local, reconstruct.SnapshotDirName(held)); err != nil {
		t.Fatal(err)
	}
	root := "s3://bucket/backups/"
	ds := stubDumpUpload(t, map[string]bool{
		root + reconstruct.SnapshotDirName(held) + "/" + baseline.SuccessMarker:   false,
		root + reconstruct.SnapshotDirName(failed) + "/" + baseline.SuccessMarker: false,
	}, nil)
	close(ds.hold)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: root}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	completeDumpOwn(sup, req, dumpOutcomeAt(t, local, at), nil)

	if st := sup.Status("a"); st.State != "succeeded" || st.Swept != 1 {
		t.Fatalf("status = %+v, want exactly the failed one swept", st)
	}
	for _, c := range ds.calls {
		if c.dest == root+reconstruct.SnapshotDirName(held) {
			t.Fatalf("the sweep sent a snapshot the upload interval held: %+v", ds.calls)
		}
	}
}

// The prune's resend, the other path that sends what the bucket lacks.
func TestResendLacking_skipsAHeldSnapshot(t *testing.T) {
	root := t.TempDir()
	name := "2026-10-19T03-15-06Z"
	writeSnapshotFiles(t, filepath.Join(root, name), baseline.SuccessMarker)
	if err := baseline.MarkUploadHeld(root, name); err != nil {
		t.Fatal(err)
	}
	var sent int
	prev := uploadSnapshot
	t.Cleanup(func() { uploadSnapshot = prev })
	uploadSnapshot = func(context.Context, string, string, string, bool) (int, error) {
		sent++
		return 1, nil
	}
	s := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	if err := s.resendLacking(context.Background(), root, "s3://bkt/p/", name); err != nil || sent != 0 {
		t.Fatalf("resend of a held snapshot: err %v, uploads %d", err, sent)
	}
}

// A quiet source after a held snapshot: the cycles the #1689 gate skips are
// the only ones left, so they must send the held one once its slot comes.
func TestSendOwedHeldUpload(t *testing.T) {
	newestLocal := time.Date(2026, 11, 1, 10, 5, 0, 0, time.UTC)
	cases := []struct {
		name     string
		now      time.Time
		bucket   time.Time
		markHeld bool
		every    time.Duration
		wantSent bool
	}{
		{name: "slot moved on: sent", now: newestLocal.Add(time.Hour), bucket: newestLocal.Add(-5 * time.Minute), markHeld: true, every: time.Hour, wantSent: true},
		{name: "same slot: not yet", now: newestLocal.Add(10 * time.Minute), bucket: newestLocal.Add(-5 * time.Minute), markHeld: true, every: time.Hour},
		{name: "not held (sent, or a failed upload): left to its own path", now: newestLocal.Add(time.Hour), bucket: newestLocal.Add(-5 * time.Minute), every: time.Hour},
		{name: "the bucket already has it", now: newestLocal.Add(time.Hour), bucket: newestLocal, markHeld: true, every: time.Hour},
		{name: "no interval", now: newestLocal.Add(time.Hour), bucket: newestLocal.Add(-5 * time.Minute), markHeld: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			local := t.TempDir()
			name := reconstruct.SnapshotDirName(newestLocal)
			writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(newestLocal.Add(-time.Hour))), baseline.SuccessMarker)
			writeSnapshotFiles(t, filepath.Join(local, name), baseline.SuccessMarker)
			if c.markHeld {
				if err := baseline.MarkUploadHeld(local, name); err != nil {
					t.Fatal(err)
				}
			}
			stubBucketNewest(t, c.bucket, nil)
			var sent []string
			prev := uploadSnapshot
			t.Cleanup(func() { uploadSnapshot = prev })
			uploadSnapshot = func(_ context.Context, dir, dest, _ string, _ bool) (int, error) {
				sent = append(sent, dir+" -> "+dest)
				return 1, nil
			}
			s := newBaselineSupervisor(context.Background(), t.TempDir(), "")
			s.sendOwedHeldUpload(refreshRequest{ServerID: "s", ServerName: "s", BaselineDir: local,
				BaselineS3: "s3://bkt/p/", UploadEvery: c.every}, c.now)
			if got := len(sent) == 1; got != c.wantSent {
				t.Fatalf("sent %v, want sent=%v", sent, c.wantSent)
			}
			if c.wantSent {
				if want := filepath.Join(local, name) + " -> s3://bkt/p/" + name; sent[0] != want {
					t.Fatalf("sent %q, want %q", sent[0], want)
				}
				if held, _ := baseline.UploadHeld(local, name); held {
					t.Fatal("the mark outlived the upload")
				}
			}
		})
	}
}

// Through runRefresh: a cycle the #1689 gate skips still sends the held
// newest snapshot once its slot came. Deleting the call in the gate's branch
// leaves TestSendOwedHeldUpload green and turns this red.
func TestRunRefresh_aSkippedCycleSendsTheOwedHeldSnapshot(t *testing.T) {
	stubCoverage(t, true, true)
	mark := indexMark{events: 100, schemaChanges: 7}
	stubIndexMark(t, &mark, true)
	local := t.TempDir()
	// Published an hour and a half ago, held, and nothing indexed since.
	heldAt := time.Now().UTC().Add(-90 * time.Minute).Truncate(time.Second)
	name := reconstruct.SnapshotDirName(heldAt)
	writeSnapshotFiles(t, filepath.Join(local, name), baseline.SuccessMarker)
	if err := baseline.MarkUploadHeld(local, name); err != nil {
		t.Fatal(err)
	}
	stubBucketNewest(t, heldAt.Add(-5*time.Minute), nil)
	var sent []string
	prev := uploadSnapshot
	t.Cleanup(func() { uploadSnapshot = prev })
	uploadSnapshot = func(_ context.Context, _, dest, _ string, _ bool) (int, error) {
		sent = append(sent, dest)
		return 1, nil
	}
	var folds int
	prevFold := foldTables
	t.Cleanup(func() { foldTables = prevFold })
	foldTables = func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		folds++
		return nil, nil, nil
	}

	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: local,
		BaselineS3: "s3://bkt/p/", UploadEvery: time.Hour}
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.foldedMarks["s"] = foldMemo{mark: mark, publishedAt: heldAt, readsFrom: heldAt, readsFromKnown: true,
		destination: refreshDestination(req), indexDSN: "d"}
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(req, time.Now().UTC(), 5*time.Minute)

	if folds != 0 {
		t.Fatalf("the gate did not skip (folded %d times), so this case tests nothing", folds)
	}
	if len(sent) != 1 || sent[0] != "s3://bkt/p/"+name {
		t.Fatalf("sent %v, want the held snapshot once", sent)
	}
	if st := sup.RefreshStatus("s"); st.State == "running" {
		t.Fatal("the skipped cycle left the slot claimed")
	}
}

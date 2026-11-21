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
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2761: a PostgreSQL full read handed the uploader the server's whole
// snapshots folder, so every full read sent every snapshot on the disk again
// (what #1725 removed for MySQL), and it recorded no index mark. It now only
// produces its snapshot; the publish, the upload of that one snapshot, the
// sweep and the record are the ones MySQL uses.

// stubPGRead replaces the PostgreSQL read with one that writes a complete
// snapshot stamped at into the folder it is asked to write to, the way
// pgbaseline does, and reports that time. A zero at reports no time and
// writes the folder under a name of its own.
func stubPGRead(t *testing.T, at time.Time, during func()) {
	t.Helper()
	prevRun, prevW := pgBaselineRun, snapshotWriterIDFunc
	t.Cleanup(func() { pgBaselineRun, snapshotWriterIDFunc = prevRun, prevW })
	snapshotWriterIDFunc = func(string) (string, error) { return "w", nil }
	pgBaselineRun = func(_ context.Context, cfg pgbaseline.Config) (pgbaseline.Stats, error) {
		if during != nil {
			during()
		}
		name := "2026-10-01T09-00-00Z"
		if !at.IsZero() {
			name = reconstruct.SnapshotDirName(at)
		}
		writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, name), baseline.SuccessMarker)
		return pgbaseline.Stats{TablesProcessed: 1, RowsWritten: 5, FilesWritten: 1, SnapshotTime: at}, nil
	}
}

func pgFullReadRequest(local, s3 string) console.BaselineRequest {
	return console.BaselineRequest{ServerID: "pg1", ServerName: "pg", Flavor: console.FlavorPostgres,
		SourceDSN: "postgres://u:p@h/appdb", Slot: "s", Publication: "p",
		LocalDir: local, S3: s3, Trigger: console.BaselineRunTriggerScheduled}
}

func pgSupervisor(t *testing.T) *baselineSupervisor {
	t.Helper()
	pgDiskCheckOff(t)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.history = openHistoryForTest(t)
	sup.jobs["pg1"] = &console.BaselineStatus{State: "running"}
	return sup
}

// snapshotFilesOf lists every file under root, relative to it, so a test can
// say that a run left the snapshots it did not write as they were.
func snapshotFilesOf(t *testing.T, root string) string {
	t.Helper()
	var got []string
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			rel, _ := filepath.Rel(root, p)
			got = append(got, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(got, "\n")
}

// The second full read, with an older snapshot already in the folder and in
// the bucket: one upload, of the new snapshot's own folder, to its own key.
func TestPGFullRead_uploadsOnlyTheSnapshotItWrote(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	older := at.Add(-24 * time.Hour)
	writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(older)), baseline.SuccessMarker)
	root := "s3://bucket/backups"
	ds := stubDumpUpload(t, map[string]bool{
		root + "/" + reconstruct.SnapshotDirName(older) + "/" + baseline.SuccessMarker: true,
	}, nil)
	close(ds.hold)
	stubPGRead(t, at, nil)
	sup := pgSupervisor(t)
	sup.run(pgFullReadRequest(local, root+"/"))

	st := sup.Status("pg1")
	if st.State != "succeeded" || !st.Published || st.Uploading || st.Uploaded != 3 || st.Swept != 0 || st.LastError != "" {
		t.Fatalf("status = %+v, want succeeded, published, 3 uploaded, none swept", st)
	}
	want := uploadCall{filepath.Join(local, reconstruct.SnapshotDirName(at)), root + "/" + reconstruct.SnapshotDirName(at)}
	if len(ds.calls) != 1 || ds.calls[0] != want || ds.retries[0] {
		t.Fatalf("uploads = %+v retries=%v, want exactly %+v without skip-existing", ds.calls, ds.retries, want)
	}
	runs := sup.history.List("pg1")
	if len(runs) != 1 || runs[0].Error != "" || runs[0].Uploaded != 3 || runs[0].SnapshotTime != at.Format(time.RFC3339) {
		t.Fatalf("history = %+v, want one successful full read naming its snapshot", runs)
	}
	assertNothingInFlight(t, sup)
}

// An older snapshot the bucket does not have (an upload that failed) used to
// reach it because the whole folder was sent. The sweep sends it now, by
// itself, to its own key, skipping objects already there.
func TestPGFullRead_sweepsAnOlderSnapshotTheBucketLacks(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	missing := at.Add(-24 * time.Hour)
	writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(missing)), baseline.SuccessMarker)
	root := "s3://bucket/backups"
	ds := stubDumpUpload(t, map[string]bool{
		root + "/" + reconstruct.SnapshotDirName(missing) + "/" + baseline.SuccessMarker: false,
	}, nil)
	close(ds.hold)
	stubPGRead(t, at, nil)
	sup := pgSupervisor(t)
	before := snapshotFilesOf(t, filepath.Join(local, reconstruct.SnapshotDirName(missing)))
	sup.run(pgFullReadRequest(local, root))

	if st := sup.Status("pg1"); st.State != "succeeded" || st.Swept != 1 {
		t.Fatalf("status = %+v, want succeeded with one swept snapshot", st)
	}
	if len(ds.calls) != 2 {
		t.Fatalf("uploads = %+v, want the new snapshot and then the one the bucket lacks", ds.calls)
	}
	wantSweep := uploadCall{filepath.Join(local, reconstruct.SnapshotDirName(missing)), root + "/" + reconstruct.SnapshotDirName(missing)}
	if ds.calls[1] != wantSweep || !ds.retries[1] {
		t.Fatalf("sweep upload = %+v retry=%v, want %+v with skip-existing", ds.calls[1], ds.retries[1], wantSweep)
	}
	for _, c := range ds.calls {
		if c.outputDir == local || strings.TrimSuffix(c.dest, "/") == root {
			t.Fatalf("an upload was handed the whole folder: %+v", c)
		}
	}
	if after := snapshotFilesOf(t, filepath.Join(local, reconstruct.SnapshotDirName(missing))); after != before {
		t.Fatalf("the older snapshot's files changed:\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

// The slot is free while the upload runs, as for MySQL: the snapshot is
// complete on the disk and a scheduled update can read it.
func TestPGFullRead_isPublishedBeforeItsUpload(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	ds := stubDumpUpload(t, map[string]bool{}, nil)
	stubPGRead(t, at, nil)
	sup := pgSupervisor(t)
	done := make(chan struct{})
	go func() { defer close(done); sup.run(pgFullReadRequest(local, "s3://bucket/backups")) }()
	waitDump(t, sup, "pg1", func(st console.BaselineStatus) bool { return st.State == "succeeded" && st.Published && st.Uploading },
		"succeeded, published, uploading")
	sup.mu.Lock()
	busy := sup.busyLocked("pg1")
	sup.mu.Unlock()
	if busy {
		t.Fatal("the server is still busy after the snapshot was published: the upload holds the slot")
	}
	close(ds.hold)
	<-done
	if st := sup.Status("pg1"); st.State != "succeeded" || st.Uploading || st.Uploaded != 3 {
		t.Fatalf("status after the upload = %+v", st)
	}
}

// A failed upload is a failed run that keeps its snapshot, and the error
// says where the snapshot is and where it did not get to. No sweep follows.
func TestPGFullRead_aFailedUploadKeepsTheSnapshotAndSaysWhere(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	older := at.Add(-24 * time.Hour)
	writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(older)), baseline.SuccessMarker)
	ds := stubDumpUpload(t, map[string]bool{}, errors.New("s3: access denied"))
	close(ds.hold)
	stubPGRead(t, at, nil)
	sup := pgSupervisor(t)
	sup.run(pgFullReadRequest(local, "s3://bucket/backups"))

	st := sup.Status("pg1")
	if st.State != "failed" || !st.Published || st.Uploading || st.Failure != nil {
		t.Fatalf("status = %+v, want failed, published, no did-not-finish failure", st)
	}
	snapDir := filepath.Join(local, reconstruct.SnapshotDirName(at))
	dest := "s3://bucket/backups/" + reconstruct.SnapshotDirName(at)
	for _, want := range []string{"upload: the snapshot was written to " + snapDir, "could not be uploaded to " + dest, "s3: access denied"} {
		if !strings.Contains(st.LastError, want) {
			t.Fatalf("error = %q, want it to contain %q", st.LastError, want)
		}
	}
	if len(ds.calls) != 1 || len(ds.probes) != 0 {
		t.Fatalf("uploads = %+v probes = %v, want the one failed upload and no sweep", ds.calls, ds.probes)
	}
	runs := sup.history.List("pg1")
	if len(runs) != 1 || runs[0].Error == "" || runs[0].SnapshotTime != at.Format(time.RFC3339) || runs[0].Failure != nil {
		t.Fatalf("history = %+v, want the failed upload with its snapshot's time", runs)
	}
	if _, err := os.Stat(filepath.Join(snapDir, baseline.SuccessMarker)); err != nil {
		t.Fatalf("the snapshot whose upload failed is not on the disk: %v", err)
	}
	assertNothingInFlight(t, sup)
}

// No S3 destination: nothing is sent and nothing is asked of a bucket.
func TestPGFullRead_withoutADestinationUploadsNothing(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(at.Add(-24*time.Hour))), baseline.SuccessMarker)
	ds := stubDumpUpload(t, map[string]bool{}, nil)
	close(ds.hold)
	stubPGRead(t, at, nil)
	sup := pgSupervisor(t)
	sup.run(pgFullReadRequest(local, ""))

	st := sup.Status("pg1")
	if st.State != "succeeded" || !st.Published || st.Uploaded != 0 || st.Tables != 1 || st.Rows != 5 {
		t.Fatalf("status = %+v, want succeeded and published with nothing uploaded", st)
	}
	if len(ds.calls) != 0 || len(ds.probes) != 0 {
		t.Fatalf("uploads = %+v probes = %v, want none", ds.calls, ds.probes)
	}
}

// A read that reports no snapshot time cannot name the folder it wrote. With
// a destination it sends nothing (never the whole folder) and says why;
// without one it ends as it did before. Either way every file stays.
func TestPGFullRead_withoutASnapshotTime(t *testing.T) {
	for _, c := range []struct {
		name, s3  string
		wantState string
	}{
		{"with a destination: nothing is sent, and the run says so", "s3://bucket/backups", "failed"},
		{"without a destination: as before", "", "succeeded"},
	} {
		t.Run(c.name, func(t *testing.T) {
			local := t.TempDir()
			writeSnapshotFiles(t, filepath.Join(local, "2026-09-30T09-00-00Z"), baseline.SuccessMarker)
			ds := stubDumpUpload(t, map[string]bool{}, nil)
			close(ds.hold)
			stubPGRead(t, time.Time{}, nil)
			sup := pgSupervisor(t)
			sup.run(pgFullReadRequest(local, c.s3))

			st := sup.Status("pg1")
			if st.State != c.wantState || st.Uploading {
				t.Fatalf("status = %+v, want %s", st, c.wantState)
			}
			if len(ds.calls) != 0 || len(ds.probes) != 0 {
				t.Fatalf("uploads = %+v probes = %v, want none", ds.calls, ds.probes)
			}
			if c.s3 != "" {
				for _, want := range []string{"without the time of its snapshot", local, c.s3} {
					if !strings.Contains(st.LastError, want) {
						t.Fatalf("error = %q, want it to contain %q", st.LastError, want)
					}
				}
			}
			runs := sup.history.List("pg1")
			if len(runs) != 1 || runs[0].SnapshotTime != "" || runs[0].IndexMarkKnown {
				t.Fatalf("history = %+v, want one run that names no snapshot and no mark", runs)
			}
			want := "2026-09-30T09-00-00Z/_SUCCESS\n2026-09-30T09-00-00Z/shop/orders.parquet\n" +
				"2026-10-01T09-00-00Z/_SUCCESS\n2026-10-01T09-00-00Z/shop/orders.parquet"
			if got := snapshotFilesOf(t, local); got != want {
				t.Fatalf("files on the disk:\n%s\nwant:\n%s", got, want)
			}
			assertNothingInFlight(t, sup)
		})
	}
}

// The index mark is read before the read starts and recorded with the
// snapshot, so the update after it is measured (#1737).
func TestPGFullRead_recordsTheIndexMarkReadBeforeIt(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	mark := indexMark{events: 5000}
	p := stubIndexMark(t, &mark, true)
	readsBefore := -1
	stubPGRead(t, at, func() {
		readsBefore = len(p.marks())
		mark.events = 9000 // indexed during the read: after the mark, so not in the base
	})
	sup := pgSupervisor(t)
	req := pgFullReadRequest(local, "")
	req.IndexDSN = "u:p@tcp(10.0.0.1:3306)/idx"
	sup.run(req)

	if readsBefore != 1 {
		t.Fatalf("the mark was read %d times before the read started, want once", readsBefore)
	}
	runs := sup.history.List("pg1")
	if len(runs) != 1 || runs[0].Error != "" || !runs[0].IndexMarkKnown || runs[0].IndexMark != 5000 {
		t.Fatalf("history = %+v, want the full read with the mark read before it (5000)", runs)
	}
	if base, ok := sup.history.IndexMarkFor("pg1", at.Format(time.RFC3339)); !ok || base != 5000 {
		t.Fatalf("IndexMarkFor the full read's snapshot = %d,%v, want 5000", base, ok)
	}
}

// A read that failed names no snapshot: nothing is published, sent or marked.
func TestPGFullRead_aFailedReadSendsNothing(t *testing.T) {
	prevRun, prevW := pgBaselineRun, snapshotWriterIDFunc
	t.Cleanup(func() { pgBaselineRun, snapshotWriterIDFunc = prevRun, prevW })
	snapshotWriterIDFunc = func(string) (string, error) { return "w", nil }
	pgBaselineRun = func(context.Context, pgbaseline.Config) (pgbaseline.Stats, error) {
		return pgbaseline.Stats{}, errors.New("connection refused")
	}
	ds := stubDumpUpload(t, map[string]bool{}, nil)
	close(ds.hold)
	sup := pgSupervisor(t)
	sup.run(pgFullReadRequest(t.TempDir(), "s3://bucket/backups"))

	st := sup.Status("pg1")
	if st.State != "failed" || st.Published || !strings.Contains(st.LastError, "pg baseline: connection refused") {
		t.Fatalf("status = %+v, want a failed, unpublished read", st)
	}
	if st.Failure == nil || !st.Failure.Postgres {
		t.Fatalf("failure = %+v, want the PostgreSQL did-not-finish failure", st.Failure)
	}
	if len(ds.calls) != 0 {
		t.Fatalf("uploads = %+v, want none", ds.calls)
	}
}

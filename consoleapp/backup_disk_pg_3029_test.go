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
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
)

// The disk check before a PostgreSQL snapshot (#3029): it refuses below the
// floor the snapshot updates keep, warns below the highest ratio measured,
// and never refuses on a guess.

func stubPGCopyEstimate(t *testing.T, est pgbaseline.CopyEstimate, err error) *[]string {
	t.Helper()
	var calls []string
	prev := pgCopyEstimateFn
	pgCopyEstimateFn = func(_ context.Context, dsn, publication string, filters event.Filters) (pgbaseline.CopyEstimate, error) {
		schemas := make([]string, 0, len(filters.Schemas))
		for s := range filters.Schemas {
			schemas = append(schemas, s)
		}
		calls = append(calls, dsn+" "+publication+" "+strings.Join(schemas, ","))
		return est, err
	}
	t.Cleanup(func() { pgCopyEstimateFn = prev })
	return &calls
}

func TestPGCopyDiskVerdict(t *testing.T) {
	dir := t.TempDir()
	est := pgbaseline.CopyEstimate{Bytes: int64(10 * gib), Tables: 3}
	verdict := func(t *testing.T, free uint64, est pgbaseline.CopyEstimate, estErr error) (string, string, error) {
		t.Helper()
		diskByPath(t, map[string]uint64{dir: free})
		return pgCopyDiskVerdict(dir, est, estErr)
	}

	t.Run("room", func(t *testing.T) {
		check, note, err := verdict(t, 100*gib, est, nil)
		want := "Disk check: the tables to copy take up about 10.0 GiB on the database server, and their snapshot at most about 13.0 GiB; 100.0 GiB free at " + dir + "."
		if err != nil || check != dumpDiskOK || note != want {
			t.Errorf("check %q err %v\n got %s\nwant %s", check, err, note, want)
		}
	})
	t.Run("exactly the peak is room, one byte less is low", func(t *testing.T) {
		if check, _, err := verdict(t, 13*gib, est, nil); err != nil || check != dumpDiskOK {
			t.Errorf("at the peak: check %q err %v", check, err)
		}
		if check, _, err := verdict(t, 13*gib-1, est, nil); err != nil || check != dumpDiskLow {
			t.Errorf("one byte under the peak: check %q err %v", check, err)
		}
	})
	t.Run("low: under the highest ratio measured, the snapshot runs with a warning", func(t *testing.T) {
		check, note, err := verdict(t, 12*gib, est, nil)
		want := "Low disk: the snapshot folder " + dir + " has 12.0 GiB free. The tables to copy take up about 10.0 GiB on the database server, " +
			"and their snapshot can reach about 13.0 GiB for binary data, which does not compress; for most data it is much smaller. " +
			"This read may fail with a full disk. Free space there, or give this server a Local folder on a bigger disk (Snapshots page, under Where and how often)."
		if err != nil || check != dumpDiskLow || note != want {
			t.Fatalf("check %q err %v\n got %s\nwant %s", check, err, note, want)
		}
		// Once the snapshot is written the warning is about the next one.
		after, afterNote := dumpDiskOnceItFit(check, note)
		if after != dumpDiskTight || !strings.Contains(afterNote, "This read fit. The next one may not.") || strings.Contains(afterNote, "may fail") {
			t.Errorf("after the snapshot was written: %q %s", after, afterNote)
		}
	})
	t.Run("free far under the tables' size is still a warning, never a refusal by size", func(t *testing.T) {
		// 2 GiB free for 10 GiB of tables: ordinary rows were measured at
		// 0.2x, so this snapshot can fit.
		if check, _, err := verdict(t, 2*gib, est, nil); err != nil || check != dumpDiskLow {
			t.Errorf("check %q err %v, want low and no refusal", check, err)
		}
	})
	t.Run("below the floor: refused with the numbers", func(t *testing.T) {
		check, note, err := verdict(t, 500<<20, est, nil)
		want := "not enough free disk space for this snapshot: the snapshot was not started. The snapshot folder " + dir + " has 500.0 MiB free, " +
			"and DBTrail keeps 1.0 GiB free there: below that no update of a snapshot is written either. " +
			"The tables to copy take up about 10.0 GiB on the database server. Nothing was read from your database, and earlier snapshots are unchanged. " +
			"Free space there, or give this server a Local folder on a bigger disk (Snapshots page, under Where and how often)."
		if err == nil || err.Error() != want || check != "" || note != "" {
			t.Fatalf("check %q note %q\n got %v\nwant %s", check, note, err, want)
		}
		if !errors.Is(err, errFoldDiskFull) || !foldDiskRefused(err) {
			t.Error("the refusal is not a disk refusal to the schedule")
		}
	})
	t.Run("exactly the floor is not refused; one byte less is, and the two sizes do not print alike", func(t *testing.T) {
		small := pgbaseline.CopyEstimate{Bytes: 1 << 20, Tables: 1}
		if check, _, err := verdict(t, gib, small, nil); err != nil || check != dumpDiskOK {
			t.Errorf("at the floor: check %q err %v", check, err)
		}
		_, _, err := verdict(t, gib-1, small, nil)
		if err == nil || !strings.Contains(err.Error(), "has 1073741823 bytes free, and DBTrail keeps 1073741824 bytes free there") {
			t.Errorf("one byte under the floor: %v", err)
		}
	})
	t.Run("below the floor with sizes that could not be read: refused, without a size", func(t *testing.T) {
		_, _, err := verdict(t, 500<<20, pgbaseline.CopyEstimate{}, errors.New("connection refused"))
		if err == nil || !errors.Is(err, errFoldDiskFull) || strings.Contains(err.Error(), "take up about") || strings.Contains(err.Error(), "connection refused") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("sizes that could not be read: the snapshot goes ahead and says so", func(t *testing.T) {
		check, note, err := verdict(t, 100*gib, pgbaseline.CopyEstimate{}, errors.New("timeout: context deadline exceeded\nsecond line"))
		want := "Disk check did not run: the table sizes could not be read from the source (timeout: context deadline exceeded). 100.0 GiB free at " + dir + ". The snapshot went ahead."
		if err != nil || check != dumpDiskUnchecked || note != want {
			t.Errorf("check %q err %v\n got %s\nwant %s", check, err, note, want)
		}
	})
	t.Run("no tables is its own fact, and the disk is not measured", func(t *testing.T) {
		diskByPath(t, map[string]uint64{})
		check, note, err := pgCopyDiskVerdict(dir, pgbaseline.CopyEstimate{}, nil)
		if err != nil || check != dumpDiskOK || note != "Disk check: the publication holds no table this snapshot copies, so there was nothing to size." {
			t.Errorf("check %q note %q err %v", check, note, err)
		}
	})
	t.Run("no size for any table: cannot tell, never a pass", func(t *testing.T) {
		check, note, err := verdict(t, 100*gib, pgbaseline.CopyEstimate{Tables: 2, Unsized: 2}, nil)
		want := "Disk check cannot tell whether this snapshot fits: the source gave no size for any of the 2 table(s) to copy. 100.0 GiB free at " + dir + ". The snapshot went ahead."
		if err != nil || check != dumpDiskUnchecked || note != want {
			t.Errorf("check %q err %v\n got %s\nwant %s", check, err, note, want)
		}
	})
	t.Run("empty tables are sized at nothing and fit", func(t *testing.T) {
		check, note, err := verdict(t, 100*gib, pgbaseline.CopyEstimate{Tables: 2}, nil)
		want := "Disk check: the tables to copy take up about 0 B on the database server, and their snapshot at most about 0 B; 100.0 GiB free at " + dir + "."
		if err != nil || check != dumpDiskOK || note != want {
			t.Errorf("check %q err %v\n got %s\nwant %s", check, err, note, want)
		}
	})
	t.Run("a table without a size, or on another storage: room by the sizes is not a pass", func(t *testing.T) {
		part := pgbaseline.CopyEstimate{Bytes: int64(10 * gib), Tables: 3, Unsized: 1, OtherStorage: 1, OtherStorageTop: []string{"ts.metrics"}}
		check, note, err := verdict(t, 100*gib, part, nil)
		want := "Disk check cannot tell whether this snapshot fits. By the sizes the server reports: the tables to copy take up about 10.0 GiB on the database server, " +
			"and their snapshot at most about 13.0 GiB; 100.0 GiB free at " + dir + ". The source gave no size for 1 table(s), so the real need is higher." +
			" 1 table is not stored as a PostgreSQL heap (ts.metrics): the size the server reports for it can be a compressed one, and a snapshot is written from its rows, so it can need more than the sizes say."
		if err != nil || check != dumpDiskUnchecked || note != want {
			t.Errorf("check %q err %v\n got %s\nwant %s", check, err, note, want)
		}
		// Low stays low, and says both.
		check, note, err = verdict(t, 5*gib, part, nil)
		if err != nil || check != dumpDiskLow || !strings.Contains(note, "no size for 1 table(s)") || !strings.Contains(note, "ts.metrics") || strings.Count(note, diskRisk) != 1 {
			t.Errorf("low: check %q err %v note %s", check, err, note)
		}
	})
	t.Run("a disk that cannot be measured never refuses", func(t *testing.T) {
		prev := diskSpaceFn
		t.Cleanup(func() { diskSpaceFn = prev })
		diskSpaceFn = func(string) (uint64, uint64, error) { return 0, 0, errors.New("statfs: input/output error") }
		check, note, err := pgCopyDiskVerdict(dir, est, nil)
		want := "Disk check did not run: the free space at " + dir + " could not be measured (statfs: input/output error). The snapshot went ahead."
		if err != nil || check != dumpDiskUnchecked || note != want {
			t.Errorf("check %q err %v\n got %s\nwant %s", check, err, note, want)
		}
		// A filesystem that reports no size (zero free of zero) is not a full disk.
		diskSpaceFn = func(string) (uint64, uint64, error) { return 0, 0, nil }
		check, note, err = pgCopyDiskVerdict(dir, est, nil)
		if err != nil || check != dumpDiskUnchecked || !strings.Contains(note, "the filesystem reports no size") {
			t.Errorf("no size reported: check %q err %v note %s", check, err, note)
		}
	})
	t.Run("a full disk that reports its size is refused", func(t *testing.T) {
		if _, _, err := verdict(t, 0, est, nil); err == nil || !errors.Is(err, errFoldDiskFull) || !strings.Contains(err.Error(), "has 0 B free") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("a snapshot folder that does not exist yet is measured where it will be created", func(t *testing.T) {
		missing := filepath.Join(dir, "not", "yet", "there")
		diskByPath(t, map[string]uint64{dir: 12 * gib})
		check, note, err := pgCopyDiskVerdict(missing, est, nil)
		if err != nil || check != dumpDiskLow || !strings.Contains(note, "the snapshot folder "+missing+" has 12.0 GiB free") {
			t.Errorf("check %q err %v note %s", check, err, note)
		}
	})
	t.Run("an estimate that reads as negative is nothing, not a huge number", func(t *testing.T) {
		if check, _, err := verdict(t, 100*gib, pgbaseline.CopyEstimate{Bytes: -5, Tables: 1}, nil); err != nil || check != dumpDiskOK {
			t.Errorf("check %q err %v", check, err)
		}
	})
}

// executePG is the one function every PostgreSQL snapshot goes through (the
// web's button, `snapshot run`, the schedule): the check runs there, before
// the snapshot is asked for anything.
func TestExecutePG_checksTheDiskBeforeTheSnapshot_3029(t *testing.T) {
	at := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	ran := 0
	prevRun, prevW := pgBaselineRun, snapshotWriterIDFunc
	t.Cleanup(func() { pgBaselineRun, snapshotWriterIDFunc = prevRun, prevW })
	snapshotWriterIDFunc = func(string) (string, error) { return "w", nil }
	pgBaselineRun = func(_ context.Context, cfg pgbaseline.Config) (pgbaseline.Stats, error) {
		ran++
		writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, "2026-11-20T12-00-00Z"), baseline.SuccessMarker)
		return pgbaseline.Stats{TablesProcessed: 1, SnapshotTime: at}, nil
	}
	est := pgbaseline.CopyEstimate{Bytes: int64(10 * gib), Tables: 3}

	t.Run("below the floor: refused, and the snapshot is never started", func(t *testing.T) {
		ran = 0
		local := t.TempDir()
		calls := stubPGCopyEstimate(t, est, nil)
		diskByPath(t, map[string]uint64{local: 200 << 20})
		sup := pgSupervisor(t)
		req := pgFullReadRequest(local, "")
		req.Schemas = []string{"shop"}
		_, err := sup.executePG(req)
		if err == nil || !errors.Is(err, errFoldDiskFull) || !strings.Contains(err.Error(), "has 200.0 MiB free") {
			t.Fatalf("err = %v, want the disk refusal", err)
		}
		if ran != 0 {
			t.Error("the snapshot was started after the refusal")
		}
		if entries, _ := os.ReadDir(local); len(entries) != 0 {
			t.Errorf("the refused snapshot left %v in its folder", entries)
		}
		// Sized: this server's source, its publication, its schemas.
		if len(*calls) != 1 || (*calls)[0] != "postgres://u:p@h/appdb p shop" {
			t.Errorf("the estimate was asked for %q", *calls)
		}
		if st := sup.Status("pg1"); st.DiskCheck != "" {
			t.Errorf("a refusal left a verdict on the status: %q", st.DiskCheck)
		}
	})
	t.Run("low: the snapshot runs and its status carries the warning", func(t *testing.T) {
		ran = 0
		local := t.TempDir()
		stubPGCopyEstimate(t, est, nil)
		diskByPath(t, map[string]uint64{local: 12 * gib})
		sup := pgSupervisor(t)
		if _, err := sup.executePG(pgFullReadRequest(local, "")); err != nil {
			t.Fatal(err)
		}
		st := sup.Status("pg1")
		if ran != 1 || st.DiskCheck != dumpDiskLow || !strings.HasPrefix(st.DiskNote, "Low disk: the snapshot folder "+local+" has 12.0 GiB free.") {
			t.Errorf("ran %d, status check %q note %s", ran, st.DiskCheck, st.DiskNote)
		}
	})
	t.Run("sizes that cannot be read: the snapshot runs and says the check did not", func(t *testing.T) {
		ran = 0
		local := t.TempDir()
		stubPGCopyEstimate(t, pgbaseline.CopyEstimate{}, errors.New("no route to host"))
		diskByPath(t, map[string]uint64{local: 50 * gib})
		sup := pgSupervisor(t)
		if _, err := sup.executePG(pgFullReadRequest(local, "")); err != nil {
			t.Fatal(err)
		}
		if st := sup.Status("pg1"); ran != 1 || st.DiskCheck != dumpDiskUnchecked || !strings.Contains(st.DiskNote, "(no route to host)") {
			t.Errorf("ran %d, status check %q note %s", ran, st.DiskCheck, st.DiskNote)
		}
	})
	t.Run("no local folder: refused for that, before any size is read", func(t *testing.T) {
		ran = 0
		calls := stubPGCopyEstimate(t, est, nil)
		_, err := pgSupervisor(t).executePG(console.BaselineRequest{ServerID: "pg1", Flavor: console.FlavorPostgres,
			SourceDSN: "postgres://u:p@h/appdb", Slot: "s", Publication: "p", S3: "s3://b/p"})
		if err == nil || !strings.Contains(err.Error(), "has no local snapshot folder") || errors.Is(err, errFoldDiskFull) {
			t.Fatalf("err = %v", err)
		}
		if ran != 0 || len(*calls) != 0 {
			t.Errorf("ran %d, estimate calls %q", ran, *calls)
		}
	})
}

// A refused first snapshot is recorded as a disk refusal, with its numbers,
// through the whole run: status, and the flag that keeps the schedule from
// answering it with another full read into the same disk.
func TestPGSnapshotRun_recordsTheDiskRefusal_3029(t *testing.T) {
	prevRun, prevW := pgBaselineRun, snapshotWriterIDFunc
	t.Cleanup(func() { pgBaselineRun, snapshotWriterIDFunc = prevRun, prevW })
	snapshotWriterIDFunc = func(string) (string, error) { return "w", nil }
	pgBaselineRun = func(context.Context, pgbaseline.Config) (pgbaseline.Stats, error) {
		t.Error("the snapshot was started")
		return pgbaseline.Stats{}, nil
	}
	local := t.TempDir()
	stubPGCopyEstimate(t, pgbaseline.CopyEstimate{Bytes: int64(40 * gib), Tables: 12}, nil)
	diskByPath(t, map[string]uint64{local: 300 << 20})
	sup := pgSupervisor(t)
	sup.run(pgFullReadRequest(local, ""))
	st := sup.Status("pg1")
	if st.State != "failed" || !st.DiskRefused {
		t.Fatalf("status = %+v, want failed and a disk refusal", st)
	}
	for _, want := range []string{"has 300.0 MiB free", "DBTrail keeps 1.0 GiB free there", "take up about 40.0 GiB", "Nothing was read from your database"} {
		if !strings.Contains(st.LastError, want) {
			t.Errorf("the status error lacks %q:\n%s", want, st.LastError)
		}
	}
}

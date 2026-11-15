//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	pgUpTable = "pgup_orders"
	pgUpSlot  = "bintrail_pgup_slot"
	pgUpPub   = "bintrail_pgup_pub"
)

// TestIntegrationPGFullReadUploadsItsOwnSnapshot runs two full reads of a real
// PostgreSQL source into one folder, with capture writing a real index, and a
// destination that only records what it is handed (no bucket is reached).
//
// #2761: the second read used to hand the uploader the whole folder, the
// first read's snapshot included. Each read must send the folder the database
// named for it, and record the index mark read from the PostgreSQL capture's
// index.
func TestIntegrationPGFullReadUploadsItsOwnSnapshot(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	_, source, exec1 := pgTestSource(t, ctx, pgUpTable, pgUpSlot, pgUpPub, "id int PRIMARY KEY, status text")
	exec1("INSERT INTO " + pgUpTable + " VALUES (1, 'new'), (2, 'new')")
	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	_, reg, sup := newScheduleFixture(t, true)

	snapshots := t.TempDir()
	e, err := reg.Add(console.ServerEntry{
		Name: "pg-upload", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pgUpSlot, SourcePublication: pgUpPub,
		Schemas: "public", BaselineDir: snapshots,
	})
	if err != nil {
		t.Fatalf("add the server: %v", err)
	}
	if e.DSN, err = mon.DeriveIndexDSN(e.ID); err != nil {
		t.Fatal(err)
	}
	if err := reg.Update(e); err != nil {
		t.Fatalf("save the index connection: %v", err)
	}
	pgTestStart(t, ctx, mon, bootDSN, source, e)

	// The destination is on the request only, so nothing but the full read's
	// own upload and sweep, both stubbed, ever asks for it.
	const root = "s3://bintrail-test-no-such-bucket/pg"
	present := map[string]bool{}
	ds := stubDumpUpload(t, present, nil)
	close(ds.hold)
	req := console.BaselineRequestFor(e)
	req.S3 = root

	fullRead := func(what string) {
		t.Helper()
		if err := sup.Trigger(req); err != nil {
			t.Fatalf("start %s: %v", what, err)
		}
		pgTestWait(t, what, 2*time.Minute, func() (bool, string) {
			st := sup.Status(e.ID)
			if st.State == "failed" {
				t.Fatalf("%s failed: %s", what, st.LastError)
			}
			return st.State == "succeeded" && !st.Uploading, st.State
		})
	}
	uploads := func() []uploadCall {
		ds.mu.Lock()
		defer ds.mu.Unlock()
		return append([]uploadCall(nil), ds.calls...)
	}

	fullRead("the first full read")
	first := pgUpdSnapshots(t, snapshots)
	if len(first) != 1 {
		t.Fatalf("after the first full read: want one snapshot, got %v", first)
	}
	want1 := uploadCall{filepath.Join(snapshots, first[0]), root + "/" + first[0]}
	if got := uploads(); len(got) != 1 || got[0] != want1 {
		t.Fatalf("first full read's uploads = %+v, want exactly %+v", got, want1)
	}
	if _, err := os.Stat(filepath.Join(want1.outputDir, baseline.SuccessMarker)); err != nil {
		t.Fatalf("the folder handed to the uploader is not a complete snapshot: %v", err)
	}

	// A change captured into the index, so the second read's mark is past zero.
	exec1("INSERT INTO " + pgUpTable + " VALUES (3, 'new')")
	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	var indexed uint64
	pgTestWait(t, "the change indexed", 90*time.Second, func() (bool, string) {
		var max sql.NullInt64
		if err := idx.QueryRowContext(ctx, "SELECT MAX(event_id) FROM binlog_events").Scan(&max); err != nil {
			return false, err.Error()
		}
		indexed = uint64(max.Int64)
		return max.Int64 > 0, "max event_id=" + strconv.FormatInt(max.Int64, 10)
	})

	// Snapshot folders are named to the second. The bucket has the first one.
	time.Sleep(1100 * time.Millisecond)
	present[root+"/"+first[0]+"/"+baseline.SuccessMarker] = true
	firstFiles := snapshotFilesOf(t, filepath.Join(snapshots, first[0]))

	fullRead("the second full read")
	both := pgUpdSnapshots(t, snapshots)
	if len(both) != 2 || both[0] != first[0] {
		t.Fatalf("after the second full read: want the first snapshot and a new one, got %v", both)
	}
	want2 := uploadCall{filepath.Join(snapshots, both[1]), root + "/" + both[1]}
	got := uploads()
	if len(got) != 2 || got[1] != want2 {
		t.Fatalf("uploads = %+v, want the second full read to add exactly %+v", got, want2)
	}
	if after := snapshotFilesOf(t, filepath.Join(snapshots, first[0])); after != firstFiles {
		t.Fatalf("the first snapshot's files changed:\nbefore:\n%s\nafter:\n%s", firstFiles, after)
	}

	// The second read's record names its snapshot and the mark read from the
	// index the PostgreSQL capture writes.
	at, ok := snapshotdir.ParseTime(both[1])
	if !ok {
		t.Fatalf("snapshot folder %q does not parse as a time", both[1])
	}
	mark, ok := sup.history.IndexMarkFor(e.ID, at.UTC().Format(time.RFC3339))
	if !ok || mark < indexed {
		t.Fatalf("index mark recorded for the second full read = %d (known=%v), want at least %d", mark, ok, indexed)
	}
	if st := sup.Status(e.ID); st.Uploaded != 3 || st.Swept != 0 || !st.Published {
		t.Fatalf("status = %+v, want published, 3 uploaded, none swept", st)
	}
}

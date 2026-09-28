package console

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
)

// writeChainFixture writes <dir>/<snapshot>/<schema>/<table>.parquet, a real
// Parquet file, and the pairs 0..last of a chain of deltas beside it that
// records chainStart. last < 0 writes the table alone.
func writeChainFixture(t *testing.T, dir string, snapshot time.Time, schema, table string, last int, chainStart time.Time) string {
	t.Helper()
	folder := filepath.Join(dir, reconstruct.SnapshotDirName(snapshot), schema)
	if err := os.MkdirAll(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(folder, table+".parquet")
	cols := []baseline.Column{{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)}}
	w, err := baseline.NewWriter(base, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
		baseline.MetaKeyBinlogFile: "binlog.000042", baseline.MetaKeyBinlogPos: "12345",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for seq := 0; seq <= last; seq++ {
		md := baseline.WithDeltaSeq(map[string]string{
			baseline.MetaKeyBinlogFile: "binlog.000042", baseline.MetaKeyBinlogPos: "20000",
			baseline.MetaKeyDeltaBaseAnchor: "binlog.000042:12345", baseline.MetaKeyDeltaBaseSize: "1",
			baseline.MetaKeyDeltaChainStart: chainStart.UTC().Format(time.RFC3339),
		}, seq)
		if err := baseline.WriteTableDeltaPair(base, seq, cols, md, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	return base
}

// baselinesWithFloor serves GET /api/baselines over dir with the index's
// delta coverage starting at floor.
func baselinesWithFloor(t *testing.T, dir string, floor time.Time) baselinesResponse {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("PARTITION_NAME FROM information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow(floor.Format("p_2006010215")))
	mock.ExpectQuery(`MIN\(partition_name\)`).
		WillReturnRows(sqlmock.NewRows([]string{"min", "max", "sources"}).AddRow(nil, nil, 0))
	srv := newBaselineServer(t, dir, true)
	srv.cm.boot.db = db
	srv.cm.boot.dbName = "binlog_index"
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got baselinesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

// TestBaselinesAPI_gradesOnTheChainStart is #1707 on the Snapshots page. The
// newest snapshot is an hour old and delta coverage starts ten hours back,
// so graded on its own time it is "ok". Its table is read from where its
// chain of deltas started.
func TestBaselinesAPI_gradesOnTheChainStart(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	floor := now.Add(-10 * time.Hour).Truncate(time.Hour)
	snap := now.Add(-time.Hour)

	for _, tc := range []struct {
		name               string
		write              func(t *testing.T, dir string)
		row, headline      string
		readsFrom          time.Time
		secondRow, comment string
	}{
		{
			name: "the chain started before coverage",
			write: func(t *testing.T, dir string) {
				writeChainFixture(t, dir, snap, "shop", "orders", 3, floor.Add(-8*time.Hour))
			},
			row: "broken", headline: "broken", readsFrom: floor.Add(-8 * time.Hour),
		},
		{
			name: "the chain started inside coverage",
			write: func(t *testing.T, dir string) {
				writeChainFixture(t, dir, snap, "shop", "orders", 3, snap.Add(-2*time.Hour))
			},
			row: "ok", headline: "ok", readsFrom: snap.Add(-2 * time.Hour),
		},
		{
			name: "a full backup: the chain starts at the snapshot",
			write: func(t *testing.T, dir string) {
				writeChainFixture(t, dir, snap, "shop", "orders", 0, snap)
			},
			row: "ok", headline: "ok",
		},
		{
			name: "no chain: graded on the snapshot, as before",
			write: func(t *testing.T, dir string) {
				writeChainFixture(t, dir, snap, "shop", "orders", -1, time.Time{})
			},
			row: "ok", headline: "ok",
		},
		{
			name: "one table of the snapshot is past coverage, the other is not",
			write: func(t *testing.T, dir string) {
				writeChainFixture(t, dir, snap, "shop", "orders", 1, floor.Add(-time.Hour))
				writeChainFixture(t, dir, snap, "shop", "users", 1, snap.Add(-time.Hour))
			},
			row: "broken", headline: "broken", readsFrom: floor.Add(-time.Hour),
		},
		{
			name: "half a pair: the start cannot be read",
			write: func(t *testing.T, dir string) {
				base := writeChainFixture(t, dir, snap, "shop", "orders", 1, snap.Add(-2*time.Hour))
				posdel, _ := baseline.TableDeltaPaths(base, 1)
				if err := os.Remove(posdel); err != nil {
					t.Fatal(err)
				}
			},
			row: "unknown", headline: "unknown",
		},
		{
			name: "a chain that does not say where it starts",
			write: func(t *testing.T, dir string) {
				base := writeChainFixture(t, dir, snap, "shop", "orders", 0, snap)
				_, upserts := baseline.TableDeltaPaths(base, 0)
				if err := os.WriteFile(upserts, []byte("not parquet"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			row: "unknown", headline: "unknown",
		},
		{
			name: "unreadable next to a table past coverage: broken is the worse",
			write: func(t *testing.T, dir string) {
				writeChainFixture(t, dir, snap, "shop", "orders", 1, floor.Add(-time.Hour))
				base := writeChainFixture(t, dir, snap, "shop", "users", 1, snap)
				posdel, _ := baseline.TableDeltaPaths(base, 1)
				if err := os.Remove(posdel); err != nil {
					t.Fatal(err)
				}
			},
			row: "broken", headline: "broken", readsFrom: floor.Add(-time.Hour),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.write(t, dir)
			got := baselinesWithFloor(t, dir, floor)
			if len(got.Snapshots) != 1 {
				t.Fatalf("snapshots = %+v", got.Snapshots)
			}
			row := got.Snapshots[0]
			if row.Staleness != tc.row || got.Staleness != tc.headline {
				t.Errorf("row %q, headline %q; want %q and %q", row.Staleness, got.Staleness, tc.row, tc.headline)
			}
			want := ""
			if !tc.readsFrom.IsZero() {
				want = tc.readsFrom.Format(consoleTSFormat)
			}
			if row.ReadsFrom != want {
				t.Errorf("reads_from = %q, want %q", row.ReadsFrom, want)
			}
			if row.Time != snap.Format(consoleTSFormat) {
				t.Errorf("time = %q: the snapshot is still named by its own time", row.Time)
			}
		})
	}
}

// The page reads a footer for the rows whose verdict it shows and decides
// on, not for the window of fifty: every table of the newest snapshot and
// each table's newest one. An older snapshot with chains has NO verdict
// (never "ok" on a start nobody read), one with none is graded as before,
// and one already past coverage is broken with no footer read at all.
func TestBaselinesAPI_readsAFooterOnlyForTheRowsItGrades(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	floor := now.Add(-10 * time.Hour).Truncate(time.Hour)
	dir := t.TempDir()
	const tables = 4
	names := []string{"t0", "t1", "t2", "t3"}
	// Six snapshots an hour apart, the newest an hour old, every table with
	// a chain that started with the oldest of them.
	for age := 1; age <= 6; age++ {
		for _, n := range names {
			writeChainFixture(t, dir, now.Add(-time.Duration(age)*time.Hour), "shop", n, 6-age, now.Add(-6*time.Hour))
		}
	}
	// One table that left the backup set: its newest snapshot is the third.
	writeChainFixture(t, dir, now.Add(-3*time.Hour), "shop", "gone", 0, now.Add(-3*time.Hour))
	// An old snapshot with no chains, inside coverage, and one past it whose
	// table has a chain.
	writeChainFixture(t, dir, now.Add(-9*time.Hour-30*time.Minute), "shop", "plain", -1, time.Time{})
	writeChainFixture(t, dir, floor.Add(-5*time.Hour), "shop", "t0", 2, floor.Add(-7*time.Hour))

	prev := readBoundsOf
	t.Cleanup(func() { readBoundsOf = prev })
	var asked []string
	readBoundsOf = func(ctx context.Context, files []reconstruct.BaselineFile) []status.ReadBound {
		for _, f := range files {
			asked = append(asked, reconstruct.SnapshotDirName(f.SnapshotTime)+"/"+f.Table)
		}
		return prev(ctx, files)
	}
	got := baselinesWithFloor(t, dir, floor)

	var want []string
	for _, n := range names {
		want = append(want, reconstruct.SnapshotDirName(now.Add(-time.Hour))+"/"+n)
	}
	want = append(want, reconstruct.SnapshotDirName(now.Add(-3*time.Hour))+"/gone")
	sort.Strings(asked)
	sort.Strings(want)
	if len(asked) != tables+1 || !equalStrings(asked, want) {
		t.Fatalf("footers read for %d files, want %d (the newest snapshot's tables and each table's newest):\n got %v\nwant %v",
			len(asked), tables+1, asked, want)
	}

	if len(got.Snapshots) != 8 {
		t.Fatalf("snapshots = %d, want 8", len(got.Snapshots))
	}
	rows := map[string]string{}
	for _, s := range got.Snapshots {
		rows[s.Time] = s.Staleness
	}
	at := func(d time.Duration) string { return now.Add(-d).Format(consoleTSFormat) }
	for when, verdict := range map[string]string{
		at(time.Hour):                    "ok", // read: the chain started six hours back
		at(2 * time.Hour):                "",   // chains nobody read: no verdict
		at(3 * time.Hour):                "",   // "gone" was read, its neighbours were not
		at(6 * time.Hour):                "",
		at(9*time.Hour + 30*time.Minute): "aging", // no chain: graded on itself, as before
		floor.Add(-5 * time.Hour).Format(consoleTSFormat): "broken",
	} {
		if got, ok := rows[when]; !ok || got != verdict {
			t.Errorf("snapshot %s: staleness %q (listed %v), want %q", when, got, ok, verdict)
		}
	}
	if got.Staleness != "aging" {
		t.Errorf("headline = %q, want aging (plain's newest snapshot)", got.Staleness)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A footer read that runs out of time must not leave the page at "ok".
func TestBaselinesAPI_boundsNotReadInTimeAreUnknown(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	floor := now.Add(-10 * time.Hour).Truncate(time.Hour)
	dir := t.TempDir()
	writeChainFixture(t, dir, now.Add(-time.Hour), "shop", "orders", 1, now.Add(-2*time.Hour))
	prev := readBoundsOf
	t.Cleanup(func() { readBoundsOf = prev })
	readBoundsOf = func(_ context.Context, files []reconstruct.BaselineFile) []status.ReadBound {
		out := make([]status.ReadBound, len(files))
		for i := range out {
			out[i].Unread = true
		}
		return out
	}
	got := baselinesWithFloor(t, dir, floor)
	if got.Staleness != "unknown" || got.Snapshots[0].Staleness != "unknown" || got.Snapshots[0].ReadsFrom != "" {
		t.Fatalf("headline %q, row %+v: want unknown", got.Staleness, got.Snapshots[0])
	}
}

// The same snapshot in a folder and in a bucket is listed once, from the
// folder. The chain named beside it has to be the folder's too: the bucket's
// file name under the folder's path is a file that may not be there.
func TestMergedBaselines_theChainFollowsThePath(t *testing.T) {
	const dir, s3 = "/data/baselines", "s3://bucket/baselines"
	damaged := errors.New("half a pair")
	inS3 := bf("2026-06-10T12:00:00Z", "shop", "orders", s3+"/2026-06-10T12-00-00Z/shop/orders.parquet")
	inS3.DeltaUpserts = "orders.000007.upserts"
	inDir := bf("2026-06-10T12:00:00Z", "shop", "orders", dir+"/2026-06-10T12-00-00Z/shop/orders.parquet")
	inDir.DeltaUpserts = "orders.000002-000007.upserts"
	usersS3 := bf("2026-06-10T12:00:00Z", "shop", "users", s3+"/2026-06-10T12-00-00Z/shop/users.parquet")
	usersS3.DeltaErr = damaged
	usersDir := bf("2026-06-10T12:00:00Z", "shop", "users", dir+"/2026-06-10T12-00-00Z/shop/users.parquet")
	usersDir.DeltaUpserts = "users.000000.upserts"

	for _, order := range [][]string{{s3, dir}, {dir, s3}} {
		got := listBaselinesMerged(context.Background(), order, fakeLister(
			map[string][]reconstruct.BaselineFile{dir: {inDir, usersDir}, s3: {inS3, usersS3}}, nil))
		if len(got.Files) != 2 {
			t.Fatalf("%v: merged %+v", order, got.Files)
		}
		for i, want := range []reconstruct.BaselineFile{inDir, usersDir} {
			if f := got.Files[i]; f.Path != want.Path || f.DeltaUpserts != want.DeltaUpserts || f.DeltaErr != want.DeltaErr {
				t.Errorf("%v: %s kept %+v, want the folder's %+v", order, want.Table, f, want)
			}
		}
	}
}

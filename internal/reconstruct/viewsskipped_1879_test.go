package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

func TestSnapshotOfTable(t *testing.T) {
	for in, want := range map[string]string{
		"": "",
		"s3://bucket/backups/2026-09-27T10-00-00Z/shop/orders.parquet": "s3://bucket/backups/2026-09-27T10-00-00Z",
		"s3://bucket/2026-09-27T10-00-00Z/shop/orders.parquet":         "s3://bucket/2026-09-27T10-00-00Z",
		"s3://bucket/orders.parquet":                                   "",
		"s3://bucket/shop/orders.parquet":                              "",
		filepath.Join("/data", "2026-09-27T10-00-00Z", "shop", "orders.parquet"): filepath.Join("/data", "2026-09-27T10-00-00Z"),
	} {
		if got := snapshotOfTable(in); got != want {
			t.Errorf("snapshotOfTable(%q) = %q, want %q", in, got, want)
		}
	}
}

// fullRead writes a snapshot directory under root holding the record of a
// full read that skipped views, and returns it.
func fullRead(t *testing.T, root, name string, views ...string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteViewsSkipped(dir, baseline.NewViewsSkipped(views, mustDirTime(t, name))); err != nil {
		t.Fatal(err)
	}
	return dir
}

func carryInto(t *testing.T, reports ...*TableReport) (string, baseline.ViewsSkipped, bool) {
	t.Helper()
	out := t.TempDir()
	carryViewsSkipped(context.Background(), out, reports)
	rec, ok, err := baseline.ReadViewsSkipped(out)
	if err != nil {
		t.Fatalf("the carried record cannot be read: %v", err)
	}
	return out, rec, ok
}

func TestCarryViewsSkipped(t *testing.T) {
	root := t.TempDir()
	first := fullRead(t, root, "2026-09-27T10-00-00Z", "shop.big_orders", "shop.totals")

	t.Run("the record is carried, and says it is", func(t *testing.T) {
		_, rec, ok := carryInto(t, &TableReport{SourceSnapshot: first})
		if !ok || rec.Count != 2 || !slices.Equal(rec.Views, []string{"shop.big_orders", "shop.totals"}) {
			t.Fatalf("carried %+v ok %v", rec, ok)
		}
		if !rec.Carried {
			t.Error("the record of a snapshot that read no dump is not marked carried")
		}
		if rec.ReadAt != "2026-09-27T10:00:00Z" {
			t.Errorf("read_at = %q, want the time of the full read", rec.ReadAt)
		}
	})

	t.Run("carried again, the date is still the full read's", func(t *testing.T) {
		second := filepath.Join(root, "2026-09-27T11-00-00Z")
		if err := os.MkdirAll(second, 0o755); err != nil {
			t.Fatal(err)
		}
		carryViewsSkipped(context.Background(), second, []*TableReport{{SourceSnapshot: first}})
		_, rec, ok := carryInto(t, &TableReport{SourceSnapshot: second})
		if !ok || !rec.Carried || rec.ReadAt != "2026-09-27T10:00:00Z" || rec.Count != 2 {
			t.Fatalf("carried twice: %+v ok %v", rec, ok)
		}
	})

	t.Run("the full read's record is not touched", func(t *testing.T) {
		carryInto(t, &TableReport{SourceSnapshot: first})
		rec, ok, err := baseline.ReadViewsSkipped(first)
		if !ok || err != nil || rec.Carried {
			t.Fatalf("the source record is now %+v (ok %v err %v)", rec, ok, err)
		}
	})

	t.Run("a snapshot with no record carries nothing", func(t *testing.T) {
		old := filepath.Join(root, "2026-09-20T10-00-00Z")
		if err := os.MkdirAll(old, 0o755); err != nil {
			t.Fatal(err)
		}
		out, _, ok := carryInto(t, &TableReport{SourceSnapshot: old})
		if ok {
			t.Fatal("a record was written for a snapshot whose source has none")
		}
		if _, err := os.Stat(filepath.Join(out, baseline.ViewsSkippedName)); !os.IsNotExist(err) {
			t.Errorf("stat err %v", err)
		}
	})

	t.Run("the newest source decides, and only it", func(t *testing.T) {
		newer := fullRead(t, root, "2026-09-28T10-00-00Z", "shop.only_this")
		_, rec, ok := carryInto(t, &TableReport{SourceSnapshot: first}, nil, &TableReport{}, &TableReport{SourceSnapshot: newer})
		if !ok || !slices.Equal(rec.Views, []string{"shop.only_this"}) || rec.ReadAt != "2026-09-28T10:00:00Z" {
			t.Fatalf("carried %+v ok %v, want the newer snapshot's", rec, ok)
		}
		// The newest has no record: the older one's list is not taken.
		bare := filepath.Join(root, "2026-09-29T10-00-00Z")
		if err := os.MkdirAll(bare, 0o755); err != nil {
			t.Fatal(err)
		}
		if _, rec, ok := carryInto(t, &TableReport{SourceSnapshot: first}, &TableReport{SourceSnapshot: bare}); ok {
			t.Fatalf("carried %+v from an older snapshot past a newer one with no record", rec)
		}
	})

	t.Run("no table had a snapshot", func(t *testing.T) {
		if _, rec, ok := carryInto(t, &TableReport{BinlogOnly: true}); ok {
			t.Fatalf("carried %+v", rec)
		}
		if _, rec, ok := carryInto(t); ok {
			t.Fatalf("carried %+v", rec)
		}
	})

	t.Run("a record that cannot be read carries nothing and says so", func(t *testing.T) {
		log := captureLog(t)
		bad := filepath.Join(root, "2026-09-30T10-00-00Z")
		if err := os.MkdirAll(bad, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bad, baseline.ViewsSkippedName), []byte("{"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, rec, ok := carryInto(t, &TableReport{SourceSnapshot: bad}); ok {
			t.Fatalf("carried %+v", rec)
		}
		if !strings.Contains(log.String(), "could not read the skipped views") {
			t.Errorf("nothing was logged:\n%s", log.String())
		}
	})

	t.Run("a record left by an earlier attempt goes when there is nothing to carry", func(t *testing.T) {
		out := fullRead(t, t.TempDir(), "2026-09-27T12-00-00Z", "shop.stale")
		carryViewsSkipped(context.Background(), out, nil)
		if _, err := os.Stat(filepath.Join(out, baseline.ViewsSkippedName)); !os.IsNotExist(err) {
			t.Errorf("the earlier record is still there (stat err %v)", err)
		}
	})

	t.Run("a directory that cannot be written costs nothing", func(t *testing.T) {
		captureLog(t)
		carryViewsSkipped(context.Background(), filepath.Join(t.TempDir(), "gone"), []*TableReport{{SourceSnapshot: first}})
	})

	t.Run("a full read's record with no date takes its snapshot's", func(t *testing.T) {
		dir := filepath.Join(root, "2026-09-26T10-00-00Z")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, baseline.ViewsSkippedName),
			[]byte(`{"version":1,"count":1,"views":["a.v"]}`), 0o644); err != nil {
			t.Fatal(err)
		}
		_, rec, ok := carryInto(t, &TableReport{SourceSnapshot: dir})
		if !ok || rec.ReadAt != "2026-09-26T10:00:00Z" {
			t.Fatalf("carried %+v ok %v", rec, ok)
		}
		// A CARRIED record with no date keeps none: its snapshot did not
		// read the source, so its time is not the time of the read.
		if err := os.WriteFile(filepath.Join(dir, baseline.ViewsSkippedName),
			[]byte(`{"version":1,"carried":true,"count":1,"views":["a.v"]}`), 0o644); err != nil {
			t.Fatal(err)
		}
		_, rec, ok = carryInto(t, &TableReport{SourceSnapshot: dir})
		if !ok || rec.ReadAt != "" || !rec.Carried {
			t.Fatalf("carried %+v ok %v, want no date", rec, ok)
		}
	})
}

func TestCarryViewsSkipped_fromS3(t *testing.T) {
	const snap = "s3://bucket/backups/2026-09-27T10-00-00Z"
	prev := readViewsSkippedS3
	t.Cleanup(func() { readViewsSkippedS3 = prev })

	var asked []string
	readViewsSkippedS3 = func(_ context.Context, url string) (baseline.ViewsSkipped, bool, error) {
		asked = append(asked, url)
		return baseline.NewViewsSkipped([]string{"shop.big_orders"}, time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)), true, nil
	}
	_, rec, ok := carryInto(t, &TableReport{SourceSnapshot: snapshotOfTable(snap + "/shop/orders.parquet")})
	if !ok || !rec.Carried || rec.Count != 1 || rec.ReadAt != "2026-09-27T10:00:00Z" {
		t.Fatalf("carried %+v ok %v", rec, ok)
	}
	if !slices.Equal(asked, []string{snap}) {
		t.Errorf("asked for %q, want one read of %s", asked, snap)
	}

	readViewsSkippedS3 = func(context.Context, string) (baseline.ViewsSkipped, bool, error) {
		return baseline.ViewsSkipped{}, false, nil
	}
	if _, rec, ok := carryInto(t, &TableReport{SourceSnapshot: snap}); ok {
		t.Fatalf("the bucket has no record and %+v was carried", rec)
	}

	log := captureLog(t)
	readViewsSkippedS3 = func(context.Context, string) (baseline.ViewsSkipped, bool, error) {
		return baseline.ViewsSkipped{}, false, errors.New("AccessDenied")
	}
	if _, rec, ok := carryInto(t, &TableReport{SourceSnapshot: snap}); ok {
		t.Fatalf("the read failed and %+v was carried", rec)
	}
	if !strings.Contains(log.String(), "AccessDenied") {
		t.Errorf("the failed read was not logged:\n%s", log.String())
	}
}

// A snapshot that holds the record is found, listed and read like one that
// does not: what a program from before the record sees.
func TestSnapshotWithViewsRecordIsFoundLikeBefore(t *testing.T) {
	captureLog(t)
	root := t.TempDir()
	writeListFixture(t, root, dirAt(1), "shop", "orders.parquet")
	writeListFixture(t, root, dirAt(1), "_SUCCESS")
	at := mustDirTime(t, dirAt(3))
	plainPath, plainAt, _, err := FindBaseline(context.Background(), root, "shop", "orders", at)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := ListBaselines(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	fullRead(t, root, dirAt(1), "shop.big_orders", "onlyviews.v1")

	path, gotAt, stale, err := FindBaseline(context.Background(), root, "shop", "orders", at)
	if err != nil || path != plainPath || !gotAt.Equal(plainAt) || stale.Stale() {
		t.Fatalf("with the record: %s at %s (stale %v, err %v); without: %s at %s", path, gotAt, stale.Stale(), err, plainPath, plainAt)
	}
	listed, err := ListBaselines(context.Background(), root)
	if err != nil || len(listed) != len(plain) || listed[0].Path != plain[0].Path {
		t.Fatalf("listing with the record: %+v (err %v); without: %+v", listed, err, plain)
	}
	if _, tables, err := NewestSnapshot(context.Background(), root); err != nil || !slices.Equal(tables, []string{"shop.orders"}) {
		t.Fatalf("NewestSnapshot tables = %q, %v", tables, err)
	}
	if w := SnapshotWritersSeen(root); len(w) != 0 {
		t.Errorf("the record was read as a writer's signature: %q", w)
	}
}

// The same over S3: the record is one more key in the snapshot's prefix.
func TestSnapshotWithViewsRecordIsListedLikeBefore_s3(t *testing.T) {
	captureLog(t)
	keys := snapshotKeys(dirAt(1), []string{"shop/orders"}, "_SUCCESS", "_MANIFEST")
	plain := listS3ForTest(t, keys)
	with := listS3ForTest(t, append(slices.Clone(keys), dirAt(1)+"/"+baseline.ViewsSkippedName))
	if len(plain) != 1 || len(with) != 1 || plain[0].Path != with[0].Path {
		t.Fatalf("without the record: %+v\nwith it: %+v", plain, with)
	}
}

func listS3ForTest(t *testing.T, keys []string) []BaselineFile {
	t.Helper()
	stubS3Snapshots(t, &fakeS3Snapshots{keys: keys})
	files, err := ListBaselines(context.Background(), "s3://b/base/")
	if err != nil {
		t.Fatal(err)
	}
	return files
}

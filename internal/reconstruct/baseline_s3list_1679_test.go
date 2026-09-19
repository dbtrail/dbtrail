package reconstruct

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// fakeS3Snapshots is an s3SnapshotLister over a set of keys, answering the
// two calls the way S3 does (directories from the keys' first segment, in
// byte order; objects from a start-after key on, in byte order) and
// recording what was asked, so a test can pin the REQUESTS as well as the
// answer.
type fakeS3Snapshots struct {
	keys        []string
	dirCalls    int
	startAfters []string
	err         error // fails ListDirs
	infoErr     error // fails ListInfoFrom
}

func (f *fakeS3Snapshots) ListDirs(context.Context, string) ([]string, error) {
	f.dirCalls++
	if f.err != nil {
		return nil, f.err
	}
	seen := map[string]bool{}
	var out []string
	for _, k := range slices.Sorted(slices.Values(f.keys)) {
		if d, _, ok := strings.Cut(k, "/"); ok && !seen[d] {
			seen[d] = true
			out = append(out, d)
		}
	}
	return out, nil
}

func (f *fakeS3Snapshots) ListInfoFrom(_ context.Context, _ string, startAfter string) ([]storage.ObjectInfo, error) {
	f.startAfters = append(f.startAfters, startAfter)
	if f.infoErr != nil {
		return nil, f.infoErr
	}
	var out []storage.ObjectInfo
	for _, k := range slices.Sorted(slices.Values(f.keys)) {
		if k > startAfter {
			out = append(out, storage.ObjectInfo{Key: k})
		}
	}
	return out, nil
}

// snapshotKeys writes a snapshot's keys: its tables, its markers, and the
// clutter a real snapshot carries (views.sql, _MANIFEST, delta pairs).
func snapshotKeys(dir string, tables []string, markers ...string) []string {
	keys := []string{dir + "/views.sql", dir + "/_MANIFEST"}
	for _, t := range tables {
		keys = append(keys, dir+"/"+t+".parquet", dir+"/"+strings.TrimSuffix(t, ".parquet")+".000001.posdel", dir+"/"+t+".000001.upserts")
	}
	for _, m := range markers {
		keys = append(keys, dir+"/"+m)
	}
	return keys
}

func stubS3Snapshots(t *testing.T, f *fakeS3Snapshots) {
	t.Helper()
	prev := newS3SnapshotLister
	t.Cleanup(func() { newS3SnapshotLister = prev })
	newS3SnapshotLister = func(context.Context, string) (s3SnapshotLister, error) { return f, nil }
}

func dirAt(i int) string { return fmt.Sprintf("2026-09-18T%02d-00-00Z", i) }

func TestListBaselinesS3_readsTheLayoutOffTwoRequests(t *testing.T) {
	var keys []string
	keys = append(keys, snapshotKeys(dirAt(1), []string{"shop/orders", "shop/items"}, "_SUCCESS")...)
	keys = append(keys, snapshotKeys(dirAt(2), []string{"shop/orders"}, "_INCOMPLETE")...)             // partial: out
	keys = append(keys, snapshotKeys(dirAt(3), []string{"shop/orders"}, "_INCOMPLETE", "_SUCCESS")...) // finished after all: in
	keys = append(keys, snapshotKeys(dirAt(4), []string{"shop/orders"})...)                            // pre-marker: in
	keys = append(keys, "current/shop/orders.parquet", ".compact/shop/orders/x/orders.000000-000003.upserts", "notes.txt", dirAt(4)+"/deep/er/path/x.parquet")
	f := &fakeS3Snapshots{keys: keys}
	stubS3Snapshots(t, f)
	files, err := ListBaselines(context.Background(), "s3://b/base/")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, x := range files {
		got = append(got, x.SnapshotTime.Format("15")+" "+x.Schema+"."+x.Table+" "+x.Path)
	}
	want := []string{
		"04 shop.orders s3://b/base/" + dirAt(4) + "/shop/orders.parquet",
		"03 shop.orders s3://b/base/" + dirAt(3) + "/shop/orders.parquet",
		"01 shop.items s3://b/base/" + dirAt(1) + "/shop/items.parquet",
		"01 shop.orders s3://b/base/" + dirAt(1) + "/shop/orders.parquet",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("files =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	// Two requests: the directories, then the objects from the
	// byte-smallest wanted name on (every snapshot wanted, so the oldest).
	if f.dirCalls != 1 || !reflect.DeepEqual(f.startAfters, []string{dirAt(1)}) {
		t.Fatalf("dirCalls=%d startAfters=%v", f.dirCalls, f.startAfters)
	}
}

func TestListBaselinesS3_newestWindow(t *testing.T) {
	var keys []string
	for i := 1; i <= 6; i++ {
		keys = append(keys, snapshotKeys(dirAt(i), []string{"shop/orders"}, "_SUCCESS")...)
	}
	f := &fakeS3Snapshots{keys: keys}
	stubS3Snapshots(t, f)
	files, skipped, more, err := ListBaselinesNewestReport(context.Background(), "s3://b/base", 2)
	if err != nil || skipped != 0 || !more || len(files) != 2 || files[0].SnapshotTime.Hour() != 6 || files[1].SnapshotTime.Hour() != 5 {
		t.Fatalf("files=%+v skipped=%d more=%v err=%v, want the two newest and more", files, skipped, more, err)
	}
	// The object listing started at the oldest WANTED directory, so the
	// four older snapshots' objects were never fetched.
	if !reflect.DeepEqual(f.startAfters, []string{dirAt(5)}) {
		t.Fatalf("startAfters = %v, want from %s on", f.startAfters, dirAt(5))
	}
	if _, _, more, _ = ListBaselinesNewestReport(context.Background(), "s3://b/base", 6); more {
		t.Fatal("a window as wide as the inventory reported more")
	}
	if all, _, more, _ := ListBaselinesNewestReport(context.Background(), "s3://b/base", 0); more || len(all) != 6 {
		t.Fatalf("0 = all: len=%d more=%v", len(all), more)
	}
	// An empty prefix: one request, nothing, no error.
	f2 := &fakeS3Snapshots{}
	stubS3Snapshots(t, f2)
	if files, _, more, err := ListBaselinesNewestReport(context.Background(), "s3://b/empty", 3); err != nil || len(files) != 0 || more || len(f2.startAfters) != 0 {
		t.Fatalf("empty: files=%v more=%v err=%v startAfters=%v", files, more, err, f2.startAfters)
	}
	// A listing error is the caller's error, never a shorter answer: from
	// either listing.
	stubS3Snapshots(t, &fakeS3Snapshots{err: errors.New("AccessDenied")})
	if _, _, _, err := ListBaselinesNewestReport(context.Background(), "s3://b/base", 3); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("directories: err = %v", err)
	}
	stubS3Snapshots(t, &fakeS3Snapshots{keys: keys, infoErr: errors.New("SlowDown")})
	if _, _, _, err := ListBaselinesNewestReport(context.Background(), "s3://b/base", 3); err == nil || !strings.Contains(err.Error(), "SlowDown") {
		t.Fatalf("objects: err = %v", err)
	}
	// A page whose newest directories are all incomplete widens until it
	// holds a snapshot: never an empty page with good ones one older.
	var stale []string
	stale = append(stale, snapshotKeys(dirAt(0), []string{"shop/orders"}, "_SUCCESS")...)
	for i := 1; i <= 9; i++ {
		stale = append(stale, snapshotKeys(dirAt(i), []string{"shop/orders"}, "_INCOMPLETE")...)
	}
	f4 := &fakeS3Snapshots{keys: stale}
	stubS3Snapshots(t, f4)
	if files, _, more, err := ListBaselinesNewestReport(context.Background(), "s3://b/base", 2); err != nil || len(files) != 1 || files[0].SnapshotTime.Hour() != 0 || more {
		t.Fatalf("stale window: files=%+v more=%v err=%v, want the one complete snapshot found by widening", files, more, err)
	}
	if !reflect.DeepEqual(f4.startAfters, []string{dirAt(8), dirAt(2), dirAt(0)}) {
		t.Fatalf("startAfters = %v, want 2, 8 then all", f4.startAfters)
	}
}

// NewestSnapshot reads a window of the newest few and widens it while every
// snapshot there is incomplete; the whole inventory is read only when it
// has to be.
func TestNewestSnapshot_widensPastIncompleteSnapshots(t *testing.T) {
	var keys []string
	keys = append(keys, snapshotKeys(dirAt(0), []string{"shop/orders", "shop/items"}, "_SUCCESS")...)
	for i := 1; i <= 20; i++ {
		keys = append(keys, snapshotKeys(dirAt(i), []string{"shop/orders"}, "_INCOMPLETE")...)
	}
	f := &fakeS3Snapshots{keys: keys}
	stubS3Snapshots(t, f)
	at, tables, err := NewestSnapshot(context.Background(), "s3://b/base")
	if err != nil || at.Hour() != 0 || !reflect.DeepEqual(tables, []string{"shop.items", "shop.orders"}) {
		t.Fatalf("at=%s tables=%v err=%v", at, tables, err)
	}
	// 8, then 32 (which covers all 21): two object listings, the second
	// from the oldest directory.
	if !reflect.DeepEqual(f.startAfters, []string{dirAt(13), dirAt(0)}) || f.dirCalls != 1 {
		t.Fatalf("startAfters = %v dirCalls = %d, want the probe then the widened window over one directory listing", f.startAfters, f.dirCalls)
	}
	// The common case is one request pair.
	f2 := &fakeS3Snapshots{keys: snapshotKeys(dirAt(0), []string{"shop/orders", "shop/items"}, "_SUCCESS")}
	stubS3Snapshots(t, f2)
	if _, _, err := NewestSnapshot(context.Background(), "s3://b/base"); err != nil || len(f2.startAfters) != 1 {
		t.Fatalf("err=%v startAfters=%v", err, f2.startAfters)
	}
	// Nothing complete anywhere: no snapshot, no error, and the widening
	// stopped once the window covered everything.
	var incompleteOnly []string
	for i := 1; i <= 20; i++ {
		incompleteOnly = append(incompleteOnly, snapshotKeys(dirAt(i), []string{"shop/orders"}, "_INCOMPLETE")...)
	}
	f3 := &fakeS3Snapshots{keys: incompleteOnly}
	stubS3Snapshots(t, f3)
	if at, tables, err := NewestSnapshot(context.Background(), "s3://b/base"); err != nil || !at.IsZero() || tables != nil || len(f3.startAfters) != 2 {
		t.Fatalf("at=%s tables=%v err=%v startAfters=%v", at, tables, err, f3.startAfters)
	}
}

// The local path cuts the same way, after its full (cheap) directory read.
func TestListBaselinesNewestReport_local(t *testing.T) {
	root := t.TempDir()
	for i := 1; i <= 4; i++ {
		d := filepath.Join(root, dirAt(i), "shop")
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, n := range []string{"orders.parquet", "items.parquet"} {
			if err := os.WriteFile(filepath.Join(d, n), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	files, _, more, err := ListBaselinesNewestReport(context.Background(), root, 2)
	if err != nil || !more || len(files) != 4 || files[0].SnapshotTime.Hour() != 4 || files[3].SnapshotTime.Hour() != 3 {
		t.Fatalf("files=%d more=%v err=%v first=%v", len(files), more, err, files)
	}
	if files, _, more, _ := ListBaselinesNewestReport(context.Background(), root, 4); more || len(files) != 8 {
		t.Fatalf("exact window: len=%d more=%v", len(files), more)
	}
}

// The object listing starts at the byte-smallest wanted NAME, not the
// time-oldest: a hand-copied directory whose name carries a fraction parses
// newer than a plain one it sorts before, and starting after the plain one
// would skip its keys with no error.
func TestListBaselinesS3_startsAtTheByteSmallestWantedName(t *testing.T) {
	older, plain, fraction := "2026-09-17T00-00-00Z", "2026-09-18T00-00-00Z", "2026-09-18T00-00-00.5Z"
	if fraction >= plain {
		t.Fatal("fixture: the fractional name must sort before the plain one")
	}
	var keys []string
	for _, d := range []string{older, plain, fraction} {
		keys = append(keys, snapshotKeys(d, []string{"shop/orders"}, "_SUCCESS")...)
	}
	f := &fakeS3Snapshots{keys: keys}
	stubS3Snapshots(t, f)
	files, _, more, err := ListBaselinesNewestReport(context.Background(), "s3://b/base", 2)
	if err != nil || !more || len(files) != 2 {
		t.Fatalf("files=%+v more=%v err=%v, want the two newest", files, more, err)
	}
	if files[0].Path != "s3://b/base/"+fraction+"/shop/orders.parquet" || files[1].Path != "s3://b/base/"+plain+"/shop/orders.parquet" {
		t.Fatalf("files = %+v, want the fractional (newer) then the plain", files)
	}
	if !reflect.DeepEqual(f.startAfters, []string{fraction}) {
		t.Fatalf("startAfters = %v, want the byte-smallest wanted name", f.startAfters)
	}
}

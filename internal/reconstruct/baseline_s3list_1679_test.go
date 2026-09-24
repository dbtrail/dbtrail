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
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// fakeS3Snapshots is an s3SnapshotLister over a set of keys, answering the
// two calls the way S3 does (directories from the keys' first segment, in
// byte order; objects under a prefix from a start-after key on, in byte
// order) and recording what was asked, so a test can pin the REQUESTS as
// well as the answer. The listing reads directories concurrently, so the
// fake locks.
type fakeS3Snapshots struct {
	mu       sync.Mutex
	keys     []string
	dirCalls int
	prefixes []string // every ListInfoFrom prefix asked, in call order
	err      error    // fails ListDirs
	infoErr  error    // fails ListInfoFrom
	// failPrefix fails ListInfoFrom for that one prefix only.
	failPrefix string
	// sloppy answers ListInfoFrom with every key, ignoring the prefix: what a
	// store that misread the request would do, which the listing must not
	// mistake for the directory's own contents.
	sloppy bool
}

func (f *fakeS3Snapshots) ListDirs(context.Context, string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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

func (f *fakeS3Snapshots) ListInfoFrom(_ context.Context, prefix, startAfter string) ([]storage.ObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prefixes = append(f.prefixes, prefix)
	if f.infoErr != nil {
		return nil, f.infoErr
	}
	if f.failPrefix != "" && prefix == f.failPrefix {
		return nil, errors.New("SlowDown on " + prefix)
	}
	var out []storage.ObjectInfo
	for _, k := range slices.Sorted(slices.Values(f.keys)) {
		if k > startAfter && (f.sloppy || strings.HasPrefix(k, prefix)) {
			out = append(out, storage.ObjectInfo{Key: k})
		}
	}
	return out, nil
}

// asked returns the prefixes ListInfoFrom was asked, sorted: the directory
// reads of one round go out concurrently, so their order is not a fact.
func (f *fakeS3Snapshots) asked() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(slices.Values(f.prefixes))
}

// dirsRead returns the prefixes asked and forgets them, so the next call
// pins the NEXT round's requests alone.
func (f *fakeS3Snapshots) dirsRead() []string {
	out := f.asked()
	f.mu.Lock()
	f.prefixes = nil
	f.mu.Unlock()
	return out
}

func (f *fakeS3Snapshots) add(keys ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys = append(f.keys, keys...)
}

func (f *fakeS3Snapshots) remove(dir string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys = slices.DeleteFunc(f.keys, func(k string) bool { return strings.HasPrefix(k, dir+"/") })
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

// stubS3Snapshots installs the fake as every s3:// source and starts from
// an empty inventory cache, so one test's warm cache is never another's.
func stubS3Snapshots(t *testing.T, f *fakeS3Snapshots) {
	t.Helper()
	resetS3Inventories()
	prev := newS3SnapshotLister
	t.Cleanup(func() { newS3SnapshotLister = prev; resetS3Inventories() })
	newS3SnapshotLister = func(context.Context, string) (s3SnapshotLister, error) { return f, nil }
}

// prefixesOf is the ListInfoFrom prefixes that reading these directories
// costs, sorted like fakeS3Snapshots.asked.
func prefixesOf(dirs ...string) []string {
	var out []string
	for _, d := range dirs {
		out = append(out, d+"/")
	}
	return slices.Sorted(slices.Values(out))
}

func dirAt(i int) string { return fmt.Sprintf("2026-09-18T%02d-00-00Z", i) }

func TestListBaselinesS3_readsEachDirectoryOnce(t *testing.T) {
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
	// One directory listing, then one read per SNAPSHOT directory; the keys
	// that are not snapshot directories (current/, .compact/) cost nothing.
	if f.dirCalls != 1 || !reflect.DeepEqual(f.dirsRead(), prefixesOf(dirAt(1), dirAt(2), dirAt(3), dirAt(4))) {
		t.Fatalf("dirCalls=%d prefixes=%v", f.dirCalls, f.asked())
	}
	// Again, inside the TTL: no directory listing and no directory read for
	// the three complete snapshots. The partial one (dirAt(2)) is read again:
	// it may be an upload in progress, and it joins the cache only once its
	// _SUCCESS lands.
	again, err := ListBaselines(context.Background(), "s3://b/base")
	if err != nil || len(again) != len(files) {
		t.Fatalf("second read: %d files err=%v", len(again), err)
	}
	if f.dirCalls != 1 || !reflect.DeepEqual(f.dirsRead(), prefixesOf(dirAt(2))) {
		t.Fatalf("warm: dirCalls=%d prefixes=%v, want the incomplete directory alone", f.dirCalls, f.asked())
	}
	f.add(dirAt(2) + "/_SUCCESS")
	if files, err := ListBaselines(context.Background(), "s3://b/base"); err != nil || len(files) != 5 {
		t.Fatalf("after _SUCCESS: %d files err=%v, want dirAt(2) in", len(files), err)
	}
	if !reflect.DeepEqual(f.dirsRead(), prefixesOf(dirAt(2))) {
		t.Fatalf("prefixes=%v, want dirAt(2) read once more", f.asked())
	}
	if _, err := ListBaselines(context.Background(), "s3://b/base"); err != nil || len(f.dirsRead()) != 0 {
		t.Fatalf("now complete, dirAt(2) must be cached: err=%v prefixes=%v", err, f.asked())
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
	// Only the two WANTED directories were read.
	if !reflect.DeepEqual(f.dirsRead(), prefixesOf(dirAt(6), dirAt(5))) {
		t.Fatalf("prefixes = %v, want the two newest", f.asked())
	}
	// A wider window reads the directories it adds, and nothing twice.
	if _, _, more, _ = ListBaselinesNewestReport(context.Background(), "s3://b/base", 6); more {
		t.Fatal("a window as wide as the inventory reported more")
	}
	if !reflect.DeepEqual(f.dirsRead(), prefixesOf(dirAt(1), dirAt(2), dirAt(3), dirAt(4))) {
		t.Fatalf("prefixes = %v, want the four older ones only", f.asked())
	}
	if all, _, more, _ := ListBaselinesNewestReport(context.Background(), "s3://b/base", 0); more || len(all) != 6 || len(f.dirsRead()) != 0 {
		t.Fatalf("0 = all: len=%d more=%v prefixes=%v (want none: everything is cached)", len(all), more, f.asked())
	}
	// An empty prefix: one request, nothing, no error.
	f2 := &fakeS3Snapshots{}
	stubS3Snapshots(t, f2)
	if files, _, more, err := ListBaselinesNewestReport(context.Background(), "s3://b/empty", 3); err != nil || len(files) != 0 || more || len(f2.asked()) != 0 {
		t.Fatalf("empty: files=%v more=%v err=%v prefixes=%v", files, more, err, f2.asked())
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
	// 2, then 8, then all: every directory read once on the way.
	var all []string
	for i := 0; i <= 9; i++ {
		all = append(all, dirAt(i))
	}
	if got := f4.dirsRead(); !reflect.DeepEqual(got, prefixesOf(all...)) || f4.dirCalls != 1 {
		t.Fatalf("prefixes = %v dirCalls=%d, want each directory once over one directory listing", got, f4.dirCalls)
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
	// 8, then 32 (which covers all 21): every directory read once, over one
	// directory listing.
	var all []string
	for i := 0; i <= 20; i++ {
		all = append(all, dirAt(i))
	}
	if got := f.dirsRead(); !reflect.DeepEqual(got, prefixesOf(all...)) || f.dirCalls != 1 {
		t.Fatalf("prefixes = %v dirCalls = %d, want each directory once", got, f.dirCalls)
	}
	// The common case is one request pair: the directories, then the
	// newest one.
	f2 := &fakeS3Snapshots{keys: snapshotKeys(dirAt(0), []string{"shop/orders", "shop/items"}, "_SUCCESS")}
	stubS3Snapshots(t, f2)
	if _, _, err := NewestSnapshot(context.Background(), "s3://b/base"); err != nil || !reflect.DeepEqual(f2.asked(), prefixesOf(dirAt(0))) {
		t.Fatalf("err=%v prefixes=%v", err, f2.asked())
	}
	// Nothing complete anywhere: no snapshot, no error, and the widening
	// stopped once the window covered everything.
	var incompleteOnly []string
	for i := 1; i <= 20; i++ {
		incompleteOnly = append(incompleteOnly, snapshotKeys(dirAt(i), []string{"shop/orders"}, "_INCOMPLETE")...)
	}
	f3 := &fakeS3Snapshots{keys: incompleteOnly}
	stubS3Snapshots(t, f3)
	if at, tables, err := NewestSnapshot(context.Background(), "s3://b/base"); err != nil || !at.IsZero() || tables != nil || len(f3.asked()) != 20 {
		t.Fatalf("at=%s tables=%v err=%v prefixes=%v", at, tables, err, f3.asked())
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

// A directory is read by its OWN prefix, so a hand-copied directory whose
// name carries a fraction (which parses newer than a plain name it sorts
// before) is read like any other; the #1679 start-after trap is gone with
// the range listing.
func TestListBaselinesS3_readsAFractionalNameByItsOwnPrefix(t *testing.T) {
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
	if !reflect.DeepEqual(f.asked(), prefixesOf(plain, fraction)) {
		t.Fatalf("prefixes = %v, want the two wanted directories by name", f.asked())
	}
}

// The directory listing is reused inside s3DirsTTL and made again past it;
// a directory gone from the fresh listing is evicted, so a snapshot removed
// by the bucket's lifecycle rule is never answered from the cache.
func TestListBaselinesS3_directoryListingTTLAndEviction(t *testing.T) {
	f := &fakeS3Snapshots{}
	f.add(snapshotKeys(dirAt(1), []string{"shop/orders"}, "_SUCCESS")...)
	f.add(snapshotKeys(dirAt(2), []string{"shop/orders"}, "_SUCCESS")...)
	stubS3Snapshots(t, f)
	now := time.Date(2026, 9, 24, 3, 0, 0, 0, time.UTC)
	prev := s3Clock
	t.Cleanup(func() { s3Clock = prev })
	s3Clock = func() time.Time { return now }

	if files, err := ListBaselines(context.Background(), "s3://b/base"); err != nil || len(files) != 2 || f.dirCalls != 1 {
		t.Fatalf("cold: files=%d err=%v dirCalls=%d", len(files), err, f.dirCalls)
	}
	// A snapshot removed and one added, inside the TTL: the listing does
	// not see either yet (no request), and answers the cached inventory.
	f.remove(dirAt(1))
	f.add(snapshotKeys(dirAt(3), []string{"shop/orders"}, "_SUCCESS")...)
	now = now.Add(s3DirsTTL - time.Second)
	f.dirsRead()
	if files, err := ListBaselines(context.Background(), "s3://b/base"); err != nil || len(files) != 2 || files[0].SnapshotTime.Hour() != 2 || f.dirCalls != 1 || len(f.asked()) != 0 {
		t.Fatalf("inside TTL: files=%+v err=%v dirCalls=%d prefixes=%v", files, err, f.dirCalls, f.asked())
	}
	// Past it: listed again, the new directory read, the removed one gone
	// from the answer AND from the cache.
	now = now.Add(2 * time.Second)
	files, err := ListBaselines(context.Background(), "s3://b/base")
	if err != nil || len(files) != 2 || files[0].SnapshotTime.Hour() != 3 || files[1].SnapshotTime.Hour() != 2 || f.dirCalls != 2 {
		t.Fatalf("past TTL: files=%+v err=%v dirCalls=%d", files, err, f.dirCalls)
	}
	if !reflect.DeepEqual(f.dirsRead(), prefixesOf(dirAt(3))) {
		t.Fatalf("prefixes=%v, want the new directory alone", f.asked())
	}
	inv := s3InventoryFor("s3://b/base")
	inv.mu.Lock()
	_, evicted := inv.contents[dirAt(1)]
	inv.mu.Unlock()
	if evicted {
		t.Fatal("the removed directory is still cached")
	}
}

// InvalidateS3Inventory makes the next read list the directories again
// inside the TTL, keeping what was read: the daemon calls it after an
// upload so the snapshot it just wrote is seen at once.
func TestInvalidateS3Inventory(t *testing.T) {
	f := &fakeS3Snapshots{}
	f.add(snapshotKeys(dirAt(1), []string{"shop/orders"}, "_SUCCESS")...)
	stubS3Snapshots(t, f)
	if _, err := ListBaselines(context.Background(), "s3://b/base"); err != nil || f.dirCalls != 1 {
		t.Fatalf("cold: err=%v dirCalls=%d", err, f.dirCalls)
	}
	f.add(snapshotKeys(dirAt(2), []string{"shop/orders"}, "_SUCCESS")...)
	f.dirsRead()
	// The daemon names the snapshot's OWN directory when it uploads; that
	// invalidates the source holding it.
	InvalidateS3Inventory("s3://b/base/" + dirAt(2) + "/")
	files, err := ListBaselines(context.Background(), "s3://b/base")
	if err != nil || len(files) != 2 || f.dirCalls != 2 {
		t.Fatalf("after invalidate: files=%d err=%v dirCalls=%d", len(files), err, f.dirCalls)
	}
	if got := f.dirsRead(); !reflect.DeepEqual(got, prefixesOf(dirAt(2))) {
		t.Fatalf("prefixes=%v, want the new directory alone (the old one is kept)", got)
	}
	// The source itself, in either spelling, invalidates too.
	f.add(snapshotKeys(dirAt(3), []string{"shop/orders"}, "_SUCCESS")...)
	InvalidateS3Inventory("s3://b/base/")
	if files, err := ListBaselines(context.Background(), "s3://b/base"); err != nil || len(files) != 3 || f.dirCalls != 3 {
		t.Fatalf("source spelling: files=%d err=%v dirCalls=%d", len(files), err, f.dirCalls)
	}
	// A sibling prefix is not this source: no listing, and no inventory
	// created for it.
	InvalidateS3Inventory("s3://b/baseline-other/" + dirAt(4))
	InvalidateS3Inventory("/var/lib/backups")
	if _, err := ListBaselines(context.Background(), "s3://b/base"); err != nil || f.dirCalls != 3 {
		t.Fatalf("sibling: dirCalls=%d, want no re-listing", f.dirCalls)
	}
	s3InventoriesMu.Lock()
	n := len(s3Inventories)
	s3InventoriesMu.Unlock()
	if n != 1 {
		t.Fatalf("%d inventories, want the one source only", n)
	}
}

// A round in which one directory read fails is the caller's error, and the
// complete directories that round DID read are kept: a retry reads only
// what it has to.
func TestListBaselinesS3_failedRoundKeepsWhatItRead(t *testing.T) {
	f := &fakeS3Snapshots{failPrefix: dirAt(2) + "/"}
	for i := 1; i <= 3; i++ {
		f.add(snapshotKeys(dirAt(i), []string{"shop/orders"}, "_SUCCESS")...)
	}
	stubS3Snapshots(t, f)
	if _, err := ListBaselines(context.Background(), "s3://b/base"); err == nil || !strings.Contains(err.Error(), "SlowDown") {
		t.Fatalf("err = %v, want the failed read's error", err)
	}
	f.mu.Lock()
	f.failPrefix = ""
	f.mu.Unlock()
	// The errgroup cancels the siblings on the first error, so the two good
	// directories may or may not have been read in the failed round; the
	// retry reads at most all three and always the failed one.
	f.dirsRead()
	files, err := ListBaselines(context.Background(), "s3://b/base")
	if err != nil || len(files) != 3 {
		t.Fatalf("retry: files=%d err=%v", len(files), err)
	}
	if asked := f.asked(); !slices.Contains(asked, dirAt(2)+"/") {
		t.Fatalf("retry prefixes=%v, want the failed directory read again", asked)
	}
	f.dirsRead()
	if _, err := ListBaselines(context.Background(), "s3://b/base"); err != nil || len(f.asked()) != 0 {
		t.Fatalf("warm after retry: err=%v prefixes=%v, want nothing read", err, f.asked())
	}
}

// A store that answered a directory read with keys outside that directory
// must not have them counted as the directory's tables at the directory's
// instant.
func TestListBaselinesS3_ignoresKeysOutsideTheDirectoryRead(t *testing.T) {
	f := &fakeS3Snapshots{sloppy: true}
	f.add(snapshotKeys(dirAt(1), []string{"shop/orders"}, "_SUCCESS")...)
	f.add(snapshotKeys(dirAt(2), []string{"shop/items"}, "_SUCCESS")...)
	stubS3Snapshots(t, f)
	files, err := ListBaselines(context.Background(), "s3://b/base")
	if err != nil || len(files) != 2 {
		t.Fatalf("files=%+v err=%v", files, err)
	}
	for _, x := range files {
		if !strings.HasPrefix(x.Path, "s3://b/base/"+x.SnapshotTime.Format("2006-01-02T15-04-05Z")+"/") {
			t.Fatalf("file %+v attributed to the wrong snapshot", x)
		}
	}
}

// Concurrent readers of one source share the inventory without racing;
// run under -race this is the guard, and the answers must agree.
func TestListBaselinesS3_concurrentReaders(t *testing.T) {
	f := &fakeS3Snapshots{}
	for i := 0; i < 20; i++ {
		f.add(snapshotKeys(dirAt(i), []string{"shop/orders", "shop/items"}, "_SUCCESS")...)
	}
	stubS3Snapshots(t, f)
	var wg sync.WaitGroup
	got := make([]int, 8)
	for i := range got {
		wg.Add(1)
		go func() {
			defer wg.Done()
			files, err := ListBaselines(context.Background(), "s3://b/base")
			if err != nil {
				t.Error(err)
			}
			got[i] = len(files)
		}()
	}
	wg.Wait()
	for _, n := range got {
		if n != 40 {
			t.Fatalf("a reader got %d files, want 40", n)
		}
	}
}

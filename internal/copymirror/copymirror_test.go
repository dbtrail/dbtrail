package copymirror

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/storage"
)

var errGone = errors.New("no such key")

// fakeSource is a snapshot root held in memory, keys relative to it.
type fakeSource struct {
	objects map[string][]byte
	// gets counts Get calls per key.
	gets map[string]int
	// fail makes Get of a key return the error (errGone reads as "not there").
	fail map[string]error
	// body replaces what Get returns for a key, leaving the listing as it is.
	body map[string][]byte
	// listErr fails ListDirs.
	listErr error
	// onGet runs before each Get.
	onGet func(key string)
	// read counts the bytes taken from the bodies Get returned.
	read int64
	// hang makes Get of a key wait for its context to end, like a request
	// nobody answers. stallAfter makes the body of a key give that many
	// bytes and then wait the same way.
	hang       map[string]bool
	stallAfter map[string]int
}

type stallingReader struct {
	ctx   context.Context
	data  []byte
	after int
}

func (r *stallingReader) Read(p []byte) (int, error) {
	if r.after == 0 {
		<-r.ctx.Done()
		return 0, r.ctx.Err()
	}
	n := copy(p, r.data[:min(r.after, len(r.data))])
	r.data, r.after = r.data[n:], r.after-n
	return n, nil
}

type countingReader struct {
	r io.Reader
	n *int64
}

func (c countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	*c.n += int64(n)
	return n, err
}

func newSource() *fakeSource {
	return &fakeSource{objects: map[string][]byte{}, gets: map[string]int{}, fail: map[string]error{}, body: map[string][]byte{}}
}

func (f *fakeSource) ListDirs(_ context.Context, prefix string) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	seen := map[string]bool{}
	for k := range f.objects {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if d, _, ok := strings.Cut(strings.TrimPrefix(k, prefix), "/"); ok {
			seen[d] = true
		}
	}
	var out []string
	for d := range seen {
		out = append(out, d)
	}
	sort.Strings(out)
	return out, nil
}

func (f *fakeSource) ListInfo(_ context.Context, prefix string) ([]storage.ObjectInfo, error) {
	var out []storage.ObjectInfo
	for k, v := range f.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, storage.ObjectInfo{Key: k, Size: int64(len(v))})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (f *fakeSource) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if f.onGet != nil {
		f.onGet(key)
	}
	f.gets[key]++
	if f.hang[key] {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if n, ok := f.stallAfter[key]; ok {
		return io.NopCloser(&stallingReader{ctx: ctx, data: f.objects[key], after: n}), nil
	}
	if err := f.fail[key]; err != nil {
		return nil, err
	}
	b, ok := f.body[key]
	if !ok {
		if b, ok = f.objects[key]; !ok {
			return nil, errGone
		}
	}
	return io.NopCloser(countingReader{bytes.NewReader(b), &f.read}), nil
}

// dataGets counts Get calls for the snapshot's files other than its manifest.
func (f *fakeSource) dataGets(snap string) int {
	n := 0
	for k, c := range f.gets {
		if strings.HasPrefix(k, snap+"/") && !strings.HasSuffix(k, "/"+baselineintegrity.ManifestName) {
			n += c
		}
	}
	return n
}

func digest(b []byte) string {
	return fmt.Sprintf("%08x", crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)))
}

// publish puts a complete snapshot in the source as an upload leaves it:
// its files, a manifest over them, the s3:// views file and _SUCCESS.
func (f *fakeSource) publish(name string, files map[string]string) {
	man := baselineintegrity.Manifest{Version: 1, Algo: "crc32c", Files: map[string]string{}}
	for rel, content := range files {
		f.objects[name+"/"+rel] = []byte(content)
		man.Files[rel] = digest([]byte(content))
	}
	b, _ := json.Marshal(man)
	f.objects[name+"/"+baselineintegrity.ManifestName] = b
	f.objects[name+"/"+baseline.SnapshotViewsName] = []byte("-- reads s3://bucket/" + name + "/shop/orders.parquet\n")
	f.objects[name+"/"+baseline.SuccessMarker] = nil
}

func (f *fakeSource) remove(name string) {
	for k := range f.objects {
		if strings.HasPrefix(k, name+"/") {
			delete(f.objects, k)
		}
	}
}

var (
	t1 = snapshotdir.Name(time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC))
	t2 = snapshotdir.Name(time.Date(2026, 10, 1, 11, 0, 0, 0, time.UTC))
	t3 = snapshotdir.Name(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
)

func testConfig(t *testing.T, src *fakeSource) Config {
	t.Helper()
	return Config{
		Dir:        filepath.Join(t.TempDir(), "mirror"),
		Source:     src,
		SourceName: "s3://bucket/baselines",
		Gone:       func(err error) bool { return errors.Is(err, errGone) },
		FreeBytes:  func(string) (uint64, bool) { return 1 << 40, true },
		Now:        func() time.Time { return time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC) },
	}
}

func mustSync(t *testing.T, cfg Config) Result {
	t.Helper()
	res, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	return res
}

func readFile(t *testing.T, parts ...string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(parts...))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func exists(parts ...string) bool {
	_, err := os.Lstat(filepath.Join(parts...))
	return err == nil
}

// visibleSnapshots are the directories a reader would take for snapshots:
// names that parse, complete by the readers' own rule.
func visibleSnapshots(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if _, ok := snapshotdir.ParseTime(e.Name()); ok && e.IsDir() && baseline.SnapshotComplete(filepath.Join(dir, e.Name())) {
			out = append(out, e.Name())
		}
	}
	return out
}

func TestSync_mirrorsTheNewestCompleteSnapshot(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "old"})
	src.publish(t2, map[string]string{"shop/orders.parquet": "orders", "shop/orders.000000.posdel": "", "sh[o]p/items.parquet": "items"})
	cfg := testConfig(t, src)

	var viewsFor string
	baseline.SetSnapshotViewsWriter(func(dir string) { viewsFor = dir })
	t.Cleanup(func() { baseline.SetSnapshotViewsWriter(nil) })

	res := mustSync(t, cfg)
	if res.Downloaded != t2 || res.Newest != t2 || res.SourceNewest != t2 {
		t.Fatalf("result = %+v, want %s downloaded and newest", res, t2)
	}
	snap := filepath.Join(cfg.Dir, t2)
	if got := readFile(t, snap, "shop", "orders.parquet"); got != "orders" {
		t.Errorf("orders = %q", got)
	}
	if got := readFile(t, snap, "sh[o]p", "items.parquet"); got != "items" {
		t.Errorf("items = %q", got)
	}
	if !exists(snap, "shop", "orders.000000.posdel") {
		t.Error("an empty file of the snapshot is missing")
	}
	if !exists(snap, baseline.SuccessMarker) || exists(snap, baseline.IncompleteMarker) {
		t.Error("the mirrored snapshot must carry _SUCCESS and no _INCOMPLETE")
	}
	if got := readFile(t, cfg.Dir, SourceFile); got != "s3://bucket/baselines\n" {
		t.Errorf("source file = %q", got)
	}
	// The source's views file names s3:// paths; it must not reach the disk.
	if exists(snap, baseline.SnapshotViewsName) {
		t.Errorf("views.sql was copied from the source: %q", readFile(t, snap, baseline.SnapshotViewsName))
	}
	if src.gets[t2+"/"+baseline.SnapshotViewsName] != 0 {
		t.Error("views.sql was downloaded")
	}
	// The local one is written through the producers' door, for the FINAL path.
	if viewsFor != snap {
		t.Errorf("views writer ran for %q, want %q", viewsFor, snap)
	}
	if target, err := os.Readlink(filepath.Join(cfg.Dir, baseline.CurrentLinkName)); err != nil || target != t2 {
		t.Errorf("current -> %q, %v; want %s", target, err, t2)
	}
	// Only the newest is brought: the older one is not what statements read.
	if exists(cfg.Dir, t1) || src.dataGets(t1) != 0 {
		t.Error("an older snapshot was mirrored")
	}
	if res.FilesDownloaded != 4 || res.BytesDownloaded == 0 {
		t.Errorf("counts = %d files, %d bytes", res.FilesDownloaded, res.BytesDownloaded)
	}

	// A second round has nothing to do and asks for no file.
	before := src.dataGets(t2)
	res = mustSync(t, cfg)
	if res.Downloaded != "" || res.Newest != t2 || src.dataGets(t2) != before {
		t.Errorf("idle round: %+v, gets %d -> %d", res, before, src.dataGets(t2))
	}
}

func TestSync_emptySource(t *testing.T) {
	cfg := testConfig(t, newSource())
	res := mustSync(t, cfg)
	if res.Newest != "" || res.SourceNewest != "" || res.Downloaded != "" {
		t.Errorf("result = %+v", res)
	}
}

func TestSync_skipsWhatIsNotACompleteSnapshot(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "one"})
	// An upload in flight: _INCOMPLETE and data, no _SUCCESS.
	src.objects[t2+"/"+baseline.IncompleteMarker] = nil
	src.objects[t2+"/shop/orders.parquet"] = []byte("half")
	// Dated after the clock: no reader takes it for the newest.
	future := snapshotdir.Name(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
	src.publish(future, map[string]string{"shop/orders.parquet": "future"})
	// Not snapshots at all.
	src.objects["_NEWEST"] = []byte(t2)
	src.objects["notes/readme.txt"] = []byte("x")
	cfg := testConfig(t, src)

	res := mustSync(t, cfg)
	if res.Downloaded != t1 {
		t.Fatalf("downloaded %q, want %s", res.Downloaded, t1)
	}
	if got := visibleSnapshots(t, cfg.Dir); len(got) != 1 || got[0] != t1 {
		t.Errorf("visible = %v", got)
	}
	if exists(cfg.Dir, "notes") || exists(cfg.Dir, "_NEWEST") {
		t.Error("something that is not a snapshot was mirrored")
	}
}

func TestSync_legacySnapshotWithNoMarkerAndNoManifest(t *testing.T) {
	src := newSource()
	src.objects[t1+"/shop/orders.parquet"] = []byte("legacy")
	cfg := testConfig(t, src)
	res := mustSync(t, cfg)
	if res.Downloaded != t1 || readFile(t, cfg.Dir, t1, "shop", "orders.parquet") != "legacy" {
		t.Fatalf("result = %+v", res)
	}
}

func inode(t *testing.T, path string) os.FileInfo {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi
}

func TestSync_linksWhatAnEarlierSnapshotAlreadyHolds(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders-base", "shop/items.parquet": "items-v1", "shop/same.parquet": "12345"})
	cfg := testConfig(t, src)
	mustSync(t, cfg)

	// The update: orders unchanged (a copy inside S3, the same bytes), items
	// rewritten, and one file with the same size and other bytes.
	src.publish(t2, map[string]string{"shop/orders.parquet": "orders-base", "shop/items.parquet": "items-v2", "shop/same.parquet": "54321",
		"shop/orders.000001.upserts": "delta"})
	res := mustSync(t, cfg)
	if res.Downloaded != t2 {
		t.Fatalf("result = %+v", res)
	}
	if !os.SameFile(inode(t, filepath.Join(cfg.Dir, t1, "shop", "orders.parquet")), inode(t, filepath.Join(cfg.Dir, t2, "shop", "orders.parquet"))) {
		t.Error("the unchanged table was not linked")
	}
	if src.gets[t2+"/shop/orders.parquet"] != 0 {
		t.Error("the unchanged table was downloaded")
	}
	for _, f := range []string{"items.parquet", "same.parquet"} {
		if os.SameFile(inode(t, filepath.Join(cfg.Dir, t1, "shop", f)), inode(t, filepath.Join(cfg.Dir, t2, "shop", f))) {
			t.Errorf("%s changed and was linked", f)
		}
	}
	if got := readFile(t, cfg.Dir, t2, "shop", "same.parquet"); got != "54321" {
		t.Errorf("same.parquet = %q", got)
	}
	if res.FilesLinked != 1 || res.BytesLinked != int64(len("orders-base")) {
		t.Errorf("linked = %d files, %d bytes", res.FilesLinked, res.BytesLinked)
	}
	// Both stay: a statement may still be reading the one before.
	if got := visibleSnapshots(t, cfg.Dir); len(got) != 2 {
		t.Errorf("visible = %v", got)
	}
}

func TestSync_noLinkWithoutAManifestDigest(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	cfg := testConfig(t, src)
	mustSync(t, cfg)
	// Same path and size, no manifest in the new snapshot: nothing vouches.
	src.objects[t2+"/shop/orders.parquet"] = []byte("ORDERS")
	src.objects[t2+"/"+baseline.SuccessMarker] = nil
	res := mustSync(t, cfg)
	if res.FilesLinked != 0 || readFile(t, cfg.Dir, t2, "shop", "orders.parquet") != "ORDERS" {
		t.Errorf("result = %+v", res)
	}
}

// Two manifests that both say nothing about a file do not agree about it.
func TestSync_noLinkWhenNeitherManifestCoversTheFile(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	src.objects[t1+"/shop/extra.bin"] = []byte("AAAA") // beside the manifest, not in it
	cfg := testConfig(t, src)
	mustSync(t, cfg)
	src.publish(t2, map[string]string{"shop/orders.parquet": "orders"})
	src.objects[t2+"/shop/extra.bin"] = []byte("BBBB")
	res := mustSync(t, cfg)
	if got := readFile(t, cfg.Dir, t2, "shop", "extra.bin"); got != "BBBB" {
		t.Errorf("extra.bin = %q: a file no manifest covers was linked from the snapshot before", got)
	}
	if res.FilesLinked != 1 {
		t.Errorf("linked %d files, want only the one both manifests vouch for", res.FilesLinked)
	}
}

func TestSync_retention(t *testing.T) {
	src := newSource()
	cfg := testConfig(t, src)
	for _, n := range []string{t1, t2, t3} {
		src.publish(n, map[string]string{"shop/orders.parquet": "orders at " + n})
		mustSync(t, cfg)
	}
	got := visibleSnapshots(t, cfg.Dir)
	if len(got) != 2 || got[0] != t2 || got[1] != t3 {
		t.Fatalf("visible = %v, want the newest two", got)
	}
	entries, _ := os.ReadDir(cfg.Dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), removingSuffix) || strings.HasSuffix(e.Name(), downloadingSuffix) {
			t.Errorf("leftover %s", e.Name())
		}
	}

	// What the source stops listing is not thrown away: neither one
	// snapshot, nor all of them (an emptied prefix and an empty answer look
	// the same).
	src.remove(t3)
	res := mustSync(t, cfg)
	if len(res.Removed) != 0 || res.Newest != t3 {
		t.Fatalf("result = %+v", res)
	}
	src.remove(t1)
	src.remove(t2)
	res = mustSync(t, cfg)
	if len(res.Removed) != 0 || res.Newest != t3 {
		t.Errorf("an empty listing emptied the mirror: %+v", res)
	}
	if target, err := os.Readlink(filepath.Join(cfg.Dir, baseline.CurrentLinkName)); err != nil || target != t3 {
		t.Errorf("current -> %q, %v; want %s", target, err, t3)
	}
}

// The source keeps one snapshot, and the one it has now cannot be brought:
// the mirror must go on serving what it holds.
func TestSync_aFailedRoundRemovesNothing(t *testing.T) {
	src := newSource()
	cfg := testConfig(t, src)
	for _, n := range []string{t1, t2} {
		src.publish(n, map[string]string{"shop/orders.parquet": "orders at " + n})
		mustSync(t, cfg)
	}
	src.remove(t1)
	src.remove(t2)
	src.publish(t3, map[string]string{"shop/orders.parquet": "orders at three"})
	src.body[t3+"/shop/orders.parquet"] = []byte("ORDERS AT THREE")
	res, err := Sync(context.Background(), cfg)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v", err)
	}
	if len(res.Removed) != 0 || res.Newest != t2 || len(visibleSnapshots(t, cfg.Dir)) != 2 {
		t.Fatalf("a failed round removed snapshots: %+v, visible %v", res, visibleSnapshots(t, cfg.Dir))
	}
	// Once it lands, the oldest goes and the one before stays for whoever
	// is still reading it.
	delete(src.body, t3+"/shop/orders.parquet")
	res = mustSync(t, cfg)
	got := visibleSnapshots(t, cfg.Dir)
	if len(res.Removed) != 1 || res.Removed[0] != t1 || len(got) != 2 || got[0] != t2 || got[1] != t3 {
		t.Errorf("result = %+v, visible %v", res, got)
	}
}

func TestSync_snapshotMissingAFileItsManifestRecords(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders", "shop/items.parquet": "items"})
	delete(src.objects, t1+"/shop/items.parquet") // expired on its own; _SUCCESS still there
	cfg := testConfig(t, src)
	_, err := Sync(context.Background(), cfg)
	if !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("err = %v, want ErrSourceChanged", err)
	}
	if len(visibleSnapshots(t, cfg.Dir)) != 0 || src.dataGets(t1) != 0 {
		t.Error("a snapshot missing a table was mirrored")
	}
}

func TestSync_folderPlaceholderObjectsAreNotFiles(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	src.objects[t1+"/shop/"] = nil
	src.objects[t1+"/"] = nil
	cfg := testConfig(t, src)
	if res := mustSync(t, cfg); res.Downloaded != t1 {
		t.Errorf("result = %+v", res)
	}
}

func TestSync_manifestChangedSinceAnInterruptedDownload(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"a/one.parquet": "one", "b/two.parquet": "two"})
	src.fail[t1+"/b/two.parquet"] = errors.New("connection reset")
	cfg := testConfig(t, src)
	if _, err := Sync(context.Background(), cfg); err == nil {
		t.Fatal("the interrupted round reported no error")
	}
	// Published again under the same name: same sizes, other bytes.
	delete(src.fail, t1+"/b/two.parquet")
	src.publish(t1, map[string]string{"a/one.parquet": "ONE", "b/two.parquet": "TWO"})
	res := mustSync(t, cfg)
	if got := readFile(t, cfg.Dir, t1, "a", "one.parquet"); got != "ONE" {
		t.Errorf("one = %q: a file checked against the old manifest was kept", got)
	}
	if res.FilesResumed != 0 {
		t.Errorf("result = %+v", res)
	}
}

func TestSync_linkThatFailsIsDownloadedOnlyIfItFits(t *testing.T) {
	src := newSource()
	big := strings.Repeat("x", 1000)
	src.publish(t1, map[string]string{"shop/orders.parquet": big})
	cfg := testConfig(t, src)
	cfg.Headroom = -1
	mustSync(t, cfg)
	src.publish(t2, map[string]string{"shop/orders.parquet": big})
	linkFile = func(string, string) error { return errors.New("operation not supported") }
	t.Cleanup(func() { linkFile = os.Link })

	// Room for the manifest, which is all the plan counted; not for the
	// table the plan meant to link.
	cfg.FreeBytes = func(string) (uint64, bool) { return 500, true }
	_, err := Sync(context.Background(), cfg)
	var noSpace *NoSpaceError
	if !errors.As(err, &noSpace) || noSpace.Need != 1000 {
		t.Fatalf("err = %v, want NoSpaceError for the 1000 bytes of the table", err)
	}
	if src.gets[t2+"/shop/orders.parquet"] != 0 {
		t.Error("the file was downloaded past the disk check")
	}
	cfg.FreeBytes = func(string) (uint64, bool) { return 1 << 20, true }
	res := mustSync(t, cfg)
	if res.Downloaded != t2 || res.FilesLinked != 0 || readFile(t, cfg.Dir, t2, "shop", "orders.parquet") != big {
		t.Errorf("result = %+v", res)
	}
}

func TestSync_refusesTheMirrorOfAnotherSource(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "from one"})
	cfg := testConfig(t, src)
	mustSync(t, cfg)

	other := newSource()
	other.publish(t2, map[string]string{"shop/orders.parquet": "from two"})
	other.publish(t3, map[string]string{"shop/orders.parquet": "from two"})
	cfg2 := cfg
	cfg2.Source, cfg2.SourceName, cfg2.Keep = other, "s3://other/baselines", 1
	res, err := Sync(context.Background(), cfg2)
	if !errors.Is(err, ErrOtherSource) {
		t.Fatalf("err = %v, want ErrOtherSource", err)
	}
	if len(res.Removed) != 0 || res.Downloaded != "" || len(other.gets) != 0 || readFile(t, cfg.Dir, t1, "shop", "orders.parquet") != "from one" {
		t.Errorf("one source touched another's mirror: %+v", res)
	}
	// What is there is still what a statement reads, and the round says so.
	if res.Newest != t1 {
		t.Errorf("newest = %q", res.Newest)
	}
}
func TestSync_unreadableMirrorIsAnErrorNotAnEmptyOne(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root reads any directory")
	}
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	cfg := testConfig(t, src)
	mustSync(t, cfg)
	src.publish(t2, map[string]string{"shop/orders.parquet": "orders"})
	if err := os.Chmod(cfg.Dir, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(cfg.Dir, 0o700) })
	_, err := Sync(context.Background(), cfg)
	if err == nil {
		t.Fatal("a mirror that cannot be listed was treated as empty")
	}
	if src.dataGets(t2) != 0 {
		t.Error("the round went on to download")
	}
}

func TestSync_snapshotThatCannotBeMarkedKeepsFailing(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root writes into any directory")
	}
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	cfg := testConfig(t, src)
	snap := filepath.Join(cfg.Dir, t1)
	if err := os.MkdirAll(snap, 0o700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(cfg.Dir, SourceFile), []byte(cfg.SourceName+"\n"), 0o600)
	os.WriteFile(filepath.Join(snap, baseline.IncompleteMarker), nil, 0o600)
	if err := os.Chmod(snap, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(snap, 0o700) })
	for round := 1; round <= 2; round++ {
		res, err := Sync(context.Background(), cfg)
		if err == nil || !strings.Contains(err.Error(), "cannot be marked complete") {
			t.Fatalf("round %d: err = %v, want the one that says the snapshot cannot be marked complete (%+v)", round, err, res)
		}
		if len(src.gets) != 0 {
			t.Fatalf("round %d: the round went on to the source: %v", round, src.gets)
		}
		if res.Newest != "" {
			t.Errorf("round %d: newest = %q", round, res.Newest)
		}
	}
}

func TestSync_listingFailureChangesNothing(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	cfg := testConfig(t, src)
	mustSync(t, cfg)
	src.listErr = errors.New("throttled")
	res, err := Sync(context.Background(), cfg)
	if err == nil || res.Newest != t1 || len(res.Removed) != 0 {
		t.Errorf("res = %+v, err = %v", res, err)
	}
}

func TestSync_corruptFileIsNeverVisibleAndTheNextRoundResumes(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"a/one.parquet": "one", "b/two.parquet": "two", "c/three.parquet": "three"})
	cfg := testConfig(t, src)
	src.body[t1+"/b/two.parquet"] = []byte("tWo") // same size, other bytes

	res, err := Sync(context.Background(), cfg)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	if res.Newest != "" || len(visibleSnapshots(t, cfg.Dir)) != 0 {
		t.Fatalf("a snapshot is visible after a corrupt download: %+v %v", res, visibleSnapshots(t, cfg.Dir))
	}
	if exists(cfg.Dir, "."+t1+downloadingSuffix, "b", "two.parquet") || exists(cfg.Dir, "."+t1+downloadingSuffix, "b", "two.parquet"+partSuffix) {
		t.Error("the corrupt file was kept")
	}

	delete(src.body, t1+"/b/two.parquet")
	res = mustSync(t, cfg)
	if res.Downloaded != t1 || res.Newest != t1 {
		t.Fatalf("result = %+v", res)
	}
	if src.gets[t1+"/a/one.parquet"] != 1 {
		t.Errorf("a file already downloaded was fetched %d times", src.gets[t1+"/a/one.parquet"])
	}
	if res.FilesResumed == 0 {
		t.Errorf("nothing counted as resumed: %+v", res)
	}
	if got := readFile(t, cfg.Dir, t1, "b", "two.parquet"); got != "two" {
		t.Errorf("two = %q", got)
	}
}

func TestSync_sizeMismatch(t *testing.T) {
	src := newSource()
	src.objects[t1+"/shop/orders.parquet"] = []byte("orders") // no manifest: size is all there is
	src.body[t1+"/shop/orders.parquet"] = []byte("ord")
	cfg := testConfig(t, src)
	if _, err := Sync(context.Background(), cfg); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	if len(visibleSnapshots(t, cfg.Dir)) != 0 {
		t.Error("a truncated snapshot is visible")
	}
}

func TestSync_objectGoneMidDownload(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	src.fail[t1+"/shop/orders.parquet"] = errGone
	cfg := testConfig(t, src)
	if _, err := Sync(context.Background(), cfg); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("err = %v, want ErrSourceChanged", err)
	}
	if len(visibleSnapshots(t, cfg.Dir)) != 0 {
		t.Error("a snapshot with a missing file is visible")
	}
	// The source finishes removing it and publishes another: the abandoned
	// download is swept, not left to hold disk.
	src.remove(t1)
	delete(src.fail, t1+"/shop/orders.parquet")
	src.publish(t2, map[string]string{"shop/orders.parquet": "orders"})
	mustSync(t, cfg)
	if exists(cfg.Dir, "."+t1+downloadingSuffix) {
		t.Error("the abandoned download is still there")
	}
}

func TestSync_networkErrorIsNotCalledAMissingObject(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	src.fail[t1+"/shop/orders.parquet"] = errors.New("connection reset")
	cfg := testConfig(t, src)
	_, err := Sync(context.Background(), cfg)
	if err == nil || errors.Is(err, ErrSourceChanged) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v", err)
	}
}

func TestSync_refusesBeforeDownloadingWhenTheDiskIsShort(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": strings.Repeat("x", 1000)})
	cfg := testConfig(t, src)
	cfg.Headroom = 500
	cfg.FreeBytes = func(string) (uint64, bool) { return 1400, true } // 1000 + 500 > 1400
	_, err := Sync(context.Background(), cfg)
	var noSpace *NoSpaceError
	if !errors.As(err, &noSpace) || !errors.Is(err, ErrNoSpace) {
		t.Fatalf("err = %v, want NoSpaceError", err)
	}
	if noSpace.Need < 1000 || noSpace.Need > 1200 || noSpace.Free != 1400 || noSpace.Headroom != 500 || noSpace.Snapshot != t1 {
		t.Errorf("numbers = %+v", noSpace)
	}
	if src.dataGets(t1) != 0 {
		t.Errorf("%d files were downloaded before the refusal", src.dataGets(t1))
	}
	if len(visibleSnapshots(t, cfg.Dir)) != 0 {
		t.Error("a snapshot is visible")
	}

	// Exactly enough passes.
	cfg.FreeBytes = func(string) (uint64, bool) { return uint64(noSpace.Need + 500), true }
	mustSync(t, cfg)
}

func TestSync_linkedBytesDoNotCountAgainstTheDisk(t *testing.T) {
	src := newSource()
	big := strings.Repeat("x", 1000)
	src.publish(t1, map[string]string{"shop/orders.parquet": big})
	cfg := testConfig(t, src)
	cfg.Headroom = -1
	mustSync(t, cfg)
	src.publish(t2, map[string]string{"shop/orders.parquet": big, "shop/orders.000001.upserts": "tiny"})
	cfg.FreeBytes = func(string) (uint64, bool) { return 500, true }
	res := mustSync(t, cfg)
	if res.Downloaded != t2 || res.FilesLinked != 1 {
		t.Errorf("result = %+v", res)
	}
}

func TestSync_unknownFreeSpaceDoesNotRefuse(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	cfg := testConfig(t, src)
	cfg.FreeBytes = func(string) (uint64, bool) { return 0, false }
	mustSync(t, cfg)
}

func TestSync_refusesNamesThatWalkOutOfTheSnapshot(t *testing.T) {
	for _, rel := range []string{"../escape.parquet", "shop/../../escape.parquet", "shop//orders.parquet", "shop\\orders.parquet", "./orders.parquet", "shop/orders.parquet" + partSuffix} {
		t.Run(rel, func(t *testing.T) {
			src := newSource()
			src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
			src.objects[t1+"/"+rel] = []byte("x")
			cfg := testConfig(t, src)
			_, err := Sync(context.Background(), cfg)
			if !errors.Is(err, ErrUnsafeName) {
				t.Fatalf("err = %v, want ErrUnsafeName", err)
			}
			if src.dataGets(t1) != 0 {
				t.Error("files were downloaded from a snapshot that is refused")
			}
			if exists(filepath.Dir(cfg.Dir), "escape.parquet") || exists(cfg.Dir, "escape.parquet") || len(visibleSnapshots(t, cfg.Dir)) != 0 {
				t.Error("something was written")
			}
		})
	}
}

// A snapshot root some producer writes, given as the mirror directory.
func TestSync_refusesADirectoryThatHoldsSnapshotsAndIsNotAMirror(t *testing.T) {
	src := newSource()
	src.publish(t2, map[string]string{"shop/orders.parquet": "from s3"})
	cfg := testConfig(t, src)
	old := snapshotdir.Name(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	for _, n := range []string{t2, old} {
		if err := os.MkdirAll(filepath.Join(cfg.Dir, n, "shop"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(cfg.Dir, n, "shop", "orders.parquet"), []byte("mine"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// One complete, one with no marker at all: both are somebody's.
	if err := os.WriteFile(filepath.Join(cfg.Dir, old, baseline.SuccessMarker), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for round := 1; round <= 2; round++ {
		res, err := Sync(context.Background(), cfg)
		if !errors.Is(err, ErrNotAMirror) {
			t.Fatalf("round %d: err = %v, want ErrNotAMirror", round, err)
		}
		if len(res.Removed) != 0 || res.Downloaded != "" || len(src.gets) != 0 {
			t.Errorf("round %d: result = %+v, gets = %v", round, res, src.gets)
		}
	}
	for _, n := range []string{t2, old} {
		if got := readFile(t, cfg.Dir, n, "shop", "orders.parquet"); got != "mine" {
			t.Errorf("%s was overwritten: %q", n, got)
		}
	}
	if exists(cfg.Dir, SourceFile) || exists(cfg.Dir, t2, baseline.SuccessMarker) {
		t.Error("the directory was claimed or a snapshot in it was marked")
	}
}
func TestSync_busy(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	cfg := testConfig(t, src)
	var inner Result
	var innerErr error
	src.onGet = func(string) {
		src.onGet = nil
		inner, innerErr = Sync(context.Background(), cfg)
	}
	res := mustSync(t, cfg)
	if innerErr != nil || !inner.Busy {
		t.Fatalf("a round started during another: %+v, %v", inner, innerErr)
	}
	if res.Busy || res.Downloaded != t1 {
		t.Errorf("outer = %+v", res)
	}

	// With a snapshot already mirrored, the round that steps aside still
	// says what statements read meanwhile.
	src.publish(t2, map[string]string{"shop/orders.parquet": "orders two"})
	src.onGet = func(string) {
		src.onGet = nil
		inner, innerErr = Sync(context.Background(), cfg)
	}
	mustSync(t, cfg)
	if innerErr != nil || !inner.Busy || inner.Newest != t1 {
		t.Errorf("the round that stepped aside reported %+v, %v; want Busy with newest %s", inner, innerErr, t1)
	}
}

func TestSync_finishesASnapshotRenamedAndNotMarked(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	cfg := testConfig(t, src)
	// What a round killed between its rename and _SUCCESS leaves.
	snap := filepath.Join(cfg.Dir, t1)
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{"shop/orders.parquet": "orders", baseline.IncompleteMarker: ""} {
		if err := os.WriteFile(filepath.Join(snap, filepath.FromSlash(name)), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(cfg.Dir, SourceFile), []byte(cfg.SourceName+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := visibleSnapshots(t, cfg.Dir); len(got) != 0 {
		t.Fatalf("the unmarked snapshot is visible before the round: %v", got)
	}
	res := mustSync(t, cfg)
	if res.Newest != t1 || src.dataGets(t1) != 0 {
		t.Errorf("result = %+v, data gets = %d", res, src.dataGets(t1))
	}
	if !exists(snap, baseline.SuccessMarker) || exists(snap, baseline.IncompleteMarker) {
		t.Error("markers were not settled")
	}
}

func TestSync_noPartFileTravelsIntoTheSnapshot(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	cfg := testConfig(t, src)
	tmp := filepath.Join(cfg.Dir, "."+t1+downloadingSuffix, "shop")
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	// A whole file under its own name with a stale part beside it.
	os.WriteFile(filepath.Join(tmp, "orders.parquet"), []byte("orders"), 0o600)
	os.WriteFile(filepath.Join(tmp, "orders.parquet"+partSuffix), []byte("or"), 0o600)
	mustSync(t, cfg)
	if exists(cfg.Dir, t1, "shop", "orders.parquet"+partSuffix) {
		t.Error("a part file is inside the mirrored snapshot")
	}
}

func TestWorkingNamesAreNotSnapshots(t *testing.T) {
	for _, n := range []string{"." + t1 + downloadingSuffix, "." + t1 + removingSuffix} {
		if _, ok := snapshotdir.ParseTime(n); ok {
			t.Errorf("%s parses as a snapshot directory", n)
		}
	}
}

func TestSync_configRefusals(t *testing.T) {
	// Named under the temporary directory, so that a build that took it
	// would write there and not into the package's own.
	t.Chdir(t.TempDir())
	if _, err := Sync(context.Background(), Config{Dir: "relative/dir", Source: newSource(), SourceName: "s3://b/p"}); err == nil {
		t.Error("a relative mirror directory was accepted")
	}
	if _, err := Sync(context.Background(), Config{Dir: t.TempDir(), SourceName: "s3://b/p"}); err == nil {
		t.Error("a mirror with no source was accepted")
	}
	if _, err := Sync(context.Background(), Config{Dir: t.TempDir(), Source: newSource(), SourceName: "/"}); err == nil {
		t.Error("a mirror with no source name was accepted")
	}
}

func TestSync_manifestThatCannotBeUsedStillMirrors(t *testing.T) {
	for name, manifest := range map[string]string{
		"not json":      "{{{",
		"other version": `{"version":9,"algo":"crc32c","files":{"shop/orders.parquet":"00000000"}}`,
		"other algo":    `{"version":1,"algo":"sha256","files":{"shop/orders.parquet":"00000000"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			src := newSource()
			src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
			src.objects[t1+"/"+baselineintegrity.ManifestName] = []byte(manifest)
			cfg := testConfig(t, src)
			res := mustSync(t, cfg)
			if res.Downloaded != t1 || len(res.Notes) == 0 {
				t.Errorf("result = %+v", res)
			}
		})
	}
}

func TestSync_cancelled(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"a/one.parquet": "one", "b/two.parquet": "two"})
	cfg := testConfig(t, src)
	ctx, cancel := context.WithCancel(context.Background())
	src.onGet = func(key string) {
		if !strings.HasSuffix(key, baselineintegrity.ManifestName) {
			cancel()
		}
	}
	if _, err := Sync(ctx, cfg); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if len(visibleSnapshots(t, cfg.Dir)) != 0 {
		t.Error("a cancelled download is visible")
	}
}

func TestSync_theSameSourceSpelledWithAndWithoutASlash(t *testing.T) {
	src := newSource()
	cfg := testConfig(t, src)
	for _, n := range []string{t1, t2} {
		src.publish(n, map[string]string{"shop/orders.parquet": "orders at " + n})
		mustSync(t, cfg)
	}
	// The other process sharing the directory names the root with a slash.
	other := cfg
	other.SourceName = cfg.SourceName + "/"
	src.publish(t3, map[string]string{"shop/orders.parquet": "orders at three"})
	res := mustSync(t, other)
	if res.Downloaded != t3 || len(res.Removed) != 1 || res.Removed[0] != t1 {
		t.Errorf("result = %+v: the other spelling did not take the mirror's snapshots for its own", res)
	}
	before := src.dataGets(t3)
	if res := mustSync(t, cfg); res.Downloaded != "" || res.Newest != t3 || src.dataGets(t3) != before {
		t.Errorf("the first spelling downloaded again: %+v", res)
	}
}

func TestSync_bodyLongerThanItsListingIsCutAndRefused(t *testing.T) {
	src := newSource()
	src.objects[t1+"/shop/orders.parquet"] = []byte("orders")
	src.body[t1+"/shop/orders.parquet"] = []byte(strings.Repeat("x", 1<<20))
	cfg := testConfig(t, src)
	if _, err := Sync(context.Background(), cfg); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	if src.read > 100 {
		t.Errorf("%d bytes were taken from a body its listing put at 6", src.read)
	}
}

// A name that parses as a time and is not spelled as a directory of one.
func TestSync_snapshotNameWithOtherCharactersIsNotMirrored(t *testing.T) {
	src := newSource()
	odd := "2026-10-01T10:00:00Z"
	if _, ok := snapshotdir.ParseTime(odd); !ok {
		t.Fatalf("%q no longer parses; pick another name the parser takes", odd)
	}
	src.publish(odd, map[string]string{"shop/orders.parquet": "orders"})
	cfg := testConfig(t, src)
	if res := mustSync(t, cfg); res.Downloaded != "" || res.SourceNewest != "" {
		t.Errorf("result = %+v", res)
	}
}

func TestSync_oversizedManifestIsRefusedUnfetched(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	src.objects[t1+"/"+baselineintegrity.ManifestName] = make([]byte, maxManifestBytes+1)
	cfg := testConfig(t, src)
	if _, err := Sync(context.Background(), cfg); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	if len(src.gets) != 0 {
		t.Errorf("fetched: %v", src.gets)
	}
}

func TestPlainName(t *testing.T) {
	for name, want := range map[string]bool{
		t1: true, "2026-10-01T10-00-00+05-00": true, "2026-10-01T10-00-00.5Z": true,
		"": false, ".2026-10-01T10-00-00Z": false, "2026-10-01T10-00-00Z/x": false,
		"2026-10-01T10-00-00Z\\x": false, "2026-10-01T10-00-00Z x": false,
	} {
		if got := plainName(name); got != want {
			t.Errorf("plainName(%q) = %v, want %v", name, got, want)
		}
	}
}

// The operator deleted the newest snapshot from the bucket: the source's
// newest is now older than the mirror's. Bringing it would only have it
// removed again as past Keep, in every round.
func TestSync_sourceBehindTheMirrorBringsNothing(t *testing.T) {
	src := newSource()
	cfg := testConfig(t, src)
	cfg.Keep = 1
	for _, n := range []string{t2, t3} {
		src.publish(n, map[string]string{"shop/orders.parquet": "orders at " + n})
		mustSync(t, cfg)
	}
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders at one"})
	src.remove(t3)
	for round := 1; round <= 2; round++ {
		res := mustSync(t, cfg)
		if res.Downloaded != "" || len(res.Removed) != 0 || res.Newest != t3 || res.SourceNewest != "" {
			t.Fatalf("round %d: %+v", round, res)
		}
	}
	if n := src.dataGets(t2) - 1; n != 0 {
		t.Errorf("the older snapshot was downloaded %d more times", n)
	}
	if src.dataGets(t1) != 0 {
		t.Error("a snapshot older still was downloaded")
	}
}

// A file of the mirrored snapshot went bad on this disk, same size: it must
// not be linked into the next one on its manifest's word.
func TestSync_linksOnlyBytesThatHashToTheManifest(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders-base"})
	cfg := testConfig(t, src)
	mustSync(t, cfg)
	if err := os.WriteFile(filepath.Join(cfg.Dir, t1, "shop", "orders.parquet"), []byte("orders-BASE"), 0o600); err != nil {
		t.Fatal(err)
	}
	src.publish(t2, map[string]string{"shop/orders.parquet": "orders-base"})
	res := mustSync(t, cfg)
	if got := readFile(t, cfg.Dir, t2, "shop", "orders.parquet"); got != "orders-base" {
		t.Errorf("orders = %q: damaged bytes were linked forward", got)
	}
	if res.FilesLinked != 0 || src.gets[t2+"/shop/orders.parquet"] != 1 {
		t.Errorf("result = %+v", res)
	}
}

// What an interrupted round left is kept only if it is the manifest's bytes,
// and nothing else it left travels into the snapshot.
func TestSync_resumesOnlyBytesThatHashToTheManifest(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"a/one.parquet": "one", "b/two.parquet": "two"})
	cfg := testConfig(t, src)
	tmp := filepath.Join(cfg.Dir, "."+t1+downloadingSuffix)
	for rel, content := range map[string]string{"a/one.parquet": "ONE", "b/two.parquet": "two", "c/stray.parquet": "stray", "_MANIFEST": "{}"} {
		p := filepath.Join(tmp, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	res := mustSync(t, cfg)
	if got := readFile(t, cfg.Dir, t1, "a", "one.parquet"); got != "one" {
		t.Errorf("one = %q: a leftover of the right size and other bytes was kept", got)
	}
	if res.FilesResumed != 1 || src.gets[t1+"/b/two.parquet"] != 0 {
		t.Errorf("the good leftover was not kept: %+v", res)
	}
	if exists(cfg.Dir, t1, "c", "stray.parquet") {
		t.Error("a file that is not the snapshot's is inside it")
	}
	if err := baselineintegrity.ValidateLocalFile(filepath.Join(cfg.Dir, t1, "a", "one.parquet")); err != nil {
		t.Errorf("the mirrored snapshot does not validate against its own manifest: %v", err)
	}
}

// With no manifest nothing on disk can be shown to be the snapshot's file.
func TestSync_noManifestNoResume(t *testing.T) {
	src := newSource()
	src.objects[t1+"/shop/orders.parquet"] = []byte("orders")
	cfg := testConfig(t, src)
	tmp := filepath.Join(cfg.Dir, "."+t1+downloadingSuffix, "shop")
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(tmp, "orders.parquet"), []byte("ORDERS"), 0o600)
	res := mustSync(t, cfg)
	if got := readFile(t, cfg.Dir, t1, "shop", "orders.parquet"); got != "orders" || res.FilesResumed != 0 {
		t.Errorf("orders = %q, result = %+v", got, res)
	}
}

func TestSync_manifestThatIsNotItsListedSize(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	src.body[t1+"/"+baselineintegrity.ManifestName] = []byte(`{"version":1,"algo":"crc32c","files":{}}`)
	cfg := testConfig(t, src)
	if _, err := Sync(context.Background(), cfg); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	if src.dataGets(t1) != 0 {
		t.Error("files were downloaded under a manifest that is not the listed one")
	}
}

// A snapshot some producer is still writing has no marker yet. Given as the
// mirror directory it must be refused like any other: taken for the mirror's
// own unfinished snapshot, it would be marked complete.
func TestSync_refusesADirectoryHoldingOnlyAnUnmarkedSnapshot(t *testing.T) {
	src := newSource()
	src.publish(t2, map[string]string{"shop/orders.parquet": "from s3"})
	cfg := testConfig(t, src)
	if err := os.MkdirAll(filepath.Join(cfg.Dir, t1, "shop"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Sync(context.Background(), cfg); !errors.Is(err, ErrNotAMirror) {
		t.Fatalf("err = %v, want ErrNotAMirror", err)
	}
	if exists(cfg.Dir, t1, baseline.SuccessMarker) || exists(cfg.Dir, SourceFile) {
		t.Error("somebody else's unfinished snapshot was marked complete, or the directory claimed")
	}
}

// A file system that cannot link: what was to be linked is downloaded, and
// it competes for the free space with everything still to download.
func TestSync_failedLinksDoNotOutrunTheDisk(t *testing.T) {
	src := newSource()
	big := strings.Repeat("x", 1000)
	src.publish(t1, map[string]string{"shop/a.parquet": big})
	cfg := testConfig(t, src)
	cfg.Headroom = -1
	mustSync(t, cfg)
	src.publish(t2, map[string]string{"shop/a.parquet": big, "shop/b.parquet": strings.Repeat("y", 1000)})
	linkFile = func(string, string) error { return errors.New("operation not supported") }
	t.Cleanup(func() { linkFile = os.Link })
	before := src.read
	const free = 1500
	cfg.FreeBytes = func(string) (uint64, bool) { return uint64(max(free-(src.read-before), 0)), true }
	_, err := Sync(context.Background(), cfg)
	if !errors.Is(err, ErrNoSpace) {
		t.Fatalf("err = %v, want ErrNoSpace: 2000 bytes do not fit in %d", err, free)
	}
	if got := src.read - before; got > free {
		t.Errorf("%d bytes were downloaded into %d free", got, free)
	}
	if len(visibleSnapshots(t, cfg.Dir)) != 1 {
		t.Error("the snapshot that did not fit is visible")
	}
}

// A source file that was created and never written (a full disk) is not
// another source's name.
func TestSync_emptySourceFileIsNoSourceFile(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	cfg := testConfig(t, src)
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.Dir, SourceFile), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if res := mustSync(t, cfg); res.Downloaded != t1 {
		t.Errorf("result = %+v", res)
	}
	if got := readFile(t, cfg.Dir, SourceFile); got != cfg.SourceName+"\n" {
		t.Errorf("source file = %q", got)
	}
	entries, _ := os.ReadDir(cfg.Dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), partSuffix) {
			t.Errorf("leftover %s", e.Name())
		}
	}
}

// A snapshot brought while the clock ran ahead must not keep real ones out
// once the clock is right.
func TestSync_futureDatedSnapshotInTheMirrorDoesNotFreezeIt(t *testing.T) {
	src := newSource()
	future := snapshotdir.Name(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
	src.publish(future, map[string]string{"shop/orders.parquet": "ahead"})
	cfg := testConfig(t, src)
	cfg.Now = func() time.Time { return time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC) }
	if res := mustSync(t, cfg); res.Downloaded != future {
		t.Fatalf("setup: %+v", res)
	}
	cfg.Now = func() time.Time { return time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC) }
	src.publish(t2, map[string]string{"shop/orders.parquet": "real"})
	if res := mustSync(t, cfg); res.Downloaded != t2 {
		t.Errorf("the real snapshot was not brought: %+v", res)
	}
}

func TestSync_sourceNewestIsNeverAnIncompleteSnapshot(t *testing.T) {
	src := newSource()
	src.publish(t2, map[string]string{"shop/orders.parquet": "orders"})
	cfg := testConfig(t, src)
	mustSync(t, cfg)
	src.remove(t2)
	src.objects[t1+"/"+baseline.IncompleteMarker] = nil
	src.objects[t1+"/shop/orders.parquet"] = []byte("half")
	if res := mustSync(t, cfg); res.SourceNewest != "" || res.Newest != t2 {
		t.Errorf("result = %+v", res)
	}
}

func TestSync_aSourceThatGoesSilentIsGivenUp(t *testing.T) {
	for name, silence := range map[string]func(src *fakeSource){
		"no answer to a request": func(src *fakeSource) { src.hang = map[string]bool{t1 + "/shop/orders.parquet": true} },
		"a download that stops":  func(src *fakeSource) { src.stallAfter = map[string]int{t1 + "/shop/orders.parquet": 3} },
		"no answer for the manifest": func(src *fakeSource) {
			src.hang = map[string]bool{t1 + "/" + baselineintegrity.ManifestName: true}
		},
	} {
		t.Run(name, func(t *testing.T) {
			src := newSource()
			src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
			silence(src)
			cfg := testConfig(t, src)
			cfg.StallTimeout = 50 * time.Millisecond
			done := make(chan error, 1)
			go func() { _, err := Sync(context.Background(), cfg); done <- err }()
			select {
			case err := <-done:
				if err == nil || !(strings.Contains(err.Error(), "no answer in") || strings.Contains(err.Error(), "no bytes for")) {
					t.Errorf("err = %v, want one that says the source went silent", err)
				}
				if errors.Is(err, ErrSourceChanged) || errors.Is(err, ErrCorrupt) {
					t.Errorf("a silent source was called something else: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the round is still waiting on a source that does not answer")
			}
			if len(visibleSnapshots(t, cfg.Dir)) != 0 {
				t.Error("a snapshot is visible")
			}
		})
	}
}

// A slow download that keeps arriving is not a silent one.
func TestSync_slowIsNotSilent(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": strings.Repeat("x", 64)})
	cfg := testConfig(t, src)
	cfg.StallTimeout = 200 * time.Millisecond
	slow := &slowSource{fakeSource: src, pause: 20 * time.Millisecond}
	cfg.Source = slow
	if res := mustSync(t, cfg); res.Downloaded != t1 {
		t.Errorf("result = %+v", res)
	}
}

// slowSource hands each body out a byte at a time with a pause between,
// longer in all than the stall timeout and never silent for as long.
type slowSource struct {
	*fakeSource
	pause time.Duration
}

func (s *slowSource) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, err := s.fakeSource.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(&dripReader{ctx: ctx, r: rc, pause: s.pause}), nil
}

// dripReader ends when its request's context does, as a real body would.
type dripReader struct {
	ctx   context.Context
	r     io.Reader
	pause time.Duration
}

func (d *dripReader) Read(p []byte) (int, error) {
	select {
	case <-d.ctx.Done():
		return 0, d.ctx.Err()
	case <-time.After(d.pause):
	}
	return d.r.Read(p[:1])
}

func TestSync_saysWhatItIsAboutToDownload(t *testing.T) {
	src := newSource()
	src.publish(t1, map[string]string{"shop/orders.parquet": "orders"})
	cfg := testConfig(t, src)
	var calls int
	cfg.OnDownload = func(name string, files int, bytes int64) {
		calls++
		if name != t1 || files != 1 || bytes != int64(len("orders")) {
			t.Errorf("OnDownload(%q, %d, %d)", name, files, bytes)
		}
		if src.dataGets(t1) != 0 {
			t.Error("OnDownload ran after the first file was fetched")
		}
	}
	mustSync(t, cfg)
	mustSync(t, cfg) // nothing to bring: no call
	if calls != 1 {
		t.Errorf("OnDownload ran %d times, want once", calls)
	}
}

// What the plan has already downloaded no longer competes with a file whose
// link failed: a snapshot that fits is brought.
func TestSync_failedLinkAfterADownloadStillFits(t *testing.T) {
	src := newSource()
	big := strings.Repeat("x", 1000)
	src.publish(t1, map[string]string{"shop/b.parquet": big})
	cfg := testConfig(t, src)
	cfg.Headroom = -1
	mustSync(t, cfg)
	// a sorts first and is new; b is unchanged and its link will fail.
	src.publish(t2, map[string]string{"shop/a.parquet": strings.Repeat("y", 1000), "shop/b.parquet": big})
	linkFile = func(string, string) error { return errors.New("operation not supported") }
	t.Cleanup(func() { linkFile = os.Link })
	before := src.read
	cfg.FreeBytes = func(string) (uint64, bool) { return uint64(max(2300-(src.read-before), 0)), true }
	if res := mustSync(t, cfg); res.Downloaded != t2 || res.FilesLinked != 0 {
		t.Errorf("result = %+v", res)
	}
}

// The manifests agree that the table did not change, and the file on this
// disk is no longer those bytes: it has to be downloaded, so it has to fit.
func TestSync_damagedPriorCountsAgainstTheDisk(t *testing.T) {
	src := newSource()
	big := strings.Repeat("x", 1000)
	src.publish(t1, map[string]string{"shop/orders.parquet": big})
	cfg := testConfig(t, src)
	cfg.Headroom = -1
	mustSync(t, cfg)
	if err := os.WriteFile(filepath.Join(cfg.Dir, t1, "shop", "orders.parquet"), []byte(strings.Repeat("z", 1000)), 0o600); err != nil {
		t.Fatal(err)
	}
	src.publish(t2, map[string]string{"shop/orders.parquet": big})
	cfg.FreeBytes = func(string) (uint64, bool) { return 500, true }
	if _, err := Sync(context.Background(), cfg); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("err = %v, want ErrNoSpace", err)
	}
	if src.gets[t2+"/shop/orders.parquet"] != 0 {
		t.Error("the table was downloaded into a disk it does not fit")
	}
}

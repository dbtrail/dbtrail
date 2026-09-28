package reconstruct

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// captureLog points slog at a buffer for one test and forgets what the
// listings already said, so a finding logged by an earlier test is logged
// again here.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	resetSnapshotWriters()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev); resetSnapshotWriters() })
	return &buf
}

const (
	writerA = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	writerB = "bbbbbbbb-0000-0000-0000-000000000002"
	sharedS = "more than one DBTrail installation"
)

// writersCase is one snapshot location: per snapshot directory, the marker
// files it holds beside its one table.
type writersCase struct {
	name string
	dirs [][]string
	want []string
	// logs is a fragment the log must hold; "" means the shared-location
	// warning must NOT be there.
	shared bool
	// unreadable: the log names a signature it could not read.
	unreadable bool
	// listed is how many snapshots the listing returns.
	listed int
}

func writersCases() []writersCase {
	return []writersCase{
		{name: "only unsigned snapshots", dirs: [][]string{{"_SUCCESS"}, {}, {"_SUCCESS", "_MANIFEST"}}, listed: 3},
		{name: "one writer", dirs: [][]string{{"_SUCCESS", "_WRITER." + writerA}, {"_SUCCESS", "_WRITER." + writerA}}, want: []string{writerA}, listed: 2},
		{name: "unsigned snapshots and one writer", dirs: [][]string{{"_SUCCESS"}, {}, {"_SUCCESS", "_WRITER." + writerA}}, want: []string{writerA}, listed: 3},
		{name: "two writers", dirs: [][]string{{"_SUCCESS", "_WRITER." + writerA}, {"_SUCCESS", "_WRITER." + writerB}}, want: []string{writerA, writerB}, shared: true, listed: 2},
		{name: "two spellings of one id are one writer", dirs: [][]string{{"_SUCCESS", "_WRITER." + writerA}, {"_SUCCESS", "_WRITER." + strings.ToUpper(writerA)}}, want: []string{writerA}, listed: 2},
		{name: "an empty signature is unsigned", dirs: [][]string{{"_SUCCESS", "_WRITER." + writerA}, {"_SUCCESS", "_WRITER."}}, want: []string{writerA}, unreadable: true, listed: 2},
		{name: "a signature with spaces is unsigned", dirs: [][]string{{"_SUCCESS", "_WRITER." + writerA}, {"_SUCCESS", "_WRITER. " + writerB + " "}}, want: []string{writerA}, unreadable: true, listed: 2},
		{name: "an unreadable signature is unsigned", dirs: [][]string{{"_SUCCESS", "_WRITER." + writerA}, {"_SUCCESS", "_WRITER.who?*"}}, want: []string{writerA}, unreadable: true, listed: 2},
		{name: "an incomplete snapshot of another writer", dirs: [][]string{{"_SUCCESS", "_WRITER." + writerA}, {"_INCOMPLETE", "_WRITER." + writerB}}, want: []string{writerA, writerB}, shared: true, listed: 1},
		{name: "two writers published the same timestamp", dirs: [][]string{{"_SUCCESS", "_WRITER." + writerA, "_WRITER." + writerB}}, want: []string{writerA, writerB}, shared: true, listed: 1},
	}
}

func checkWritersCase(t *testing.T, tc writersCase, source string, log *bytes.Buffer) {
	t.Helper()
	files, err := ListBaselines(context.Background(), source)
	if err != nil {
		t.Fatalf("the listing failed: %v", err)
	}
	if len(files) != tc.listed {
		t.Fatalf("listed %d snapshots, want %d: %+v", len(files), tc.listed, files)
	}
	if got := SnapshotWritersSeen(source); !slices.Equal(got, tc.want) {
		t.Fatalf("writers = %q, want %q", got, tc.want)
	}
	out := log.String()
	if tc.shared {
		// The warning names every writer, so whichever installation reads
		// the log finds the other one in it.
		for _, w := range tc.want {
			if !strings.Contains(out, w) {
				t.Fatalf("the log does not name writer %s:\n%s", w, out)
			}
		}
		if !strings.Contains(out, sharedS) || !strings.Contains(out, "its own folder or S3 prefix") {
			t.Fatalf("the log does not say the location is shared and what to do:\n%s", out)
		}
	} else if strings.Contains(out, sharedS) {
		t.Fatalf("a shared-location warning with writers %q:\n%s", tc.want, out)
	}
	if tc.unreadable != strings.Contains(out, "cannot be read; it is treated as unsigned") {
		t.Fatalf("unreadable signature logged = %v, want %v:\n%s", !tc.unreadable, tc.unreadable, out)
	}
	// Said once: a second listing of the same location adds nothing.
	before := log.Len()
	if _, err := ListBaselines(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(log.String()[before:], sharedS) {
		t.Fatalf("the warning was logged again by the second listing:\n%s", log.String()[before:])
	}
	if got := SnapshotWritersSeen(source); !slices.Equal(got, tc.want) {
		t.Fatalf("writers after the second listing = %q, want %q", got, tc.want)
	}
}

func TestSnapshotWriters_local(t *testing.T) {
	for _, tc := range writersCases() {
		t.Run(tc.name, func(t *testing.T) {
			log := captureLog(t)
			root := t.TempDir()
			for i, markers := range tc.dirs {
				writeListFixture(t, root, dirAt(i+1), "shop", "orders.parquet")
				for _, m := range markers {
					writeListFixture(t, root, dirAt(i+1), m)
				}
			}
			checkWritersCase(t, tc, root, log)
		})
	}
}

func TestSnapshotWriters_s3(t *testing.T) {
	for _, tc := range writersCases() {
		t.Run(tc.name, func(t *testing.T) {
			log := captureLog(t)
			var keys []string
			for i, markers := range tc.dirs {
				keys = append(keys, snapshotKeys(dirAt(i+1), []string{"shop/orders"}, markers...)...)
			}
			f := &fakeS3Snapshots{keys: keys}
			stubS3Snapshots(t, f)
			checkWritersCase(t, tc, "s3://b/base/", log)
			// Both spellings of the source are one location.
			if got := SnapshotWritersSeen("s3://b/base"); !slices.Equal(got, tc.want) {
				t.Fatalf("writers without the trailing slash = %q, want %q", got, tc.want)
			}
		})
	}
}

// The signature costs no request of its own: a location of signed snapshots
// is read with the same requests as one of unsigned snapshots.
func TestSnapshotWriters_s3AddsNoRequest(t *testing.T) {
	requests := func(markers ...string) (int, []string) {
		var keys []string
		for i := 1; i <= 5; i++ {
			keys = append(keys, snapshotKeys(dirAt(i), []string{"shop/orders"}, markers...)...)
		}
		f := &fakeS3Snapshots{keys: keys}
		stubS3Snapshots(t, f)
		if _, err := ListBaselines(context.Background(), "s3://b/base"); err != nil {
			t.Fatal(err)
		}
		_ = SnapshotWritersSeen("s3://b/base")
		return f.dirCalls, f.asked()
	}
	captureLog(t)
	plainDirs, plainReads := requests("_SUCCESS")
	signedDirs, signedReads := requests("_SUCCESS", "_WRITER."+writerA)
	if plainDirs != signedDirs || !slices.Equal(plainReads, signedReads) {
		t.Fatalf("signed snapshots cost %d directory listings and reads %q; unsigned cost %d and %q",
			signedDirs, signedReads, plainDirs, plainReads)
	}
	if plainDirs != 1 || len(plainReads) != 5 {
		t.Fatalf("the fixture costs %d directory listings and %d reads, want 1 and 5", plainDirs, len(plainReads))
	}
}

// A writer whose snapshots are gone stops being reported.
func TestSnapshotWriters_forgetsAWriterWhoseSnapshotsAreGone(t *testing.T) {
	t.Run("local", func(t *testing.T) {
		captureLog(t)
		root := t.TempDir()
		writeListFixture(t, root, dirAt(1), "shop", "orders.parquet")
		writeListFixture(t, root, dirAt(1), "_WRITER."+writerA)
		writeListFixture(t, root, dirAt(2), "shop", "orders.parquet")
		writeListFixture(t, root, dirAt(2), "_WRITER."+writerB)
		if _, err := ListBaselines(context.Background(), root); err != nil {
			t.Fatal(err)
		}
		if got := SnapshotWritersSeen(root); len(got) != 2 {
			t.Fatalf("writers = %q, want two", got)
		}
		if err := os.RemoveAll(filepath.Join(root, dirAt(2))); err != nil {
			t.Fatal(err)
		}
		if _, err := ListBaselines(context.Background(), root); err != nil {
			t.Fatal(err)
		}
		if got := SnapshotWritersSeen(root); !slices.Equal(got, []string{writerA}) {
			t.Fatalf("writers = %q, want only %s", got, writerA)
		}
	})
	t.Run("s3", func(t *testing.T) {
		captureLog(t)
		var keys []string
		keys = append(keys, snapshotKeys(dirAt(1), []string{"shop/orders"}, "_SUCCESS", "_WRITER."+writerA)...)
		keys = append(keys, snapshotKeys(dirAt(2), []string{"shop/orders"}, "_SUCCESS", "_WRITER."+writerB)...)
		f := &fakeS3Snapshots{keys: keys}
		stubS3Snapshots(t, f)
		if _, err := ListBaselines(context.Background(), "s3://b/base"); err != nil {
			t.Fatal(err)
		}
		if got := SnapshotWritersSeen("s3://b/base"); len(got) != 2 {
			t.Fatalf("writers = %q, want two", got)
		}
		f.remove(dirAt(2))
		InvalidateS3Inventory("s3://b/base")
		if _, err := ListBaselines(context.Background(), "s3://b/base"); err != nil {
			t.Fatal(err)
		}
		if got := SnapshotWritersSeen("s3://b/base"); !slices.Equal(got, []string{writerA}) {
			t.Fatalf("writers = %q, want only %s", got, writerA)
		}
	})
}

// A source never listed answers nothing, and asking creates no inventory.
func TestSnapshotWritersSeen_neverListed(t *testing.T) {
	captureLog(t)
	resetS3Inventories()
	t.Cleanup(resetS3Inventories)
	for _, src := range []string{"s3://b/never", t.TempDir(), ""} {
		if got := SnapshotWritersSeen(src); len(got) != 0 {
			t.Fatalf("SnapshotWritersSeen(%q) = %q", src, got)
		}
	}
	s3InventoriesMu.Lock()
	n := len(s3Inventories)
	s3InventoriesMu.Unlock()
	if n != 0 {
		t.Fatalf("asking created %d inventories", n)
	}
}

// A signed snapshot is found and read like an unsigned one.
func TestSignedSnapshotIsFoundLikeBefore(t *testing.T) {
	captureLog(t)
	root := t.TempDir()
	writeListFixture(t, root, dirAt(1), "shop", "orders.parquet")
	writeListFixture(t, root, dirAt(1), "_SUCCESS")
	plainPath, plainAt, _, err := FindBaseline(context.Background(), root, "shop", "orders", mustDirTime(t, dirAt(3)))
	if err != nil {
		t.Fatal(err)
	}
	writeListFixture(t, root, dirAt(1), "_WRITER."+writerA)
	path, at, stale, err := FindBaseline(context.Background(), root, "shop", "orders", mustDirTime(t, dirAt(3)))
	if err != nil {
		t.Fatal(err)
	}
	if path != plainPath || !at.Equal(plainAt) || stale.Stale() {
		t.Fatalf("signed: %s at %s (stale %v); unsigned: %s at %s", path, at, stale.Stale(), plainPath, plainAt)
	}
	at2, tables, err := NewestSnapshot(context.Background(), root)
	if err != nil || !at2.Equal(plainAt) || !slices.Equal(tables, []string{"shop.orders"}) {
		t.Fatalf("NewestSnapshot = %s, %q, %v", at2, tables, err)
	}
}

func mustDirTime(t *testing.T, name string) time.Time {
	t.Helper()
	at, ok := parseDirTimestamp(name)
	if !ok {
		t.Fatalf("%q is not a snapshot directory name", name)
	}
	return at
}

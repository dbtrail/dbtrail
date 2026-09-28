package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// findLate is an `at` after every snapshot of every fixture here.
var findLate = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

// requestFixture is a prefix of n snapshots, five tables each with their
// delta pairs: 18 objects a snapshot.
func requestFixture(n int) []string {
	var keys []string
	for i := range n {
		keys = append(keys, snapshotKeys(findDay(i), []string{"shop/orders", "shop/items", "shop/users", "crm/leads", "crm/notes"}, "_SUCCESS")...)
	}
	return keys
}

// The lookup of a table that is in the newest snapshot costs the directory
// listing and one directory read, whatever the prefix holds, and nothing
// the second time.
func TestFindBaselineS3_requestsDoNotFollowThePrefix(t *testing.T) {
	for _, n := range []int{1, 100, 2000} {
		t.Run(fmt.Sprintf("%d snapshots", n), func(t *testing.T) {
			captureLog(t)
			f := &fakeS3Snapshots{keys: requestFixture(n)}
			stubS3Snapshots(t, f)
			path, _, stale, err := findBaselineS3(context.Background(), findRoot, "shop", "orders", findLate)
			if err != nil {
				t.Fatal(err)
			}
			if want := findRoot + "/" + findDay(n-1) + "/shop/orders.parquet"; path != want || stale.Stale() {
				t.Fatalf("path = %q stale = %+v, want %q and not stale", path, stale, want)
			}
			if got, want := f.dirsRead(), prefixesOf(findDay(n-1)); f.dirCalls != 1 || !equalStrings(got, want) {
				t.Fatalf("cold: %d directory listings and reads %q, want 1 and %q", f.dirCalls, got, want)
			}
			if _, _, _, err := findBaselineS3(context.Background(), findRoot, "crm", "leads", findLate); err != nil {
				t.Fatal(err)
			}
			if got := f.dirsRead(); f.dirCalls != 1 || len(got) != 0 {
				t.Fatalf("warm, another table: %d directory listings and reads %q, want 1 and none", f.dirCalls, got)
			}
		})
	}
}

func equalStrings(a, b []string) bool {
	return strings.Join(a, "\n") == strings.Join(b, "\n")
}

// A table that is further back costs the snapshots between it and `at`, not
// the ones older than it.
func TestFindBaselineS3_stopsAtTheSnapshotThatHoldsTheTable(t *testing.T) {
	captureLog(t)
	keys := requestFixture(200)
	keys = append(keys, findDay(190)+"/shop/rare.parquet")
	f := &fakeS3Snapshots{keys: keys}
	stubS3Snapshots(t, f)
	path, snap, stale, err := findBaselineS3(context.Background(), findRoot, "shop", "rare", findLate)
	if err != nil {
		t.Fatal(err)
	}
	if want := findRoot + "/" + findDay(190) + "/shop/rare.parquet"; path != want || !snap.Equal(findTime(t, findDay(190))) {
		t.Fatalf("path = %q at %s, want %q", path, snap, want)
	}
	if !stale.NewestSnapshot.Equal(findTime(t, findDay(199))) {
		t.Fatalf("stale = %+v, want the newest snapshot, %s", stale, findDay(199))
	}
	// Rounds of 1, 4 and 16 directories: 199 down to 179.
	got := f.dirsRead()
	if len(got) != 21 || got[0] != findDay(179)+"/" || got[20] != findDay(199)+"/" {
		t.Fatalf("read %d directories, %q to %q; want the 21 from %s to %s", len(got), got[0], got[len(got)-1], findDay(179), findDay(199))
	}
}

// The same count over HTTP, where a listing is cut in pages of 1,000: what
// the lookup costs in requests to the store, against what the three globs
// cost on the same prefix. The globs are measured when this machine can load
// DuckDB's httpfs, and the numbers are logged either way.
func TestFindBaselineS3_requestsOverHTTP(t *testing.T) {
	for _, n := range []int{1, 100, 2000} {
		t.Run(fmt.Sprintf("%d snapshots", n), func(t *testing.T) {
			captureLog(t)
			keys := requestFixture(n)
			store := newHTTPS3(t, "base/", keys)
			source := "s3://" + httpS3Bucket + "/base"
			want := source + "/" + findDay(n-1) + "/shop/orders.parquet"

			path, _, stale, err := FindBaselineUnbounded(context.Background(), source, "shop", "orders", findLate)
			if err != nil {
				t.Fatal(err)
			}
			if path != want || stale.Stale() {
				t.Fatalf("path = %q stale = %+v, want %q and not stale", path, stale, want)
			}
			cold := store.asked()
			// The directory listing, a request per 1,000 directories, and
			// one read of a directory of 18 objects.
			if wantCold := (n+999)/1000 + 1; cold != wantCold {
				t.Fatalf("cold lookup: %d requests, want %d", cold, wantCold)
			}
			if _, _, _, err := FindBaselineUnbounded(context.Background(), source, "crm", "notes", findLate); err != nil {
				t.Fatal(err)
			}
			warm := store.asked()
			if warm != 0 {
				t.Fatalf("warm lookup: %d requests, want none", warm)
			}

			globs := "not measured: " + globRequests(t, store, source, want)
			if n, ok := strings.CutPrefix(globs, "not measured: measured "); ok {
				globs = n
			}
			t.Logf("REQUESTS snapshots=%d objects=%d globs=%s listing_cold=%d listing_warm=%d", n, len(keys), globs, cold, warm)
		})
	}
}

// FindBaselineUnbounded is findBaselineS3 under the name the test reads
// better with: the lookup itself, without the delta-chain read FindBaseline
// makes on the file it found (the fake store holds no file contents).
var FindBaselineUnbounded = findBaselineS3

// globRequests runs the three globs against the store and returns
// "measured <n>", or why it could not.
func globRequests(t *testing.T, store *httpS3, source, want string) string {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return err.Error()
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		return "httpfs does not load here"
	}
	store.asked()
	path, _, _, err := findBaselineS3Globs(ctx, source, "shop", "orders", findLate)
	if err != nil {
		return "the globs failed against this store: " + firstLine(err)
	}
	if path != want {
		t.Fatalf("the globs picked %q, the listing %q", path, want)
	}
	return fmt.Sprintf("measured %d", store.asked())
}

func firstLine(err error) string {
	s, _, _ := strings.Cut(err.Error(), "\n")
	return s
}

// A listing S3 cut in pages is read to its last page: the table of the
// 1,100th directory, and the 1,100th object of a directory.
func TestFindBaselineS3_readsEveryPage(t *testing.T) {
	t.Run("directories", func(t *testing.T) {
		captureLog(t)
		// Byte order is oldest first, so the newest 100 snapshots are on
		// the second page of the directory listing.
		store := newHTTPS3(t, "base/", requestFixture(1100))
		source := "s3://" + httpS3Bucket + "/base/"
		path, snap, _, err := findBaselineS3(context.Background(), source, "shop", "orders", findLate)
		if err != nil {
			t.Fatal(err)
		}
		if want := findDay(1099); !snap.Equal(findTime(t, want)) || !strings.Contains(path, want) {
			t.Fatalf("picked %q (%s), want the snapshot %s, which is on the second page", path, snap, want)
		}
		if got := store.asked(); got != 3 {
			t.Fatalf("%d requests, want 3: two pages of directories and one directory", got)
		}
	})
	t.Run("objects of a directory", func(t *testing.T) {
		captureLog(t)
		var tables []string
		for i := range 1100 {
			tables = append(tables, fmt.Sprintf("shop/t%04d", i))
		}
		keys := snapshotKeys(findDay(1), []string{"shop/t1099"}, "_SUCCESS")
		// 3,300 objects under shop/ and the marker after them in byte
		// order: a reader of the first page sees neither t1099 nor that the
		// snapshot is incomplete.
		keys = append(keys, snapshotKeys(findDay(2), tables, "_INCOMPLETE")...)
		keys = append(keys, snapshotKeys(findDay(3), tables, "zz/last")...)
		keys = append(keys, findDay(3)+"/zz/last.parquet")
		store := newHTTPS3(t, "base/", keys)
		source := "s3://" + httpS3Bucket + "/base"

		path, _, _, err := findBaselineS3(context.Background(), source, "zz", "last", findLate)
		if err != nil {
			t.Fatal(err)
		}
		if want := source + "/" + findDay(3) + "/zz/last.parquet"; path != want {
			t.Fatalf("path = %q, want %q, which is on the last page of its directory", path, want)
		}
		store.asked()
		resetS3Inventories()
		at := findTime(t, findDay(2))
		path, snap, _, err := findBaselineS3(context.Background(), source, "shop", "t1099", at)
		if err != nil {
			t.Fatal(err)
		}
		if !snap.Equal(findTime(t, findDay(1))) {
			t.Fatalf("picked %q (%s): snapshot 2 is incomplete, and its marker is on the last page of its listing", path, snap)
		}
	})
}

// A listing that fails is an error that names the source. It is never "no
// baseline", which callers answer by reading the binary log alone or another
// location, and never an older snapshot.
func TestFindBaselineS3_aFailedListingIsNotAnAnswer(t *testing.T) {
	keys := cat(
		snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"),
		snapshotKeys(findDay(2), []string{"shop/orders"}, "_SUCCESS"),
		snapshotKeys(findDay(3), []string{"shop/users"}, "_SUCCESS"),
	)
	// tryAgain is the part of the error that says what to do.
	const tryAgain = "no older snapshot was used in its place. Check that the store answers and that these credentials can list that location, then try again"
	refused := func(t *testing.T, path string, err error, names ...string) {
		t.Helper()
		if err == nil {
			t.Fatalf("the lookup answered %q", path)
		}
		if errors.Is(err, ErrNoBaseline) {
			t.Fatalf("a failed listing reads as no baseline: %v", err)
		}
		if path != "" {
			t.Fatalf("an error came with a path: %q", path)
		}
		if !strings.Contains(err.Error(), `"`+findRoot+`"`) || !strings.Contains(err.Error(), "shop.orders") {
			t.Fatalf("the error does not name the source and the table: %v", err)
		}
		t.Logf("ERROR TEXT %s", err)
		for _, want := range append(names, tryAgain) {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("the error does not say %q: %v", want, err)
			}
		}
	}
	t.Run("the directory listing fails", func(t *testing.T) {
		captureLog(t)
		f := &fakeS3Snapshots{keys: keys, err: errors.New("AccessDenied")}
		stubS3Snapshots(t, f)
		path, _, _, err := findBaselineS3(context.Background(), findRoot, "shop", "orders", findLate)
		refused(t, path, err, `could not list the snapshot folders under "`+findRoot+`"`)
	})
	t.Run("every directory read fails", func(t *testing.T) {
		captureLog(t)
		f := &fakeS3Snapshots{keys: keys, infoErr: errors.New("SlowDown")}
		stubS3Snapshots(t, f)
		path, _, _, err := findBaselineS3(context.Background(), findRoot, "shop", "orders", findLate)
		refused(t, path, err, "could not read the snapshot folder "+findRoot+"/"+findDay(3))
	})
	t.Run("the read of a newer snapshot fails", func(t *testing.T) {
		captureLog(t)
		// Snapshot 3 cannot be read. Snapshots 2 and 1 hold the table, and
		// neither may be used: snapshot 3 may hold it too.
		f := &fakeS3Snapshots{keys: keys, failPrefix: findDay(3) + "/"}
		stubS3Snapshots(t, f)
		path, _, _, err := findBaselineS3(context.Background(), findRoot, "shop", "orders", findLate)
		refused(t, path, err, "could not read the snapshot folder "+findRoot+"/"+findDay(3))
		if got := f.dirsRead(); len(got) != 1 {
			t.Fatalf("read %q after the failure, want the failed read alone", got)
		}
	})
	t.Run("the read of a snapshot in a later round fails", func(t *testing.T) {
		captureLog(t)
		f := &fakeS3Snapshots{keys: keys, failPrefix: findDay(2) + "/"}
		stubS3Snapshots(t, f)
		path, _, _, err := findBaselineS3(context.Background(), findRoot, "shop", "orders", findLate)
		refused(t, path, err, "could not read the snapshot folder "+findRoot+"/"+findDay(2))
	})
	t.Run("the caller gave up", func(t *testing.T) {
		captureLog(t)
		store := newHTTPS3(t, "base/", keys)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		path, _, _, err := findBaselineS3(ctx, "s3://"+httpS3Bucket+"/base", "shop", "orders", findLate)
		if err == nil || errors.Is(err, ErrNoBaseline) || path != "" {
			t.Fatalf("path = %q err = %v, want an error that is not no-baseline", path, err)
		}
		if got := store.asked(); got != 0 {
			t.Fatalf("%d requests went out after the caller gave up", got)
		}
	})
	t.Run("the listing dies between two pages", func(t *testing.T) {
		captureLog(t)
		store := newHTTPS3(t, "base/", requestFixture(1100))
		store.cutAfter = 1
		source := "s3://" + httpS3Bucket + "/base"
		path, _, _, err := findBaselineS3(context.Background(), source, "shop", "orders", findLate)
		if err == nil || errors.Is(err, ErrNoBaseline) || path != "" {
			t.Fatalf("path = %q err = %v: the first page of directories alone must not be the inventory", path, err)
		}
		if !strings.Contains(err.Error(), `"`+source+`"`) {
			t.Fatalf("the error does not name the source: %v", err)
		}
	})
	t.Run("the store is a real client with no store behind it", func(t *testing.T) {
		captureLog(t)
		store := newHTTPS3(t, "base/", keys)
		store.srv.Close()
		path, _, _, err := findBaselineS3(context.Background(), "s3://"+httpS3Bucket+"/base", "shop", "orders", findLate)
		if err == nil || errors.Is(err, ErrNoBaseline) || path != "" {
			t.Fatalf("path = %q err = %v, want an error that is not no-baseline", path, err)
		}
	})
}

// emptyDirs is the fake store answering some directories with no keys, for
// as long as they are in empty.
type emptyDirs struct {
	*fakeS3Snapshots
	empty map[string]bool
}

func (e *emptyDirs) ListInfoFrom(ctx context.Context, prefix, startAfter string) ([]storage.ObjectInfo, error) {
	if e.empty[prefix] {
		e.mu.Lock()
		e.prefixes = append(e.prefixes, prefix)
		e.mu.Unlock()
		return nil, nil
	}
	return e.fakeS3Snapshots.ListInfoFrom(ctx, prefix, startAfter)
}

// A directory the listing names and whose own listing comes back empty is
// not a snapshot without the table: it is a snapshot that could not be read.
func TestFindBaselineS3_aDirectoryThatComesBackEmpty(t *testing.T) {
	keys := cat(
		snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"),
		snapshotKeys(findDay(2), []string{"shop/orders"}, "_SUCCESS"),
	)
	t.Run("once", func(t *testing.T) {
		captureLog(t)
		f := &fakeS3Snapshots{keys: keys, emptyOnce: findDay(2) + "/"}
		stubS3Snapshots(t, f)
		path, snap, stale, err := findBaselineS3(context.Background(), findRoot, "shop", "orders", findLate)
		if err != nil {
			t.Fatal(err)
		}
		if !snap.Equal(findTime(t, findDay(2))) || stale.Stale() {
			t.Fatalf("picked %q (%s) stale=%+v, want snapshot 2, read again", path, snap, stale)
		}
		if f.dirCalls != 2 {
			t.Fatalf("%d directory listings, want 2: the retry lists again", f.dirCalls)
		}
	})
	t.Run("every time", func(t *testing.T) {
		captureLog(t)
		f := &fakeS3Snapshots{keys: keys}
		resetS3Inventories()
		prev := newS3SnapshotLister
		t.Cleanup(func() { newS3SnapshotLister = prev; resetS3Inventories() })
		newS3SnapshotLister = func(context.Context, string) (s3SnapshotLister, error) {
			return &emptyDirs{f, map[string]bool{findDay(2) + "/": true}}, nil
		}
		path, _, _, err := findBaselineS3(context.Background(), findRoot, "shop", "orders", findLate)
		if err == nil || errors.Is(err, ErrNoBaseline) || path != "" {
			t.Fatalf("path = %q err = %v: snapshot 1 must not be used while snapshot 2 cannot be read", path, err)
		}
		for _, want := range []string{`"` + findRoot + `"`, "could not read the snapshot folder " + findRoot + "/" + findDay(2), "shop.orders", "then try again"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("the error does not name %s: %v", want, err)
			}
		}
	})
	t.Run("removed since it was listed", func(t *testing.T) {
		captureLog(t)
		f := &fakeS3Snapshots{keys: keys}
		stubS3Snapshots(t, f)
		// The directory listing is taken and kept; snapshot 2 is then
		// removed (a lifecycle rule), unread.
		if _, _, _, err := findBaselineS3(context.Background(), findRoot, "shop", "orders", findTime(t, findDay(1))); err != nil {
			t.Fatal(err)
		}
		f.remove(findDay(2))
		path, snap, stale, err := findBaselineS3(context.Background(), findRoot, "shop", "orders", findLate)
		if err != nil {
			t.Fatal(err)
		}
		if !snap.Equal(findTime(t, findDay(1))) || stale.Stale() {
			t.Fatalf("picked %q (%s) stale=%+v, want snapshot 1: snapshot 2 is gone from the store", path, snap, stale)
		}
		if f.dirCalls != 2 {
			t.Fatalf("%d directory listings, want 2: the first, and the one that found snapshot 2 gone", f.dirCalls)
		}
	})
}

// A snapshot uploaded by this process is looked up at once: the upload
// invalidates the inventory the lookup reads.
func TestFindBaselineS3_seesASnapshotJustUploaded(t *testing.T) {
	captureLog(t)
	f := &fakeS3Snapshots{keys: snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS")}
	stubS3Snapshots(t, f)
	if _, _, _, err := findBaselineS3(context.Background(), findRoot, "shop", "orders", findLate); err != nil {
		t.Fatal(err)
	}
	f.add(snapshotKeys(findDay(2), []string{"shop/orders"}, "_SUCCESS")...)
	InvalidateS3Inventory(findRoot + "/" + findDay(2))
	_, snap, _, err := findBaselineS3(context.Background(), findRoot, "shop", "orders", findLate)
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Equal(findTime(t, findDay(2))) {
		t.Fatalf("picked %s, want the snapshot just uploaded", snap)
	}
}

// An incomplete snapshot is read again by the next lookup, and used once
// its _SUCCESS lands.
func TestFindBaselineS3_usesASnapshotOnceItCompletes(t *testing.T) {
	captureLog(t)
	f := &fakeS3Snapshots{keys: cat(
		snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"),
		snapshotKeys(findDay(2), []string{"shop/orders"}, "_INCOMPLETE"),
	)}
	stubS3Snapshots(t, f)
	_, snap, _, err := findBaselineS3(context.Background(), findRoot, "shop", "orders", findLate)
	if err != nil || !snap.Equal(findTime(t, findDay(1))) {
		t.Fatalf("picked %s (%v), want snapshot 1 while snapshot 2 is incomplete", snap, err)
	}
	f.add(findDay(2) + "/_SUCCESS")
	_, snap, _, err = findBaselineS3(context.Background(), findRoot, "shop", "orders", findLate)
	if err != nil || !snap.Equal(findTime(t, findDay(2))) {
		t.Fatalf("picked %s (%v), want snapshot 2 now that it is complete", snap, err)
	}
}

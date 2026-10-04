package views

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func listedFixture(t *testing.T, root, stamp string) string {
	t.Helper()
	return Generate(Input{
		GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
		BaselineSource: root, BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Follow: FollowNewest,
		Baselines: []BaselineTable{{Schema: "shop", Table: "orders",
			Path: filepath.Join(root, stamp, "shop", "orders.parquet"), Rel: "shop/orders.parquet", SchemaKnown: true}},
	})
}

// The views read through the session's ONE listing: a chain file written into
// the listed snapshot afterwards is not seen until the listing is taken again.
// That is the proof the listing is used at all; a view that still globbed
// would see the file at once, and every other test here would pass the same.
// (A published snapshot never gains a file, so this staleness is not one a
// reader meets.)
func TestListedView_readsThroughTheSessionListing_2064(t *testing.T) {
	root, stamp := t.TempDir(), "2026-04-30T03-00-00Z"
	base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
	sqlText := listedFixture(t, root, stamp)
	db := execViews(t, sqlText)
	count := func() int {
		t.Helper()
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM shop.orders`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := count(); n != 3 {
		t.Fatalf("before: %d rows, want 3", n)
	}
	writeDeltaPair(t, base, 0, []int64{0}, nil)
	if n := count(); n != 3 {
		t.Fatalf("a file written after the listing was read at once (%d rows): the view is not using the listing", n)
	}
	relist(t, db, sqlText)
	if n := count(); n != 2 {
		t.Fatalf("after listing again: %d rows, want 2", n)
	}
}

// A listing taken for another snapshot is never used: re-running the first
// statement alone (what the header used to say picks up a refresh), or naming
// the snapshot by hand, moves newestVar and leaves the listing behind. The
// views then ask the store, and read the snapshot newestVar names.
func TestListedView_staleListingFallsBackToTheStore_2064(t *testing.T) {
	root := t.TempDir()
	writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "old")
	sqlText := listedFixture(t, root, "2026-04-30T03-00-00Z")
	db := execViews(t, sqlText)
	writeSnapshot(t, root, "2026-05-30T03-00-00Z", true, "new-a", "new-b")

	for _, c := range []struct {
		name, stmt, first string
		n                 int
	}{
		// In this order: the first leaves newestVar on the old snapshot.
		{"no listing left", "SET VARIABLE " + filesVar + " = NULL", "old", 1},
		{"named by hand", "SET VARIABLE " + newestVar + " = " + sqlString(filepath.Join(root, "2026-05-30T03-00-00Z")+string(os.PathSeparator)), "new-a", 2},
	} {
		if _, err := db.Exec(c.stmt); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		var n int
		var first string
		if err := db.QueryRow(`SELECT count(*), min("status") FROM shop.orders`).Scan(&n, &first); err != nil {
			t.Fatalf("%s: the view failed: %v", c.name, err)
		}
		if n != c.n || first != c.first {
			t.Errorf("%s: read %d row(s) starting %q; want %d starting %q", c.name, n, first, c.n, c.first)
		}
	}
}

// TestGenerate_newestListedGolden pins the one-listing render as bytes, with
// and without the pointer. views.newest.golden.sql stays the globbed render,
// which a file reading S3 archives keeps.
func TestGenerate_newestListedGolden(t *testing.T) {
	in := newestInput()
	in.OmitEvents = true
	in.NewestPointer = "2026-04-30T03-00-00Z"
	got := Generate(in)
	if !strings.Contains(got, "SET VARIABLE "+filesVar) {
		t.Fatal("fixture does not render the one-listing shape")
	}
	golden := filepath.Join("testdata", "views.newest.listed.golden.sql")
	if *update {
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden (run with -update to create it): %v", err)
	}
	if got != string(want) {
		t.Errorf("generated SQL differs from %s.\n--- got ---\n%s\n--- want ---\n%s", golden, got, want)
	}
}

// The GLOB operator lets `*` cross a "/", which the store's glob does not. A
// file in a subdirectory named like the table's chain must stay out of the
// list: the posdel read takes its columns from the list's first entry, and a
// stray first entry with no "pos" column would make the dead rows read as
// none, with no error.
func TestListedView_ignoresAChainShapedSubdirectory_2064(t *testing.T) {
	root, stamp := t.TempDir(), "2026-04-30T03-00-00Z"
	base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
	writeDeltaPair(t, base, 1, []int64{0}, nil)
	// shop/orders.000000x/y.posdel sorts before orders.000001.posdel.
	stray := filepath.Join(filepath.Dir(base), "orders.000000x")
	if err := os.MkdirAll(stray, 0o755); err != nil {
		t.Fatal(err)
	}
	src, err := os.ReadFile(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stray, "y.posdel"), src, 0o644); err != nil {
		t.Fatal(err)
	}
	in := Input{
		GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
		BaselineSource: root, BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Follow:    FollowNewest,
		Baselines: []BaselineTable{{Schema: "shop", Table: "orders", Path: base, Rel: "shop/orders.parquet", SchemaKnown: true, Delta: true}},
	}
	db := execViews(t, Generate(in))
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM shop.orders`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("%d rows, want 2: the chain's dead row was not applied", n)
	}
}

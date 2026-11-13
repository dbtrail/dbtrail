package baseline

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// A reader of one table at a past moment pins the snapshot its lookup
// returned (#2691). The two names come from the path, whatever its spelling.
func TestTableFileSnapshot_namesTheFolderAndTheSnapshot(t *testing.T) {
	for _, c := range []struct {
		path, root, name string
		ok               bool
	}{
		{"/data/b/2026-06-01T00-00-00Z/shop/orders.parquet", "/data/b", "2026-06-01T00-00-00Z", true},
		// A doubled or trailing separator does not move the snapshot.
		{"/data/b//2026-06-01T00-00-00Z/shop//orders.parquet", "/data/b", "2026-06-01T00-00-00Z", true},
		{"rel/2026-06-01T00-00-00Z/shop/orders.parquet", "rel", "2026-06-01T00-00-00Z", true},
		// A folder at the root of the disk.
		{"/2026-06-01T00-00-00Z/shop/orders.parquet", "/", "2026-06-01T00-00-00Z", true},
		// A table named like a snapshot is still a table.
		{"/b/s/shop/2026-06-01T00-00-00Z.parquet", "/b", "s", true},
		// Not a local table file in a snapshot: nothing to pin.
		{"", "", "", false},
		{"s3://bkt/b/2026-06-01T00-00-00Z/shop/orders.parquet", "", "", false},
		{"orders.parquet", "", "", false},
		{"shop/orders.parquet", "", "", false},
		{"/orders.parquet", "", "", false},
		{"/shop/orders.parquet", "", "", false},
	} {
		root, name, ok := TableFileSnapshot(c.path)
		if root != c.root || name != c.name || ok != c.ok {
			t.Errorf("TableFileSnapshot(%q) = %q, %q, %v; want %q, %q, %v", c.path, root, name, ok, c.root, c.name, c.ok)
		}
	}
}

// The size a pin's deadline is worked out from: the table's file and its
// delta, not a sibling table whose name starts the same, nothing for what is
// not there.
func TestTableReadBytes_theTableAndItsDelta(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "2026-06-01T00-00-00Z", "shop")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name string, n int) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, n), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	base := filepath.Join(dir, "orders.parquet")
	write("orders.parquet", 1000)
	if got := TableReadBytes(base); got != 1000 {
		t.Fatalf("a table alone: %d bytes, want 1000", got)
	}
	write("orders.000000"+TableDeltaPosdelSuffix, 20)
	write("orders.000000"+TableDeltaUpsertsSuffix, 300)
	write("orders_old.parquet", 50000)
	write("orders_old.000000"+TableDeltaUpsertsSuffix, 7000)
	write("orders_old.000000"+TableDeltaPosdelSuffix, 7000)
	if got := TableReadBytes(base); got != 1320 {
		t.Fatalf("a table and its delta: %d bytes, want 1320 (a sibling table counted?)", got)
	}
	for _, p := range []string{"", "s3://bkt/x/orders.parquet", filepath.Join(dir, "gone.parquet")} {
		if got := TableReadBytes(p); got != 0 {
			t.Errorf("TableReadBytes(%q) = %d, want 0", p, got)
		}
	}
}

// Downloads and reads of a past moment share ONE cap (#2691): counted apart,
// a client refused a download would keep the same snapshot with a read, and
// each kind would hold back its own 2N. A pin taken with PinSnapshot (a SQL
// statement, an Explain) does not count.
func TestPinCounted_readsAndDownloadsShareTheCap(t *testing.T) {
	root := t.TempDir()
	newest := snapName(time.Minute)
	old := []string{snapName(30 * time.Minute), snapName(25 * time.Minute), snapName(20 * time.Minute)}
	kept := map[string]bool{newest: true}
	const max, ceiling = 1, 2

	// A SQL statement on an old snapshot: not counted.
	defer PinSnapshot(root, old[2], "a SQL statement", time.Hour)()
	// A download takes the one place past the count.
	r1, ok := PinDownload(root, old[0], time.Hour, kept, max, ceiling)
	defer r1()
	if !ok {
		t.Fatal("the first download past the count was refused")
	}
	// A read of another old snapshot finds that place taken.
	if r, ok := PinCounted(root, old[1], "a time-travel read", time.Hour, kept, max, ceiling); ok {
		r()
		t.Fatal("a read of a second old snapshot was let in past the cap the download filled: the cap counts downloads only")
	}
	// A read of the snapshot the download pins holds nothing more.
	r2, ok := PinCounted(root, old[0], "a _snapshot query", time.Hour, kept, max, ceiling)
	defer r2()
	if !ok {
		t.Fatal("a read of the snapshot a download already pins was refused")
	}
	// The newest: kept anyway, room under the ceiling.
	r3, ok := PinCounted(root, newest, "a time-travel read", time.Hour, kept, max, ceiling)
	defer r3()
	if !ok {
		t.Fatal("a read of the newest snapshot was refused with room under the ceiling")
	}
	// The ceiling: two different snapshots counted. A download of a third is
	// refused, the read's pin counted like a download's.
	if r, ok := PinDownload(root, snapName(2*time.Minute), time.Hour, map[string]bool{newest: true, snapName(2 * time.Minute): true}, max, ceiling); ok {
		r()
		t.Fatal("a download was let in over the ceiling the reads filled")
	}
	if got, want := Pinned(root), []string{old[0], old[2], newest}; !slices.Equal(got, sorted(want)) {
		t.Fatalf("pinned %v, want %v", got, sorted(want))
	}
}

func sorted(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	return out
}

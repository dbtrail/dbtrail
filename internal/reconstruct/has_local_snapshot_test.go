package reconstruct

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// HasLocalSnapshot answers what the listing would: a complete,
// timestamp-named snapshot with a table file, and nothing less.
func TestHasLocalSnapshot(t *testing.T) {
	write := func(root string, parts ...string) {
		t.Helper()
		p := filepath.Join(append([]string{root}, parts...)...)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	const snap = "2026-04-30T03-00-00Z"
	cases := map[string]struct {
		build func(root string)
		want  bool
	}{
		"empty directory":                                   {func(string) {}, false},
		"a complete snapshot":                               {func(r string) { write(r, snap, "shop", "orders.parquet") }, true},
		"a name that is not a time":                         {func(r string) { write(r, "notes", "shop", "orders.parquet") }, false},
		"a snapshot with no table":                          {func(r string) { write(r, snap, "shop", "README") }, false},
		"a file at the snapshot's top, no schema directory": {func(r string) { write(r, snap, "orders.parquet") }, false},
		"an incomplete snapshot": {func(r string) {
			write(r, snap, "shop", "orders.parquet")
			if err := baseline.WriteIncompleteMarker(filepath.Join(r, snap)); err != nil {
				t.Fatal(err)
			}
		}, false},
		"an incomplete one beside a complete one": {func(r string) {
			write(r, snap, "shop", "orders.parquet")
			if err := baseline.WriteIncompleteMarker(filepath.Join(r, snap)); err != nil {
				t.Fatal(err)
			}
			write(r, "2026-04-29T03-00-00Z", "shop", "orders.parquet")
		}, true},
	}
	for name, c := range cases {
		root := t.TempDir()
		c.build(root)
		if got := HasLocalSnapshot(root); got != c.want {
			t.Errorf("%s: HasLocalSnapshot = %v, want %v", name, got, c.want)
		}
	}
	if HasLocalSnapshot(filepath.Join(t.TempDir(), "missing")) {
		t.Error("a missing directory holds no snapshot")
	}
}

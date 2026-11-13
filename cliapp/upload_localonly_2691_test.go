package cliapp

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// `bintrail upload --source <snapshots root>` sends no entry the folder keeps
// for itself (#2691): an interrupted prune's leftover and the daemon's work
// directories hold Parquet, which the suffix rule alone let through. An
// archive tree (no dot entries) is sent whole, as before.
func TestWalkUploadable_skipsTheFoldersOwnEntries(t *testing.T) {
	root := t.TempDir()
	for _, f := range []string{
		"2026-10-04T12-00-03Z/shop/orders.parquet",
		"2026-10-04T12-00-03Z/_SUCCESS",
		".2026-10-04T11-00-00Z.pruning/shop/orders.parquet",
		".resolved/2026-10-04T12-00-03Z/shop/orders.parquet",
		".compact/shop/orders.parquet",
		".a-dir-added-later/x.parquet",
		".upload-held.2026-10-04T12-00-03Z",
		"bintrail_id=abc/event_date=2026-10-04/event_hour=12/events.parquet",
	} {
		p := filepath.Join(root, f)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	if err := walkUploadable(root, func(p string) error {
		rel, _ := filepath.Rel(root, p)
		got = append(got, rel)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	want := []string{
		"2026-10-04T12-00-03Z/shop/orders.parquet",
		"bintrail_id=abc/event_date=2026-10-04/event_hour=12/events.parquet",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("would upload %v, want %v", got, want)
	}
}

// Any other tree is sent as before, dot-named directories included: they
// are the operator's data there, not a snapshots folder's own entries.
func TestWalkUploadable_otherTreesAreSentWhole(t *testing.T) {
	root := t.TempDir()
	for _, f := range []string{
		".archive/bintrail_id=abc/event_date=2026-10-04/event_hour=12/events.parquet",
		"bintrail_id=abc/event_date=2026-10-04/event_hour=13/events.parquet",
	} {
		p := filepath.Join(root, f)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	if err := walkUploadable(root, func(p string) error {
		rel, _ := filepath.Rel(root, p)
		got = append(got, rel)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("would upload %v, want both archive files, the dot-named one included", got)
	}
}

package cliapp

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// A PostgreSQL table's delta is uploaded with it, and nothing else that looks
// like one is: a table file sent without its delta is the table as it was at
// an earlier snapshot update (#2757).
func TestUploadable_pgDelta(t *testing.T) {
	const dir = "/b/2026-05-01T12-00-00Z/public/"
	for name, want := range map[string]bool{
		"orders.000000.pgdead":         true,
		"orders.000000.pgnew":          true,
		"orders.000017.pgnew":          true,
		"or.ders.000000.pgdead":        true, // a dot in the table's name
		"orders.000000.pgnew.tmp-4821": false,
		"orders.000000.pgdead.tmp-4821":false,
		"orders.pgnew":                 false,
		"orders.00000.pgnew":           false,
		"orders.0000000.pgnew":         false,
		"orders.00000a.pgdead":         false,
		"orders.000000.PGNEW":          false,
		".000000.pgnew":                false,
		"orders.000000.pgnewer":        false,
		"orders.000000.pgnew.bak":      false,
		"orders.000000.pgnew.parquet":  true, // a table file by its name
		"orders.parquet":               true,
		"orders.000000.upserts":        true,
		"orders.000000-000003.upserts": true,
		"orders.000000.posdel":         true,
		"orders.000000.posdel.tmp":     false,
		"_SUCCESS":                     false,
	} {
		if got := uploadable(dir + name); got != want {
			t.Errorf("uploadable(%q) = %v, want %v", name, got, want)
		}
	}
}

// The walk sends a table's delta before the table's file: in the store a
// table file never stands for a moment without the delta that completes it.
// And what a write that was cut short left behind stays.
func TestWalkUploadable_pgDelta(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "2026-05-01T12-00-00Z", "public")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"orders.parquet", "orders.000000.pgdead", "orders.000000.pgnew",
		"orders.000000.pgnew.tmp-77", "items.parquet",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	if err := walkUploadable(root, func(path string) error {
		got = append(got, filepath.Base(path))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{"items.parquet", "orders.000000.pgdead", "orders.000000.pgnew", "orders.parquet"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sent %v, want %v", got, want)
	}
}

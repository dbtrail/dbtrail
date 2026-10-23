package baselineintegrity

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// #2496: the key index beside a table's file is NOT a manifested file. The
// manifest certifies the snapshot's data; the index is derived from the base,
// carries its own CRCs, is tied to its base by the base's size and footer,
// and is rebuilt rather than certified. Listing it would also make every
// copy of a snapshot that selects data files by suffix (upload, mirror)
// answer "a listed file is missing".
func TestManifest_leavesTheKeyIndexOut_2496(t *testing.T) {
	for _, name := range []string{"t.keyidx", "t.keyidx.tmp", "orders.keyidx"} {
		if manifested(name) {
			t.Errorf("%s is manifested", name)
		}
	}
	for _, name := range []string{"t.parquet", "t.000001.posdel", "t.000001.upserts"} {
		if !manifested(name) {
			t.Errorf("%s is not manifested", name)
		}
	}
	snap := t.TempDir()
	if err := os.MkdirAll(filepath.Join(snap, "s"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"t.parquet", "t.keyidx", "t.keyidx.tmp"} {
		if err := os.WriteFile(filepath.Join(snap, "s", name), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := WriteManifest(snap); err != nil {
		t.Fatal(err)
	}
	m, ok, err := LoadManifest(snap)
	if err != nil || !ok {
		t.Fatalf("manifest: ok=%v err=%v", ok, err)
	}
	var files []string
	for f := range m.Files {
		files = append(files, f)
	}
	if !reflect.DeepEqual(files, []string{"s/t.parquet"}) {
		t.Fatalf("manifest lists %v, want the table's file alone", files)
	}
}

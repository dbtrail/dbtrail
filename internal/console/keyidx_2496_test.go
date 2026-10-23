package console

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// #2496: the snapshot file listing shows the key index beside a table (it
// is a file of the snapshot, with a size) and not a build's .tmp.
func TestSnapshotFiles_leaveTheKeyIndexTempOut_2496(t *testing.T) {
	root := t.TempDir()
	snap := "2026-10-22T00-00-00Z"
	if err := os.MkdirAll(filepath.Join(root, snap, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"shop/t.parquet", "shop/t.keyidx", "shop/t.keyidx.tmp", "_SUCCESS"} {
		if err := os.WriteFile(filepath.Join(root, snap, f), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ss := &snapshotSource{localRoot: root}
	files, err := ss.files(context.Background(), snap)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range files {
		got = append(got, f.RelPath)
	}
	slices.Sort(got)
	if want := []string{snap + "/_SUCCESS", snap + "/shop/t.keyidx", snap + "/shop/t.parquet"}; !slices.Equal(got, want) {
		t.Fatalf("listed %v, want %v", got, want)
	}
}

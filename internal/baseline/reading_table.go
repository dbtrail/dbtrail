package baseline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
)

// TableFileSnapshot returns the folder and the snapshot that hold the table
// file at path, laid out as <root>/<snapshot>/<schema>/<table>.parquet (what
// a snapshot lookup returns): the two names a pin takes (#2691). ok is false
// for an empty path, an s3:// one (the local cleanup does not remove those)
// and one too short to have that layout.
func TableFileSnapshot(path string) (root, name string, ok bool) {
	if path == "" || strings.HasPrefix(path, "s3://") {
		return "", "", false
	}
	snap := filepath.Dir(filepath.Dir(filepath.Clean(path)))
	name = filepath.Base(snap)
	if name == "." || name == string(filepath.Separator) || snap == filepath.Dir(snap) {
		return "", "", false
	}
	return filepath.Dir(snap), name, true
}

// TableReadBytes is the size of the table file at path and of the table
// delta beside it: what a read of that table at a past moment reads. A
// file that cannot be looked at counts 0, and so does a delta that cannot
// be listed: this sizes a pin's deadline, and the read itself reports what
// it cannot open.
func TableReadBytes(path string) int64 {
	if path == "" || strings.HasPrefix(path, "s3://") {
		return 0
	}
	var total int64
	if fi, err := os.Stat(path); err == nil {
		total += fi.Size()
	}
	chain, err := ListTableDelta(context.Background(), path)
	if err != nil || chain == nil {
		return total
	}
	for _, p := range chain.Paths() {
		if fi, err := os.Stat(p); err == nil {
			total += fi.Size()
		}
	}
	return total
}

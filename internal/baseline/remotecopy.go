package baseline

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// RemoteCopy is one file of a snapshot being published that is NOT on local
// disk: the identical object of an earlier snapshot in S3, which the upload
// copies into the new snapshot's prefix inside S3 (#2212). It is how an
// S3-only server's update publishes a table that did not change without
// downloading, rewriting or uploading its bytes, the S3 analog of the hard
// link a local update makes (reconstruct.carryForward).
//
// Copies travel BESIDE the local snapshot directory, as a list, never as a
// file in it: every reader of a snapshot directory walks it, and a marker
// file there would have to be taught to each of them. Instead the readers
// that must see the whole snapshot take the list (the upload, the integrity
// manifest, the views file, the read bound).
type RemoteCopy struct {
	// Rel is the file's path below the snapshot directory, forward-slashed
	// (shop/orders.parquet, shop/orders.000000.posdel).
	Rel string
	// Src is the s3:// URL of the object to copy.
	Src string
	// CRC32C is the digest the source snapshot's own integrity manifest
	// records for Src, carried into the new manifest: a copy inside S3 is the
	// same bytes, so the digest computed over them still holds, and rot in
	// the source is still caught when the copy is read. Empty is "no digest".
	CRC32C string
}

// remoteCopyFile reports whether name is a file a snapshot may hold as a
// copy: a table file or one file of a table delta. The same set the
// integrity manifest covers.
func remoteCopyFile(name string) bool {
	return strings.HasSuffix(name, ".parquet") || strings.HasSuffix(name, TableDeltaPosdelSuffix) ||
		strings.HasSuffix(name, TableDeltaUpsertsSuffix)
}

// validateRemoteCopies refuses a list the upload cannot place safely, before
// anything is sent: a copy lands at the key the same file would have had on
// disk, so a path that leaves the snapshot, is not a table file, is listed
// twice, or names a file that IS on disk would put a wrong or doubled object
// in a snapshot about to be certified.
func validateRemoteCopies(snapDir string, copies []RemoteCopy) error {
	seen := make(map[string]bool, len(copies))
	for _, c := range copies {
		rel := c.Rel
		clean := path.Clean(rel)
		switch {
		case rel == "" || clean != rel || strings.HasPrefix(rel, "/") || strings.Contains(rel, `\`) ||
			clean == ".." || strings.HasPrefix(clean, "../"):
			return fmt.Errorf("refusing to copy %q into the snapshot: not a plain path inside it", rel)
		case strings.Count(rel, "/") != 1:
			return fmt.Errorf("refusing to copy %q into the snapshot: a table file sits at <schema>/<file>", rel)
		case !remoteCopyFile(rel):
			return fmt.Errorf("refusing to copy %q into the snapshot: only table files and table deltas are copied", rel)
		case !strings.HasPrefix(c.Src, "s3://"):
			return fmt.Errorf("refusing to copy %q into the snapshot: its source %q is not in S3", rel, c.Src)
		case seen[rel]:
			return fmt.Errorf("refusing to copy %q into the snapshot: it is listed twice", rel)
		}
		seen[rel] = true
		if _, err := os.Lstat(filepath.Join(snapDir, filepath.FromSlash(rel))); err == nil {
			return fmt.Errorf("refusing to copy %q into the snapshot: the same file is on disk and would be sent twice", rel)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("refusing to copy %q into the snapshot: %w", rel, err)
		}
	}
	return nil
}

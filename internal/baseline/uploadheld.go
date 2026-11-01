package baseline

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// uploadHeldPrefix names the mark a snapshot gets when the server's upload
// interval kept it on this machine on purpose (#2596): ".upload-held.<name>",
// an empty file at the ROOT beside the snapshot directory.
//
// Beside it, not inside it: the upload, the integrity manifest, carry-forward
// and the vacancy check all read a snapshot's own files, and none of them may
// learn about a new one. The root already holds the prune's own records
// (LastPruneFile, NotInDestinationFile), and every listing skips what does not
// parse as a snapshot directory.
//
// The mark is what separates "not sent on purpose" from "the upload failed".
// The two look the same in the bucket, and the failed one must still be sent
// again (the full read's sweep, the prune's resend) while the held one must
// not: re-sending every held snapshot at once is the burst the interval exists
// to avoid. Read off the setting instead, turning the interval off later would
// make hours of held snapshots look like failed uploads.
const uploadHeldPrefix = ".upload-held."

func uploadHeldPath(root, name string) (string, error) {
	if _, ok := parseBaselineDirTimestamp(name); !ok || filepath.Base(name) != name {
		return "", fmt.Errorf("baseline: %q is not a snapshot directory name", name)
	}
	return filepath.Join(root, uploadHeldPrefix+name), nil
}

// MarkUploadHeld records that the snapshot root/name was kept on this machine
// by the upload interval. Marking an already marked snapshot is not an error.
func MarkUploadHeld(root, name string) error {
	p, err := uploadHeldPath(root, name)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("baseline: mark snapshot %s as held on this machine: %w", name, err)
	}
	return f.Close()
}

// UploadHeld reports whether root/name carries the held mark. An error other
// than "no mark" is returned with false, and each caller picks its own safe
// side: the prune keeps the snapshot, a resend sends it.
func UploadHeld(root, name string) (bool, error) {
	p, err := uploadHeldPath(root, name)
	if err != nil {
		return false, err
	}
	if _, err := os.Stat(p); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ClearUploadHeld removes the mark, once the snapshot reached the destination
// after all or left the disk. Clearing an absent mark is not an error.
func ClearUploadHeld(root, name string) error {
	p, err := uploadHeldPath(root, name)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// heldSnapshots returns the snapshot names under root carrying the mark, and
// removes the marks whose snapshot directory is gone (a prune that crashed
// between the two, or a snapshot removed by hand). A root that cannot be read
// returns none: the enumeration right after reports it.
func heldSnapshots(root string, dryRun bool) map[string]bool {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	held := map[string]bool{}
	for _, e := range entries {
		name, ok := strings.CutPrefix(e.Name(), uploadHeldPrefix)
		if !ok || e.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(root, name)); errors.Is(err, os.ErrNotExist) {
			if !dryRun {
				if rerr := ClearUploadHeld(root, name); rerr != nil {
					slog.Warn("baseline prune: could not remove the held-on-this-machine mark of a snapshot that is gone",
						"dir", root, "snapshot", name, "error", rerr)
				}
			}
			continue
		}
		held[name] = true
	}
	return held
}

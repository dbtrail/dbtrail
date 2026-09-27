package baseline

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Snapshot writer signature (#1762).
//
// A baselines root has no per-source segment the way archives have one
// (bintrail_id=<uuid>/), so two installations pointed at one root interleave
// their snapshots and a reader that follows the newest one alternates between
// them. The signature records WHO wrote a snapshot, so a listing can see that
// a root holds the work of more than one writer and say so.
//
// It is a marker beside _SUCCESS whose NAME carries the identity:
//
//	<root>/<timestamp>/_WRITER.<id>
//
// In the name, not in a file's content and not in the Parquet footers, for
// three reasons:
//
//   - Every listing already reads the names in a snapshot directory (the
//     local walk reads the directory to find its schemas, the S3 inventory
//     lists the prefix to find its table files), so the signature costs no
//     read of its own. A field in _MANIFEST would cost one GET per snapshot
//     on S3, and a footer key one per table file.
//   - A refresh links unchanged table files forward from the previous
//     snapshot. A hard link IS the older file, so its footer cannot say who
//     wrote the NEW snapshot without editing the old one through the same
//     inode. The marker belongs to the directory, which is always new.
//   - It travels with the snapshot: the S3 upload copies every file of the
//     directory and sends _SUCCESS last, so a snapshot that reads as complete
//     already has its signature.
//
// The file is empty. Only the name is read.
//
// Backward compatibility: every snapshot written before this has no
// signature. An unsigned snapshot names no writer at all; it is never counted
// as "another writer", or every existing installation would wake up to a
// warning about its own snapshots.
const WriterMarkerPrefix = "_WRITER."

// maxWriterLen bounds the identity. A bintrail_id is a 36-character UUID; the
// bound only keeps a marker name inside what every filesystem accepts.
const maxWriterLen = 128

// NormalizeWriter returns the form an identity is signed and compared in:
// surrounding whitespace removed, lower case. ok is false when nothing usable
// is left: an empty value, or one holding anything but letters, digits, '.',
// '_' and '-' (a path separator, a space inside, a second line), or one with
// no letter or digit at all.
//
// Lower case because the same UUID is spelled both ways by different tools,
// and two spellings of one writer must never read as two writers.
func NormalizeWriter(id string) (string, bool) {
	id = strings.ToLower(strings.TrimSpace(id))
	if id == "" || len(id) > maxWriterLen {
		return "", false
	}
	named := false
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			named = true
		case r == '.', r == '_', r == '-':
		default:
			return "", false
		}
	}
	if !named {
		return "", false
	}
	return id, true
}

// WriteWriterMarker signs snapshotDir as written by writer, and removes any
// other signature the directory holds: a retried run completes a directory an
// earlier attempt may have signed, and the writer that completes it is its
// author.
//
// An empty writer signs nothing (the snapshot stays unsigned, which readers
// treat as "no writer named") and is not an error. A writer that does not
// normalize is an error, and the directory is left unsigned.
//
// Callers treat an error as a warning: a signature is how a shared root is
// noticed, never a condition for a snapshot to be complete.
func WriteWriterMarker(snapshotDir, writer string) error {
	want := ""
	var bad error
	if strings.TrimSpace(writer) != "" {
		id, ok := NormalizeWriter(writer)
		if !ok {
			bad = fmt.Errorf("writer identity %q cannot sign a snapshot: it must be letters, digits, '.', '_' or '-', at most %d characters", writer, maxWriterLen)
		} else {
			want = WriterMarkerPrefix + id
		}
	}
	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		return errors.Join(bad, fmt.Errorf("read %s to sign it: %w", snapshotDir, err))
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), WriterMarkerPrefix) || e.Name() == want {
			continue
		}
		if err := os.Remove(filepath.Join(snapshotDir, e.Name())); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return errors.Join(bad, fmt.Errorf("remove the earlier signature %s: %w", e.Name(), err))
		}
	}
	if bad != nil || want == "" {
		return bad
	}
	if err := os.WriteFile(filepath.Join(snapshotDir, want), nil, 0o644); err != nil {
		return fmt.Errorf("write the %s signature: %w", want, err)
	}
	return nil
}

// WritersFromNames reads the signatures among the file names of ONE snapshot
// directory: the writers it names, normalized, distinct and sorted, and the
// names that look like a signature and cannot be read as one.
//
// No signature returns nothing, which is every snapshot written before
// signatures existed. More than one writer is returned as it is found: two
// installations that published the same timestamp into one S3 prefix leave
// both signatures in the directory, and that is two writers.
func WritersFromNames(names []string) (writers, unreadable []string) {
	for _, n := range names {
		if !strings.HasPrefix(n, WriterMarkerPrefix) {
			continue
		}
		id, ok := NormalizeWriter(strings.TrimPrefix(n, WriterMarkerPrefix))
		// A name whose identity has whitespace around it is not one this
		// code writes; normalizing it away would accept what nobody signed.
		if !ok || !strings.EqualFold(id, strings.TrimPrefix(n, WriterMarkerPrefix)) {
			unreadable = append(unreadable, n)
			continue
		}
		if !slices.Contains(writers, id) {
			writers = append(writers, id)
		}
	}
	slices.Sort(writers)
	return writers, unreadable
}

// ReadSnapshotWriters is WritersFromNames over a local snapshot directory.
func ReadSnapshotWriters(snapshotDir string) (writers, unreadable []string, err error) {
	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		return nil, nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	writers, unreadable = WritersFromNames(names)
	return writers, unreadable, nil
}

// SignSnapshot is WriteWriterMarker for a producer closing a snapshot out: it
// signs, and turns a failure into a warning. The snapshot is complete either
// way; what an unsigned one loses is its part in noticing a shared root.
// Every producer calls it right before the integrity manifest, so a snapshot
// that reads as complete already carries its signature.
func SignSnapshot(snapshotDir, writer string) {
	if err := WriteWriterMarker(snapshotDir, writer); err != nil {
		slog.Warn("could not sign the snapshot with its writer; it is complete and is published unsigned, so it takes no part in noticing two writers on one snapshot location",
			"dir", snapshotDir, "error", err)
	}
}

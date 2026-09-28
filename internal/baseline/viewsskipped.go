package baseline

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// Record of the views a full read left out (#1879).
//
// A view holds no rows to copy, so a snapshot has no file for it. The run
// that read the source says which views it skipped (Stats.ViewsSkipped), and
// this record keeps that answer with the snapshot, so it can be shown later
// without reading the dump again:
//
//	<root>/<timestamp>/_VIEWS_SKIPPED
//
// A file of its own beside _SUCCESS, not a key in _MANIFEST, for three
// reasons:
//
//   - _MANIFEST is what every read of a table is checked against, and a
//     snapshot whose manifest cannot be written is not published. This record
//     is a note for the operator. Writing it must never be able to cost a
//     snapshot or a check, so it does not share a file with them.
//   - The manifest is read whole on every validation, with a size limit. A
//     source with thousands of views would grow it for no reader that
//     validates anything.
//   - A program from before this record reads a snapshot that holds it as it
//     read one signed by its writer (writersig.go) or one that holds
//     views.sql: a plain file in the snapshot directory that names no table.
//     It is uploaded before _SUCCESS like every other file.
//
// A snapshot updated from recorded changes reads no dump. It carries the
// record of the snapshot it started from, marked Carried, and ReadAt stays
// the time of the full read: the list says what the source held THEN. A view
// created since is not in it and a view dropped since still is.
//
// Backward compatibility: a snapshot written before this has no record,
// which reads as "not recorded", never as "no views". A run that skipped no
// view writes no record either, so the two read the same, and neither says
// anything about views.
const ViewsSkippedName = "_VIEWS_SKIPPED"

const (
	viewsSkippedVersion = 1
	// ViewsSkippedKept is how many names one record keeps. The count is
	// exact whatever this is; the log of the run names every view.
	ViewsSkippedKept = 1000
	// maxViewsSkippedBytes caps the read. A record of ViewsSkippedKept names
	// of viewNameCap characters is under this; anything larger at that name
	// is not a record.
	maxViewsSkippedBytes = 1 << 20
	// viewNameCap bounds one name, in runes. MySQL's own limit is 64 per
	// identifier, so it only cuts a name that did not come from a server.
	viewNameCap = 200
)

// ViewsSkipped is the record. Count is how many views the full read skipped
// and Views names them, "db.view", sorted, up to ViewsSkippedKept.
type ViewsSkipped struct {
	Version int `json:"version"`
	// ReadAt is the time of the snapshot whose run read the source and found
	// these views, RFC3339 UTC. "" when the record does not say.
	ReadAt string `json:"read_at,omitempty"`
	// Carried says this snapshot did not read the source: the record was
	// copied from the snapshot it was updated from.
	Carried bool     `json:"carried,omitempty"`
	Count   int      `json:"count"`
	Views   []string `json:"views"`
}

// Omitted is how many views the record counts and does not name.
func (v ViewsSkipped) Omitted() int {
	return max(0, v.Count-len(v.Views))
}

// NewViewsSkipped is the record of a full read that skipped views, taken at
// readAt. Every entry of views counts; the names kept are the ones that are
// text, once each.
func NewViewsSkipped(views []string, readAt time.Time) ViewsSkipped {
	names := cleanViewNames(views)
	rec := ViewsSkipped{Version: viewsSkippedVersion, Views: names}
	rec.Count = len(names) + (len(views) - countNamed(views))
	if len(rec.Views) > ViewsSkippedKept {
		rec.Views = rec.Views[:ViewsSkippedKept]
	}
	if rec.Count > 0 && !readAt.IsZero() {
		rec.ReadAt = readAt.UTC().Format(time.RFC3339)
	}
	return rec
}

// countNamed is how many entries of views are a name at all.
func countNamed(views []string) int {
	n := 0
	for _, v := range views {
		if viewNameText(v) != "" {
			n++
		}
	}
	return n
}

// viewNameText folds a name to one line of text and cuts it to viewNameCap.
func viewNameText(s string) string {
	s = strings.Join(strings.Fields(strings.ToValidUTF8(s, "?")), " ")
	if utf8.RuneCountInString(s) <= viewNameCap {
		return s
	}
	return string([]rune(s)[:viewNameCap-3]) + "..."
}

// cleanViewNames returns the names that are text, folded, sorted, once each.
func cleanViewNames(views []string) []string {
	var out []string
	for _, v := range views {
		if name := viewNameText(v); name != "" {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// ParseViewsSkipped reads a record. ok is false, with no error, for a record
// that counts no view. A record this program cannot read (not JSON, no
// version, a later version) is an error and carries no count: a number from
// a file that was not understood is never shown.
func ParseViewsSkipped(b []byte) (rec ViewsSkipped, ok bool, err error) {
	var parsed ViewsSkipped
	if err := json.Unmarshal(b, &parsed); err != nil {
		return ViewsSkipped{}, false, fmt.Errorf("parse %s: %w", ViewsSkippedName, err)
	}
	if parsed.Version != viewsSkippedVersion {
		return ViewsSkipped{}, false, fmt.Errorf("%s has version %d, and this program reads version %d",
			ViewsSkippedName, parsed.Version, viewsSkippedVersion)
	}
	parsed.Views = cleanViewNames(parsed.Views)
	if len(parsed.Views) > ViewsSkippedKept {
		parsed.Views = parsed.Views[:ViewsSkippedKept]
	}
	parsed.Count = max(parsed.Count, len(parsed.Views))
	if parsed.Count <= 0 {
		return ViewsSkipped{}, false, nil
	}
	if _, err := time.Parse(time.RFC3339, parsed.ReadAt); err != nil {
		parsed.ReadAt = ""
	}
	return parsed, true, nil
}

// ReadViewsSkippedFrom is ParseViewsSkipped over a stream, with the size
// limit: the local read and the S3 read share it.
func ReadViewsSkippedFrom(r io.Reader) (ViewsSkipped, bool, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxViewsSkippedBytes+1))
	if err != nil {
		return ViewsSkipped{}, false, fmt.Errorf("read %s: %w", ViewsSkippedName, err)
	}
	if len(b) > maxViewsSkippedBytes {
		return ViewsSkipped{}, false, fmt.Errorf("%s is over %d bytes, which is not a record of skipped views",
			ViewsSkippedName, maxViewsSkippedBytes)
	}
	return ParseViewsSkipped(b)
}

// ReadViewsSkipped reads the record of a local snapshot directory. ok is
// false with no error when the snapshot has none, which is every snapshot
// written before the record existed and every one whose source had no view.
func ReadViewsSkipped(snapshotDir string) (ViewsSkipped, bool, error) {
	f, err := os.Open(filepath.Join(snapshotDir, ViewsSkippedName))
	if errors.Is(err, fs.ErrNotExist) {
		return ViewsSkipped{}, false, nil
	}
	if err != nil {
		return ViewsSkipped{}, false, err
	}
	defer f.Close()
	return ReadViewsSkippedFrom(f)
}

// WriteViewsSkipped writes rec into snapshotDir. A record that counts no
// view is not written, and one left there by an earlier attempt at the same
// directory is removed: a retried run is the author of what it completes.
func WriteViewsSkipped(snapshotDir string, rec ViewsSkipped) error {
	path := filepath.Join(snapshotDir, ViewsSkippedName)
	if rec.Count <= 0 {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove the earlier %s: %w", ViewsSkippedName, err)
		}
		return nil
	}
	rec.Version = viewsSkippedVersion
	if rec.Views == nil {
		rec.Views = []string{}
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("write %s: %w", ViewsSkippedName, err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", ViewsSkippedName, err)
	}
	return nil
}

// RecordViewsSkipped is WriteViewsSkipped for a producer closing a snapshot
// out: it writes, and turns a failure into a warning. The snapshot is
// complete either way; what it loses is the count on the Snapshots page, and
// the log of the run still names every view.
func RecordViewsSkipped(snapshotDir string, rec ViewsSkipped) {
	if err := WriteViewsSkipped(snapshotDir, rec); err != nil {
		slog.Warn("could not record the skipped views in the snapshot; it is complete and is published without the record, so the Snapshots page shows no count of views for it",
			"dir", snapshotDir, "views", rec.Count, "error", err)
	}
}

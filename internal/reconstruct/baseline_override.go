package reconstruct

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// A table read again alone (#2640).
//
// A refresh folds every table from the newest snapshot that holds it. A table
// that snapshot can no longer be folded from (a TRUNCATE, a dropped
// partition, a capture gap) is refused, and the run publishes nothing. The
// cure is a new read of the table from the source. FullTableConfig.
// BaselineOverride lets a run take that read, one file per table kept OUTSIDE
// the snapshots root, as the table's base in place of the snapshot's, while
// every other table folds as usual. All of them are folded to the run's one
// cut, so the snapshot the run publishes holds one point in time.
//
// Every check here refuses. A file that cannot be shown to be a whole read of
// that table, at a coordinate the run's cut has reached, must not be folded:
// the alternative to a refusal is a snapshot with one table at another point
// in time than the rest, or an older table under a newer name.

// ErrBaselineOverride is a FullTableConfig.BaselineOverride entry that cannot
// be used as given: the table is not in the run, the file is not there, its
// folder is not a finished read, it is not a MySQL read. Nothing is folded
// from it, and trying the same entry again gives the same answer.
var ErrBaselineOverride = errors.New("the table file given in place of the table's snapshot cannot be used")

// ErrBaselineOverrideAhead is a BaselineOverride file read from the source at
// a point the run has not reached: it was read after the run's target time,
// or at a binlog coordinate past the run's cut (capture is behind the read).
// Folding it would publish that table as of a later point than every other
// table of the snapshot. A later run, once capture has passed the read's
// coordinate, can take the same file.
var ErrBaselineOverrideAhead = errors.New("the table file given in place of the table's snapshot was read past the point this run reaches")

// overrideBase is a BaselineOverride entry that passed every check a path
// and a folder can answer.
type overrideBase struct {
	path string
	// dir is the folder of the read, <dir>/<schema>/<table>.parquet.
	dir string
	// at is the time in dir's name: when the read started, as for any
	// snapshot directory.
	at time.Time
}

// checkBaselineOverrides checks every BaselineOverride entry before a run
// creates anything: an entry for a table the run does not fold is refused
// (a name spelled another way would otherwise leave the table on its old
// snapshot without a word), and so is one whose file or folder fails
// baselineOverrideFor.
func checkBaselineOverrides(ctx context.Context, cfg FullTableConfig) error {
	keys := make([]string, 0, len(cfg.BaselineOverride))
	for k := range cfg.BaselineOverride {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		if !slices.Contains(cfg.Tables, k) {
			return fmt.Errorf("%w: %q is not one of the tables of this run (names are compared as written)", ErrBaselineOverride, k)
		}
		schema, table, _ := splitSchemaTable(k)
		if _, _, err := baselineOverrideFor(ctx, cfg, schema, table); err != nil {
			return err
		}
	}
	return nil
}

// baselineOverrideFor is the BaselineOverride entry of schema.table, checked,
// and whether there is one.
func baselineOverrideFor(ctx context.Context, cfg FullTableConfig, schema, table string) (overrideBase, bool, error) {
	raw, ok := cfg.BaselineOverride[schema+"."+table]
	if !ok {
		return overrideBase{}, false, nil
	}
	refuse := func(sentinel error, format string, args ...any) (overrideBase, bool, error) {
		return overrideBase{}, true, fmt.Errorf("%w: %s.%s: %s", sentinel, schema, table, fmt.Sprintf(format, args...))
	}
	if cfg.OutputFormat != OutputFormatParquet {
		return refuse(ErrBaselineOverride, "only a run that writes a snapshot takes one")
	}
	if raw == "" || strings.HasPrefix(raw, "s3://") || !filepath.IsAbs(raw) {
		return refuse(ErrBaselineOverride, "%q is not an absolute local path", raw)
	}
	p := filepath.Clean(raw)
	schemaDir := filepath.Dir(p)
	dir := filepath.Dir(schemaDir)
	// The layout of a snapshot directory, by the names as written: what the
	// manifest, the archive record and the table delta beside a base all
	// derive from the path. A name the layout cannot hold (one with a path
	// separator) is refused here.
	if filepath.Base(p) != table+".parquet" || filepath.Base(schemaDir) != schema {
		return refuse(ErrBaselineOverride, "%s is not this table's file, <folder>/%s/%s.parquet", p, schema, table)
	}
	at, ok := parseDirTimestamp(filepath.Base(dir))
	if !ok {
		return refuse(ErrBaselineOverride, "the folder %s is not named after the time of a read", dir)
	}
	// Outside every root this run reads snapshots from or writes them to: in
	// there, the folder would be found as a snapshot of its own, holding only
	// these tables, and the next refresh would take its table list from it.
	realDir := resolvedPath(dir)
	for _, root := range []string{cfg.BaselineSrc, cfg.OutputDir} {
		if root == "" || strings.HasPrefix(root, "s3://") {
			continue
		}
		rel, err := filepath.Rel(resolvedPath(root), realDir)
		if err != nil {
			return refuse(ErrBaselineOverride, "cannot tell whether the folder %s is inside the snapshots root %s: %v", dir, root, err)
		}
		if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return refuse(ErrBaselineOverride, "the folder %s is inside the snapshots root %s, where it would be found as a snapshot holding only its own tables", dir, root)
		}
	}
	// A finished read, said by the folder itself. Stricter than
	// baseline.SnapshotComplete, which takes a folder with no marker as a
	// snapshot written before markers existed: a read interrupted before its
	// first marker has none either.
	if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); err != nil {
		return refuse(ErrBaselineOverride, "the folder %s is not marked as a finished read (%v)", dir, err)
	}
	switch _, err := os.Stat(filepath.Join(dir, baseline.IncompleteMarker)); {
	case err == nil:
		return refuse(ErrBaselineOverride, "the folder %s is marked as an unfinished read", dir)
	case !errors.Is(err, os.ErrNotExist):
		return refuse(ErrBaselineOverride, "cannot tell whether the folder %s is marked as an unfinished read: %v", dir, err)
	}
	// Lstat: a link to a file somewhere else is not this read's file.
	fi, err := os.Lstat(p)
	if err != nil {
		return refuse(ErrBaselineOverride, "its file cannot be read: %v", err)
	}
	if !fi.Mode().IsRegular() {
		return refuse(ErrBaselineOverride, "%s is not a file", p)
	}
	// A table delta beside it would make the file a base carried forward from
	// an earlier point, with a window that starts before the folder's time
	// (FindBaseline moves the time back for one). A read from the source has
	// none.
	chain, err := baseline.ListTableDelta(ctx, p)
	if err != nil {
		return refuse(ErrBaselineOverride, "cannot tell whether %s has a table delta beside it: %v", p, err)
	}
	if chain != nil {
		return refuse(ErrBaselineOverride, "%s has a table delta beside it, so it is not a read from the source", p)
	}
	if at.After(cfg.At) {
		return refuse(ErrBaselineOverrideAhead, "the read started at %s, after this run's target %s",
			at.UTC().Format(time.RFC3339), cfg.At.UTC().Format(time.RFC3339))
	}
	return overrideBase{path: p, dir: dir, at: at}, true, nil
}

// resolvedPath is p made absolute with its links followed, as far as it
// exists: a root that is not created yet has no link to follow.
func resolvedPath(p string) string {
	if abs, err := filepath.Abs(p); err == nil {
		p = abs
	}
	if real, err := filepath.EvalSymlinks(p); err == nil {
		return real
	}
	return filepath.Clean(p)
}

// checkOverrideReached refuses a BaselineOverride file whose footer does not
// show the run's cut has reached the coordinate it was read at. postgres says
// the index records a PostgreSQL source.
//
// The cut is where every table of the run is folded to, and the file is
// folded from its own coordinate. At or before the cut, the table ends at the
// cut like the others. Past it, the fold would have an empty window and the
// table would be published as of its own, later, coordinate.
//
// The footer must also say the file IS the read its folder is named after: a
// read of the source, stamped with the folder's time. The capture-gap check
// looks from that time on, so a file older than its folder's name (a table
// carried forward, the output of a fold, a file copied from an earlier
// snapshot) would be folded across a loss the check never looked at.
func checkOverrideReached(schema, table string, bmeta baseline.DumpMetadata, readAt time.Time, postgres bool, cut *query.BinlogPos) error {
	refuse := func(sentinel error, format string, args ...any) error {
		return fmt.Errorf("%w: %s.%s: %s", sentinel, schema, table, fmt.Sprintf(format, args...))
	}
	if postgres || bmeta.LSN != 0 {
		return refuse(ErrBaselineOverride, "a PostgreSQL table is not read again this way")
	}
	if bmeta.Producer != baseline.ProducerDump {
		return refuse(ErrBaselineOverride, "the file does not say it was read from the source (it records %q as what wrote it)", bmeta.Producer)
	}
	if !bmeta.SnapshotTimestamp.Equal(readAt) {
		return refuse(ErrBaselineOverride, "the file was written by the read of %s, not the one its folder is named after (%s)",
			bmeta.SnapshotTimestamp.UTC().Format(time.RFC3339), readAt.UTC().Format(time.RFC3339))
	}
	anchor := AnchorOf(bmeta)
	if anchor == nil || anchor.IsLSN() {
		return refuse(ErrBaselineOverride, "the file records no binlog coordinate, so nothing shows where its rows were read")
	}
	if cut == nil {
		return refuse(ErrBaselineOverrideAhead, "the file was read at %s:%d and the index holds no change to show capture has reached it",
			anchor.File, anchor.Pos)
	}
	if cut.IsLSN() || !anchor.AtOrBefore(*cut) {
		return refuse(ErrBaselineOverrideAhead, "the file was read at %s:%d and this run ends at %s:%d, before it; capture has to pass the read first",
			anchor.File, anchor.Pos, cut.File, cut.Pos)
	}
	return nil
}

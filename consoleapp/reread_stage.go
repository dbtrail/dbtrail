package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The read of some tables alone, staged for an update (#2640).
//
// An update that refuses a table can be cured by reading that table again
// from the source, where today the whole server is read. stageReread is the
// read: mydumper with --tables-list, through everything a full read's dump
// goes through (dumpAnchored), converted by the code a full read is converted
// by, into a folder of the working folder. It publishes nothing and folds
// nothing: the caller hands the files to reconstruct.FullTableConfig.
// BaselineOverride and removes the folder when that run has returned.
//
// Measured before this was written, with the real mydumper under writes that
// never paused, on a source without GTIDs and on one with them captured by
// the stream: under ftwrl and under lock-all the binlog position such a dump
// records is the instant its rows were read at, and the table folded from it
// is the source's (TestRereadCoordinate_tablesListDumpUnderWrites,
// TestIntegrationRereadCoordinateGTIDSource_2640). no-lock promises nothing
// and safe-no-lock stops under writes, so neither is used.

// errRereadUnavailable is a server, or a list of tables, that cannot be read
// by table at all: the caller takes a full read, as it does today.
var errRereadUnavailable = errors.New("these tables cannot be read again alone")

// errRereadTableMissing is a read that ended well without the file of a
// table it was asked for: the table is gone from the source, or mydumper did
// not match its name. Nothing is handed out for the other tables either.
var errRereadTableMissing = errors.New("the read of some tables alone ended without a table it was asked for")

// rereadDirPrefix names the folder of a staged read in the working folder.
// It is one of stagingNamePrefixes: a start after a kill removes the folder
// whole, finished or not.
const rereadDirPrefix = "reread-"

// stagedReread is a finished read of some tables.
type stagedReread struct {
	// files is, per table as it was asked for ("schema.table"), the absolute
	// path of its file: <root>/<time of the read>/<schema>/<table>.parquet,
	// in a folder marked as a finished read, as BaselineOverride takes it.
	files map[string]string
	// root holds the read and nothing else.
	root string
	// at is when the read started: the name of its folder.
	at time.Time
}

// discard removes the read. For after the run that folded from it: the
// snapshot that run published may share a file with it by hard link, so the
// folder is removed, never written again in place.
func (r stagedReread) discard() error { return os.RemoveAll(r.root) }

// rereadConvert converts the dump; a test replaces it.
var rereadConvert = baseline.Run

type dumpTablesKey struct{}

// withDumpTables makes the dump under ctx read only tables ("schema.table"
// each). None leaves ctx as it is: a full read.
func withDumpTables(ctx context.Context, tables []string) context.Context {
	if len(tables) == 0 {
		return ctx
	}
	return context.WithValue(ctx, dumpTablesKey{}, tables)
}

func dumpTablesFrom(ctx context.Context) []string {
	tables, _ := ctx.Value(dumpTablesKey{}).([]string)
	return tables
}

// schemasOfTables is the schemas of tables, each once, in the order met.
func schemasOfTables(tables []string) []string {
	var schemas []string
	for _, st := range tables {
		if schema, _, _ := strings.Cut(st, "."); !slices.Contains(schemas, schema) {
			schemas = append(schemas, schema)
		}
	}
	return schemas
}

// rereadTableList is tables, each once, after the checks a name alone can
// answer. A name is "schema.table", split at its first dot like
// FullTableConfig.Tables.
func rereadTableList(tables []string) ([]string, error) {
	if len(tables) == 0 {
		// Never an empty --tables-list: mydumper would read every table.
		return nil, errors.New("no table was given to read again")
	}
	var out []string
	for _, st := range tables {
		schema, table, ok := strings.Cut(st, ".")
		if !ok || schema == "" || table == "" {
			return nil, fmt.Errorf("%q is not a table name as schema.table", st)
		}
		if strings.Contains(st, ",") {
			// mydumper splits --tables-list at commas and has no way to
			// quote one.
			return nil, fmt.Errorf("%w: %q has a comma in its name, which mydumper's table list cannot hold", errRereadUnavailable, st)
		}
		if strings.Contains(table, ".") {
			// Measured with mydumper 1.0.3-1: a table whose name holds a dot
			// is not read from a table list, and the dump ends well without
			// it. A dot in the schema's name cannot be told from this one.
			return nil, fmt.Errorf("%w: %q has a dot in its table or schema name, which mydumper does not match in a table list", errRereadUnavailable, st)
		}
		if !slices.Contains(out, st) {
			out = append(out, st)
		}
	}
	return out, nil
}

// rereadNotInDump is the tables of want the dump in dir does not hold under
// their real names. A dump that cannot be listed answers none: the conversion
// reads the same files and reports what is wrong with them.
//
// leftOut is why the dump's conversion would leave tables out, when it would:
// such a table may well exist on the source, so the error must not say it was
// dropped.
func rereadNotInDump(dir string, want []string) (missing, leftOut []string) {
	found, _, left, err := baseline.DiscoverDumpNames(dir)
	if err != nil {
		return nil, nil
	}
	for _, st := range want {
		if !slices.ContainsFunc(found, func(tf baseline.TableFiles) bool { return tf.Database+"."+tf.Table == st }) {
			missing = append(missing, st)
		}
	}
	if len(missing) > 0 {
		for _, l := range left {
			leftOut = append(leftOut, l.Table+": "+l.Reason)
		}
	}
	return missing, leftOut
}

func rereadMissingError(missing, leftOut []string) error {
	if len(leftOut) > 0 {
		return fmt.Errorf("%w: %s (the dump holds tables it cannot store under their real names: %s)",
			errRereadTableMissing, strings.Join(missing, ", "), strings.Join(leftOut, "; "))
	}
	return fmt.Errorf("%w: %s (dropped from the source since, or a name mydumper does not match)", errRereadTableMissing, strings.Join(missing, ", "))
}

// stageReread reads tables of req's server again and returns one file per
// table, or an error and nothing: never some of the tables. Nothing calls it
// yet.
//
// errRereadUnavailable says no table of this server (or not these) can be
// read alone, and the caller takes a full read: a PostgreSQL server, a lock
// mode other than ftwrl and lock-all, a mydumper that cannot be told ftwrl, a
// comma or a dot in a name. A mydumper too old for lock-all fails with the
// error a full read gets from it, which is not errRereadUnavailable: a full
// read would fail the same way.
func (s *baselineSupervisor) stageReread(req console.BaselineRequest, tables []string) (stagedReread, error) {
	want, err := rereadTableList(tables)
	if err != nil {
		return stagedReread{}, err
	}
	if req.Flavor == console.FlavorPostgres {
		return stagedReread{}, fmt.Errorf("%w: a PostgreSQL table is not read again this way", errRereadUnavailable)
	}
	lockMode, src, err := s.lockModeFor(req)
	if err != nil {
		return stagedReread{}, err
	}
	if lockMode != baseline.LockModeFTWRL && lockMode != baseline.LockModeLockAll {
		return stagedReread{}, fmt.Errorf("%w: this server is read with lock mode %s, and the position a read of some tables records is only "+
			"the instant of the read under ftwrl or lock-all", errRereadUnavailable, lockMode)
	}
	if err := os.MkdirAll(s.stagingDir, 0o755); err != nil {
		return stagedReread{}, fmt.Errorf("create the working folder %s: %w", s.stagingDir, err)
	}
	att, err := s.dumpAnchored(req, lockMode, src, want)
	if err != nil {
		return stagedReread{}, err
	}
	defer func() {
		// The journal entry goes when the job ends, so a restart would not
		// remove this folder either: the log is the only trace of it.
		if rmErr := os.RemoveAll(att.dir); rmErr != nil {
			slog.Error("console snapshot: could not remove the dump of a read of some tables; it still takes room in the working folder",
				"server", req.ServerID, "path", att.dir, "error", rmErr)
		}
	}()
	// Before the conversion: a dump that holds none of the tables would fail
	// there as "no tables found", which does not say a table is gone.
	if missing, leftOut := rereadNotInDump(att.dir, want); len(missing) > 0 {
		return stagedReread{}, rereadMissingError(missing, leftOut)
	}

	root, err := os.MkdirTemp(s.stagingDir, rereadDirPrefix)
	if err != nil {
		return stagedReread{}, fmt.Errorf("create the folder of the read: %w", err)
	}
	journalDir(req, s.stagingDir, filepath.Base(root))
	// fail removes everything the read staged: its folder, with the `current`
	// link and the lock baseline.Run leaves beside the read.
	fail := func(err error) (stagedReread, error) {
		if rmErr := os.RemoveAll(root); rmErr != nil {
			slog.Error("console snapshot: could not remove the folder of a read of some tables that failed; it still takes room in the working folder",
				"server", req.ServerID, "path", root, "error", rmErr)
			err = fmt.Errorf("%w. The folder this read started, %s, could not be removed (%s) and still takes room", err, root, firstLineOf(rmErr.Error()))
		}
		return stagedReread{}, err
	}
	cfg := s.dumpBaselineConfig(req, att.dir, root, att.startedAt, att.ddlMark, att.eventMark)
	// A table delta beside a file makes it a base carried forward, which the
	// fold refuses in place of a table's snapshot. A read has none.
	cfg.TableDeltas = false
	if _, err := rereadConvert(s.ctx, cfg); err != nil {
		if errors.Is(err, syscall.ENOSPC) {
			// The same disk the dump is on, filled a step later (#1938).
			// Free space that cannot be measured is not "0 B free": the error
			// keeps its own words, and ENOSPC still reads as a disk failure.
			if free, ok, _ := measureFree(s.stagingDir); ok {
				rmErr := os.RemoveAll(att.dir)
				return fail(&workingFolderFullError{err: fmt.Errorf("convert: %w", err), folder: s.stagingDir, free: free, dumpDir: att.dir, rmErr: rmErr, someTables: true})
			}
		}
		return fail(fmt.Errorf("convert: %w", err))
	}
	folder := filepath.Join(root, reconstruct.SnapshotDirName(att.startedAt))
	if _, err := os.Stat(filepath.Join(folder, baseline.SuccessMarker)); err != nil {
		return fail(fmt.Errorf("the read of %s was converted but its folder is not marked finished: %w", strings.Join(want, ", "), err))
	}
	files := make(map[string]string, len(want))
	var missing []string
	for _, st := range want {
		schema, table, _ := strings.Cut(st, ".")
		p := filepath.Join(folder, schema, table+".parquet")
		// Lstat, and a regular file: what the fold asks of it.
		fi, err := os.Lstat(p)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			// A file that cannot be looked at is not a table that is gone.
			return fail(fmt.Errorf("check the staged file of %s: %w", st, err))
		}
		if err != nil || !fi.Mode().IsRegular() {
			missing = append(missing, st)
			continue
		}
		files[st] = p
	}
	if len(missing) > 0 {
		return fail(rereadMissingError(missing, nil))
	}
	// To the second, as the folder's name and each file's footer carry it.
	return stagedReread{files: files, root: root, at: att.startedAt.UTC().Truncate(time.Second)}, nil
}

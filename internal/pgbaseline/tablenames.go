package pgbaseline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/event"
)

// A quoted PostgreSQL name can hold anything but a NUL, and a snapshot stores
// a table at <snapshot>/<schema>/<table>.parquet (#3113). A name that does not
// stay one path component would put the file somewhere else, outside the
// snapshot folder included. Such a table is left out and reported and the rest
// of the snapshot is published, the rule the MySQL read follows (#2006).
//
// The name is not encoded into a safe file name because every reader finds a
// table by its plain name: the snapshot update, the views, the delta chains
// and the port would all have to learn the encoding, and a real table could
// then be named like another table's encoded form.
//
// The refusal is narrower than the MySQL read's, on purpose. A table named
// "." or ".." is the file "..parquet" or "...parquet", and a "\" is an
// ordinary byte in a Linux file name: those files stay in their folder and
// the readers open them, so leaving them out would take away a table that
// works.

// longestFileSuffix is the longest thing appended to a table's name to make a
// file name in a snapshot: a PostgreSQL delta file while it is being written,
// "<table>.NNNNNN.pgdead.tmp-" and os.CreateTemp's up to ten digits.
const longestFileSuffix = len(".000000.pgdead.tmp-") + 10

// maxFileNameBytes is the longest file name the common file systems take.
const maxFileNameBytes = 255

// snapshotControlFiles are the files a snapshot keeps in its own top folder,
// where each schema has its directory. A schema of one of these names would
// take the file's place, and the snapshot could not be completed.
var snapshotControlFiles = map[string]bool{
	baseline.SuccessMarker:         true,
	baseline.IncompleteMarker:      true,
	baseline.ChangesOnlyMarkerFile: true,
	baseline.ViewsSkippedName:      true,
	baseline.NewestPointerName:     true,
	baselineintegrity.ManifestName: true,
	"_ARCHIVE_CHECKED":             true, // internal/reconstruct, unexported
	"_PRUNING":                     true, // reconstruct.PruneMarker; not imported here (read layer)
	"_ARCHIVE_CHECKED.tmp":         true, // the same file while it is written
	baseline.SnapshotViewsName:     true,
}

// viewsTempPrefix starts the staging file of a snapshot's views.sql
// (views.SnapshotFileTempPrefix). The upload and the download skip every file
// whose name starts with it, in any folder, so a table file named that way
// would be missing from S3 and from the download with nothing saying so.
const viewsTempPrefix = "." + baseline.SnapshotViewsName + ".tmp"

// unstorableComponent is what a schema and a table name have in common.
func unstorableComponent(name string) error {
	switch {
	case name == "":
		return errors.New("the name is empty")
	case strings.ContainsRune(name, '/'):
		return errors.New(`the name holds a "/", which a snapshot cannot store as a file name`)
	case strings.ContainsRune(name, 0):
		return errors.New("the name holds a NUL byte")
	}
	return nil
}

// unstorableTable says why a table name cannot be a file name in a snapshot,
// nil when it can.
func unstorableTable(name string) error {
	if err := unstorableComponent(name); err != nil {
		return err
	}
	if strings.HasPrefix(name, viewsTempPrefix) {
		return fmt.Errorf("the name starts with %q, and files named that way are skipped when a snapshot is uploaded or downloaded", viewsTempPrefix)
	}
	if len(name)+longestFileSuffix > maxFileNameBytes {
		return fmt.Errorf("the name is %d bytes long, and a snapshot's file names for it would pass the %d bytes a file name can hold",
			len(name), maxFileNameBytes)
	}
	return nil
}

// unstorableSchema says why a schema name cannot be a folder in a snapshot,
// nil when it can. The control files are compared exactly: PostgreSQL names
// are case-sensitive and so is the file system a snapshot is written on, so
// "_success" is an ordinary schema.
func unstorableSchema(name string) error {
	if err := unstorableComponent(name); err != nil {
		return err
	}
	switch {
	case name == "." || name == "..":
		return fmt.Errorf("the name %q is a directory name", name)
	case len(name) > maxFileNameBytes:
		return fmt.Errorf("the name is %d bytes long, and a folder name can hold %d", len(name), maxFileNameBytes)
	case snapshotControlFiles[name] || strings.HasPrefix(name, baseline.WriterMarkerPrefix) || strings.HasPrefix(name, viewsTempPrefix):
		return fmt.Errorf("the name %q is the name of a file the snapshot keeps for itself", name)
	}
	return nil
}

// splitStorable separates the tables a snapshot can hold from the ones it
// leaves out, with why. Schema and Name are the source's own spelling: the
// update's new-tables check matches on them, so a table left out on purpose
// does not start a new full read at every update.
func splitStorable(tables []tableInfo, logger *slog.Logger) ([]tableInfo, []baseline.LeftOutTable) {
	keep := make([]tableInfo, 0, len(tables))
	var left []baseline.LeftOutTable
	for _, t := range tables {
		var reason string
		if err := unstorableSchema(t.Schema); err != nil {
			reason = "its schema name cannot be stored: " + err.Error()
		} else if err := unstorableTable(t.Table); err != nil {
			reason = err.Error()
		}
		if reason == "" {
			keep = append(keep, t)
			continue
		}
		left = append(left, baseline.LeftOutTable{Table: displayName(t.Schema, t.Table), Reason: reason, Schema: t.Schema, Name: t.Table})
		logger.Warn("pgbaseline: a table is left out of the snapshot because its name cannot be stored",
			"schema", t.Schema, "table", t.Table, "reason", reason)
	}
	return keep, left
}

// refuseAllUnstorable stops a run none of whose tables can be stored before
// the replication slot is made: a slot nothing will read keeps WAL on the
// source. A discovery that fails or finds nothing is left to the run's own,
// which words those errors.
func refuseAllUnstorable(ctx context.Context, conn *pgx.Conn, publication string, filters event.Filters, logger *slog.Logger) ([]baseline.LeftOutTable, error) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	tables, err := discoverTables(ctx, conn, publication, filters, quiet)
	if err != nil || len(tables) == 0 {
		return nil, nil
	}
	keep, left := splitStorable(tables, quiet)
	if len(keep) > 0 {
		return nil, nil
	}
	// Said now with the real logger: the error does not carry the names.
	_, left = splitStorable(tables, logger)
	return left, allLeftOutError(left)
}

// displayName is a left-out table as a message shows it: quoted as SQL,
// because public.../x would not say where the schema ends, and with control
// characters replaced, because the name comes from the source and a line
// break in it could forge a line of a log or of the command's output.
func displayName(schema, table string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return '?'
		}
		return r
	}, pgx.Identifier{schema, table}.Sanitize())
}

// allLeftOutError is the run's error when no table of the publication can be
// stored: there is nothing to publish, and "no tables" would hide why. It
// holds no table name: it becomes the job's last error, which the console
// also serves to sessions with a data profile, and splitStorable has already
// logged each table with its reason.
func allLeftOutError(left []baseline.LeftOutTable) error {
	return fmt.Errorf(`pgbaseline: none of the publication's %d table(s) can be stored in a snapshot because of their schema or table names (a "/", a schema named "." or "..", or a name the snapshot keeps for its own files); this run's log names each one`, len(left))
}

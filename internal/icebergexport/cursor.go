package icebergexport

import (
	"fmt"
	"strconv"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Table properties the export owns. They are written in the same commit as
// the data they describe, so the table is its own state: a reader can see
// where the next run resumes with `iceberg_table_properties()`, and a run
// that dies before committing leaves the previous cursor with the previous
// snapshot.
const (
	propVersion  = "bintrail.export.version"
	propFile     = "bintrail.export.binlog_file"
	propPos      = "bintrail.export.binlog_position"
	propAt       = "bintrail.export.at"
	propOrigin   = "bintrail.export.cursor_origin" // "baseline" after a load, "cut" after a delta
	propBaseline = "bintrail.export.source_baseline"
	// propJSONColumns lists the MySQL JSON columns the first load rendered
	// (lower-case names, comma-separated, "" for none), so every later run
	// renders the same ones whatever the schema snapshot says (#1508).
	propJSONColumns = "bintrail.export.json_columns"
	// propSource is the bintrail_id of the source the table was created
	// from, when the index registers exactly one.
	propSource = "bintrail.export.source_id"
	// propSourceTable is the exact source schema.table the table was
	// created for (case kept).
	propSourceTable = "bintrail.export.source_table"

	// exportVersion is the newest table layout this build writes and reads.
	// A table stamped with a higher one was written by a newer bintrail, and
	// its cursor may mean something this build would misread; refusing is
	// the one check that cannot be added to a binary after it ships.
	exportVersion = "1"

	originBaseline = "baseline"
	originCut      = "cut"
)

// cursorKeys are the properties that make up a cursor: all present or none.
var cursorKeys = [...]string{propFile, propPos, propAt}

// Snapshot summary properties, informational, one set per commit.
const (
	summaryRowsLoaded  = "bintrail.export.rows_loaded"
	summaryEvents      = "bintrail.export.events"
	summaryUpserts     = "bintrail.export.upserts"
	summaryDeletes     = "bintrail.export.deletes"
	summaryWindowSince = "bintrail.export.window_since"
	summaryWindowUntil = "bintrail.export.window_until"
)

// cursor is where the table's deltas resume: a binlog coordinate (the run's
// positional cut, or the baseline anchor right after the first load) and the
// wall-clock instant the run targeted, which the next run uses as the time
// floor for pruning partitions and archives. FromBaseline says which of the
// two the coordinate is; the first window after a load gets one extra check
// that later windows do not.
type cursor struct {
	File         string
	Pos          uint64
	At           time.Time
	FromBaseline bool
}

// readCursor reads the cursor from table properties. A table with none of
// the keys is a table whose first load has not committed (nil, nil); a table
// with some of them is corrupt; a table stamped by a newer export is refused.
func readCursor(props iceberg.Properties) (*cursor, error) {
	if v, ok := props[propVersion]; ok && v != exportVersion {
		return nil, fmt.Errorf("table was written by a newer export (%s=%s, this build reads %s); upgrade bintrail", propVersion, v, exportVersion)
	}
	present := 0
	for _, k := range cursorKeys {
		if _, ok := props[k]; ok {
			present++
		}
	}
	if present == 0 {
		return nil, nil
	}
	if present != len(cursorKeys) {
		return nil, fmt.Errorf("table properties carry a partial export cursor (%s=%q %s=%q %s=%q); remove the table directory to reload it",
			propFile, props[propFile], propPos, props[propPos], propAt, props[propAt])
	}
	p, err := strconv.ParseUint(props[propPos], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("table property %s=%q is not a position: %w", propPos, props[propPos], err)
	}
	t, err := time.Parse(time.RFC3339Nano, props[propAt])
	if err != nil {
		return nil, fmt.Errorf("table property %s=%q is not a time: %w", propAt, props[propAt], err)
	}
	return &cursor{File: props[propFile], Pos: p, At: t.UTC(), FromBaseline: props[propOrigin] == originBaseline}, nil
}

// properties renders the cursor for a delta commit.
func (c cursor) properties() iceberg.Properties {
	origin := originCut
	if c.FromBaseline {
		origin = originBaseline
	}
	return iceberg.Properties{
		propFile:   c.File,
		propPos:    strconv.FormatUint(c.Pos, 10),
		propAt:     c.At.UTC().Format(time.RFC3339Nano),
		propOrigin: origin,
	}
}

// loadProperties renders the cursor for the first load, which also records
// the baseline it was seeded from.
func (c cursor) loadProperties(baselinePath string) iceberg.Properties {
	p := c.properties()
	p[propBaseline] = baselinePath
	return p
}

func (c cursor) String() string {
	return fmt.Sprintf("%s:%d at %s", c.File, c.Pos, c.At.UTC().Format(time.RFC3339))
}

// checkOwned refuses a table this export did not create. Every table the
// export creates carries propVersion from its first commit (tableProperties,
// at CreateTable, before any data), so a table without it was made by
// someone else: another engine, a person, an older tool. The export never
// adopts one, empty or not, because loading a baseline into it would mix
// two writers' rows under one cursor. This holds for every catalog; on Glue
// or REST a same-named table is far more likely to exist already than in a
// local directory nobody else writes to. A HIGHER version is readCursor's
// refusal (a newer bintrail).
func checkOwned(tbl *table.Table, schema, name string) error {
	if _, ok := tbl.Properties()[propVersion]; ok {
		return nil
	}
	return fmt.Errorf("the catalog already holds a table %s.%s at %s, and it carries no %s property, so bintrail did not create it; the export never adopts a table it did not create. Drop it, or export this table to another warehouse or catalog",
		schema, name, tbl.Location(), propVersion)
}

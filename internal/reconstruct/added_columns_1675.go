package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// A restore to before a column was added (#1675).
//
// The fold compares the baseline's column names with the LATEST schema
// snapshot, because that snapshot is the only record of a DDL that ran between
// the baseline and the target and left nothing else behind (file mode without
// a source, a failed snapshot, a statement parseDDL did not recognize, a
// schema_changes row that failed to insert). So a column added after the
// target refused every restore to before it.
//
// A column the latest snapshot adds is left out of the comparison only on
// positive proof that it did not exist at the target: a recorded ALTER TABLE
// on this exact table, after the target by time AND by binlog position, that
// does nothing but add columns and names it. That statement is in the binlog,
// so it ran, so the column did not exist right before it; and every other
// recorded DDL on the table after the target must be such a statement too, so
// none of them removed it in between. A column nothing places stays in the
// comparison and refuses, as before: refusing too much is recoverable, a
// published definition that misses a column is not visible.
//
// What this cannot see is two unrecorded DDLs that cancel out: the column
// added before the target and dropped after it, both with no row, then added
// again by a recorded statement. The comparison with the latest snapshot has
// always been blind to an unrecorded add and drop; the snapshot in effect at
// the target is compared too (atTarget), which catches it when one was taken
// in between.

// maxRecordedDDLBytes is what schema_changes.ddl_query (TEXT) holds. An ALTER
// TABLE's text is stored whole, and a server outside strict mode cuts a longer
// one without a word, so a text this long may be missing its tail.
const maxRecordedDDLBytes = 65535

// recordedDDL is one schema_changes row, with its place in time decided by
// the index server: the same comparison, on the same values, as every other
// lookup on detected_at.
type recordedDDL struct {
	DetectedAt    time.Time
	File          string
	Pos           uint64
	Schema, Table string
	Type, Query   string
	// BeforeAsOf: detected_at is earlier than when the baseline's CREATE
	// TABLE was read. AfterTarget: detected_at is later than the target.
	// Both are whole seconds against an instant, and both are strict, so a
	// statement in the same second as either is inside the window.
	BeforeAsOf, AfterTarget bool
}

func (d recordedDDL) at() string {
	return fmt.Sprintf("%s (%s:%d)", d.DetectedAt.UTC().Format(time.RFC3339), d.File, d.Pos)
}

// loadRecordedDDLs reads every DDL recorded for the table. The name match is
// the index's collation, which ignores case, and rows indexed before #1435
// carry an empty schema: both are loaded so placeAddedColumns can refuse on
// them, never so they can count as this table's.
func loadRecordedDDLs(ctx context.Context, db *sql.DB, schema, table string, asOf, target time.Time) ([]recordedDDL, error) {
	rows, err := db.QueryContext(ctx, `SELECT detected_at, binlog_file, binlog_pos, schema_name, table_name,
			ddl_type, ddl_query, detected_at < ?, detected_at > ?
		FROM schema_changes
		WHERE (schema_name = ? OR schema_name = '') AND table_name = ?
		ORDER BY id`, asOf, target, schema, table)
	if err != nil {
		var me *mysqldriver.MySQLError
		if errors.As(err, &me) && me.Number == 1146 {
			return nil, errSchemaChangesMissing
		}
		return nil, err
	}
	defer rows.Close()
	var out []recordedDDL
	for rows.Next() {
		var d recordedDDL
		if err := rows.Scan(&d.DetectedAt, &d.File, &d.Pos, &d.Schema, &d.Table,
			&d.Type, &d.Query, &d.BeforeAsOf, &d.AfterTarget); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// placeAddedColumns reports why the columns in added (lower case) cannot all
// be placed after the target, or "" when every one of them can.
//
// anchor is the baseline's own binlog coordinate, nil when it recorded none;
// cut is the run's positional cut, the same coordinate that bounds the fold.
func placeAddedColumns(added []string, ddls []recordedDDL, schema, table string, anchor, cut *query.BinlogPos) string {
	if cut == nil {
		return "the index holds no event to place the target by binlog position"
	}
	placed := map[string]bool{}
	for _, d := range ddls {
		pos := query.BinlogPos{File: d.File, Pos: d.Pos}
		hasPos := d.File != "" && d.Pos > 0
		// Before the baseline: its effect is in the baseline's CREATE TABLE.
		if d.BeforeAsOf && (anchor == nil || hasPos && pos.AtOrBefore(*anchor)) {
			continue
		}
		if d.Schema != schema || d.Table != table {
			return fmt.Sprintf("the %s recorded at %s names %q.%q, which cannot be told apart from this table",
				d.Type, d.at(), d.Schema, d.Table)
		}
		// Time and position, both: the fold keeps events by both.
		if !d.AfterTarget || !hasPos || pos.AtOrBefore(*cut) {
			return fmt.Sprintf("the %s recorded at %s is not after the target by both its time and its binlog position (the cut is %s:%d)",
				d.Type, d.at(), cut.File, cut.Pos)
		}
		if d.Type != string(parser.DDLAlterTable) {
			return fmt.Sprintf("a %s is recorded after the target, at %s", d.Type, d.at())
		}
		if len(d.Query) >= maxRecordedDDLBytes {
			return fmt.Sprintf("the text of the ALTER TABLE recorded at %s may be cut short", d.at())
		}
		tbl, cols, ok := parser.AddedColumns(d.Query, d.Schema)
		if !ok {
			return fmt.Sprintf("the ALTER TABLE recorded at %s cannot be read as only adding columns", d.at())
		}
		if tbl.Schema != schema || tbl.Table != table {
			return fmt.Sprintf("the ALTER TABLE recorded at %s names %q.%q in its text", d.at(), tbl.Schema, tbl.Table)
		}
		for _, c := range cols {
			key := strings.ToLower(c)
			if placed[key] {
				return fmt.Sprintf("%s is added more than once after the target, so it was dropped in between by a statement that is not recorded", c)
			}
			placed[key] = true
		}
	}
	var unplaced []string
	for _, c := range added {
		if !placed[strings.ToLower(c)] {
			unplaced = append(unplaced, c)
		}
	}
	if len(unplaced) > 0 {
		sort.Strings(unplaced)
		return "no ALTER TABLE recorded after the target adds " + strings.Join(unplaced, ", ")
	}
	return ""
}

// namesAtTarget returns the table description the fold compares the
// baseline's column NAMES with: tm, the latest snapshot's, without the columns
// proven to have been added after the target. It returns tm itself, and says
// why in the second value, when there are added columns and not every one of
// them is proven; "" when there is nothing to explain.
//
// atTarget is the table in the schema snapshot in effect at the target, nil
// when there is none. It can only refuse: a column it already has existed at
// or before the target, whatever the recorded statements say.
func namesAtTarget(ctx context.Context, db *sql.DB, createSQL string, tm, atTarget *metadata.TableMeta,
	schema, table string, asOf, target time.Time, anchor, cut *query.BinlogPos) (*metadata.TableMeta, string) {
	if tm == nil || strings.TrimSpace(createSQL) == "" {
		return tm, ""
	}
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		return tm, "" // the comparison reports it
	}
	inBaseline := make(map[string]bool, len(cols))
	for _, c := range cols {
		inBaseline[strings.ToLower(c.Name)] = true
	}
	current := map[string]bool{}
	var added []string
	for _, c := range tm.Columns {
		if c.IsGenerated {
			continue
		}
		name := strings.ToLower(c.Name)
		current[name] = true
		if !inBaseline[name] {
			added = append(added, name)
		}
	}
	if len(added) == 0 {
		return tm, ""
	}
	isAdded := make(map[string]bool, len(added))
	for _, c := range added {
		isAdded[c] = true
	}
	for name := range inBaseline {
		if !current[name] {
			return tm, "a column is also gone since the baseline, so the added ones are not looked up"
		}
	}
	for _, c := range tm.Columns {
		if c.IsPK && isAdded[strings.ToLower(c.Name)] {
			return tm, c.Name + " is part of the primary key now"
		}
	}
	for _, name := range tm.PKColumns {
		if isAdded[strings.ToLower(name)] {
			return tm, name + " is part of the primary key now"
		}
	}
	if atTarget != nil {
		for _, c := range atTarget.Columns {
			if isAdded[strings.ToLower(c.Name)] {
				return tm, "the schema snapshot in effect at the target already has " + c.Name
			}
		}
	}
	ddls, err := loadRecordedDDLs(ctx, db, schema, table, asOf, target)
	switch {
	case errors.Is(err, errSchemaChangesMissing):
		return tm, "this index keeps no record of DDL statements (no schema_changes table)"
	case err != nil:
		return tm, "the recorded DDL statements could not be read: " + err.Error()
	}
	if why := placeAddedColumns(added, ddls, schema, table, anchor, cut); why != "" {
		return tm, why
	}
	trimmed := *tm
	trimmed.Columns = make([]metadata.ColumnMeta, 0, len(tm.Columns))
	for _, c := range tm.Columns {
		if !isAdded[strings.ToLower(c.Name)] {
			trimmed.Columns = append(trimmed.Columns, c)
		}
	}
	slog.Info("columns added after the target are left out of the schema comparison",
		"schema", schema, "table", table, "columns", strings.Join(added, ", "),
		"target", target.UTC().Format(time.RFC3339))
	return &trimmed, ""
}

// foldNamesAtTarget is namesAtTarget for one table of a Parquet fold: the
// target and the cut are the run's, the anchor is the baseline's own, and the
// snapshot in effect at the target is asked for the table whatever its age (an
// older one than the baseline still proves a column it has existed).
func foldNamesAtTarget(ctx context.Context, db *sql.DB, cfg FullTableConfig, bmeta baseline.DumpMetadata,
	tm *metadata.TableMeta, asOf time.Time, schema, table string) (*metadata.TableMeta, string) {
	var atTarget *metadata.TableMeta
	if cfg.schemaAt != nil {
		if t, err := cfg.schemaAt.Resolve(schema, table); err == nil {
			atTarget = t
		}
	}
	var anchor *query.BinlogPos
	if bmeta.BinlogFile != "" && bmeta.BinlogPos > 0 {
		anchor = &query.BinlogPos{File: bmeta.BinlogFile, Pos: uint64(bmeta.BinlogPos)}
	}
	return namesAtTarget(ctx, db, bmeta.CreateTableSQL, tm, atTarget, schema, table, asOf, cfg.At, anchor, cfg.cut)
}

// explainUnplacedColumns adds to a schema refusal why its added columns could
// not be left out for a target before they existed. The refusal keeps its
// text and its ErrSchemaChanged tag.
func explainUnplacedColumns(err error, why string) error {
	if err == nil || why == "" {
		return err
	}
	return fmt.Errorf("%w. A column added after the restore's target is left out only when the index proves it; here it does not: %s", err, why)
}

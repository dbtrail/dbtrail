package reconstruct

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/sysversion"
)

// expandSysVersionedKeys spells declared-key lookup values the way a MariaDB
// system-versioned table stores them (#2007).
//
// MariaDB appends ROW END to such a table's primary key, so a captured change
// is stored under "<declared key>|<row_end>" (at ROW END's position in the
// key, which need not be last), and an UPDATE or a versioned DELETE is keyed
// by its BEFORE image, whose row_end is the current-row marker. An operator
// types the declared key; this adds, for each value that holds exactly one
// component fewer than the stored key, that value with each current marker
// put back at ROW END's position. The values themselves stay in the list, so
// the expansion only ever adds candidates.
//
// It applies when the key has exactly one generated member and it is a
// TIMESTAMP (a transaction-precise table's ROW END is a transaction id, which
// has no marker to spell). History versions, keyed by a past row_end, are not
// matched; the readers that care skip them anyway.
func expandSysVersionedKeys(pkMetas []metadata.ColumnMeta, values []string) ([]string, bool) {
	endIdx := -1
	for i, c := range pkMetas {
		if !c.IsGenerated {
			continue
		}
		if endIdx >= 0 {
			return nil, false
		}
		endIdx = i
	}
	if endIdx < 0 || !strings.EqualFold(strings.TrimSpace(pkMetas[endIdx].DataType), "timestamp") {
		return nil, false
	}
	out := make([]string, 0, len(values)*3)
	expanded := false
	for _, v := range values {
		out = append(out, v)
		for _, m := range sysversion.CurrentMarkers() {
			if full, ok := event.InsertPKComponent(v, endIdx, len(pkMetas), m); ok {
				out = append(out, full)
				expanded = true
			}
		}
	}
	return out, expanded
}

// expandSysVersionedPKFilter rewrites opts' --pk/--pks values into the stored
// spellings of a system-versioned table, reading the table's key from the
// newest schema snapshot that describes it. It moves PKValues (and its
// escape alternate) into PKValuesIn, since the two are mutually exclusive.
//
// Best-effort and additive: a snapshot read that fails is logged and leaves
// the lookup as typed, which is what it was before #2007.
func expandSysVersionedPKFilter(ctx context.Context, db *sql.DB, opts *query.Options) {
	metas, err := snapshotPKShape(ctx, db, opts.Schema, opts.Table)
	if err != nil {
		slog.Warn("could not read the schema snapshot to tell whether the table is system-versioned; "+
			"the key is looked up as typed, which finds nothing for a system-versioned table",
			"schema", opts.Schema, "table", opts.Table, "error", err)
		return
	}
	values := opts.PKValuesIn
	if opts.PKValues != "" {
		values = []string{opts.PKValues}
		if opts.PKValuesAlt != "" {
			values = append(values, opts.PKValuesAlt)
		}
	}
	out, ok := expandSysVersionedKeys(metas, values)
	if !ok {
		return
	}
	opts.PKValues, opts.PKValuesAlt, opts.PKValuesIn = "", "", out
}

// snapshotPKShape reads the primary-key columns of schema.table (name, type,
// generated flag, in key order) from the newest schema snapshot describing
// it, in one query: this runs on every key lookup, and loading a whole
// resolver for it would read every table of the snapshot. No snapshot
// describing the table returns nil, nil.
func snapshotPKShape(ctx context.Context, db *sql.DB, schema, table string) ([]metadata.ColumnMeta, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT column_name, data_type, is_generated FROM schema_snapshots "+
			"WHERE schema_name = ? AND table_name = ? AND column_key = 'PRI' AND snapshot_id = "+
			"(SELECT MAX(snapshot_id) FROM schema_snapshots WHERE schema_name = ? AND table_name = ?) "+
			"ORDER BY ordinal_position",
		schema, table, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []metadata.ColumnMeta
	for rows.Next() {
		c := metadata.ColumnMeta{IsPK: true}
		if err := rows.Scan(&c.Name, &c.DataType, &c.IsGenerated); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

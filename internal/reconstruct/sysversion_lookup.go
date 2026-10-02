package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

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
// spellings of a system-versioned table. It moves PKValues (and its escape
// alternate) into PKValuesIn, since the two are mutually exclusive, keeps
// the typed values among the candidates, and records each added spelling in
// PKAliases.
//
// The key shape is the newest schema snapshot in which the table HAD a
// generated key member, not simply the newest: versioning dropped since
// leaves the events of its versioned period stored under the versioned key,
// and the typed values still cover the events from after the drop.
// Versioning added after the newest snapshot is not seen; taking a snapshot
// (which the stream does on the DDL) fixes that.
//
// A source recorded as MySQL or PostgreSQL has no system versioning, so the
// snapshot is not read. Best-effort and additive: a read that fails is
// logged and leaves the lookup as typed, which is what it was before #2007.
func expandSysVersionedPKFilter(ctx context.Context, db *sql.DB, opts *query.Options) {
	switch query.SourceFlavor(db) {
	case "mysql", "postgres":
		return
	}
	metas, err := versionedPKShape(ctx, db, opts.Schema, opts.Table)
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
	typed := make(map[string]bool, len(values))
	for _, v := range values {
		typed[v] = true
	}
	aliases := map[string]string{}
	for _, v := range values {
		for _, m := range sysversion.CurrentMarkers() {
			if full, ok := event.InsertPKComponent(v, sysVersionedEndIndex(metas), len(metas), m); ok && !typed[full] {
				aliases[full] = v
			}
		}
	}
	opts.PKValues, opts.PKValuesAlt, opts.PKValuesIn = "", "", out
	opts.PKAliases = aliases
}

// sysVersionedEndIndex is the position of the one generated key member, or
// -1 (expandSysVersionedKeys has already vetted the shape).
func sysVersionedEndIndex(metas []metadata.ColumnMeta) int {
	for i, c := range metas {
		if c.IsGenerated {
			return i
		}
	}
	return -1
}

// versionedPKShape reads the primary-key columns of schema.table (name, type,
// generated flag, in key order) from the newest schema snapshot in which the
// table's key had a generated member, or nil when none did. Two indexed
// reads on idx_table_snapshot (schema_name, table_name, snapshot_id): this
// runs on every key lookup of a MariaDB or unknown source.
func versionedPKShape(ctx context.Context, db *sql.DB, schema, table string) ([]metadata.ColumnMeta, error) {
	var maxID sql.NullInt64
	if err := db.QueryRowContext(ctx,
		"SELECT MAX(snapshot_id) FROM schema_snapshots WHERE schema_name = ? AND table_name = ? "+
			"AND column_key = 'PRI' AND is_generated = 1", schema, table).Scan(&maxID); err != nil {
		return nil, err
	}
	if !maxID.Valid {
		return nil, nil
	}
	rows, err := db.QueryContext(ctx,
		"SELECT column_name, data_type, is_generated FROM schema_snapshots "+
			"WHERE schema_name = ? AND table_name = ? AND snapshot_id = ? AND column_key = 'PRI' "+
			"ORDER BY ordinal_position", schema, table, maxID.Int64)
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

// versioningDDLRe matches the ALTER that turns system versioning on or off.
var versioningDDLRe = regexp.MustCompile(`(?i)\b(ADD|DROP)\s+SYSTEM\s+VERSIONING\b`)

// CheckSysVersioningChange refuses a single-row lookup over a window in which
// schema.table gained or lost MariaDB system versioning (#2007). Its changes
// are then stored under two key spellings (with and without ROW END) and
// read under two meanings, and a lookup shaped by one snapshot would miss
// the other period silently.
//
// Two signals, either enough: an ADD/DROP SYSTEM VERSIONING recorded in
// schema_changes inside [since, until], and the schema snapshots in effect at
// since and at until disagreeing on whether the key has a generated member
// (the signal a file-mode index, which records no DDL, still has). An index
// without schema_changes skips the first.
func CheckSysVersioningChange(ctx context.Context, db *sql.DB, schema, table string, since, until time.Time) error {
	refusal := func(why string) error {
		return fmt.Errorf("%s.%s %s between the snapshot this starts from (%s) and the target moment (%s), so its "+
			"changes are stored under two different keys in that window and a single-row lookup would miss part of "+
			"them; reconstruct the whole table, or start from a snapshot taken after the change: %w",
			schema, table, why, since.UTC().Format(time.RFC3339), until.UTC().Format(time.RFC3339), ErrSchemaChanged)
	}
	rows, err := db.QueryContext(ctx, `SELECT ddl_query FROM schema_changes
		WHERE (schema_name = ? OR schema_name = '') AND table_name = ? AND detected_at >= ? AND detected_at <= ?
		AND ddl_query LIKE '%SYSTEM%VERSIONING%'`, schema, table, since, until)
	if err != nil {
		var me *mysqldriver.MySQLError
		if !errors.As(err, &me) || me.Number != 1146 {
			return fmt.Errorf("check schema_changes for system versioning changes on %s.%s: %w", schema, table, err)
		}
	} else {
		defer rows.Close()
		for rows.Next() {
			var q string
			if err := rows.Scan(&q); err != nil {
				return err
			}
			if versioningDDLRe.MatchString(q) {
				return refusal("gained or lost system versioning")
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
	}
	_, before := GeneratedPKColumn(ResolvePKMetasAt(db, schema, table, since))
	_, after := GeneratedPKColumn(ResolvePKMetasAt(db, schema, table, until))
	if before != after {
		return refusal("changed its key shape (system versioning turned on or off)")
	}
	return nil
}

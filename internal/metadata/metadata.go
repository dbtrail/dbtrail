package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
)

// latestPerTableLoadTimeout bounds NewLatestPerTableResolver's union query.
// 15s sits comfortably under the shim's 30s resolverCacheTTL (the natural
// ceiling: a load slower than the TTL can never keep the cache fresh) while
// leaving generous headroom over the milliseconds a healthy index answers in
// — only a genuinely hung or drowning index DB trips it.
const latestPerTableLoadTimeout = 15 * time.Second

// ErrNoSnapshots signals "schema_snapshots is queryable but empty"
// — a benign first-install state, not a real failure. Callers that
// degrade gracefully when no snapshot exists (e.g. the shim's
// columnOrderFor falling back to alphabetical column ordering) can
// errors.Is against this sentinel to distinguish "operator hasn't
// run `bintrail snapshot` yet" from a genuine DB-side failure
// (table dropped post-upgrade, permissions revoked, connection
// lost) — which deserve a louder log channel.
//
// It is a comparable value, not an errors.New sentinel, so it can declare its
// usage-telemetry class (not_found: the snapshot the command needs does not
// exist yet) while errors.Is(err, ErrNoSnapshots) keeps working.
var ErrNoSnapshots error = noSnapshotsError{}

type noSnapshotsError struct{}

func (noSnapshotsError) Error() string { return "no snapshots found; run `bintrail snapshot` first" }

// TelemetryClass implements telemetry.Classed.
func (noSnapshotsError) TelemetryClass() string { return "not_found" }

// ─── Types ───────────────────────────────────────────────────────────────────

// ColumnMeta holds metadata for a single table column.
type ColumnMeta struct {
	Name            string
	OrdinalPosition int
	IsPK            bool
	DataType        string // e.g. "int", "datetime", "varchar" (information_schema.COLUMNS.DATA_TYPE)
	ColumnType      string // full type declaration, e.g. "int(11) unsigned", "datetime(6)" (COLUMN_TYPE). Empty on pre-#212 snapshots.
	IsGenerated     bool   // true for STORED or VIRTUAL generated columns
	// CharacterSet is information_schema.COLUMNS.CHARACTER_SET_NAME (#756).
	// Only meaningful for CHAR/VARCHAR — MySQL reports NULL for BINARY/
	// VARBINARY/BLOB/numeric columns (absorbed here as ""), and coerceTextEncoding
	// only consults it for those two types. Empty on pre-#756 snapshots (re-run
	// `bintrail snapshot` to capture it) and on PostgreSQL snapshots (#533 has no
	// MySQL-style charset concept), in which case an invalid-UTF-8 CHAR/VARCHAR
	// value cannot be safely transcoded and MapRow fails loud on it rather than
	// let json.Marshal replace it with U+FFFD.
	CharacterSet string
	// IsIdentityAlways marks a PostgreSQL GENERATED ALWAYS AS IDENTITY column (#557).
	// Recovery keeps it on a reverse-INSERT (with OVERRIDING SYSTEM VALUE) but omits
	// it from a reverse-UPDATE SET (PostgreSQL rejects SET on it). Always false for
	// MySQL/MariaDB snapshots (AUTO_INCREMENT accepts explicit values freely).
	IsIdentityAlways bool
}

// TableMeta holds the column mapping for a table, derived from a schema snapshot.
// Columns are in ordinal_position order (matching the binlog row value order).
type TableMeta struct {
	Schema    string
	Table     string
	Columns   []ColumnMeta // ordered by ordinal_position
	PKColumns []string     // PK column names in ordinal order
}

// PKColumnMetas returns the ColumnMeta entries for primary key columns,
// preserving their ordinal order. Used by BuildPKValues.
func (t *TableMeta) PKColumnMetas() []ColumnMeta {
	var pks []ColumnMeta
	for _, c := range t.Columns {
		if c.IsPK {
			pks = append(pks, c)
		}
	}
	return pks
}

// SnapshotStats is returned by TakeSnapshot with counts of what was captured.
type SnapshotStats struct {
	SnapshotID  int
	TableCount  int
	ColumnCount int
	FKCount     int
	// ExcludedTables lists "schema.table" names that failed validation
	// (non-InnoDB or no explicit primary key) and were left OUT of the
	// snapshot by TakeSnapshotExcludingInvalid (#1051). Sorted. Always nil
	// from the strict TakeSnapshot, which errors on the same condition.
	ExcludedTables []string
}

// ─── Resolver ────────────────────────────────────────────────────────────────

// Resolver provides table metadata lookups from a single schema snapshot.
// It holds the full snapshot in memory for fast per-event lookups during indexing.
type Resolver struct {
	snapshotID int
	// snapshotTime is the snapshot's creation time (schema_snapshots.
	// snapshot_time, stamped from the bintrail host clock by TakeSnapshot
	// and WritePGSnapshot). Zero = unknown: any constructor that does not
	// set it — NewResolverFromTables without the At variant — which the
	// #700 drift guard treats as strict. The guard uses it to tell a STALE
	// snapshot (event at-or-after the snapshot) from a routine HISTORICAL
	// event (before it).
	snapshotTime time.Time
	tables       map[string]*TableMeta // key: "schema.table"
	// excluded maps "schema.table" → validation reason for tables a DEGRADED
	// snapshot (TakeSnapshotExcludingInvalid, #1051) deliberately left out.
	// It lets consumers tell "excluded by validation — re-snapshotting
	// excludes it again, the table itself must change" apart from "absent
	// because the snapshot is stale — `bintrail snapshot` converges" (#1199).
	// Nil when the snapshot recorded no exclusions (strict snapshots always)
	// or the constructor has no index DB to read them from.
	excluded map[string]string
}

// NewResolver loads all table metadata for the given snapshot from the index
// database. Pass snapshotID=0 to load the most recent snapshot automatically.
func NewResolver(db *sql.DB, snapshotID int) (*Resolver, error) {
	if snapshotID == 0 {
		row := db.QueryRow("SELECT COALESCE(MAX(snapshot_id), 0) FROM schema_snapshots")
		if err := row.Scan(&snapshotID); err != nil {
			return nil, fmt.Errorf("failed to query latest snapshot ID: %w", err)
		}
		if snapshotID == 0 {
			return nil, ErrNoSnapshots
		}
	}

	// column_type was added in #212; existing snapshots may lack the column
	// entirely. COALESCE handles the post-ALTER default empty string; for
	// databases still on the pre-ALTER schema, the caller must run
	// indexer.EnsureSchema first.
	rows, err := db.Query(`
		SELECT schema_name, table_name, column_name, ordinal_position,
		       column_key, data_type, COALESCE(column_type, '') AS column_type,
		       is_generated, is_identity_always,
		       COALESCE(character_set_name, '') AS character_set_name
		FROM schema_snapshots
		WHERE snapshot_id = ?
		ORDER BY schema_name, table_name, ordinal_position`,
		snapshotID)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshot %d: %w", snapshotID, err)
	}
	defer rows.Close()

	r := &Resolver{snapshotID: snapshotID, tables: make(map[string]*TableMeta)}

	// Snapshot creation time (all rows of one snapshot share it; MIN is
	// defensive). Best-effort: a failure leaves it zero (unknown), which the
	// #700 drift guard treats as STRICT — silently flipping historical
	// events from warn-and-proceed to hard-error — so the cause must be in
	// the logs, never swallowed.
	var snapTime sql.NullTime
	if err := db.QueryRow(
		"SELECT MIN(snapshot_time) FROM schema_snapshots WHERE snapshot_id = ?",
		snapshotID).Scan(&snapTime); err != nil {
		slog.Warn("could not read the snapshot's creation time — the schema-drift guard will treat ALL diverging events as stale (hard error), including historical ones",
			"snapshot_id", snapshotID, "error", err)
	} else if snapTime.Valid {
		r.snapshotTime = snapTime.Time
	}

	// Validation exclusions of a degraded snapshot (#1051). Best-effort like
	// the snapshot time, but the failure must be in the logs: without them an
	// excluded table's skips are misdiagnosed as a stale snapshot, and file
	// mode marks files failed with a remediation ("run `bintrail snapshot`")
	// that cannot converge (#1199).
	if excluded, exErr := loadSnapshotExclusions(db, snapshotID); exErr != nil {
		slog.Warn("could not read snapshot_exclusions — skips on a validation-excluded table will be misdiagnosed as a stale snapshot (file mode may fail files with a non-converging remediation)",
			"snapshot_id", snapshotID, "error", exErr)
	} else {
		r.excluded = excluded
	}

	stats, err := scanSnapshotRows(rows, r.tables)
	if err != nil {
		return nil, err
	}

	// Pre-#212 snapshots have no column_type, so coerceUnsigned cannot tell which
	// integer columns are UNSIGNED and silently indexes them as-is. Warn once (not
	// per row) so an operator who upgraded for the unsigned fix (#490) isn't misled
	// into thinking a stale snapshot is corrected.
	//
	// Gate on sawDataType so this MySQL-only warning never fires for a PostgreSQL
	// snapshot (#533): WritePGSnapshot leaves both data_type AND column_type empty
	// (PG carries no MySQL DATA_TYPE/COLUMN_TYPE, and UNSIGNED sign-correction is a
	// MySQL-only concern coerceUnsigned never runs for PG rows). A genuine pre-#212
	// MySQL snapshot always has a non-empty data_type (from information_schema), so
	// it still trips the warning; only the all-empty-data_type PG signature is
	// suppressed.
	if len(r.tables) > 0 && stats.sawDataType && !stats.sawColumnType {
		slog.Warn("snapshot predates column_type capture (#212); UNSIGNED integer "+
			"columns cannot be sign-corrected and are indexed with the wrong value when "+
			"the high bit is set (unsigned PKs also corrupt pk_hash) — re-run "+
			"`bintrail snapshot` to enable the fix",
			"snapshot_id", snapshotID)
	}

	return r, nil
}

// snapshotScanStats is what scanSnapshotRows learned about the rows it
// consumed, feeding the callers' pre-#212 warnings.
type snapshotScanStats struct {
	sawColumnType bool // any row carried a non-empty column_type
	sawDataType   bool // any row carried a non-empty data_type
	// pre212Tables lists (sorted, "schema.table") the tables whose rows carry
	// a data_type but NO column_type anywhere — the pre-#212 MySQL-snapshot
	// signature, tracked PER TABLE so a mixed per-table-newest union (one
	// post-#212 table alongside a retained pre-#212 one) still names the
	// affected tables instead of one table's freshness silencing the rest.
	// All-empty-data_type tables (the PG signature, #533) are never listed.
	pre212Tables []string
}

// scanSnapshotRows consumes a schema_snapshots result set (the 10-column
// SELECT shared by NewResolver and NewLatestPerTableResolver) into tables,
// keyed "schema.table", and reports the pre-#212 signals callers warn on.
func scanSnapshotRows(rows *sql.Rows, tables map[string]*TableMeta) (snapshotScanStats, error) {
	var stats snapshotScanStats
	tableSawColumnType := make(map[string]bool)
	tableSawDataType := make(map[string]bool)
	dupRows := make(map[string]int) // "schema.table" → identical duplicate rows dropped

	for rows.Next() {
		var schemaName, tableName, columnName, columnKey, dataType, columnType, characterSet string
		var ordinalPosition int
		var isGenerated, isIdentityAlways bool

		if err := rows.Scan(&schemaName, &tableName, &columnName, &ordinalPosition, &columnKey, &dataType, &columnType, &isGenerated, &isIdentityAlways, &characterSet); err != nil {
			return stats, fmt.Errorf("failed to scan snapshot row: %w", err)
		}

		key := schemaName + "." + tableName
		tm, ok := tables[key]
		if !ok {
			tm = &TableMeta{Schema: schemaName, Table: tableName}
			tables[key] = tm
		}

		col := ColumnMeta{
			Name:             columnName,
			OrdinalPosition:  ordinalPosition,
			IsPK:             columnKey == "PRI",
			DataType:         dataType,
			ColumnType:       columnType,
			IsGenerated:      isGenerated,
			IsIdentityAlways: isIdentityAlways,
			CharacterSet:     characterSet,
		}
		// Duplicate (schema, table, ordinal_position) rows within one snapshot:
		// pre-#844 concurrent snapshot writers could share a snapshot_id and
		// re-insert every column (doubled or worse). Loading them verbatim
		// inflates the column count, and the parser's column-count guard then
		// skips every row event for the table ("column count mismatch") until
		// an operator re-snapshots (#1033). Rows arrive ordered by ordinal
		// (both callers' SELECTs ORDER BY ends with ordinal_position — any new
		// caller must preserve that), so duplicates are adjacent to the last
		// kept column.
		if n := len(tm.Columns); n > 0 && tm.Columns[n-1].OrdinalPosition == ordinalPosition {
			if tm.Columns[n-1] == col {
				dupRows[key]++
				continue
			}
			return stats, fmt.Errorf("snapshot is corrupt: %s has two different columns at ordinal_position %d (%q %s vs %q %s) — re-run `bintrail snapshot` to write a clean snapshot; if the table no longer exists at the source, delete that snapshot's rows from schema_snapshots instead",
				key, ordinalPosition, tm.Columns[n-1].Name, tm.Columns[n-1].ColumnType, columnName, columnType)
		}

		if columnType != "" {
			stats.sawColumnType = true
			tableSawColumnType[key] = true
		}
		if dataType != "" {
			stats.sawDataType = true
			tableSawDataType[key] = true
		}
		tm.Columns = append(tm.Columns, col)
		if col.IsPK {
			tm.PKColumns = append(tm.PKColumns, columnName)
		}
	}

	if err := rows.Err(); err != nil {
		return stats, fmt.Errorf("failed to iterate snapshot rows: %w", err)
	}

	for key := range tableSawDataType {
		if !tableSawColumnType[key] {
			stats.pre212Tables = append(stats.pre212Tables, key)
		}
	}
	sort.Strings(stats.pre212Tables)

	if len(dupRows) > 0 {
		names := make([]string, 0, len(dupRows))
		total := 0
		for k, n := range dupRows {
			names = append(names, k)
			total += n
		}
		sort.Strings(names)
		const nameCap = 20
		if len(names) > nameCap {
			names = names[:nameCap]
		}
		slog.Warn("snapshot contains duplicated column rows (typically pre-#844 concurrent snapshot writers); "+
			"loaded deduplicated — re-run `bintrail snapshot` to write a clean snapshot",
			"duplicate_rows", total,
			"table_count", len(dupRows),
			"tables", strings.Join(names, ", "))
	}

	return stats, nil
}

// NewLatestPerTableResolver loads, for EVERY (schema, table) present anywhere
// in schema_snapshots, that table's NEWEST snapshot rows — a whole-schema
// union view that is correct for both snapshot layouts (#603):
//
//   - MySQL/MariaDB: TakeSnapshot writes the whole schema under one
//     snapshot_id, so for every table present in the latest snapshot the
//     per-table-newest rows ARE the latest snapshot — a strict
//     generalization of NewResolver(db, 0).
//   - PostgreSQL: WritePGSnapshot writes ONE table per snapshot_id (a fresh
//     MAX+1 on every pgoutput RelationMessage), so "latest snapshot" is just
//     the last table that saw DML. The per-table-newest union is the only
//     whole-schema view a PG index has.
//
// Deliberate semantic (both source families): a table that appears ONLY in
// older snapshots — dropped (or renamed) at the source and re-snapshotted
// since — is still included, under its last-known shape. Its indexed history
// remains addressable (`SELECT * FROM _flashback.dropped AS OF <ts>` works
// off binlog_events, not the live schema), so hiding it from SHOW TABLES /
// column-order lookups would be the table-level analog of the dropped-column
// view bug fixed in #600. A re-created table resolves to its newest shape
// (last-write-wins per table).
//
// Scope: read/list surfaces only (the shim's SHOW TABLES, columnOrderFor and
// PK validation). Do NOT hand this resolver to the indexing/stream paths:
// SnapshotID() is 0 (the union spans many snapshot_ids, so there is no
// single id to stamp schema_version with) and SnapshotTime() is zero, which
// the #700 drift guard treats as strict.
//
// Returns ErrNoSnapshots when schema_snapshots is empty — same benign
// first-install sentinel as NewResolver.
func NewLatestPerTableResolver(db *sql.DB) (*Resolver, error) {
	// Bound the load: schema_snapshots grows without bound on a PG source (a
	// fresh snapshot per RelationMessage per stream restart), and the shim's
	// FIRST load after start runs inline in a customer's connection — an
	// unbounded query against a hung/slow index DB would freeze that mysql
	// session with nothing logged. On timeout we fail loud; the shim's
	// resolverCache surfaces the error with attribution (or serves its
	// sticky stale copy on later refreshes).
	ctx, cancel := context.WithTimeout(context.Background(), latestPerTableLoadTimeout)
	defer cancel()

	// The derived table groups on (schema_name, table_name) — not a leftmost
	// prefix of idx_snapshot_table (snapshot_id, schema_name, table_name), so
	// MySQL does a covering scan of that index plus a temp-table group-by.
	// Acceptable: callers cache the resolver (the shim's 30s resolverCache),
	// so the scan cost is bounded per TTL window, not per query.
	rows, err := db.QueryContext(ctx, `
		SELECT s.schema_name, s.table_name, s.column_name, s.ordinal_position,
		       s.column_key, s.data_type, COALESCE(s.column_type, '') AS column_type,
		       s.is_generated, s.is_identity_always,
		       COALESCE(s.character_set_name, '') AS character_set_name
		FROM schema_snapshots s
		JOIN (
			SELECT schema_name, table_name, MAX(snapshot_id) AS snapshot_id
			FROM schema_snapshots
			GROUP BY schema_name, table_name
		) latest
		  ON latest.schema_name = s.schema_name
		 AND latest.table_name  = s.table_name
		 AND latest.snapshot_id = s.snapshot_id
		ORDER BY s.schema_name, s.table_name, s.ordinal_position`)
	if err != nil {
		return nil, fmt.Errorf("failed to query per-table-newest snapshots: %w", err)
	}
	defer rows.Close()

	r := &Resolver{tables: make(map[string]*TableMeta)}
	stats, err := scanSnapshotRows(rows, r.tables)
	if err != nil {
		return nil, err
	}
	if len(r.tables) == 0 {
		return nil, ErrNoSnapshots
	}

	// Same pre-#212 stale-snapshot warning as NewResolver (see the rationale
	// there, including the PG all-empty-data_type suppression from #533), but
	// evaluated PER TABLE: the union can retain an old table's pre-#212 shape
	// next to freshly-snapshotted post-#212 tables, and a global saw-flag
	// would let the fresh ones silence the warning. Name the affected tables
	// (capped) so the operator knows what to re-snapshot or stop trusting.
	if n := len(stats.pre212Tables); n > 0 {
		names := stats.pre212Tables
		const nameCap = 20
		if len(names) > nameCap {
			names = names[:nameCap]
		}
		slog.Warn("some tables' newest snapshot predates column_type capture (#212); their "+
			"UNSIGNED integer columns cannot be sign-corrected and are indexed with the wrong "+
			"value when the high bit is set (unsigned PKs also corrupt pk_hash) — re-run "+
			"`bintrail snapshot` to enable the fix",
			"table_count", n,
			"tables", strings.Join(names, ", "))
	}

	return r, nil
}

// NewResolverFromTables creates a Resolver directly from a pre-built table map.
// The map key must be "schema.table". Primarily useful for testing. The
// snapshot time is left zero (unknown) — use NewResolverFromTablesAt to set it.
func NewResolverFromTables(snapshotID int, tables map[string]*TableMeta) *Resolver {
	return &Resolver{snapshotID: snapshotID, tables: tables}
}

// NewResolverFromTablesAt is NewResolverFromTables with an explicit snapshot
// creation time, for tests exercising the #700 historical-event distinction.
func NewResolverFromTablesAt(snapshotID int, snapshotTime time.Time, tables map[string]*TableMeta) *Resolver {
	return &Resolver{snapshotID: snapshotID, snapshotTime: snapshotTime, tables: tables}
}

// NewResolverFromTablesAtExcluding is NewResolverFromTablesAt with the
// snapshot's validation exclusions ("schema.table" → reason, #1051), for tests
// exercising the excluded-table skip diagnosis (#1199). Production resolvers
// load exclusions from snapshot_exclusions in NewResolver.
func NewResolverFromTablesAtExcluding(snapshotID int, snapshotTime time.Time, tables map[string]*TableMeta, excluded map[string]string) *Resolver {
	return &Resolver{snapshotID: snapshotID, snapshotTime: snapshotTime, tables: tables, excluded: excluded}
}

// SnapshotID returns the snapshot ID this resolver was loaded from.
func (r *Resolver) SnapshotID() int { return r.snapshotID }

// SnapshotTime returns the snapshot's creation time; zero when unknown
// (resolvers built by NewResolverFromTables without an explicit time).
func (r *Resolver) SnapshotTime() time.Time { return r.snapshotTime }

// TableCount returns the number of tables in this resolver.
func (r *Resolver) TableCount() int { return len(r.tables) }

// Tables returns every TableMeta whose schema matches the argument,
// sorted by table name for deterministic output. Used by the shim to
// answer SHOW TABLES FROM _flashback/_diff/_snapshot (#315) and by
// any future caller needing a list view of the snapshot.
//
// Returns an empty slice (not nil) when the schema is unknown — that's
// the same shape MySQL itself returns for SHOW TABLES FROM <empty db>,
// so callers can treat it as "nothing to display" without a nil check.
func (r *Resolver) Tables(schema string) []*TableMeta {
	out := make([]*TableMeta, 0)
	for _, t := range r.tables {
		if t.Schema == schema {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Table < out[j].Table })
	return out
}

// AllTables returns every TableMeta in the snapshot, sorted by schema then
// table for deterministic output. Used by verify's baseline-anchored mode to
// enumerate the full table universe, so a snapshot table with no baseline
// surfaces in the report instead of silently producing no row.
func (r *Resolver) AllTables() []*TableMeta {
	out := make([]*TableMeta, 0, len(r.tables))
	for _, t := range r.tables {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Schema != out[j].Schema {
			return out[i].Schema < out[j].Schema
		}
		return out[i].Table < out[j].Table
	})
	return out
}

// Resolve returns metadata for a given schema.table.
// Returns an error if the table is not found in the snapshot.
func (r *Resolver) Resolve(schema, table string) (*TableMeta, error) {
	key := schema + "." + table
	tm, ok := r.tables[key]
	if !ok {
		// A validation-excluded table (#1051) is absent ON PURPOSE, and
		// re-snapshotting excludes it again — suggesting `bintrail snapshot`
		// here would send the operator down a remediation that can never
		// converge (#1199). Tell the truth: the table itself must change.
		if reason, excludedByValidation := r.excluded[key]; excludedByValidation {
			return nil, fmt.Errorf("table %s.%s is excluded from snapshot %d (%s) — the table is not capturable as-is and re-snapshotting excludes it again; to capture it, add an explicit primary key and use InnoDB, then re-run `bintrail snapshot`; otherwise leave it out of --schemas/--tables",
				schema, table, r.snapshotID, reason)
		}
		if r.snapshotID == 0 {
			// Per-table-newest union resolvers (NewLatestPerTableResolver)
			// span many snapshot_ids; "snapshot 0" would misread as a real id.
			return nil, fmt.Errorf("table %s.%s not found in any schema snapshot; consider re-running `bintrail snapshot`",
				schema, table)
		}
		return nil, fmt.Errorf("table %s.%s not found in snapshot %d; consider re-running `bintrail snapshot`",
			schema, table, r.snapshotID)
	}
	return tm, nil
}

// ExclusionReason reports whether schema.table was deliberately excluded from
// this resolver's snapshot by validation (no explicit PK / non-InnoDB,
// TakeSnapshotExcludingInvalid #1051) and, if so, why. Consumers use it to
// tell a non-converging permanent skip apart from a stale snapshot (#1199):
// for an excluded table, re-running `bintrail snapshot` excludes it again.
func (r *Resolver) ExclusionReason(schema, table string) (string, bool) {
	reason, ok := r.excluded[schema+"."+table]
	return reason, ok
}

// MapRow maps a binlog row ([]any in column ordinal order) to a named
// map using column metadata from the snapshot. The binlog provides values by
// position — MapRow attaches the correct column names.
//
// Returns an error if the row length does not match the snapshot column count.
func (r *Resolver) MapRow(schema, table string, row []any) (map[string]any, error) {
	tm, err := r.Resolve(schema, table)
	if err != nil {
		return nil, err
	}
	if len(row) != len(tm.Columns) {
		return nil, fmt.Errorf(
			"column count mismatch for %s.%s: binlog has %d columns, snapshot has %d — consider re-running `bintrail snapshot`",
			schema, table, len(row), len(tm.Columns))
	}
	named := make(map[string]any, len(row))
	for i, col := range tm.Columns {
		v, err := coerceTextEncoding(coerceUnsigned(row[i], col), col)
		if err != nil {
			return nil, fmt.Errorf("%s.%s.%s: %w", schema, table, col.Name, err)
		}
		named[col.Name] = v
	}
	return named, nil
}

// coerceTextEncoding closes the byte-corruption gap left by go-mysql's
// no-transcoding delivery of BINARY/VARBINARY/CHAR/VARCHAR (#756): go-mysql
// hands back the column's exact source bytes as a Go string, with no charset
// applied. marshalRow then JSON-marshals that string, and encoding/json
// silently replaces every invalid-UTF-8 byte with U+FFFD instead of
// erroring — silent, at-rest data loss. Two distinct fixes, by DataType:
//
//   - BINARY/VARBINARY: reinterpreted as []byte, which routes the value
//     through marshalRow's existing []byte-to-base64 path — the same one
//     BLOB/TEXT already use (base64StoredKind in the recover/reconstruct/shim
//     decode paths is updated alongside this to recognize the two new
//     DataTypes). Byte-perfect regardless of content, since a BINARY/
//     VARBINARY value (an MD5 digest, a binary UUID...) has no text
//     semantics to preserve.
//   - CHAR/VARCHAR: passed through unchanged when already valid UTF-8 — the
//     overwhelming common case (utf8/utf8mb4/ascii columns, and any other
//     charset whose actual bytes happen to be 7-bit ASCII). An invalid-UTF-8
//     value is transcoded from latin1 when the snapshot recorded that
//     charset — MySQL's "latin1" is actually cp1252/Windows-1252, not the
//     ISO-8859-1 the name suggests (documented in the MySQL reference manual's
//     West European character set chapter) — or rejected with an error for
//     any other/unknown charset (including a
//     pre-#756 snapshot, which never captured CharacterSet): MapRow fails the
//     row rather than let json.Marshal corrupt it. Callers (parser.go's
//     emitInserts/emitUpdates/emitDeletes) already warn-and-skip on a MapRow
//     error, the same handling as a column-count mismatch, so this turns
//     silent corruption into a loud, actionable warning instead.
//
// TEXT/BLOB/JSON/GEOMETRY are unaffected: go-mysql already delivers those as
// []byte, so they never reach this function as a string.
//
// Residual, accepted ambiguity (same class as marshalRow's
// looksLikeJSONContainer gate, and #736's bool/json.Number repair): a
// latin1/cp1252 value whose raw bytes happen to ALSO form valid UTF-8 (e.g.
// latin1 bytes 0xC3 0xA9, which decode as UTF-8 "é") passes the
// utf8.ValidString check and is left as-is — silently misread as the
// wrong text, with no error, since bintrail cannot tell "genuinely UTF-8"
// from "coincidentally valid UTF-8" from the bytes alone. Only reachable
// content-sniffing, not a per-value type tag, distinguishes the two; a full
// fix would need to carry the source encoding out-of-band, which is out of
// scope for #756's reported corruption class (the common case — genuinely
// mis-set charsets producing INVALID UTF-8 — is what this function fixes).
// Charset support beyond latin1 is deliberately out of scope too: the issue
// this closes only reports latin1, and its accepted "at minimum" fallback is
// exactly the fail-loud default branch below — not silent corruption, and
// not a guess at an unverified charset.
func coerceTextEncoding(v any, col ColumnMeta) (any, error) {
	switch strings.ToLower(col.DataType) {
	case "binary", "varbinary":
		if s, ok := v.(string); ok {
			return []byte(s), nil
		}
		return v, nil
	case "char", "varchar":
		s, ok := v.(string)
		if !ok || utf8.ValidString(s) {
			return v, nil
		}
		switch strings.ToLower(col.CharacterSet) {
		case "latin1":
			decoded, err := charmap.Windows1252.NewDecoder().String(s)
			if err != nil {
				return nil, fmt.Errorf("failed to transcode latin1 (cp1252) value: %w", err)
			}
			// charmap.Windows1252's decoder is total — it never returns a
			// non-nil error, even for the 5 cp1252 code points cp1252 itself
			// leaves undefined (0x81, 0x8D, 0x8F, 0x90, 0x9D): those silently
			// decode to U+FFFD instead. Left unchecked, that's the exact
			// silent-corruption failure mode #756 exists to close, just
			// narrowed to 5 specific bytes. A genuine latin1/cp1252 value
			// never legitimately contains U+FFFD (MySQL's latin1 has no
			// character at those 5 positions either), so its presence here
			// means the source byte was one of the 5 undefined points, not a
			// real transcoding — fail loud instead of embedding it.
			if strings.ContainsRune(decoded, utf8.RuneError) {
				return nil, fmt.Errorf(
					"value contains a byte with no latin1 (cp1252) character assignment — cannot transcode without further corruption")
			}
			return decoded, nil
		case "":
			return nil, fmt.Errorf(
				"value is not valid UTF-8 and the schema snapshot has no captured character set for this column (pre-#756 snapshot) — re-run `bintrail snapshot` to enable safe decoding")
		default:
			return nil, fmt.Errorf(
				"value is not valid UTF-8 under charset %q, which bintrail cannot yet safely transcode (only latin1 is supported today) — indexing it as-is would silently corrupt it",
				col.CharacterSet)
		}
	default:
		return v, nil
	}
}

// coerceUnsigned reinterprets an integer value decoded by go-mysql into the
// correct unsigned value. go-mysql always decodes INT-family columns as SIGNED
// (int8/int16/int32/int64) — it parses the TABLE_MAP SIGNEDNESS bitmap but never
// applies it (UnsignedMap is used only in its Dump output). So a column declared
// UNSIGNED whose value has the high bit set comes back negative (e.g.
// BIGINT UNSIGNED 18446744073709551615 → int64(-1)). Left uncorrected, the wrong
// value lands in the index and, for unsigned PKs, corrupts pk_values/pk_hash.
//
// Signedness is taken from the snapshot's ColumnType ("... unsigned"), NOT the
// TABLE_MAP SIGNEDNESS bitmap: coerceUnsigned runs inside MapRow, which works
// only off the resolver's snapshot and never sees the live TableMapEvent, so the
// bitmap is not reachable at this layer (and go-mysql would not apply it anyway —
// see above). ColumnType is always loaded by the resolver, independent of the
// source's binlog_row_metadata setting. Width is taken from DataType so the
// reinterpretation masks to the column's real size — MEDIUMINT is 3 bytes but
// go-mysql returns it as int32, so it must be masked to 24 bits (else
// MEDIUMINT UNSIGNED -1 would become 2^32-1 instead of 2^24-1).
//
// No-op when the column is not unsigned, when ColumnType is empty (pre-#212
// snapshots can't express signedness — NewResolver warns once in that case), or
// when the value is not a signed integer (NULL, string, and DECIMAL/FLOAT/DOUBLE
// UNSIGNED — which go-mysql returns as string/float — are returned unchanged).
// BIT and SET are also reinterpreted here: go-mysql decodes both as a signed
// int64, so a BIT(64) — or a 64-member SET with member 64 active — comes back
// negative; both are mapped to uint64 (#497, #846).
func coerceUnsigned(v any, col ColumnMeta) any {
	// BIT is an unsigned bit string and SET an unsigned member bitmask. go-mysql
	// decodes both as int64 (littleDecodeBit), so BIT(64) — or a SET of exactly
	// 64 members with member 64 active — comes back negative; reinterpret as
	// uint64 — identity for smaller widths (the value is non-negative as int64,
	// so uint64() preserves it). Neither ColumnType contains "unsigned"
	// ("bit(N)" / "set('a',…)"), so handle them before the unsigned gate below.
	// BIT was fixed in #497; SET is the same class (#846).
	switch strings.ToLower(col.DataType) {
	case "bit", "set":
		if i, ok := v.(int64); ok {
			return uint64(i)
		}
		// A NULL arrives as nil and passes through here. Otherwise go-mysql
		// always decodes BIT/SET as int64, so a non-nil non-int64 value can't
		// occur today; if a future go-mysql/MariaDB path delivered []byte/string,
		// leave it uninterpreted rather than mis-coerce — the original value.
		return v
	}
	if !strings.Contains(strings.ToLower(col.ColumnType), "unsigned") {
		return v
	}
	var signed int64
	switch x := v.(type) {
	case int8:
		signed = int64(x)
	case int16:
		signed = int64(x)
	case int32:
		signed = int64(x)
	case int64:
		signed = x
	default:
		return v // not a signed integer (NULL/string/decimal/...) — leave as-is
	}
	switch strings.ToLower(col.DataType) {
	case "tinyint":
		return uint8(signed)
	case "smallint":
		return uint16(signed)
	case "mediumint":
		return uint32(signed) & 0xFFFFFF
	case "int":
		return uint32(signed)
	case "bigint":
		return uint64(signed)
	default:
		return v
	}
}

// ─── PostgreSQL schema oracle (#533) ─────────────────────────────────────────

// PGRelationColumn is one column of a PostgreSQL relation's shape, decoded in-band
// from a pgoutput RelationMessage (no information_schema). Ordinal is the 1-based
// table-column position; IsPK marks a primary-key column; TypeOID/TypeMod are the
// pg_type OID and atttypmod, persisted now for the (deferred #533) type-faithful
// renderer even though slice-1 stores values as text and does not read them.
type PGRelationColumn struct {
	Name    string
	Ordinal int
	IsPK    bool
	TypeOID uint32
	TypeMod int32
	// IsIdentityAlways = GENERATED ALWAYS AS IDENTITY; IsGenerated = STORED generated
	// column. From a catalog lookup (the RelationMessage carries neither); they drive
	// #557 recovery (OVERRIDING SYSTEM VALUE + the per-operation skip-sets). The two are
	// mutually exclusive (a column is identity OR generated, never both).
	IsIdentityAlways bool
	IsGenerated      bool
}

// PGRelationSchema is a PostgreSQL relation's shape as seen on the logical-
// replication stream — the in-band, source-neutral payload an event.EventRelation
// carries from the pgcapture decoder to the consumer, which persists it via
// WritePGSnapshot. Columns are in table-ordinal order (so a primary key declared
// out of column order still yields ordinal-order pk_values matching the resolver —
// the pgcapture decoder reorders its catalog-key-order PK to match).
//
// It lives here, not in internal/event, because event imports metadata (so the
// reverse would be an import cycle) and because WritePGSnapshot — its only
// consumer — belongs next to TakeSnapshot.
type PGRelationSchema struct {
	Schema  string
	Table   string
	Columns []PGRelationColumn // table-ordinal order
}

// WritePGSnapshot persists one PostgreSQL relation's shape as a schema_snapshots
// snapshot and returns the allocated snapshot_id. It is the PostgreSQL,
// stream-time sibling of TakeSnapshot: where TakeSnapshot reads MySQL
// information_schema for a whole schema, WritePGSnapshot takes one relation's
// in-band shape (decoded from a pgoutput RelationMessage) so the offline recover
// path can build a PK-scoped WHERE without a live PostgreSQL connection — closing
// the remainder of #531 (PK-aware recovery on the PG path).
//
// Each call writes ONE table under a fresh snapshot_id (MAX+1), unlike MySQL where
// one snapshot covers a whole schema: PostgreSQL relations arrive one
// RelationMessage at a time, interleaved with rows, so each is its own snapshot and
// each PG row is stamped (by the consumer) with its table's snapshot_id.
// Consequence to respect in later slices: NewResolver(db, 0) (latest) yields a
// SINGLE table for a PG index, not a whole-schema view — recovery is unaffected (it
// loads each row's own snapshot_id), but a whole-schema consumer (console Tables(),
// shim SHOW TABLES) must not assume MAX(snapshot_id) is the full schema; use
// NewLatestPerTableResolver instead (#603 — the shim does).
//
// PG columns leave the MySQL-only fields empty/NULL: data_type and is_nullable are
// the empty string (both NOT NULL columns, so empty string not NULL), column_type and
// column_default NULL, is_generated 0. The PostgreSQL type identity rides the nullable
// pg_type_oid/pg_type_mod columns for the deferred type-faithful renderer.
// Every write here is a bounded autocommit statement carrying the caller's ctx
// (indexer.WriteTimeout) — ensureSnapshotIDSeqTable's checks, allocateSnapshotID's
// counter INSERT, and the single multi-row schema INSERT — so a mid-statement
// stall on the index link is cut at that deadline rather than freezing the PG
// stream loop on kernel TCP retransmission (~13-16 min) (#959). No explicit
// transaction is used, deliberately: the counter INSERT is concurrency-safe on
// its own (AUTO_INCREMENT, and its value is never reclaimed on failure anyway)
// and the schema rows are one atomic multi-row INSERT, so a tx bought no
// atomicity — only an unbounded tx.Commit() round-trip (database/sql has no
// CommitContext; BeginTx's ctx watcher is per-statement, not per-commit) that
// reopened exactly this freeze window.
func WritePGSnapshot(ctx context.Context, db *sql.DB, rel *PGRelationSchema) (int, error) {
	if rel == nil || len(rel.Columns) == 0 {
		return 0, fmt.Errorf("metadata: WritePGSnapshot requires a relation with at least one column")
	}

	if err := ensureSnapshotIDSeqTable(ctx, db); err != nil {
		return 0, err
	}

	// Allocate the next snapshot_id from the dedicated AUTO_INCREMENT counter
	// (MySQL and PG snapshots coexist in one table with distinct ids; see
	// DDLSnapshotIDSeq for why this beats a MAX(snapshot_id)+1 FOR UPDATE read,
	// #844). Autocommit is safe: LastInsertId comes from this INSERT's own OK
	// packet, with no dependency on a follow-up read hitting the same connection.
	nextID, err := allocateSnapshotID(ctx, db)
	if err != nil {
		return 0, fmt.Errorf("metadata: WritePGSnapshot allocate snapshot_id: %w", err)
	}

	snapshotTime := time.Now().UTC()
	valClause := strings.TrimRight(strings.Repeat("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?),", len(rel.Columns)), ",")
	insertSQL := "INSERT INTO schema_snapshots " +
		"(snapshot_id, snapshot_time, schema_name, table_name, column_name, " +
		"ordinal_position, column_key, data_type, column_type, is_nullable, column_default, is_generated, " +
		"pg_type_oid, pg_type_mod, is_identity_always) VALUES " + valClause

	args := make([]any, 0, len(rel.Columns)*15)
	for _, c := range rel.Columns {
		columnKey := ""
		if c.IsPK {
			columnKey = "PRI"
		}
		args = append(args,
			nextID, snapshotTime, rel.Schema, rel.Table, c.Name,
			c.Ordinal, columnKey, "", nil, "", nil, c.IsGenerated,
			c.TypeOID, c.TypeMod, c.IsIdentityAlways,
		)
	}
	if _, err = db.ExecContext(ctx, insertSQL, args...); err != nil {
		return 0, fmt.Errorf("metadata: WritePGSnapshot insert %s.%s: %w", rel.Schema, rel.Table, err)
	}
	return nextID, nil
}

// ─── TakeSnapshot ────────────────────────────────────────────────────────────

// columnRow holds a single row from information_schema.COLUMNS as fetched by TakeSnapshot.
type columnRow struct {
	schemaName, tableName, columnName string
	ordinalPosition                   int
	columnKey, dataType, isNullable   string
	columnType                        string // full COLUMN_TYPE (e.g. "datetime(6)"); needed by full-table reconstruct for PK precision
	generationExpression              sql.NullString
	columnDefault                     sql.NullString
	// characterSet is CHARACTER_SET_NAME (#756): NULL for BINARY/VARBINARY/BLOB/
	// numeric columns, populated for CHAR/VARCHAR/TEXT. Only CHAR/VARCHAR consult
	// it (coerceTextEncoding) — it enables safe latin1-to-UTF8 transcoding of an
	// invalid-UTF-8 value instead of json.Marshal silently corrupting it to U+FFFD.
	characterSet sql.NullString
}

// fkRow holds a single foreign key column mapping as fetched from
// INFORMATION_SCHEMA.KEY_COLUMN_USAGE joined with REFERENTIAL_CONSTRAINTS.
type fkRow struct {
	constraintName       string
	schemaName           string
	tableName            string
	columnName           string
	ordinalPosition      int
	referencedSchemaName string
	referencedTableName  string
	referencedColumnName string
	deleteRule           string // ON DELETE rule (CASCADE/RESTRICT/SET NULL/NO ACTION)
	updateRule           string // ON UPDATE rule
}

// TakeSnapshot reads column metadata and foreign key constraints from
// information_schema on the source server and writes them atomically into
// schema_snapshots and fk_constraints in the index database.
//
// If schemas is empty, all non-system schemas are captured. The new snapshot_id
// is allocated inside the transaction from the dedicated snapshot_id_seq
// AUTO_INCREMENT counter table (see DDLSnapshotIDSeq), so concurrent snapshot
// writers (watch DDL hook, manual snapshot, console baseline trigger) can't
// merge their rows under one id (#844) — and, unlike an earlier
// MAX(snapshot_id)+1 FOR UPDATE design, can't deadlock each other either.
func TakeSnapshot(sourceDB, indexDB *sql.DB, schemas []string) (SnapshotStats, error) {
	return takeSnapshot(sourceDB, indexDB, schemas, false)
}

// addImplicitPeriodColumns appends synthetic columnRow entries for the HIDDEN
// system-versioning period columns of MariaDB tables versioned WITHOUT
// explicitly declared period columns (#1272) — `CREATE TABLE t (...) WITH
// SYSTEM VERSIONING`, the shortest spelling.
//
// Why this must exist: for the implicit form, information_schema.COLUMNS does
// not list row_start/row_end at all, but the binlog ROW image carries them —
// so a snapshot built from COLUMNS alone undercounts the table by two and the
// indexer's column-count-mismatch guard silently skips EVERY row event of the
// table (zero deltas indexed; a full-table reconstruct then returns
// baseline-only state with no refusal anywhere). Synthesizing the two columns
// makes the implicit form behave exactly like the explicit one: capture
// works, pk_values carries the extended key, and the #1266 generated-PK gates
// fire where they must.
//
// Every synthesized fact below is empirical, measured on MariaDB 11.4:
//
//   - Detection: information_schema.TABLES reports TABLE_TYPE = 'SYSTEM
//     VERSIONED' (MySQL never emits this value, so the whole function is a
//     no-op against a MySQL source).
//   - The hidden columns are named row_start/row_end — the TABLE_MAP optional
//     metadata (binlog_row_metadata=FULL) names them exactly that, so the
//     #700 drift guard agrees with the synthesized names; a user column named
//     row_start on an implicitly versioned table is impossible (MariaDB
//     refuses it with ER_DUP_FIELDNAME), so the names cannot collide.
//   - They are TIMESTAMP(6) NOT NULL, and they sit LAST in the row image —
//     ordinals max+1/max+2 — a position they keep even after ALTER TABLE ADD
//     COLUMN (the new visible column lands BEFORE them).
//   - KEY_COLUMN_USAGE shows the server extends the PRIMARY KEY with row_end
//     (never row_start), same as the explicit form (#1266).
//
// The synthetic rows carry GENERATION_EXPRESSION = "ROW START"/"ROW END" — the
// literal MariaDB stores for the explicit form — so the existing is_generated
// derivation at the insert (and everything downstream of it: recovery's
// generated-column skip, the #1266 gates) treats implicit and explicit period
// columns identically, with no new code path.
//
// An explicitly-versioned table (its period columns already present in
// COLUMNS with the ROW START/ROW END expressions, #863) is detected and left
// untouched. Runs after the #1051 exclusion filter on purpose: an excluded
// table has no rows in `columns`, is skipped as unseen, and must not re-enter
// the snapshot through synthesis. The versioned list comes from the SAME
// information_schema.TABLES scan validation runs (invalidTables) — versioned
// tables no longer bypass the InnoDB/no-PK checks, and detection costs no
// extra source round trip.
func addImplicitPeriodColumns(columns []columnRow, versioned []tableRef) []columnRow {
	if len(versioned) == 0 {
		return columns
	}
	type periodScan struct {
		seen     bool
		explicit bool
		hasPK    bool
		maxOrd   int
	}
	scans := make(map[string]*periodScan, len(versioned))
	for _, ref := range versioned {
		scans[ref.schema+"."+ref.table] = &periodScan{}
	}
	for _, c := range columns {
		s, ok := scans[c.schemaName+"."+c.tableName]
		if !ok {
			continue
		}
		s.seen = true
		if c.columnKey == "PRI" {
			s.hasPK = true
		}
		if c.generationExpression.Valid {
			switch strings.ToUpper(strings.TrimSpace(c.generationExpression.String)) {
			case "ROW START", "ROW END":
				s.explicit = true
			}
		}
		if c.ordinalPosition > s.maxOrd {
			s.maxOrd = c.ordinalPosition
		}
	}
	// Slice order = the detection query's ORDER BY: deterministic snapshot
	// content across runs without a re-sort.
	for _, ref := range versioned {
		s := scans[ref.schema+"."+ref.table]
		if !s.seen || s.explicit {
			continue
		}
		rowEndKey := ""
		if s.hasPK {
			// MariaDB only EXTENDS an existing PK with row_end; a PK-less
			// versioned table gets no key at all. Validation refuses (strict)
			// or excludes (#1051) PK-less tables before this runs, so a
			// !hasPK table should never reach here — but stamping "PRI"
			// unconditionally would FABRICATE a one-column generated PK whose
			// sentinel row_end ('2038-01-19 03:14:07.999999' on every live
			// row) collapses the whole table onto one pk_values, making a
			// reversal's WHERE match every live row. Belt, not the gate.
			rowEndKey = "PRI"
		}
		columns = append(columns,
			columnRow{
				schemaName: ref.schema, tableName: ref.table, columnName: implicitRowStartName,
				ordinalPosition: s.maxOrd + 1, dataType: implicitPeriodDataType,
				columnType: implicitPeriodColumnType, isNullable: "NO",
				generationExpression: sql.NullString{Valid: true, String: "ROW START"},
			},
			columnRow{
				schemaName: ref.schema, tableName: ref.table, columnName: implicitRowEndName,
				ordinalPosition: s.maxOrd + 2, columnKey: rowEndKey, dataType: implicitPeriodDataType,
				columnType: implicitPeriodColumnType, isNullable: "NO",
				generationExpression: sql.NullString{Valid: true, String: "ROW END"},
			},
		)
		slog.Info("snapshot: synthesized hidden period columns for implicitly system-versioned table",
			"table", ref.schema+"."+ref.table, "columns", implicitRowStartName+","+implicitRowEndName)
	}
	return columns
}

// tableRef names one source table. The pair is joined for map keys but never
// split back: a schema name may itself contain a dot, so a joined string
// cannot be decomposed unambiguously — the authoritative halves stay here.
type tableRef struct{ schema, table string }

// The shape of MariaDB's hidden implicit period columns, shared by the two
// synthesis surfaces (takeSnapshot's addImplicitPeriodColumns and the agent's
// AddImplicitPeriodColumns) so they cannot drift. Empirical, 11.4: the
// TABLE_MAP FULL metadata names them exactly row_start/row_end, TIMESTAMP(6)
// NOT NULL, always last in the row image.
const (
	implicitRowStartName     = "row_start"
	implicitRowEndName       = "row_end"
	implicitPeriodDataType   = "timestamp"
	implicitPeriodColumnType = "timestamp(6)"
)

// AddImplicitPeriodColumns is the capture-surface sibling of takeSnapshot's
// synthesis (#1272), for callers that build an in-memory resolver straight
// from information_schema.COLUMNS instead of a snapshot — the agent BYOS path.
// Without it, an implicitly system-versioned MariaDB table's resolver holds N
// columns while the row image carries N+2, and the parser's
// column-count-mismatch guard silently skips every event of the table.
//
// It appends the two hidden period columns to each implicitly-versioned table
// present in tables, extending PKColumns with row_end ONLY when the table has
// an existing PK (MariaDB extends, never creates — see the belt note in
// addImplicitPeriodColumns). Explicitly-versioned tables are detected via
// GENERATION_EXPRESSION and left untouched. One to two source round trips,
// at startup only: on MySQL the first (TABLE_TYPE probe) returns empty and
// the second (GENERATION_EXPRESSION probe) never runs. Note the agent's
// EXTRA-based IsGenerated derivation already marks EXPLICIT period columns
// (MariaDB reports EXTRA "STORED GENERATED" for them — same 11.4 run as the
// facts above), so both spellings reach the #1266 gates on this surface too.
func AddImplicitPeriodColumns(sourceDB *sql.DB, schemas []string, tables map[string]*TableMeta) error {
	versioned, err := implicitlyVersionedTables(sourceDB, schemas)
	if err != nil {
		return err
	}
	if len(versioned) == 0 {
		return nil
	}
	explicit, err := explicitPeriodTables(sourceDB, schemas)
	if err != nil {
		return err
	}
	for _, ref := range versioned {
		key := ref.schema + "." + ref.table
		if explicit[key] {
			continue
		}
		tm, ok := tables[key]
		if !ok || len(tm.Columns) == 0 {
			continue
		}
		maxOrd := 0
		for _, c := range tm.Columns {
			if c.OrdinalPosition > maxOrd {
				maxOrd = c.OrdinalPosition
			}
		}
		hasPK := len(tm.PKColumns) > 0
		tm.Columns = append(tm.Columns,
			ColumnMeta{Name: implicitRowStartName, OrdinalPosition: maxOrd + 1,
				DataType: implicitPeriodDataType, ColumnType: implicitPeriodColumnType, IsGenerated: true},
			ColumnMeta{Name: implicitRowEndName, OrdinalPosition: maxOrd + 2, IsPK: hasPK,
				DataType: implicitPeriodDataType, ColumnType: implicitPeriodColumnType, IsGenerated: true},
		)
		if hasPK {
			tm.PKColumns = append(tm.PKColumns, implicitRowEndName)
		}
		slog.Info("resolver: synthesized hidden period columns for implicitly system-versioned table",
			"table", key, "columns", implicitRowStartName+","+implicitRowEndName)
	}
	return nil
}

// implicitlyVersionedTables lists the in-scope tables MariaDB reports as
// TABLE_TYPE 'SYSTEM VERSIONED' (MySQL never emits this value). Used by the
// agent path; takeSnapshot gets the same list from invalidTables' scan.
func implicitlyVersionedTables(sourceDB *sql.DB, schemas []string) ([]tableRef, error) {
	var (
		q    string
		args []any
	)
	if len(schemas) == 0 {
		q = `SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES
			WHERE TABLE_TYPE = 'SYSTEM VERSIONED'
			  AND TABLE_SCHEMA NOT IN ('information_schema','performance_schema','mysql','sys')
			ORDER BY TABLE_SCHEMA, TABLE_NAME`
	} else {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		q = fmt.Sprintf(`SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES
			WHERE TABLE_TYPE = 'SYSTEM VERSIONED' AND TABLE_SCHEMA IN (%s)
			ORDER BY TABLE_SCHEMA, TABLE_NAME`, placeholders)
		for _, s := range schemas {
			args = append(args, s)
		}
	}
	rows, err := sourceDB.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query information_schema.TABLES for system-versioned tables: %w", err)
	}
	defer rows.Close()
	var out []tableRef
	for rows.Next() {
		var r tableRef
		if err := rows.Scan(&r.schema, &r.table); err != nil {
			return nil, fmt.Errorf("failed to scan system-versioned table row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate system-versioned tables: %w", err)
	}
	return out, nil
}

// explicitPeriodTables lists the tables whose period columns are EXPLICITLY
// declared — they appear in information_schema.COLUMNS with the literal
// GENERATION_EXPRESSION 'ROW START'/'ROW END' (#863) — so the agent-path
// synthesis leaves them alone. Empty on MySQL (no such expressions).
func explicitPeriodTables(sourceDB *sql.DB, schemas []string) (map[string]bool, error) {
	var (
		q    string
		args []any
	)
	if len(schemas) == 0 {
		q = `SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME FROM information_schema.COLUMNS
			WHERE GENERATION_EXPRESSION IN ('ROW START', 'ROW END')
			  AND TABLE_SCHEMA NOT IN ('information_schema','performance_schema','mysql','sys')`
	} else {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		q = fmt.Sprintf(`SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME FROM information_schema.COLUMNS
			WHERE GENERATION_EXPRESSION IN ('ROW START', 'ROW END') AND TABLE_SCHEMA IN (%s)`, placeholders)
		for _, s := range schemas {
			args = append(args, s)
		}
	}
	rows, err := sourceDB.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query information_schema.COLUMNS for explicit period columns: %w", err)
	}
	defer rows.Close()
	out := make(map[string]bool)
	for rows.Next() {
		var s, t string
		if err := rows.Scan(&s, &t); err != nil {
			return nil, fmt.Errorf("failed to scan explicit period column row: %w", err)
		}
		out[s+"."+t] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate explicit period columns: %w", err)
	}
	return out, nil
}

// TakeSnapshotExcludingInvalid is the degraded-validation variant of
// TakeSnapshot used by the stream's DDL auto-snapshot hook (#1051). Where
// TakeSnapshot rejects the whole snapshot when ANY base table in scope is not
// InnoDB or lacks an explicit primary key, this variant EXCLUDES those tables
// from the snapshot (their fk_constraints rows are kept — see the comment at
// the FK insert below), records each exclusion in snapshot_exclusions under
// the same snapshot_id, and reports them in SnapshotStats.ExcludedTables so
// the caller can warn loudly. Rationale: the
// parser already skips those tables' row events, so they contribute no
// recoverable data — failing the hook snapshot over them turns any DDL into an
// indefinite stream crash-loop (the #760 fail-loud abort keeps the checkpoint
// on the DDL, the restart re-reads it, validation fails again, forever).
//
// Every OTHER failure — source unreachable, empty scope, index write error,
// even "all tables in scope are invalid" — still errors, preserving the #760
// fail-loud contract for real snapshot failures. `bintrail snapshot` and
// initial setup keep the strict TakeSnapshot: refusing to START capture
// against an unsupported schema is a good preflight.
func TakeSnapshotExcludingInvalid(sourceDB, indexDB *sql.DB, schemas []string) (SnapshotStats, error) {
	return takeSnapshot(sourceDB, indexDB, schemas, true)
}

// TablesWithoutPrimaryKey reports the tables the snapshot would refuse (strict)
// or exclude (degraded) for having no primary key, as "schema.table", sorted.
//
// Exported so `doctor` can warn about them BEFORE capture hits the refusal,
// and deliberately implemented by calling the snapshot's own classifier rather
// than asking information_schema a similar question. Two oracles for "has a
// primary key" is how a preflight comes to disagree with the pipeline it is
// predicting: bintrail's row identity is COLUMN_KEY == "PRI" (see ColumnMeta),
// which MySQL also sets on a UNIQUE NOT NULL index with no declared PRIMARY
// KEY, so a TABLE_CONSTRAINTS-shaped question warns about tables the product
// keys perfectly well. The table-type filter has the same hazard in the other
// direction (#1272, see invalidTables).
func TablesWithoutPrimaryKey(sourceDB *sql.DB, schemas []string) ([]string, error) {
	columns, err := fetchColumnRows(sourceDB, schemas)
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		// No columns visible is not "every table has a primary key". The
		// caller decides what to say; it must not be the reassuring answer.
		return nil, ErrNoColumnsVisible
	}
	_, noPK, _, err := invalidTables(sourceDB, schemas, columns)
	if err != nil {
		return nil, err
	}
	return noPK, nil
}

// ErrNoColumnsVisible marks a source whose information_schema.COLUMNS answered
// with nothing for the requested scope: an empty schema, a misspelled name, or
// an account that cannot see it. Every one of those means the question was not
// answered, which is different from a clean answer.
var ErrNoColumnsVisible = errors.New("no columns are visible for the requested schemas")

// fetchColumnRows reads the source's column metadata for the requested scope.
// Shared by takeSnapshot and TablesWithoutPrimaryKey so the set of tables the
// two reason about cannot drift apart.
func fetchColumnRows(sourceDB *sql.DB, schemas []string) ([]columnRow, error) {
	var (
		query string
		args  []any
	)

	if len(schemas) == 0 {
		query = `
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
			       ORDINAL_POSITION, COLUMN_KEY, DATA_TYPE, COLUMN_TYPE,
			       IS_NULLABLE, COLUMN_DEFAULT, GENERATION_EXPRESSION, CHARACTER_SET_NAME
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA NOT IN ('information_schema','performance_schema','mysql','sys')
			ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`
	} else {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		query = fmt.Sprintf(`
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
			       ORDINAL_POSITION, COLUMN_KEY, DATA_TYPE, COLUMN_TYPE,
			       IS_NULLABLE, COLUMN_DEFAULT, GENERATION_EXPRESSION, CHARACTER_SET_NAME
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA IN (%s)
			ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`, placeholders)
		for _, s := range schemas {
			args = append(args, s)
		}
	}

	srcRows, err := sourceDB.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query information_schema.COLUMNS: %w", err)
	}
	defer srcRows.Close()

	var columns []columnRow
	for srcRows.Next() {
		var c columnRow
		if err := srcRows.Scan(
			&c.schemaName, &c.tableName, &c.columnName,
			&c.ordinalPosition, &c.columnKey, &c.dataType, &c.columnType,
			&c.isNullable, &c.columnDefault, &c.generationExpression, &c.characterSet,
		); err != nil {
			return nil, fmt.Errorf("failed to scan column row: %w", err)
		}
		columns = append(columns, c)
	}
	if err := srcRows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate source columns: %w", err)
	}
	return columns, nil
}

func takeSnapshot(sourceDB, indexDB *sql.DB, schemas []string, excludeInvalid bool) (SnapshotStats, error) {
	// ── 1. Query information_schema on the source server ─────────────────────
	var (
		query string
		args  []any
	)

	if len(schemas) == 0 {
		query = `
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
			       ORDINAL_POSITION, COLUMN_KEY, DATA_TYPE, COLUMN_TYPE,
			       IS_NULLABLE, COLUMN_DEFAULT, GENERATION_EXPRESSION, CHARACTER_SET_NAME
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA NOT IN ('information_schema','performance_schema','mysql','sys')
			ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`
	} else {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		query = fmt.Sprintf(`
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
			       ORDINAL_POSITION, COLUMN_KEY, DATA_TYPE, COLUMN_TYPE,
			       IS_NULLABLE, COLUMN_DEFAULT, GENERATION_EXPRESSION, CHARACTER_SET_NAME
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA IN (%s)
			ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`, placeholders)
		for _, s := range schemas {
			args = append(args, s)
		}
	}

	srcRows, err := sourceDB.Query(query, args...)
	if err != nil {
		return SnapshotStats{}, fmt.Errorf("failed to query information_schema.COLUMNS: %w", err)
	}
	defer srcRows.Close()

	var columns []columnRow
	seenTables := make(map[string]struct{})

	for srcRows.Next() {
		var c columnRow
		if err := srcRows.Scan(
			&c.schemaName, &c.tableName, &c.columnName,
			&c.ordinalPosition, &c.columnKey, &c.dataType, &c.columnType,
			&c.isNullable, &c.columnDefault, &c.generationExpression, &c.characterSet,
		); err != nil {
			return SnapshotStats{}, fmt.Errorf("failed to scan column row: %w", err)
		}
		columns = append(columns, c)
		seenTables[c.schemaName+"."+c.tableName] = struct{}{}
	}
	if err := srcRows.Err(); err != nil {
		return SnapshotStats{}, fmt.Errorf("failed to iterate source columns: %w", err)
	}
	srcRows.Close() // close early before the write transaction

	if len(columns) == 0 {
		return SnapshotStats{}, fmt.Errorf(
			"no columns found for the requested schemas — if the schema has no tables yet, " +
				"create at least one table first; otherwise check --schemas and source server permissions")
	}

	// ── 1b. Validate: all tables must be InnoDB with explicit PKs ────────────
	nonInnoDB, noPK, versionedTables, err := invalidTables(sourceDB, schemas, columns)
	if err != nil {
		return SnapshotStats{}, err
	}
	var (
		excludedTables []string
		exclusions     []snapshotExclusion
	)
	if len(nonInnoDB) > 0 || len(noPK) > 0 {
		if !excludeInvalid {
			return SnapshotStats{}, validationError(nonInnoDB, noPK)
		}
		// Degraded mode (#1051): drop the offending tables from the snapshot
		// instead of failing it. A table can be both non-InnoDB and PK-less —
		// one exclusion entry, combined reason.
		reasonByKey := make(map[string]string, len(nonInnoDB)+len(noPK))
		for _, key := range nonInnoDB {
			reasonByKey[key] = "not InnoDB"
		}
		for _, key := range noPK {
			if r, ok := reasonByKey[key]; ok {
				reasonByKey[key] = r + "; no primary key"
			} else {
				reasonByKey[key] = "no primary key"
			}
		}
		kept := columns[:0]
		seenExcluded := make(map[string]bool, len(reasonByKey))
		for _, c := range columns {
			key := c.schemaName + "." + c.tableName
			if reason, drop := reasonByKey[key]; drop {
				delete(seenTables, key)
				if !seenExcluded[key] {
					seenExcluded[key] = true
					exclusions = append(exclusions, snapshotExclusion{
						schema: c.schemaName, table: c.tableName, reason: reason,
					})
				}
				continue
			}
			kept = append(kept, c)
		}
		columns = kept
		if len(columns) == 0 {
			// Nothing capturable remains — an empty snapshot would silently
			// blind the resolver to every table, so this stays a hard error.
			return SnapshotStats{}, fmt.Errorf(
				"every table in scope failed validation, refusing to write an empty snapshot: %w",
				validationError(nonInnoDB, noPK))
		}
		sort.Slice(exclusions, func(i, j int) bool {
			return exclusions[i].schema+"."+exclusions[i].table < exclusions[j].schema+"."+exclusions[j].table
		})
		for _, e := range exclusions {
			excludedTables = append(excludedTables, e.schema+"."+e.table)
		}
	}

	// ── 1b-bis. Synthesize the HIDDEN period columns of implicitly
	// system-versioned MariaDB tables (#1272). After the exclusion filter on
	// purpose: an excluded table has no kept columns, so the synthesis skips
	// it — it must not re-enter the snapshot. ────────────────────────────────
	columns = addImplicitPeriodColumns(columns, versionedTables)

	// ── 1c. Query FK constraints from the source server ─────────────────────
	fkRows, err := queryFKConstraints(sourceDB, schemas)
	if err != nil {
		return SnapshotStats{}, err
	}
	// Excluded tables' fk_constraints rows are deliberately KEPT (#1051
	// review): an excluded no-PK InnoDB child can carry a real ON DELETE/
	// UPDATE CASCADE edge, and dropping its rows would erase that edge from
	// fk_constraints — recover-cascade would then load no edge, synthesize
	// nothing, and report a clean Complete over a genuine cascade (and
	// `recover` would lose its cascade-parent hint). The exclusions are
	// instead recorded EXPLICITLY in snapshot_exclusions (written below in
	// the same transaction); the cascade FK loaders flag edges from that
	// record (CascadeFK.ChildExcludedFromSnapshot) and synthesis reports the
	// recovery as provably partial.

	// ── 2. Write snapshot atomically into the index database ─────────────────
	if err := ensureSnapshotIDSeqTable(context.Background(), indexDB); err != nil {
		return SnapshotStats{}, err
	}
	// DDL is an implicit commit in MySQL, so the lazy table creation must
	// happen BEFORE the write transaction opens.
	if len(exclusions) > 0 {
		if err := ensureSnapshotExclusionsTable(context.Background(), indexDB); err != nil {
			return SnapshotStats{}, err
		}
	}

	tx, err := indexDB.Begin()
	if err != nil {
		return SnapshotStats{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Allocate the next snapshot_id inside the transaction (#844): snapshot
	// writers are no longer serial — under the watch daemon the DDL hook
	// auto-snapshot, a manual `bintrail snapshot`, and the console baseline
	// trigger can run concurrently against the same index. Without
	// serialization two writers could read the same MAX and merge both row
	// sets under one snapshot_id; the resolver then sees every table with
	// doubled columns and skips ALL its events ("column count mismatch")
	// until the next snapshot.
	//
	// A first attempt serialized this with `SELECT MAX(snapshot_id)+1 ...
	// FOR UPDATE`, which blocks a second allocator until the first commits —
	// but that next-key lock reliably deadlocked (Error 1213) under 3+
	// concurrent writers, and neither caller retries on a transient
	// deadlock, so it crashed the ingestion daemon under exactly the
	// concurrency it was meant to handle. allocateSnapshotID instead draws
	// from a dedicated snapshot_id_seq AUTO_INCREMENT counter table: InnoDB's
	// AUTO_INCREMENT allocation is a lightweight, statement-duration lock,
	// not a row/gap lock held for the transaction's lifetime, so concurrent
	// allocators serialize without ever deadlocking (see DDLSnapshotIDSeq).
	nextID, err := allocateSnapshotID(context.Background(), tx)
	if err != nil {
		return SnapshotStats{}, fmt.Errorf("failed to allocate snapshot_id: %w", err)
	}

	snapshotTime := time.Now().UTC()

	// Batch in groups of 500 rows to stay within default max_allowed_packet.
	const batchSize = 500
	for i := 0; i < len(columns); i += batchSize {
		batch := columns[i:min(i+batchSize, len(columns))]

		valClause := strings.TrimRight(strings.Repeat("(?,?,?,?,?,?,?,?,?,?,?,?,?),", len(batch)), ",")
		insertSQL := "INSERT INTO schema_snapshots " +
			"(snapshot_id, snapshot_time, schema_name, table_name, column_name, " +
			"ordinal_position, column_key, data_type, column_type, character_set_name, is_nullable, column_default, is_generated) VALUES " +
			valClause

		insertArgs := make([]any, 0, len(batch)*13)
		for _, c := range batch {
			var def any
			if c.columnDefault.Valid {
				def = c.columnDefault.String
			}
			var charset any
			if c.characterSet.Valid {
				charset = c.characterSet.String
			}
			// Generated-ness is read from GENERATION_EXPRESSION, not EXTRA: EXTRA
			// also reports "DEFAULT_GENERATED" for an ordinary column with an
			// expression default (e.g. created_at TIMESTAMP DEFAULT
			// CURRENT_TIMESTAMP), so a substring match on "GENERATED" wrongly
			// flags those real, captured data columns as generated and drops
			// them from reverse INSERT/UPDATE SQL (#758). GENERATION_EXPRESSION
			// is non-empty only for true VIRTUAL/STORED generated columns
			// (empty in MySQL, NULL in MariaDB for everything else) — same
			// signal consistency.tableColumns uses.
			isGenerated := c.generationExpression.Valid && strings.TrimSpace(c.generationExpression.String) != ""
			insertArgs = append(insertArgs,
				nextID, snapshotTime, c.schemaName, c.tableName, c.columnName,
				c.ordinalPosition, c.columnKey, c.dataType, c.columnType, charset, c.isNullable, def, isGenerated,
			)
		}

		if _, err = tx.Exec(insertSQL, insertArgs...); err != nil {
			return SnapshotStats{}, fmt.Errorf("failed to insert snapshot batch: %w", err)
		}
	}

	// ── 2b. Insert FK constraint rows ───────────────────────────────────────
	// Check that the fk_constraints table exists before inserting. Existing
	// installations that upgrade without re-running `bintrail init` would
	// otherwise fail the entire snapshot (including column metadata) because
	// the transaction rolls back.
	var fkTableExists bool
	if err = tx.QueryRow(
		"SELECT COUNT(*) > 0 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'fk_constraints'",
	).Scan(&fkTableExists); err != nil {
		return SnapshotStats{}, fmt.Errorf("failed to check fk_constraints table: %w", err)
	}

	fkCount := 0
	if !fkTableExists {
		if len(fkRows) > 0 {
			slog.Warn("fk_constraints table does not exist; skipping FK capture (run 'bintrail init' to create it)")
		}
	} else {
		for i := 0; i < len(fkRows); i += batchSize {
			batch := fkRows[i:min(i+batchSize, len(fkRows))]

			valClause := strings.TrimRight(strings.Repeat("(?,?,?,?,?,?,?,?,?,?,?),", len(batch)), ",")
			insertSQL := "INSERT INTO fk_constraints " +
				"(snapshot_id, constraint_name, schema_name, table_name, column_name, " +
				"ordinal_position, referenced_schema_name, referenced_table_name, referenced_column_name, " +
				"delete_rule, update_rule) VALUES " +
				valClause

			insertArgs := make([]any, 0, len(batch)*11)
			for _, fk := range batch {
				insertArgs = append(insertArgs,
					nextID, fk.constraintName, fk.schemaName, fk.tableName, fk.columnName,
					fk.ordinalPosition, fk.referencedSchemaName, fk.referencedTableName, fk.referencedColumnName,
					fk.deleteRule, fk.updateRule,
				)
			}

			if _, err = tx.Exec(insertSQL, insertArgs...); err != nil {
				return SnapshotStats{}, fmt.Errorf("failed to insert fk_constraints batch: %w", err)
			}
		}
		fkCount = len(fkRows)
	}

	// ── 2c. Record the exclusions (#1051) ───────────────────────────────────
	// Same transaction as the snapshot rows: a snapshot that excluded tables
	// must never commit without the record the cascade loaders flag from.
	for _, e := range exclusions {
		if _, err = tx.Exec(
			"INSERT INTO snapshot_exclusions (snapshot_id, schema_name, table_name, reason) VALUES (?, ?, ?, ?)",
			nextID, e.schema, e.table, e.reason,
		); err != nil {
			return SnapshotStats{}, fmt.Errorf("failed to insert snapshot exclusion: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return SnapshotStats{}, fmt.Errorf("failed to commit snapshot: %w", err)
	}

	return SnapshotStats{
		SnapshotID:     nextID,
		TableCount:     len(seenTables),
		ColumnCount:    len(columns),
		FKCount:        fkCount,
		ExcludedTables: excludedTables,
	}, nil
}

// queryFKConstraints reads foreign key constraint metadata from the source
// database by joining KEY_COLUMN_USAGE with REFERENTIAL_CONSTRAINTS. The result
// includes one row per FK column mapping. Returns nil (not an error) when the
// source has no foreign keys.
func queryFKConstraints(sourceDB *sql.DB, schemas []string) ([]fkRow, error) {
	var (
		query string
		args  []any
	)

	if len(schemas) == 0 {
		query = `
			SELECT kcu.CONSTRAINT_NAME, kcu.TABLE_SCHEMA, kcu.TABLE_NAME,
			       kcu.COLUMN_NAME, kcu.ORDINAL_POSITION,
			       kcu.REFERENCED_TABLE_SCHEMA, kcu.REFERENCED_TABLE_NAME,
			       kcu.REFERENCED_COLUMN_NAME, rc.DELETE_RULE, rc.UPDATE_RULE
			FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
			JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
			    ON rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA
			    AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			WHERE kcu.TABLE_SCHEMA NOT IN ('information_schema','performance_schema','mysql','sys')
			    AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
			ORDER BY kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION`
	} else {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		query = fmt.Sprintf(`
			SELECT kcu.CONSTRAINT_NAME, kcu.TABLE_SCHEMA, kcu.TABLE_NAME,
			       kcu.COLUMN_NAME, kcu.ORDINAL_POSITION,
			       kcu.REFERENCED_TABLE_SCHEMA, kcu.REFERENCED_TABLE_NAME,
			       kcu.REFERENCED_COLUMN_NAME, rc.DELETE_RULE, rc.UPDATE_RULE
			FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
			JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
			    ON rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA
			    AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			WHERE kcu.TABLE_SCHEMA IN (%s)
			    AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
			ORDER BY kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION`, placeholders)
		for _, s := range schemas {
			args = append(args, s)
		}
	}

	rows, err := sourceDB.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query FK constraints: %w", err)
	}
	defer rows.Close()

	var fks []fkRow
	for rows.Next() {
		var fk fkRow
		if err := rows.Scan(
			&fk.constraintName, &fk.schemaName, &fk.tableName,
			&fk.columnName, &fk.ordinalPosition,
			&fk.referencedSchemaName, &fk.referencedTableName, &fk.referencedColumnName,
			&fk.deleteRule, &fk.updateRule,
		); err != nil {
			return nil, fmt.Errorf("failed to scan FK row: %w", err)
		}
		fks = append(fks, fk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate FK rows: %w", err)
	}

	return fks, nil
}

// invalidTables checks that all tables in scope use InnoDB and have an
// explicit primary key. Bintrail requires InnoDB for row-format binary log
// support and needs primary keys to build pk_values for each event.
// Returns the sorted "schema.table" names of every violation (a table can
// appear in both lists); both nil when all tables pass. The caller decides
// whether violations are fatal (TakeSnapshot) or degrade to exclusion
// (TakeSnapshotExcludingInvalid, #1051) — err is reserved for probe failures.
// The third return lists the system-versioned tables the scan saw (#1272),
// in the query's schema/table ORDER — NOT re-sorted like the violation
// lists; addImplicitPeriodColumns' determinism rides that ordering.
func invalidTables(sourceDB *sql.DB, schemas []string, columns []columnRow) (nonInnoDBTables, noPKTables []string, versioned []tableRef, err error) {
	var (
		tabQuery string
		tabArgs  []any
	)
	// TABLE_TYPE IN (...) rather than = 'BASE TABLE' (#1272): MariaDB reports
	// system-versioned tables as 'SYSTEM VERSIONED', so the old filter let
	// them bypass BOTH the InnoDB check and the no-PK check entirely. That
	// bypass was inert while such tables were silently skipped by capture;
	// with the period-column synthesis it would be load-bearing — a PK-less
	// implicitly-versioned table must be refused/excluded here like any other
	// PK-less table, or the synthesis would be the only thing standing between
	// it and capture. The same scan doubles as the versioned-table detection
	// the synthesis consumes (no extra source round trip).
	if len(schemas) == 0 {
		tabQuery = `
			SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE, TABLE_TYPE
			FROM information_schema.TABLES
			WHERE TABLE_SCHEMA NOT IN ('information_schema','performance_schema','mysql','sys')
			  AND TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VERSIONED')
			ORDER BY TABLE_SCHEMA, TABLE_NAME`
	} else {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		tabQuery = fmt.Sprintf(`
			SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE, TABLE_TYPE
			FROM information_schema.TABLES
			WHERE TABLE_SCHEMA IN (%s)
			  AND TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VERSIONED')
			ORDER BY TABLE_SCHEMA, TABLE_NAME`, placeholders)
		for _, s := range schemas {
			tabArgs = append(tabArgs, s)
		}
	}

	tabRows, err := sourceDB.Query(tabQuery, tabArgs...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to query information_schema.TABLES: %w", err)
	}
	defer tabRows.Close()

	baseTables := make(map[string]struct{})
	var nonInnoDB []string

	for tabRows.Next() {
		var schemaName, tableName, tableType string
		var engine sql.NullString
		if err := tabRows.Scan(&schemaName, &tableName, &engine, &tableType); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to scan table row: %w", err)
		}
		key := schemaName + "." + tableName
		baseTables[key] = struct{}{}
		if !engine.Valid || !strings.EqualFold(engine.String, "InnoDB") {
			nonInnoDB = append(nonInnoDB, key)
		}
		if strings.EqualFold(tableType, "SYSTEM VERSIONED") {
			versioned = append(versioned, tableRef{schema: schemaName, table: tableName})
		}
	}
	if err := tabRows.Err(); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to iterate tables: %w", err)
	}

	// Build the set of tables that have at least one PK column.
	tablesWithPK := make(map[string]bool)
	for _, c := range columns {
		if c.columnKey == "PRI" {
			tablesWithPK[c.schemaName+"."+c.tableName] = true
		}
	}

	// Find base tables with no PK column.
	var noPK []string
	for key := range baseTables {
		if !tablesWithPK[key] {
			noPK = append(noPK, key)
		}
	}

	sort.Strings(nonInnoDB)
	sort.Strings(noPK)
	return nonInnoDB, noPK, versioned, nil
}

// validationError renders invalidTables' findings as the strict-mode snapshot
// error. Only called when at least one list is non-empty.
func validationError(nonInnoDB, noPK []string) error {
	var msgs []string
	if len(nonInnoDB) > 0 {
		msgs = append(msgs, fmt.Sprintf("tables not using InnoDB: %s", strings.Join(nonInnoDB, ", ")))
	}
	if len(noPK) > 0 {
		msgs = append(msgs, fmt.Sprintf("tables without a primary key: %s", strings.Join(noPK, ", ")))
	}
	return fmt.Errorf(
		"snapshot validation failed — bintrail requires all tables to use InnoDB with an explicit primary key\n%s",
		strings.Join(msgs, "\n"),
	)
}

// ─── Source pre-flight ──────────────────────────────────────────────────────

// ValidateBinlogFormat checks that the source server has binlog_format=ROW.
func ValidateBinlogFormat(db *sql.DB) error {
	return ValidateBinlogFormatContext(context.Background(), db)
}

// ValidateBinlogFormatContext is ValidateBinlogFormat with a caller-supplied
// context, so a stalled source cannot hang the probe indefinitely (#813).
func ValidateBinlogFormatContext(ctx context.Context, db *sql.DB) error {
	var varName, val string
	err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'binlog_format'").Scan(&varName, &val)
	if errors.Is(err, sql.ErrNoRows) {
		return &BinlogFormatError{msg: "binlog_format not found on source server"}
	}
	if err != nil {
		return fmt.Errorf("failed to query binlog_format: %w", err)
	}
	if !strings.EqualFold(val, "ROW") {
		return &BinlogFormatError{msg: fmt.Sprintf("source server has binlog_format=%q; bintrail requires ROW", val)}
	}
	return nil
}

// BinlogFormatError is the refusal to capture from a source whose
// binlog_format is not ROW (or is absent). It runs ahead of the row-image
// check at every call site (`stream`, `index --source-dsn`, `agent`) with no
// doctor in front, so it declares its own usage-telemetry class: a server
// setting, config_invalid. The message is unchanged from the untyped error.
type BinlogFormatError struct{ msg string }

func (e *BinlogFormatError) Error() string { return e.msg }

// TelemetryClass implements telemetry.Classed.
func (e *BinlogFormatError) TelemetryClass() string { return "config_invalid" }

// ValidateBinlogRowImage checks that the source server has binlog_row_image=FULL.
func ValidateBinlogRowImage(db *sql.DB) error {
	return ValidateBinlogRowImageContext(context.Background(), db)
}

// ValidateBinlogRowImageContext is ValidateBinlogRowImage with a caller-supplied
// context (#813).
func ValidateBinlogRowImageContext(ctx context.Context, db *sql.DB) error {
	var varName, val string
	err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'binlog_row_image'").Scan(&varName, &val)
	if errors.Is(err, sql.ErrNoRows) {
		return &RowImageError{msg: "binlog_row_image not found on source server; MySQL 5.6+ with binlog_row_image=FULL is required"}
	}
	if err != nil {
		return fmt.Errorf("failed to query binlog_row_image: %w", err)
	}
	if !strings.EqualFold(val, "FULL") {
		return &RowImageError{msg: fmt.Sprintf("source server has binlog_row_image=%q; bintrail requires FULL", val)}
	}
	return nil
}

// RowImageError is the refusal to capture from a source whose
// binlog_row_image is not FULL (or is absent). `stream`, `index --source-dsn`
// and `agent` hit it without the doctor in front, so it declares its own
// usage-telemetry class: a server setting, config_invalid. The message is
// unchanged from the untyped error.
type RowImageError struct{ msg string }

func (e *RowImageError) Error() string { return e.msg }

// TelemetryClass implements telemetry.Classed.
func (e *RowImageError) TelemetryClass() string { return "config_invalid" }

// DetectFlavor reports the source server flavor by inspecting VERSION():
// "mariadb" when the version string contains "MariaDB" (case-insensitive),
// otherwise "mysql" (which also covers Percona). On a query error it returns ""
// (unknown) rather than fabricating a flavor — detection is advisory (surfacing a
// --source-flavor mismatch), and callers must treat "" as "could not determine"
// instead of asserting a false mismatch. Returns bare strings to keep
// internal/metadata free of a go-mysql dependency.
func DetectFlavor(db *sql.DB) string {
	var version string
	if err := db.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		return ""
	}
	if strings.Contains(strings.ToLower(version), "mariadb") {
		return "mariadb"
	}
	return "mysql"
}

// buildFKCascadeQuery returns the REFERENTIAL_CONSTRAINTS query (and its args)
// used to find cascading foreign keys — a DELETE_RULE or UPDATE_RULE of CASCADE
// or SET NULL, the four referential actions whose child-side effects InnoDB
// applies below the binlog and recover-cascade synthesizes (aligned with
// CascadeConstraintsInIndex, #1125). When schemas is non-empty the scan is
// scoped to exactly those schemas — the operator explicitly asked us to index
// them, so we police them as named. When schemas is empty we scan every schema
// except (a) MySQL's own system schemas and (b) bintrail's own index schemas.
//
// A bintrail index schema is recognised structurally — by the signature tables
// `bintrail init` creates (binlog_events, schema_snapshots and stream_state must
// all be present) — not by name. This is what lets the pre-flight skip
// bintrail's own `access_rules`→`profiles` ON DELETE CASCADE so an agent does
// not fatal-fail on its own (or another agent's) index DB sharing the source
// MySQL (#347), while still flagging a genuine user schema whatever it is named
// (#365). The signature tables are created before access_rules, so any schema
// carrying that cascade necessarily carries the signature too.
func buildFKCascadeQuery(schemas []string) (string, []any) {
	query := `SELECT CONSTRAINT_SCHEMA, CONSTRAINT_NAME, DELETE_RULE, UPDATE_RULE
		FROM information_schema.REFERENTIAL_CONSTRAINTS
		WHERE (DELETE_RULE IN ('CASCADE', 'SET NULL') OR UPDATE_RULE IN ('CASCADE', 'SET NULL'))`

	var args []any
	if len(schemas) > 0 {
		placeholders := strings.Repeat("?,", len(schemas))
		query += " AND CONSTRAINT_SCHEMA IN (" + placeholders[:len(placeholders)-1] + ")"
		for _, s := range schemas {
			args = append(args, s)
		}
	} else {
		// Skip bintrail's own index schemas, identified by their signature tables
		// rather than by name: a schema counts as bintrail-internal only if it
		// holds ALL of binlog_events, schema_snapshots and stream_state (HAVING
		// = 3), so a user schema that merely shares one of those names is still
		// scanned.
		query += " AND CONSTRAINT_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys')" +
			" AND CONSTRAINT_SCHEMA NOT IN (" +
			"SELECT TABLE_SCHEMA FROM information_schema.TABLES" +
			" WHERE TABLE_TYPE = 'BASE TABLE'" +
			" AND TABLE_NAME IN ('binlog_events','schema_snapshots','stream_state')" +
			" GROUP BY TABLE_SCHEMA HAVING COUNT(DISTINCT TABLE_NAME) = 3)"
	}
	return query, args
}

// ErrFKCascadesFound is wrapped into the error ValidateNoFKCascades returns when
// the source carries FK CASCADE constraints — as opposed to an operational
// failure (a dropped connection, a permissions error reading
// information_schema). Call sites use errors.Is to tell the two apart: a cascade
// finding is now a warn-and-proceed signal, while a genuine query failure must
// still abort. Without this distinction a real fault would be silently
// downgraded to a warning and mislabeled as "cascades present".
var ErrFKCascadesFound = errors.New("FK cascade constraints present on source")

// ValidateNoFKCascades checks that none of the targeted schemas contain foreign
// key constraints with cascading (CASCADE / SET NULL) referential rules, on
// either the DELETE or the UPDATE action. When schemas is empty, all non-system,
// non-bintrail-internal schemas are checked (see buildFKCascadeQuery). FK
// cascades produce invisible side-effect row changes that make reversal SQL
// unreliable. A cascade finding is returned wrapped in ErrFKCascadesFound; any
// other returned error is an operational failure.
func ValidateNoFKCascades(db *sql.DB, schemas []string) error {
	query, args := buildFKCascadeQuery(schemas)

	// The unscoped scan skips schemas that look like a bintrail index — those
	// holding all of bintrail's signature tables (see buildFKCascadeQuery) — so a
	// clean result does not cover them. Disclose the rule, naming the signature
	// tables rather than asserting the skipped schemas are definitely bintrail's:
	// a user schema that replicated those table names would be skipped too, and
	// the operator should be able to recognise that case.
	if len(schemas) == 0 {
		slog.Info("FK cascade pre-flight skips schemas that look like a bintrail index DB " +
			"(those containing binlog_events, schema_snapshots and stream_state); a clean result does not cover them")
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return fmt.Errorf("failed to query FK cascades: %w", err)
	}
	defer rows.Close()

	type cascade struct{ schema, name, deleteRule, updateRule string }
	var found []cascade
	for rows.Next() {
		var c cascade
		if err := rows.Scan(&c.schema, &c.name, &c.deleteRule, &c.updateRule); err != nil {
			return fmt.Errorf("failed to scan FK cascade row: %w", err)
		}
		found = append(found, c)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate FK cascade rows: %w", err)
	}

	if len(found) > 0 {
		for _, c := range found {
			slog.Warn("FK cascade constraint found on source",
				"source_schema", c.schema, "constraint", c.name,
				"delete_rule", c.deleteRule, "update_rule", c.updateRule)
		}
		return fmt.Errorf("%d FK cascade constraint(s) found on source; reversal SQL from `recover` may not correctly handle cascade side-effects: %w", len(found), ErrFKCascadesFound)
	}
	return nil
}

// FKCascadeEdge describes a CASCADE foreign-key edge recorded in the index's
// fk_constraints table (latest snapshot).
type FKCascadeEdge struct {
	Schema          string
	Table           string
	Column          string
	ReferencedTable string
	DeleteRule      string
	UpdateRule      string
}

// CascadeConstraintsInIndex returns the CASCADE foreign-key edges recorded in
// the latest snapshot that captured FK rows (MAX(snapshot_id) in fk_constraints),
// optionally scoped to schemas. Unlike ValidateNoFKCascades (which queries the
// source's information_schema), this reads from the INDEX, so the source-less
// `recover` path can warn that cascade-deleted child rows are not reversible by
// plain recover. (If a newer snapshot recorded zero FKs, this can surface a
// cascade warning from an older snapshot — acceptable for a warn-only path.)
//
// Returns nil when fk_constraints is absent (index predates it) or carries no
// cascade rules — including pre-cascade-recovery snapshots whose delete_rule/
// update_rule columns are empty.
func CascadeConstraintsInIndex(indexDB *sql.DB, schemas []string) ([]FKCascadeEdge, error) {
	var exists bool
	if err := indexDB.QueryRow(
		"SELECT COUNT(*) > 0 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'fk_constraints'",
	).Scan(&exists); err != nil {
		return nil, fmt.Errorf("failed to check fk_constraints table: %w", err)
	}
	if !exists {
		return nil, nil
	}

	query := `SELECT schema_name, table_name, column_name, referenced_table_name, delete_rule, update_rule
		FROM fk_constraints
		WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM fk_constraints)
		  AND (delete_rule IN ('CASCADE', 'SET NULL') OR update_rule IN ('CASCADE', 'SET NULL'))`
	var args []any
	if len(schemas) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		query += " AND schema_name IN (" + placeholders + ")"
		for _, s := range schemas {
			args = append(args, s)
		}
	}
	query += " ORDER BY schema_name, table_name, column_name"

	rows, err := indexDB.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query cascade FK constraints: %w", err)
	}
	defer rows.Close()

	var out []FKCascadeEdge
	for rows.Next() {
		var e FKCascadeEdge
		if err := rows.Scan(&e.Schema, &e.Table, &e.Column, &e.ReferencedTable, &e.DeleteRule, &e.UpdateRule); err != nil {
			return nil, fmt.Errorf("failed to scan cascade FK row: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// IsCascadeParentInIndex reports whether schema.table is the REFERENCED (parent)
// side of an ON DELETE CASCADE / SET NULL foreign key in the latest FK snapshot,
// INCLUDING children that live in a DIFFERENT schema (a cross-schema FK to
// schema.table is legal in MySQL, #833). It matches on referenced_schema_name +
// referenced_table_name, so it is not fooled by a same-named table in another
// schema, and it is not scoped to the parent's own schema the way the child-scoped
// CascadeConstraintsInIndex is. It is the ON DELETE half only — a convenience
// wrapper for callers that care about that single action; the console and CLI
// auto-routing consult CascadeParentRulesInIndex directly, which reports the
// ON UPDATE side too (#1002).
//
// Returns false (not an error) when fk_constraints is absent (index predates it).
func IsCascadeParentInIndex(indexDB *sql.DB, schema, table string) (bool, error) {
	onDelete, _, err := CascadeParentRulesInIndex(indexDB, schema, table)
	return onDelete, err
}

// CascadeParentRulesInIndex is IsCascadeParentInIndex split by referential
// ACTION: onDelete reports an ON DELETE CASCADE / SET NULL child, onUpdate an
// ON UPDATE CASCADE / SET NULL one. The two must stay separate at every
// detection site (#1002): a DELETE only cascades through delete_rule and a
// referenced-key UPDATE only through update_rule, so routing a DELETE recover
// into cascade synthesis on the strength of an ON UPDATE edge (or vice versa)
// would surface a misleading "0 victims" and, worse, teach the operator the
// signal is noise.
//
// Both flags include cross-schema children (matching on referenced_schema_name +
// referenced_table_name, #833). Returns false/false (not an error) when
// fk_constraints is absent (index predates it).
func CascadeParentRulesInIndex(indexDB *sql.DB, schema, table string) (onDelete, onUpdate bool, err error) {
	var exists bool
	if err := indexDB.QueryRow(
		"SELECT COUNT(*) > 0 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'fk_constraints'",
	).Scan(&exists); err != nil {
		return false, false, fmt.Errorf("failed to check fk_constraints table: %w", err)
	}
	if !exists {
		return false, false, nil
	}
	if err := indexDB.QueryRow(
		// COALESCE, not a bare SUM(...)>0: with no matching rows the aggregate is
		// NULL, which fails the bool Scan outright instead of reporting "not a
		// cascade parent".
		`SELECT
			COALESCE(SUM(delete_rule IN ('CASCADE', 'SET NULL')) > 0, 0),
			COALESCE(SUM(update_rule IN ('CASCADE', 'SET NULL')) > 0, 0)
		 FROM fk_constraints
			WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM fk_constraints)
			  AND referenced_schema_name = ? AND referenced_table_name = ?`,
		schema, table,
	).Scan(&onDelete, &onUpdate); err != nil {
		return false, false, fmt.Errorf("failed to query cascade parent constraints: %w", err)
	}
	return onDelete, onUpdate, nil
}

// EnsureResolver returns a Resolver loaded from the latest snapshot, taking a
// new snapshot automatically if none exists (requires sourceDB != nil).
func EnsureResolver(indexDB, sourceDB *sql.DB, schemas []string) (*Resolver, error) {
	var snapshotID int
	if err := indexDB.QueryRow(
		"SELECT COALESCE(MAX(snapshot_id), 0) FROM schema_snapshots",
	).Scan(&snapshotID); err != nil {
		return nil, fmt.Errorf("failed to query schema snapshots: %w", err)
	}

	if snapshotID == 0 {
		if sourceDB == nil {
			return nil, fmt.Errorf(
				"no schema snapshot exists and --source-dsn was not provided; " +
					"run `bintrail snapshot` first or add --source-dsn for auto-snapshot")
		}
		fmt.Println("No snapshot found; taking schema snapshot automatically...")
		stats, err := TakeSnapshot(sourceDB, indexDB, schemas)
		if err != nil {
			return nil, fmt.Errorf("auto-snapshot failed: %w", err)
		}
		fmt.Printf("  snapshot_id=%d, tables=%d, columns=%d\n",
			stats.SnapshotID, stats.TableCount, stats.ColumnCount)
		snapshotID = stats.SnapshotID
	}

	return NewResolver(indexDB, snapshotID)
}

// HasReplPrivileges checks a list of SHOW GRANTS output lines for REPLICATION
// SLAVE and REPLICATION CLIENT privileges. A pure source pre-flight parser
// shared by `bintrail agent --validate` and the doctor's replication-grants
// check, so it lives in metadata next to the other source validators rather
// than in either consumer.
func HasReplPrivileges(grants []string) (slave, client bool) {
	for _, grant := range grants {
		upper := strings.ToUpper(grant)
		if strings.Contains(upper, "ALL PRIVILEGES") {
			return true, true
		}
		if strings.Contains(upper, "REPLICATION SLAVE") {
			slave = true
		}
		if strings.Contains(upper, "REPLICATION CLIENT") {
			client = true
		}
	}
	return
}

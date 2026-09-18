// Package query implements the binlog_events query engine — dynamic SQL
// construction from filter options and multi-format result rendering.
// It is also used by the recovery package, which calls Fetch directly.
package query

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/event"
)

// mysqlToSecondsConst is the value of MySQL's TO_SECONDS('1970-01-01 00:00:00').
// MySQL counts seconds from the proleptic Gregorian year 0, not the Unix epoch;
// the difference is exactly 719528 days (62167219200 seconds).
// TO_SECONDS(t) == t.Unix() + mysqlToSecondsConst for any datetime t expressed in UTC.
const mysqlToSecondsConst = int64(62167219200)

// mysqlToSeconds returns the MySQL TO_SECONDS() value for t, matching the
// RANGE(TO_SECONDS(event_timestamp)) partition expression stored as integers.
// t is normalised to UTC, so callers do not need to convert in advance.
func mysqlToSeconds(t time.Time) int64 {
	return t.UTC().Unix() + mysqlToSecondsConst
}

// validateCursor rejects the option pairings the keyset predicates cannot
// serve: a FORWARD cursor with a DESCENDING order, and (since #1297) a
// BACKWARD cursor with an ASCENDING one. Either pairing pages AWAY from the
// unread remainder, silently returning the wrong half of the window.
//
// The two checks also make "both cursors set" unreachable: Order names one
// direction, and whichever it names rejects one of the cursors. That is why
// there is no third check for the combination.
//
// It lives at the ENGINE entry points rather than only inside
// FetchMergedStream, which is the sole legitimate setter today: Options is
// exported and consumed by several fetch surfaces, so enforcing it where the
// predicate is actually emitted means the next paged surface inherits the
// check instead of having to remember it. buildQuery/buildFilters cannot host
// it — both return (string, []any) with no error path.
func (o Options) validateCursor() error {
	if o.AfterEvent != nil && OrderDirection(o.Order) == "DESC" {
		return errors.New("query: AfterEvent is a forward keyset cursor and cannot be combined with Order=DESC")
	}
	if o.BeforeEvent != nil && OrderDirection(o.Order) == "ASC" {
		return errors.New("query: BeforeEvent is a backward keyset cursor and cannot be combined with Order=ASC")
	}
	return nil
}

// EventCursor is a position in the (event_timestamp, event_id) ascending sort
// order, used as a keyset pagination cursor (see Options.AfterEvent, #1097).
// It is always built from a row the caller actually received, never invented:
// that is what guarantees the next page resumes without a gap.
type EventCursor struct {
	Timestamp time.Time
	EventID   uint64
}

// After reports whether c is strictly after other in the sort order. Used to
// assert forward progress between pages — a cursor that fails to advance means
// the engine returned rows at-or-before the previous cursor, which would loop
// forever if the caller kept paging.
func (c EventCursor) After(other EventCursor) bool {
	if !c.Timestamp.Equal(other.Timestamp) {
		return c.Timestamp.After(other.Timestamp)
	}
	return c.EventID > other.EventID
}

// ─── RBAC types ───────────────────────────────────────────────────────────────

// SchemaTable identifies a schema+table pair used in RBAC deny rules.
type SchemaTable struct {
	Schema string
	Table  string
}

// SchemaTableColumn identifies a specific column used in RBAC redaction rules.
type SchemaTableColumn struct {
	Schema string
	Table  string
	Column string
}

// LoadProfileRules loads the RBAC deny rules for a named profile and returns
// the set of tables whose events should be excluded (table-level deny) and the
// set of columns whose values should be nulled out in query results (column-level deny).
func LoadProfileRules(ctx context.Context, db *sql.DB, profile string) ([]SchemaTable, []SchemaTableColumn, error) {
	// Table-level deny rules: tables flagged for 'deny' by this profile.
	tableRows, err := db.QueryContext(ctx, `
		SELECT DISTINCT tf.schema_name, tf.table_name
		FROM access_rules ar
		JOIN profiles p ON ar.profile_id = p.id
		JOIN table_flags tf ON tf.flag = ar.flag AND tf.column_name = ''
		WHERE p.name = ? AND ar.permission = 'deny'`, profile)
	if err != nil {
		return nil, nil, fmt.Errorf("load table deny rules: %w", err)
	}
	defer tableRows.Close()

	var denyTables []SchemaTable
	for tableRows.Next() {
		var st SchemaTable
		if err := tableRows.Scan(&st.Schema, &st.Table); err != nil {
			return nil, nil, err
		}
		denyTables = append(denyTables, st)
	}
	if err := tableRows.Err(); err != nil {
		return nil, nil, err
	}

	// Column-level deny rules: specific columns to redact in query results.
	colRows, err := db.QueryContext(ctx, `
		SELECT DISTINCT tf.schema_name, tf.table_name, tf.column_name
		FROM access_rules ar
		JOIN profiles p ON ar.profile_id = p.id
		JOIN table_flags tf ON tf.flag = ar.flag AND tf.column_name != ''
		WHERE p.name = ? AND ar.permission = 'deny'`, profile)
	if err != nil {
		return nil, nil, fmt.Errorf("load column redact rules: %w", err)
	}
	defer colRows.Close()

	var redactCols []SchemaTableColumn
	for colRows.Next() {
		var stc SchemaTableColumn
		if err := colRows.Scan(&stc.Schema, &stc.Table, &stc.Column); err != nil {
			return nil, nil, err
		}
		redactCols = append(redactCols, stc)
	}
	if err := colRows.Err(); err != nil {
		return nil, nil, err
	}

	return denyTables, redactCols, nil
}

// ProfileExists reports whether a named RBAC profile row is present in the
// profiles table. LoadProfileRules returns empty deny/redact slices WITHOUT an
// error for a nonexistent profile name (a typo resolves to "enforce nothing"),
// so a caller that must refuse an unknown profile — rather than silently
// starting with RBAC that enforces nothing — probes existence with this first
// (#838).
func ProfileExists(ctx context.Context, db *sql.DB, profile string) (bool, error) {
	var exists bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM profiles WHERE name = ?)`, profile).Scan(&exists); err != nil {
		return false, fmt.Errorf("check profile existence: %w", err)
	}
	return exists, nil
}

// ─── Options ─────────────────────────────────────────────────────────────────

// BinlogPos is a binlog coordinate: a file name plus a byte position. It is used
// as an exact upper bound for "events up to this point" (see Options.UntilPos),
// matching events whose end position is at-or-before it. Files are compared by
// name length first, then lexicographically — for one server's sequence
// (constant basename, numeric suffix zero-padded to a minimum width) this
// equals numeric-suffix order, including past the .999999 → .1000000 rollover
// where the suffix grows a digit and plain lexicographic order inverts (#840).
type BinlogPos struct {
	File string
	Pos  uint64
}

// AtOrBefore reports whether p is at or before q in binlog order, by the same
// rule buildQuery's SincePos/UntilPos predicates use BELOW in this file: the
// file by name length first, then lexicographically (the #840 rollover), then
// the position. In SQL that rule is applied to start_pos for the lower bound
// and to end_pos for the upper one; here it compares two coordinates as such.
//
// Two packages carry their own copy: cascade.afterSnapshot compares a whole
// ResultRow against a coordinate, and icebergexport.binlogBefore is this same
// two-coordinate comparison spelled as four loose scalars (and strict, not
// at-or-before). This is the method form, and it lives here because the rule is
// part of what BinlogPos means.
//
// len() counts bytes where MySQL's CHAR_LENGTH and DuckDB's length count
// characters; identical for the ASCII basenames a binlog actually carries.
func (p BinlogPos) AtOrBefore(q BinlogPos) bool {
	if len(p.File) != len(q.File) {
		return len(p.File) < len(q.File)
	}
	if p.File != q.File {
		return p.File < q.File
	}
	return p.Pos <= q.Pos
}

// Options specifies the filter criteria for querying binlog_events.
// All fields are optional; nil / zero values are ignored when building SQL.
type Options struct {
	Schema   string
	Table    string
	PKValues string // pipe-delimited PK, e.g. "12345" or "12345|2"
	// PKValuesAlt, when set, is an alternate encoding of PKValues that is
	// ALSO matched (OR'd) alongside it. (#957) A --pk value containing a
	// literal "|"/"\" is ambiguous without the live table's actual PK column
	// count: it could be the user-typed delimiter between components of a
	// composite PK (PKValues, unescaped, is what's stored) or a literal
	// character inside a single-column PK (its event.EscapePKValue form is
	// what's stored). Rather than trust a schema snapshot resolve — which can
	// be stale relative to the live table — callers set both candidates here
	// so a stale snapshot never regresses a previously-correct lookup.
	PKValuesAlt string
	PKValuesIn  []string // multi-PK lookup (mutually exclusive with PKValues)
	// PKRange restricts results to a single-column integer primary key
	// within an inclusive window (#1440). Mutually exclusive with
	// PKValues/PKValuesIn, requires Schema and Table, and must be resolved
	// against the schema snapshot (PKRange.ResolveCast) before the fetch:
	// every engine refuses an unresolved one. See the PKRange doc for scope
	// and cost.
	PKRange   *PKRange
	EventType *event.EventType // nil = all types
	GTID      string
	Since     *time.Time
	Until     *time.Time
	// UntilPos, when set, bounds events to those at-or-before an exact binlog
	// coordinate (file + end position) — independent of wall-clock time. It is
	// the precise upper bound for "reconstruct the table to exactly this point"
	// (a baseline's recorded anchor, #641). It refines, and does not replace, the
	// Until time bound: callers pair it with Until so MySQL/the archive listing
	// can still prune partitions/files by time, then UntilPos cuts the boundary
	// exactly. nil = no position bound.
	UntilPos *BinlogPos
	// SincePos, when set, bounds events to those at-or-after an exact binlog
	// coordinate (file + start position) — the lower-bound analog of UntilPos,
	// for baseline-anchored consumers (#797). A baseline's recorded "Started
	// dump at" wall-clock time is NOT a precise anchor: row-event headers carry
	// the statement's EXECUTION time, not its commit time, so a transaction that
	// executed just before the snapshot instant but committed (and so was
	// durably logged, gaining its binlog position) just after it is invisible to
	// BOTH the dump's MVCC snapshot AND a naive `event_timestamp >= snapshotTime`
	// delta fetch — silently lost. The baseline's recorded binlog file+position
	// (bintrail.baseline_binlog_file/_position) is exact and immune to that skew.
	//
	// UNLIKE UntilPos, SincePos does not merely refine the paired Since time
	// bound — it REPLACES its exact-filter role. When SincePos is set, buildQuery
	// drops the exact filter and widens Since's coarse lower bound by one extra
	// hour of lookback (see buildQuery), because that bound is keyed on the very
	// execution-time column whose skew this field exists to route around — a
	// too-tight time bound could exclude the very row this field is meant to
	// recover. The exact correctness gate is the position comparison alone.
	// nil = no position bound; older baselines that never recorded one fall back
	// to the plain Since time filter.
	SincePos *BinlogPos
	// SinceEventID, when non-zero, restricts results to events with an
	// event_id strictly greater than it (#1720). It is a FLOOR for the index,
	// not a correctness gate: SincePos stays the exact gate. It is correct
	// only where event_id order is binlog order, which holds for an index
	// written by `stream` (one capture, ids assigned in binlog order; a resume
	// deletes and re-inserts its replay, so the order survives) and is NOT
	// guaranteed for `bintrail index --files` given files out of order. The
	// baseline-anchored fold sets it from the anchor's recorded last event id
	// only when stream_state shows a stream wrote the index
	// (query.StreamCaptured), and never otherwise.
	//
	// What it buys: idx_row_lookup (schema_name, table_name, event_timestamp)
	// carries the PRIMARY's event_id as its extension, so MySQL evaluates this
	// floor INSIDE the index range and never reads the row of an entry below
	// it — the lookback hour(s) SincePos's coarse time floor opens (#797) cost
	// index entries, not clustered-page reads. Measured on 8.4.9: the floor
	// appears in the range itself ("... AND 4500 < event_id").
	SinceEventID uint64
	// AfterEvent, when set, restricts results to events strictly AFTER this
	// point in the (event_timestamp, event_id) sort order — the keyset cursor
	// that makes a windowed fetch pageable without OFFSET (#1097).
	//
	// It filters on the SORT KEY, not on a correctness bound, which is what
	// makes it composable with every other filter here: whatever set Since /
	// SincePos / Until / UntilPos admit, this pages through that set in the
	// engine's ascending order without changing its membership. Both key
	// components are needed because event_timestamp has one-second resolution
	// and collides heavily — a timestamp-only cursor would either re-return or
	// skip the events sharing the boundary second. event_id breaks every tie,
	// so the composite key is total and each page resumes exactly where the
	// previous one stopped.
	//
	// ASCENDING ORDER ONLY. With Order="DESC" the predicate below would page
	// the wrong way (it would walk away from the unread remainder); callers
	// must not combine the two, and FetchMergedStream refuses the pairing
	// rather than returning a silently truncated stream.
	//
	// nil = no cursor (fetch from the start of the window).
	AfterEvent *EventCursor
	// EventAnchor names ONE event exactly: the (event_timestamp, event_id)
	// pair identifying a single row of binlog_events. It is a membership
	// filter, not a cursor — unlike AfterEvent/BeforeEvent it admits exactly
	// one event rather than a half-open range, and so composes with any Order.
	//
	// Why a pair when event_id alone identifies the row: event_id carries no
	// partition information, so an id-only predicate would scan every
	// partition of binlog_events (and every Parquet file of every archive).
	// The timestamp is what lets both engines prune to the one hour that can
	// hold the row. Callers that have an event in hand always have both — the
	// console's Undo bridge reads them off the row the operator clicked.
	//
	// It exists because a timestamp alone cannot name an event: the column has
	// one-second resolution, so an Until bound plus LimitPerPK=1 resolves to
	// "the newest event in that second", which on a row INSERTed and DELETEd
	// within one second is not the event the operator pointed at — and
	// reversing the wrong one of that pair inverts the outcome (#1411).
	//
	// nil = no anchor.
	EventAnchor *EventCursor
	// BeforeEvent is AfterEvent's mirror: it restricts results to events
	// strictly BEFORE this point in the (event_timestamp, event_id) sort
	// order, which is what a DESCENDING (newest-first) surface needs to reach
	// its second page (#1297).
	//
	// Why a second field instead of reusing AfterEvent: the console's Events
	// view is newest-first, and AfterEvent is a FORWARD cursor — pairing it
	// with Order=DESC pages away from the unread remainder, which is exactly
	// what validateCursor refuses. Narrowing Until instead is not a
	// substitute: Until compares event_timestamp alone, and the column has
	// one-second resolution, so a boundary second shared by several events
	// either re-returns them (Until = cursor second) or drops them entirely
	// (Until = cursor second minus one) — silently losing events in the middle
	// of an investigation is the failure mode this field exists to prevent.
	// Carrying event_id makes the cut total, so page N+1 resumes exactly where
	// page N stopped.
	//
	// DESCENDING ORDER ONLY, the symmetric constraint: with Order="ASC" a
	// backward cursor walks away from the unread remainder just as
	// AfterEvent+DESC does. Because each cursor is pinned to the opposite
	// order, setting BOTH is unreachable — whichever direction Order names,
	// one of the two checks rejects it — so no third "not both" check exists.
	//
	// nil = no cursor (fetch from the newest end of the window).
	BeforeEvent *EventCursor
	// ExtraArchiveHours lists the hour LABELS of archive files whose CONTENT
	// time range overlaps the query window even though their partition/Hive
	// path label does not (#1037: backfilled events archived under the oldest
	// live partition's label). It is a FILE-SCOPING hint consumed only by the
	// archive fetcher (parquetquery): date-scoped S3 listings and Hive-path
	// pruning include these labels' files, and label-order early termination
	// is disabled while any are present. It is NEVER a row filter — Since /
	// Until / SincePos / UntilPos still bound the returned events — and the
	// MySQL engine ignores it. Populated from QueryPlan.MisfiledArchiveHours
	// (or query.MisfiledArchiveHours) by callers that prune archives by time.
	ExtraArchiveHours []time.Time
	ChangedColumn     string // column name; matched via JSON_CONTAINS
	// QueryHash restricts results to the events produced by ONE statement
	// digest: the 64-char hex STATEMENT_DIGEST() stored in
	// binlog_events.query_hash at index time (#699). It is populated only while
	// the source logs the originating statement
	// (binlog_rows_query_log_events=ON, or MariaDB's
	// binlog_annotate_row_events) — against an index captured without it the
	// column is NULL everywhere and this filter matches nothing. That empty
	// result is ambiguous on its own, which is what DigestCaptureInWindow is
	// for. (A pre-#699 index that never ran EnsureSchema lacks the COLUMN, not
	// just the values; that is a 1054 from the SELECT list, filter or no
	// filter.)
	//
	// The digest identifies a statement SHAPE, not one execution: literals are
	// normalised away, so `WHERE id=1` and `WHERE id=999` share a digest and
	// every execution of that shape inside the window matches. That is why this
	// is a READ filter and is deliberately not offered on recover — a reversal
	// scoped to a shape would undo executions the operator never named. Pinning
	// one execution needs connection_id + digest + time, which is a separate
	// correlation problem.
	//
	// Matched case-sensitively on the archive side (DuckDB), so the canonical
	// lowercase form is required — NormalizeQueryHash produces it.
	QueryHash string
	ColumnEq  []ColumnEq // match against values inside row_after / row_before
	Flag      string     // return events from tables/columns carrying this flag
	Limit     int        // 0 → no limit (no LIMIT clause emitted)
	// LimitPerPK caps the number of latest events returned per pk_values value.
	// 0 = unlimited. Applied via ROW_NUMBER OVER (PARTITION BY pk_values
	// ORDER BY event_timestamp DESC, event_id DESC) so the kept events are
	// the most recent ones per PK. The inner DESC ordering is fixed (it
	// selects "latest N per PK"); only the outer ORDER BY direction follows
	// Order.
	LimitPerPK int
	// Order controls the direction of the outer ORDER BY applied before
	// LIMIT. "DESC" (case-insensitive) selects descending order; any other
	// value (including empty) defaults to ascending — this preserves the
	// pre-#1511 behavior for callers that don't set Order. Both sort keys
	// (event_timestamp, event_id) get the same direction so the ordering
	// is total and deterministic regardless of timestamp collisions.
	Order string

	DenyTables    []SchemaTable       // tables excluded by RBAC profile
	RedactColumns []SchemaTableColumn // column values nulled out by RBAC profile
	// AllowTables, when non-empty, switches to allow-list mode: only rows from
	// the listed tables are returned (session-policy restrictions, #1449).
	// DenyTables still composes over it — a table both allowed and denied
	// yields nothing.
	AllowTables []SchemaTable
	// AllowColumns is the column-level allow list: for each table appearing in
	// it, every column of that table's row images NOT listed is nulled by the
	// redaction pass (the complement of RedactColumns, which names the columns
	// to null). Tables not appearing in it are untouched by this rule.
	AllowColumns []SchemaTableColumn
	// ProfileActive is set by callers whenever a profile NAME was supplied,
	// even if it resolved to zero deny/redact rules (a nonexistent or empty
	// profile). It forces the redaction pass so QueryText/QueryHash are
	// withheld under EVERY named profile — see applyRedaction (#699).
	ProfileActive bool
}

// ─── ResultRow ────────────────────────────────────────────────────────────────

// ResultRow is one decoded row from binlog_events.
type ResultRow struct {
	EventID        uint64
	BinlogFile     string
	StartPos       uint64
	EndPos         uint64
	EventTimestamp time.Time
	GTID           *string // nil when GTID not enabled on the source
	ConnectionID   *uint32 // nil for events indexed before this column was added
	SchemaName     string
	TableName      string
	EventType      event.EventType
	PKValues       string
	ChangedColumns []string
	RowBefore      map[string]any // nil for INSERT
	RowAfter       map[string]any // nil for DELETE
	SchemaVersion  uint32         // snapshot_id at index time; 0 for pre-migration data
	QueryText      *string        // original SQL statement (#699); nil unless the source logs ROWS_QUERY/ANNOTATE events
	QueryHash      *string        // STATEMENT_DIGEST of QueryText computed at index time (#699); nil when text absent or digest unavailable
	// CommitTsUS is the transaction's commit time in MICROSECONDS since epoch,
	// from the source's GTID event (#18). nil for events indexed before the
	// column existed, and for sources that never write it (MariaDB, MySQL <
	// 8.0.1) — so a consumer must treat nil as "only the one-second
	// EventTimestamp is known here", never as an ordering tie.
	CommitTsUS *uint64
}

// OrderDirection normalises an Options.Order value to a SQL direction keyword
// ("ASC" or "DESC"). It is case-insensitive on "DESC"; anything else — empty,
// "ASC", garbage — returns "ASC" so the default behavior matches pre-#1511
// (ascending by event_timestamp, event_id). Exposed so the parquetquery and
// merge paths use the same normalisation rule as the MySQL SQL builder.
func OrderDirection(order string) string {
	if strings.EqualFold(order, "DESC") {
		return "DESC"
	}
	return "ASC"
}

// ─── Engine ───────────────────────────────────────────────────────────────────

// Engine executes queries against the index database.
type Engine struct {
	db *sql.DB
}

// New creates a query Engine backed by db.
func New(db *sql.DB) *Engine { return &Engine{db: db} }

// Fetch executes the query and returns raw result rows.
// This is the shared entry point used by both the query and recover commands.
func (e *Engine) Fetch(ctx context.Context, opts Options) ([]ResultRow, error) {
	if err := opts.validateCursor(); err != nil {
		return nil, err
	}
	if err := opts.ValidateStatementFilter(); err != nil {
		return nil, err
	}
	if err := opts.ValidatePKRange(); err != nil {
		return nil, err
	}
	q, args := buildQuery(opts)
	rows, err := e.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", annotateMissingLookupIndex(err, opts))
	}
	defer rows.Close()
	results, err := scanRows(rows)
	if err != nil {
		return nil, err
	}
	// The JOIN form deliberately omits the outer ORDER BY (see buildQuery for
	// why), so it may hand rows back in any order. Re-establish the total
	// (event_timestamp, event_id) ordering here — the result set is already
	// capped by the inner LIMIT, so this Go-side sort is cheap and keeps the
	// "Fetch returns ordered rows" contract that recovery.GenerateSQL and the
	// formatters rely on. The stream shape (#1720) arrives in that order
	// already, so for it the stable sort is a no-op.
	sortResults(results, OrderDirection(opts.Order))
	// Redaction also fires on DenyTables-only profiles — and on a named
	// profile with ZERO rules (ProfileActive): query_text is per-STATEMENT,
	// so rows of an ALLOWED table can carry a statement whose literals
	// belong to a denied sibling table — see applyRedaction (#699).
	if opts.RedactionActive() {
		applyRedaction(results, opts.RedactColumns, opts.AllowColumns)
	}
	return results, nil
}

// RedactionActive reports whether an RBAC policy is in force for these options.
// It is the single predicate every OPTIONS-level surface must consult: the
// redaction pass
// fires on a NAMED profile even when it resolved to zero rules (#838), and on
// deny/redact rules supplied without one. Two copies of this condition would
// drift, and the drift is silent — see ValidateStatementFilter. (Two sibling
// copies of the same three terms survive over OTHER receivers — console
// Server.rbacActive and mcptools recover-cascade — which cannot call this
// method; they are known, not overlooked.)
func (o Options) RedactionActive() bool {
	return o.ProfileActive || len(o.RedactColumns) > 0 || len(o.DenyTables) > 0 ||
		len(o.AllowTables) > 0 || len(o.AllowColumns) > 0
}

// ErrQueryHashUnderProfile is returned when a statement-digest filter is
// combined with an active RBAC policy.
//
// The digest is blanked on EVERY returned row under a policy (see
// applyRedaction) precisely because a stable digest leaks statement shape and
// permits dictionary confirmation of the literals it carried. Honouring a
// filter over that same column would hand back the answer the blanking
// withholds — "these rows came from the statement you guessed" — one candidate
// digest at a time. Refusing is the only consistent option: silently dropping
// the filter would over-return, and an empty result set would read as "that
// statement touched nothing", a false negative on a forensic question.
//
// The message names no CLI flag: this error reaches MCP clients too, and an
// agent handed a --flag it cannot type will invent one.
var ErrQueryHashUnderProfile = errors.New(
	"statement-digest filter unavailable while an RBAC profile is active: the digest is withheld from every returned row, so filtering on it would confirm the statement that redaction hides")

// ErrChangedColumnUnderRedaction is returned when a changed-column filter is
// combined with COLUMN-LEVEL redaction rules (RedactColumns or AllowColumns,
// #1449). The reasoning is ErrQueryHashUnderProfile's, transferred: the
// redaction pass nulls a hidden column's VALUES, but honouring a filter on
// "rows where <hidden column> changed" confirms that column's existence,
// change timing and affected PKs one candidate at a time — the answer the
// nulling withholds. Deny-table-only rules do not trip this: no column is
// hidden there, only whole tables, which the table clauses already enforce.
//
// The message names no CLI flag, for the same MCP-client reason as the
// digest refusal.
var ErrChangedColumnUnderRedaction = errors.New(
	"changed-column filter unavailable while column-level redaction is active: hidden columns are nulled in every returned row, so filtering on one would confirm the changes that redaction hides")

// ValidateStatementFilter rejects option combinations the redaction contract
// cannot honour. Called by Fetch, so every engine path is covered.
func (o Options) ValidateStatementFilter() error {
	if o.QueryHash != "" && o.RedactionActive() {
		return ErrQueryHashUnderProfile
	}
	if o.ChangedColumn != "" && (len(o.RedactColumns) > 0 || len(o.AllowColumns) > 0) {
		return ErrChangedColumnUnderRedaction
	}
	return nil
}

// DigestCaptureInWindow reports whether the index holds at least ONE event
// carrying a statement digest inside the window opts describes.
//
// It exists to disambiguate the one result a digest filter cannot explain by
// itself: ZERO rows. That is either "this statement touched nothing" or "no
// event here could have carried a digest" — the source was not logging
// statements (MySQL defaults binlog_rows_query_log_events OFF), a MariaDB
// stream ran without --source-flavor mariadb, the window predates #699, or the
// source is Postgres, whose capture plane writes the column never. Those are
// opposite answers to a forensic question and they print identically.
//
// Cost is why this is a separate call rather than part of every fetch: there is
// no index on query_hash, so an all-NULL window scans its partitions to prove
// the negative. Callers must invoke it ONLY after a digest-filtered fetch came
// back empty — never on the hot path — and it is bounded by the same
// schema/table/time predicates the query itself used.
func DigestCaptureInWindow(ctx context.Context, db *sql.DB, opts Options) (bool, error) {
	where := []string{"query_hash IS NOT NULL"}
	var args []any
	if opts.Schema != "" {
		where = append(where, "schema_name = ?")
		args = append(args, opts.Schema)
	}
	if opts.Table != "" {
		where = append(where, "table_name = ?")
		args = append(args, opts.Table)
	}
	// Hour-aligned TO_SECONDS literals, inherited from buildQuery. WARNING,
	// measured on MySQL 8.0.46 and 8.4.9 (#1689): a TO_SECONDS(col) >= n
	// predicate prunes NO partitions — MySQL matches a pruning predicate against
	// the partitioning COLUMN, not against the partitioning FUNCTION — while a
	// plain comparison does. These two lines are this probe's only bounds and
	// neither prunes, so the partition scan the doc comment above warns about is
	// currently unbounded. Adding the plain comparisons beside them is #1692's
	// pass, not this change.
	if opts.Since != nil {
		where = append(where, fmt.Sprintf("TO_SECONDS(event_timestamp) >= %d", mysqlToSeconds(opts.Since.Truncate(time.Hour))))
	}
	if opts.Until != nil {
		where = append(where, fmt.Sprintf("TO_SECONDS(event_timestamp) < %d", mysqlToSeconds(opts.Until.Truncate(time.Hour).Add(time.Hour))))
	}
	q := "SELECT 1 FROM binlog_events WHERE " + strings.Join(where, " AND ") + " LIMIT 1"

	var one int
	err := db.QueryRowContext(ctx, q, args...).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("probe statement-digest capture: %w", err)
	}
	return true, nil
}

// NoDigestCaptureWarning is the operator-facing text for a digest-filtered
// query that returned nothing from a window where nothing COULD have matched.
// Shared so the CLI's stderr line and the MCP notice cannot drift into saying
// different things about the same finding.
const NoDigestCaptureWarning = "Warning: no event in this window carries a statement digest, so this empty result does NOT mean the statement touched nothing. " +
	"The source was not logging statements when these events were captured (MySQL: binlog_rows_query_log_events, MariaDB: binlog_annotate_row_events + --source-flavor mariadb; PostgreSQL sources never populate it)."

// NormalizeQueryHash canonicalises a user-supplied statement digest to the form
// stored in binlog_events.query_hash: 64 lowercase hex characters, as produced
// by MySQL's STATEMENT_DIGEST(). An empty input stays empty (no filter).
//
// Validating the SHAPE here is what keeps a mistake loud. The natural error —
// pasting the statement TEXT, or a truncated digest — would otherwise match no
// row on either engine and be indistinguishable from a correct filter over a
// statement that genuinely touched nothing.
func NormalizeQueryHash(s string) (string, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return "", nil
	}
	if len(s) != 64 {
		return "", fmt.Errorf("statement digest must be the 64-character hex STATEMENT_DIGEST stored in query_hash, got %d characters", len(s))
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", fmt.Errorf("statement digest must be hexadecimal, found %q", c)
		}
	}
	return s, nil
}

// sortResults orders rows by (event_timestamp, event_id) in dir ("ASC"/"DESC"),
// matching what the old in-SQL outer ORDER BY produced. Both keys share the
// direction so the order is total and deterministic across timestamp ties.
func sortResults(rows []ResultRow, dir string) {
	desc := dir == "DESC"
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := &rows[i], &rows[j]
		if !a.EventTimestamp.Equal(b.EventTimestamp) {
			if desc {
				return a.EventTimestamp.After(b.EventTimestamp)
			}
			return a.EventTimestamp.Before(b.EventTimestamp)
		}
		if desc {
			return a.EventID > b.EventID
		}
		return a.EventID < b.EventID
	})
}

// Run executes the query and writes formatted results to w.
// format must be one of "table", "json", or "csv"; defaults to "table".
// Returns the number of rows written.
func (e *Engine) Run(ctx context.Context, opts Options, format string, w io.Writer) (int, error) {
	results, err := e.Fetch(ctx, opts)
	if err != nil {
		return 0, err
	}
	return Format(results, format, w)
}

// Format writes rows to w in the chosen format (table, json, or csv).
// It is exported so callers that fetch from multiple sources (e.g. MySQL + Parquet
// archives) can merge rows before formatting.
func Format(rows []ResultRow, format string, w io.Writer) (int, error) {
	switch strings.ToLower(format) {
	case "json":
		return writeJSON(rows, w)
	case "csv":
		return writeCSV(rows, w)
	default:
		return writeTable(rows, w)
	}
}

// ─── SQL builder ──────────────────────────────────────────────────────────────

func buildQuery(opts Options) (string, []any) {
	var where []string
	var args []any

	if opts.Schema != "" {
		where = append(where, "schema_name = ?")
		args = append(args, opts.Schema)
	}
	if opts.Table != "" {
		where = append(where, "table_name = ?")
		args = append(args, opts.Table)
	}
	if opts.PKValues != "" {
		// Use pk_hash for the index scan; pk_values for the collision guard.
		if opts.PKValuesAlt != "" {
			// Match either candidate encoding (#957) — outer parens are
			// load-bearing: where-entries are AND-joined by the caller, so an
			// unparenthesized OR here would silently widen every other filter
			// (schema/table/time range) into the second branch.
			where = append(where, "((pk_hash = SHA2(?, 256) AND pk_values = ?) OR (pk_hash = SHA2(?, 256) AND pk_values = ?))")
			args = append(args, opts.PKValues, opts.PKValues, opts.PKValuesAlt, opts.PKValuesAlt)
		} else {
			where = append(where, "pk_hash = SHA2(?, 256) AND pk_values = ?")
			args = append(args, opts.PKValues, opts.PKValues)
		}
	} else if len(opts.PKValuesIn) > 0 {
		// Multi-PK lookup. The pk_hash generated column index can't help with
		// IN-lists, so the planner falls back to per-partition scans pruned by
		// (schema_name, table_name, event_timestamp). Callers supply schema
		// and table to keep the scan bounded.
		placeholders := make([]string, len(opts.PKValuesIn))
		for i, v := range opts.PKValuesIn {
			placeholders[i] = "?"
			args = append(args, v)
		}
		where = append(where, "pk_values IN ("+strings.Join(placeholders, ",")+")")
	}
	if opts.PKRange != nil {
		// Numeric window over a single-column integer key (#1440). The cast
		// cannot use pk_hash, so within the time-pruned partitions this is a
		// scan; the bounds are inlined integer literals, see mysqlPredicates.
		where = append(where, opts.PKRange.mysqlPredicates()...)
	}
	if opts.EventType != nil {
		where = append(where, "event_type = ?")
		args = append(args, uint8(*opts.EventType))
	}
	if opts.GTID != "" {
		where = append(where, "gtid = ?")
		args = append(args, opts.GTID)
	}
	if opts.Since != nil {
		since := *opts.Since
		if opts.SincePos != nil {
			// SincePos governs correctness (below); this hint must be a SAFE,
			// non-excluding lower bound only — never the exact cut. Truncate to
			// the hour, then back off one MORE full hour: binlog_events
			// partitions by event_timestamp (statement EXECUTION time), not
			// binlog position, so a transaction that started before the
			// anchor's hour but committed (and so was durably logged, gaining
			// its binlog position) after it can have its row physically stored
			// in an EARLIER partition than the anchor's own hour. See
			// SincePos's doc comment.
			//
			// Spelled as a PLAIN COLUMN COMPARISON, not TO_SECONDS(col) >= n,
			// and that is the whole of #1689's second half. The two admit
			// exactly the same rows HERE, and the reason is narrower than it
			// looks — and it is about the FLOOR, not about the column.
			// TO_SECONDS returns an integer, so the function form compares
			// SECONDS while this one compares the full value, and the integer
			// was built by mysqlToSeconds, which is t.UTC().Unix() and DISCARDS
			// any fraction. A floor of 14:00:00.5 therefore becomes 14:00:00 on
			// the function side and stays 14:00:00.5 here: the function form
			// would admit a 14:00:00 row and this one would not.
			// event_timestamp is DATETIME(0) today so no stored value carries a
			// fraction, but that is not what makes this safe. What makes it safe
			// is that the floor is INTEGER-SECOND ALIGNED, which
			// Truncate(time.Hour) below guarantees — an argument that holds for
			// a DATETIME(n>0) column too, so it survives a precision change.
			//
			// TWO INDEPENDENT RULES, do not merge them. (1) The two spellings
			// agree only while the floor is second-aligned; un-aligning it makes
			// this a change to the window rather than a restatement. (2) #797
			// forbids tightening this floor AT ALL, aligned or not — the extra
			// hour of lookback IS the margin, and anything tighter drops the
			// execute-before / commit-after transaction this branch exists to
			// catch. An aligned tightening satisfies (1) and still breaks (2).
			//
			// Bound as a parameter, so the driver renders it in the connection's
			// location; config.Connect pins that to UTC, which is what makes it
			// safe. Note precisely what changed: the TO_SECONDS literal this
			// replaces was built by mysqlToSeconds, which forces t.UTC(), so
			// this bound used to be immune to a non-UTC loc and no longer is.
			// Under a caller that builds its own *sql.DB with a non-UTC loc the
			// PARAMETERS in this statement shift and the inlined TO_SECONDS
			// literals do not, so the two would describe different windows and
			// AND together; the Parquet leg's floor
			// (parquetquery.sinceLowerBoundHint) would not shift with either.
			// The harmful direction is a zone AHEAD of UTC: it renders the floor
			// as a LATER wall clock, which moves the bound up and drops rows —
			// the excluding direction #797's extra hour exists to prevent. A
			// zone behind UTC only over-includes. No test can see this: the
			// integration DSN carries no loc=, so every fixture connection is
			// UTC by construction.
			//
			// Wrapping the column in a function costs the
			// optimizer both of the things this bound exists to give it: it
			// cannot seek idx_row_lookup (schema_name, table_name,
			// event_timestamp) to the floor, so the range is bounded on the
			// UPPER side only and the scan starts at the table's first indexed
			// entry; and it prunes no partitions, which leaves the plain
			// `event_timestamp <= ?` from the Until block as the only predicate
			// that does. So the opened set is every retained partition from the
			// OLDEST one up through until's hour — not the window's, and on an
			// index keeping a week of hourly partitions that is ~168 of them.
			//
			// Measured on MySQL 8.0.46 and 8.4.9, same on both, in two parts.
			// Pruning, on the four-hour fixture in
			// lowerbound_1689_integration_test.go: the function form opens 4 of
			// 4, this form opens 2, same result set — and that test EXPLAINs the
			// prepared form, so it also pins that a parameter prunes and the
			// value needs no inlining. Range bounding was measured separately,
			// on a plan that chooses idx_row_lookup (the production EXPLAIN in
			// #1689): the function form leaves the range bounded above only,
			// this form bounds both. No test pins that half; the integration
			// test says why it declines to.
			floor := since.Truncate(time.Hour).Add(-time.Hour)
			where = append(where, "event_timestamp >= ?")
			args = append(args, floor)
			// Still deliberately NOT the exact Since instant — see SincePos.
			// This is a restatement of the same coarse floor in a form the
			// index can use, never a tightening of it.
		} else {
			// The exact filter below is what bounds this branch; the
			// hour-aligned TO_SECONDS literal beside it is inherited and, on
			// the supported versions, inert. Measured on MySQL 8.0.46 and
			// 8.4.9 (#1689): a TO_SECONDS(col) >= n predicate prunes NO
			// partitions, while the plain `event_timestamp >= ?` on the next
			// line prunes them correctly, parameter or not. The claim this
			// comment used to carry — that MySQL cannot prune from a
			// parameterised datetime comparison — is false on both. The
			// literal is left in place here rather than removed in this change:
			// this branch serves every caller that sets Since without a
			// SincePos, which is all of them but the baseline-anchored fetch
			// above (a Since-less fetch reaches neither branch), so retiring the
			// inherited hints is its own pass (#1692).
			outerSince := mysqlToSeconds(since.Truncate(time.Hour))
			where = append(where, fmt.Sprintf("TO_SECONDS(event_timestamp) >= %d", outerSince))
			where = append(where, "event_timestamp >= ?")
			args = append(args, since)
		}
	}
	if opts.SincePos != nil {
		// Exact binlog lower bound: events whose start position is at-or-after
		// the anchor (a later file, or the same file at-or-after the position).
		// MySQL's SHOW MASTER STATUS position is the NEXT-write position at
		// snapshot time, so an event starting exactly there is a genuine
		// post-snapshot delta. "Later file" is length-then-lexicographic (see
		// BinlogPos), mirroring UntilPos's #840 rollover fix, inverted for a
		// lower bound.
		where = append(where, "(CHAR_LENGTH(binlog_file) > CHAR_LENGTH(?)"+
			" OR (CHAR_LENGTH(binlog_file) = CHAR_LENGTH(?) AND binlog_file > ?)"+
			" OR (binlog_file = ? AND start_pos >= ?))")
		args = append(args, opts.SincePos.File, opts.SincePos.File, opts.SincePos.File, opts.SincePos.File, opts.SincePos.Pos)
	}
	if opts.SinceEventID > 0 {
		// See Options.SinceEventID: an index-evaluable floor under the exact
		// position gate above, never a gate of its own.
		where = append(where, "event_id > ?")
		args = append(args, opts.SinceEventID)
	}
	if opts.Until != nil {
		until := *opts.Until
		// Add an hour-aligned upper bound (exclusive) as a TO_SECONDS literal
		// for partition pruning. Truncate to the hour, then advance one hour.
		// E.g. 15:13 → 16:00, 15:00 → 16:00.
		outerUntil := mysqlToSeconds(until.Truncate(time.Hour).Add(time.Hour))
		where = append(where, fmt.Sprintf("TO_SECONDS(event_timestamp) < %d", outerUntil))
		where = append(where, "event_timestamp <= ?")
		args = append(args, until)
	}
	if opts.UntilPos != nil {
		// Exact binlog upper bound: events whose end position is at-or-before the
		// anchor (an earlier file, or the same file no further than the position).
		// "Earlier file" is length-then-lexicographic (see BinlogPos): MySQL pads
		// the numeric suffix to 6 digits, so after mysql-bin.999999 comes
		// mysql-bin.1000000 and plain `binlog_file < ?` inverts the cut (#840).
		// Equal-length names — the same padded width — keep the plain string
		// comparison as the fast path.
		where = append(where, "(CHAR_LENGTH(binlog_file) < CHAR_LENGTH(?)"+
			" OR (CHAR_LENGTH(binlog_file) = CHAR_LENGTH(?) AND binlog_file < ?)"+
			" OR (binlog_file = ? AND end_pos <= ?))")
		args = append(args, opts.UntilPos.File, opts.UntilPos.File, opts.UntilPos.File, opts.UntilPos.File, opts.UntilPos.Pos)
	}
	if opts.EventAnchor != nil {
		// Hour-aligned TO_SECONDS bracket so MySQL prunes to the single
		// partition that can hold this event, for the same reason the
		// cursor blocks below emit one: a parameterised datetime comparison
		// prunes nothing at parse time. The bracket is exact here rather than
		// deliberately over-inclusive — an anchor names one event, and that
		// event's own hour is the only partition it can live in.
		lo := mysqlToSeconds(opts.EventAnchor.Timestamp.Truncate(time.Hour))
		hi := mysqlToSeconds(opts.EventAnchor.Timestamp.Truncate(time.Hour).Add(time.Hour))
		where = append(where, fmt.Sprintf("TO_SECONDS(event_timestamp) >= %d", lo))
		where = append(where, fmt.Sprintf("TO_SECONDS(event_timestamp) < %d", hi))
		// The exact identity. event_timestamp is carried alongside event_id
		// (which is unique on its own) so a stale anchor whose timestamp no
		// longer matches the stored row returns nothing instead of silently
		// resolving to a different moment than the caller named.
		where = append(where, "event_timestamp = ? AND event_id = ?")
		args = append(args, opts.EventAnchor.Timestamp, opts.EventAnchor.EventID)
	}
	if opts.AfterEvent != nil {
		// Hour-aligned TO_SECONDS literal, inherited from the Since/Until hints
		// above. Safe by construction: every row still to be returned sorts
		// at-or-after the cursor, so its event_timestamp is >= the cursor's, and
		// flooring to the hour only widens that.
		//
		// Later pages DO scan fewer partitions rather than more, but not because
		// of this literal: measured on 8.0 and 8.4 (#1689) a TO_SECONDS(col)
		// predicate prunes nothing, so the tightening comes from the plain
		// `event_timestamp > ?` keyset cut below. Retiring the literal is
		// #1692's pass.
		outerAfter := mysqlToSeconds(opts.AfterEvent.Timestamp.Truncate(time.Hour))
		where = append(where, fmt.Sprintf("TO_SECONDS(event_timestamp) >= %d", outerAfter))
		// The exact keyset cut on the composite sort key. Kept as a separate
		// predicate from the pruning hint above: the hint is hour-granular and
		// deliberately over-inclusive, this is the precise boundary.
		where = append(where, "(event_timestamp > ? OR (event_timestamp = ? AND event_id > ?))")
		args = append(args, opts.AfterEvent.Timestamp, opts.AfterEvent.Timestamp, opts.AfterEvent.EventID)
	}
	if opts.BeforeEvent != nil {
		// The mirror of the AfterEvent block above, for newest-first paging
		// (#1297). Hour-aligned TO_SECONDS literal so MySQL prunes partitions at
		// parse time; the bound is EXCLUSIVE at the next hour boundary
		// (15:13 → 16:00) exactly like the Until hint, because every row still
		// to be returned sorts at-or-before the cursor and so cannot live in a
		// partition above the cursor's own hour. Flooring instead of ceiling
		// here would prune away the cursor's own partition and drop the rest of
		// the boundary hour — the events immediately after the page break.
		outerBefore := mysqlToSeconds(opts.BeforeEvent.Timestamp.Truncate(time.Hour).Add(time.Hour))
		where = append(where, fmt.Sprintf("TO_SECONDS(event_timestamp) < %d", outerBefore))
		// The exact keyset cut on the composite sort key; the hint above is
		// hour-granular and deliberately over-inclusive, this is the boundary.
		where = append(where, "(event_timestamp < ? OR (event_timestamp = ? AND event_id < ?))")
		args = append(args, opts.BeforeEvent.Timestamp, opts.BeforeEvent.Timestamp, opts.BeforeEvent.EventID)
	}
	if opts.ChangedColumn != "" {
		// json.Marshal produces the JSON string representation (with quotes),
		// which is exactly what MySQL's JSON_CONTAINS expects as the needle.
		needle, _ := json.Marshal(opts.ChangedColumn)
		where = append(where, "JSON_CONTAINS(changed_columns, ?)")
		args = append(args, string(needle))
	}
	if opts.QueryHash != "" {
		// Lowercased for parity with the archive side: under a stock
		// case-insensitive default collation (binlog_events declares none of its
		// own) MySQL compares query_hash case-insensitively, DuckDB never does, and a
		// filter that matches live rows but not archived ones would report a
		// statement as having stopped touching rows at the rotation boundary.
		where = append(where, "query_hash = ?")
		args = append(args, strings.ToLower(opts.QueryHash))
	}
	for _, ce := range opts.ColumnEq {
		// Defense-in-depth: ParseColumnEq is the canonical entry, but
		// Options.ColumnEq is exported and crosses package/process boundaries
		// (CLI, MCP, library callers). MySQL does not accept bind parameters
		// for JSON paths, so the column name MUST be interpolated into the SQL
		// string — re-validate here so a hand-built ColumnEq cannot reach the
		// concatenation. On failure, emit "1=0" so the result set is provably
		// empty rather than silently broader (a dropped filter would scoop
		// rows the operator never asked for).
		if !IsSafeColumnName(ce.Column) {
			slog.Error("query.buildQuery: rejected unsafe column name in ColumnEq filter; emitting no-match clause",
				"column", ce.Column)
			where = append(where, "1=0")
			continue
		}
		path := "$." + ce.Column
		if ce.IsNull {
			where = append(where, fmt.Sprintf(
				"(JSON_TYPE(JSON_EXTRACT(row_after, '%s')) = 'NULL' "+
					"OR JSON_TYPE(JSON_EXTRACT(row_before, '%s')) = 'NULL')",
				path, path))
			continue
		}
		where = append(where, fmt.Sprintf(
			"(JSON_UNQUOTE(JSON_EXTRACT(row_after, '%s')) = ? "+
				"OR JSON_UNQUOTE(JSON_EXTRACT(row_before, '%s')) = ?)",
			path, path))
		args = append(args, ce.Value, ce.Value)
	}
	if opts.Flag != "" {
		// EXISTS subquery: match events from tables (or columns) carrying the
		// given flag. The explicit table qualifiers (table_flags.schema_name,
		// binlog_events.schema_name) prevent MySQL from resolving unqualified
		// names against the subquery's own columns rather than the outer table.
		where = append(where, `EXISTS (
			SELECT 1 FROM table_flags
			WHERE table_flags.schema_name = binlog_events.schema_name
			  AND table_flags.table_name  = binlog_events.table_name
			  AND table_flags.flag        = ?)`)
		args = append(args, opts.Flag)
	}
	if len(opts.AllowTables) > 0 {
		// Allow-list mode (#1449): only the listed tables survive. Emitted
		// BEFORE the deny clauses so the composed WHERE reads allow-then-deny,
		// though AND makes the order semantically irrelevant — deny always wins.
		//
		// BINARY, asymmetrically on purpose: the columns' collation is the
		// server default, commonly case-insensitive, and a case-insensitive
		// ALLOW fails open — an entry for `users` would also serve a distinct
		// `Users` table's rows (lower_case_table_names=0 hosts). Deny stays on
		// the column collation: case-insensitive there withholds MORE, the
		// safe direction. BINARY rather than a COLLATE clause so an index
		// whose columns are not utf8mb4 cannot error the comparison.
		ors := make([]string, len(opts.AllowTables))
		for i, at := range opts.AllowTables {
			ors[i] = "(BINARY schema_name = ? AND BINARY table_name = ?)"
			args = append(args, at.Schema, at.Table)
		}
		where = append(where, "("+strings.Join(ors, " OR ")+")")
	}
	for _, dt := range opts.DenyTables {
		where = append(where, "NOT (schema_name = ? AND table_name = ?)")
		args = append(args, dt.Schema, dt.Table)
	}

	// Late materialization. A naive `SELECT <wide cols> ... ORDER BY
	// event_timestamp ... LIMIT N` makes MySQL carry the wide JSON columns
	// (row_before/row_after) through the filesort as "addon fields". A single
	// fat row image (e.g. a WordPress wp_options autoload blob) larger than
	// sort_buffer_size then overflows the sort buffer and the whole query dies
	// with ER_OUT_OF_SORTMEMORY (1038) — even though only a handful of rows are
	// oversized and the host has memory to spare. Raising sort_buffer_size only
	// moves the cliff; an even fatter row re-breaks it.
	//
	// Instead, sort + limit on the NARROW key columns alone (a few bytes each,
	// so the filesort never trips 1038 regardless of row width), then JOIN back
	// to binlog_events on the primary key to fetch the wide columns for just
	// those rows. No outer ORDER BY: it would re-introduce the wide-column
	// filesort. Fetch re-establishes the final ordering in Go (sortResults).
	//
	// The PK is (event_id, event_timestamp) — joining on both keys keeps the
	// match 1:1 and lets MySQL prune partitions on the eq_ref lookup.
	cols := `be.event_id, be.binlog_file, be.start_pos, be.end_pos, be.event_timestamp,
	         be.gtid, be.connection_id, be.schema_name, be.table_name, be.event_type, be.pk_values,
	         be.changed_columns, be.row_before, be.row_after, be.schema_version, be.query_text, be.query_hash,
	         be.commit_ts_us`

	dir := OrderDirection(opts.Order)
	whereSQL := ""
	if len(where) > 0 {
		whereSQL = " WHERE " + strings.Join(where, " AND ")
	}

	if isStreamShape(opts) {
		// The paged, position-anchored table window — the baseline refresh's
		// fold, and the Iceberg export, which pages the same way — is the
		// one shape whose ORDER BY the index itself satisfies (#1720):
		// idx_row_lookup is (schema_name, table_name, event_timestamp) and
		// InnoDB extends every secondary index with the PRIMARY's columns, so
		// with schema and table pinned by equality the entries come out in
		// (event_timestamp, event_id) order already. Read in that order with
		// the wide columns in the same pass and stop at the LIMIT: no
		// filesort (so no 1038 cliff, the reason the JOIN below exists), no
		// materialised key set, and no second lookup of every row on PRIMARY,
		// which under a cold buffer pool was one synchronous disk read per
		// event (#1720 measured ~1.7 ms each, 100k per page).
		//
		// FORCE INDEX is load-bearing: left to itself the optimizer picks
		// idx_pk_hash for the equality prefix and then sorts (measured on
		// 8.4.9 in the shape test's EXPLAIN), which is the plan this replaces.
		// The position bounds stay a filter on the scanned rows, as before.
		return "SELECT " + strings.ReplaceAll(cols, "be.", "") + " FROM binlog_events FORCE INDEX (idx_row_lookup)" + whereSQL +
			" ORDER BY event_timestamp " + dir + ", event_id " + dir + " LIMIT ?", append(args, opts.Limit)
	}

	var keys string
	if opts.LimitPerPK > 0 {
		// Per-PK cap via ROW_NUMBER over the narrow keys only. Inner ORDER BY
		// DESC is fixed: it selects "latest N events per pk_values" regardless
		// of the requested final direction.
		window := "SELECT event_id, event_timestamp, ROW_NUMBER() OVER (PARTITION BY pk_values" +
			" ORDER BY event_timestamp DESC, event_id DESC) AS bt_rn FROM binlog_events" + whereSQL
		keys = "SELECT event_id, event_timestamp FROM (" + window + ") AS w WHERE bt_rn <= ?"
		args = append(args, opts.LimitPerPK)
	} else {
		keys = "SELECT event_id, event_timestamp FROM binlog_events" + whereSQL
	}
	// The narrow ORDER BY + LIMIT only matters when paging: it decides WHICH N
	// rows survive. With no LIMIT the selection is the whole filtered set, so
	// the order is irrelevant here (Fetch sorts in Go anyway) — skip it.
	if opts.Limit > 0 {
		keys += " ORDER BY event_timestamp " + dir + ", event_id " + dir + " LIMIT ?"
		args = append(args, opts.Limit)
	}

	q := "SELECT " + cols + " FROM binlog_events AS be" +
		" JOIN (" + keys + ") AS k" +
		" ON be.event_id = k.event_id AND be.event_timestamp = k.event_timestamp"

	return q, args
}

// annotateMissingLookupIndex names the fix when the stream shape's FORCE INDEX
// meets an index without idx_row_lookup (MySQL 1176). Every index `bintrail
// init` ever wrote has it, so only a hand-built one gets here; without the
// hint the error reads as a bug in the refresh rather than a missing key.
func annotateMissingLookupIndex(err error, opts Options) error {
	var me *drivermysql.MySQLError
	if isStreamShape(opts) && errors.As(err, &me) && me.Number == 1176 {
		return fmt.Errorf("%w — binlog_events has no idx_row_lookup, which every index created by `bintrail init` carries; add it with `ALTER TABLE binlog_events ADD INDEX idx_row_lookup (schema_name, table_name, event_timestamp)`", err)
	}
	return err
}

// isStreamShape reports whether opts is the baseline-anchored table-window
// page (#1720): one table, an exact anchor, a page limit, ascending, and
// nothing that wants another index (a key lookup) or a window function (a
// per-PK cap) or a backward walk. Every condition is load-bearing, and the
// shape test pins each one: outside it buildQuery keeps the JOIN form.
func isStreamShape(opts Options) bool {
	return opts.Schema != "" && opts.Table != "" &&
		opts.Since != nil && opts.SincePos != nil &&
		opts.Limit > 0 && opts.LimitPerPK == 0 &&
		opts.PKValues == "" && len(opts.PKValuesIn) == 0 && opts.PKRange == nil &&
		opts.EventAnchor == nil && opts.BeforeEvent == nil &&
		OrderDirection(opts.Order) == "ASC"
}

// applyRedaction nulls out denied column values in RowBefore and RowAfter maps.
//
// It also blanks QueryText/QueryHash on EVERY row, not just rows of flagged
// tables (#699): the captured statement is per-STATEMENT, not per-table — a
// multi-table UPDATE (or a trigger cascade) stamps the SAME text, literal
// values included, onto row events of every table it touched, so per-table
// blanking would still leak a redacted column's value through an unflagged
// sibling table's rows. The hash is blanked with the text: a stable digest
// leaks statement shape and permits dictionary confirmation of embedded
// values. This mirrors the codebase's "a surface that cannot honor redaction
// is disabled entirely under a profile" pattern (archives, cascade,
// reconstruct).
// The allow parameter is the column-level allow list (Options.AllowColumns,
// #1449): for a table appearing in it, every row-image column NOT listed for
// that table is nulled — the complement of redact, composed so that an
// explicit redact entry wins even over a column the allow list names.
func applyRedaction(rows []ResultRow, redact, allow []SchemaTableColumn) {
	type colKey struct{ schema, table, column string }
	type tblKey struct{ schema, table string }
	set := make(map[colKey]struct{}, len(redact))
	for _, r := range redact {
		set[colKey{r.Schema, r.Table, r.Column}] = struct{}{}
	}
	allowed := make(map[tblKey]map[string]struct{}, len(allow))
	for _, a := range allow {
		k := tblKey{a.Schema, a.Table}
		if allowed[k] == nil {
			allowed[k] = make(map[string]struct{})
		}
		allowed[k][a.Column] = struct{}{}
	}
	for i := range rows {
		r := &rows[i]
		r.QueryText = nil
		r.QueryHash = nil
		allowSet, allowListed := allowed[tblKey{r.SchemaName, r.TableName}]
		null := func(col string) bool {
			if _, deny := set[colKey{r.SchemaName, r.TableName, col}]; deny {
				return true
			}
			if allowListed {
				_, ok := allowSet[col]
				return !ok
			}
			return false
		}
		for col := range r.RowBefore {
			if null(col) {
				r.RowBefore[col] = nil
			}
		}
		for col := range r.RowAfter {
			if null(col) {
				r.RowAfter[col] = nil
			}
		}
		// ChangedColumns carries column NAMES, and a hidden column's name is
		// itself what the restriction withholds — under an allow list every
		// unlisted column of the table would otherwise be enumerated by name
		// on every UPDATE row. Strip the hidden names; rows of tables with no
		// column rule are untouched.
		if r.ChangedColumns != nil && (allowListed || len(set) > 0) {
			kept := r.ChangedColumns[:0:0]
			for _, col := range r.ChangedColumns {
				if !null(col) {
					kept = append(kept, col)
				}
			}
			r.ChangedColumns = kept
		}
	}
}

// ─── Row scanner ─────────────────────────────────────────────────────────────

func scanRows(rows *sql.Rows) ([]ResultRow, error) {
	var results []ResultRow
	for rows.Next() {
		var r ResultRow
		// Every NOT NULL column is scanned defensively. The migrations
		// declare them NOT NULL, but production has shown that customer
		// indexes can carry NULL in multiple columns simultaneously —
		// likely from external pipelines, partial-write paths, or
		// pre-constraint backfills. The first sighting (#318) was
		// binlog_file; #1484's deploy verification surfaced start_pos
		// on the same byos-202 tenant. Defending the entire Scan closes
		// the pattern. event_id stays a bare uint64 because
		// AUTO_INCREMENT cannot return NULL on read.
		// start_pos/end_pos are BIGINT UNSIGNED: scanned through the generic
		// sql.Null[uint64], not sql.NullInt64 — a legitimate position above
		// 2^63 (the #986/#1117 MariaDB underflow shape stored by pre-#1180
		// builds, or any future >8EiB offset) would be a hard Scan failure
		// through the int64 path. The mysql driver returns uint64 for
		// unsigned BIGINT on the TEXT protocol (ParseUint in textRows) and,
		// above 2^63, []byte on the BINARY protocol (binaryRows falls back to
		// uint64ToString; below 2^63 it is int64) — both shapes are reachable
		// (parameterized queries use binary, argument-free ones text);
		// convertAssign handles all of them losslessly into uint64.
		var (
			binlogFile     sql.NullString
			startPos       sql.Null[uint64]
			endPos         sql.Null[uint64]
			eventTimestamp sql.NullTime
			gtid           sql.NullString
			connID         sql.NullInt64
			schemaName     sql.NullString
			tableName      sql.NullString
			eventType      sql.NullInt32
			pkValues       sql.NullString
			schemaVersion  sql.NullInt32
			queryText      sql.NullString
			queryHash      sql.NullString
			commitTsUS     sql.NullInt64
		)
		var changedCols, rowBefore, rowAfter []byte

		if err := rows.Scan(
			&r.EventID, &binlogFile, &startPos, &endPos, &eventTimestamp,
			&gtid, &connID, &schemaName, &tableName, &eventType, &pkValues,
			&changedCols, &rowBefore, &rowAfter, &schemaVersion, &queryText, &queryHash,
			&commitTsUS,
		); err != nil {
			return nil, fmt.Errorf("failed to scan result row: %w", err)
		}
		if binlogFile.Valid {
			r.BinlogFile = binlogFile.String
		}
		if startPos.Valid {
			r.StartPos = startPos.V
		}
		if endPos.Valid {
			r.EndPos = endPos.V
		}
		if eventTimestamp.Valid {
			r.EventTimestamp = eventTimestamp.Time
		}
		if gtid.Valid {
			r.GTID = &gtid.String
		}
		if connID.Valid {
			v := uint32(connID.Int64)
			r.ConnectionID = &v
		}
		if schemaName.Valid {
			r.SchemaName = schemaName.String
		}
		if tableName.Valid {
			r.TableName = tableName.String
		}
		if eventType.Valid {
			r.EventType = event.EventType(eventType.Int32)
		}
		if pkValues.Valid {
			r.PKValues = pkValues.String
		}
		if schemaVersion.Valid {
			r.SchemaVersion = uint32(schemaVersion.Int32)
		}
		if queryText.Valid {
			r.QueryText = &queryText.String
		}
		if queryHash.Valid {
			r.QueryHash = &queryHash.String
		}
		// BIGINT UNSIGNED microseconds scanned through NullInt64: epoch µs stays
		// well inside int64 (year 294247), so the signed hop is lossless.
		if commitTsUS.Valid && commitTsUS.Int64 > 0 {
			v := uint64(commitTsUS.Int64)
			r.CommitTsUS = &v
		}
		if changedCols != nil {
			_ = json.Unmarshal(changedCols, &r.ChangedColumns)
		}
		if rowBefore != nil {
			r.RowBefore = UnmarshalRowImage(rowBefore)
		}
		if rowAfter != nil {
			r.RowAfter = UnmarshalRowImage(rowAfter)
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// UnmarshalRowImage decodes a row_before/row_after JSON blob into a named map,
// keeping numbers as json.Number rather than coercing them to float64. Default
// encoding/json turns every JSON number into a float64, which silently rounds
// integers above 2^53 (BIGINT UNSIGNED > 2^63, large signed BIGINT) — so recover
// SQL and query/CSV output would emit the wrong value even though storage was
// exact (#496). json.Number preserves the exact literal; the downstream
// formatters handle it (recovery.FormatSQLValue, metadata.ordinalValue, the
// shim's resultsetValue/fullTableTextCell). Returns nil on empty input or a
// decode error — still best-effort: a malformed blob yields no row image rather
// than aborting the scan.
func UnmarshalRowImage(data []byte) map[string]any {
	if len(data) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil
	}
	return m
}

// ─── Formatters ───────────────────────────────────────────────────────────────

const tsFormat = "2006-01-02 15:04:05"

// writeTable renders results as a human-readable aligned table.
// row_before and row_after are omitted to keep the output scannable;
// use --format json for full row data.
func writeTable(rows []ResultRow, w io.Writer) (int, error) {
	if len(rows) == 0 {
		fmt.Fprintln(w, "No results.")
		return 0, nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	defer tw.Flush()

	fmt.Fprintln(tw, "ID\tTIMESTAMP\tTYPE\tSCHEMA\tTABLE\tPK_VALUES\tCHANGED_COLS\tGTID\tCONN_ID")
	fmt.Fprintln(tw, "──\t─────────\t────\t──────\t─────\t─────────\t────────────\t────\t───────")

	for i := range rows {
		r := &rows[i]
		gtid := "-"
		if r.GTID != nil {
			gtid = *r.GTID
		}
		connID := "-"
		if r.ConnectionID != nil {
			connID = fmt.Sprintf("%d", *r.ConnectionID)
		}
		changed := strings.Join(r.ChangedColumns, ",")
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			r.EventID,
			r.EventTimestamp.Format(tsFormat),
			eventTypeName(r.EventType),
			r.SchemaName,
			r.TableName,
			r.PKValues,
			changed,
			gtid,
			connID,
		)
	}
	return len(rows), nil
}

// jsonRow is the JSON-serialisable view of a ResultRow with string event type.
type jsonRow struct {
	EventID        uint64         `json:"event_id"`
	BinlogFile     string         `json:"binlog_file"`
	StartPos       uint64         `json:"start_pos"`
	EndPos         uint64         `json:"end_pos"`
	EventTimestamp string         `json:"event_timestamp"`
	GTID           *string        `json:"gtid"`
	ConnectionID   *uint32        `json:"connection_id"`
	SchemaName     string         `json:"schema_name"`
	TableName      string         `json:"table_name"`
	EventType      string         `json:"event_type"`
	PKValues       string         `json:"pk_values"`
	ChangedColumns []string       `json:"changed_columns"`
	RowBefore      map[string]any `json:"row_before"`
	RowAfter       map[string]any `json:"row_after"`
	QueryText      *string        `json:"query_text"`
	QueryHash      *string        `json:"query_hash"`
	CommitTsUS     *uint64        `json:"commit_ts_us"`
}

func writeJSON(rows []ResultRow, w io.Writer) (int, error) {
	out := make([]jsonRow, len(rows))
	for i, r := range rows {
		out[i] = jsonRow{
			EventID:        r.EventID,
			BinlogFile:     r.BinlogFile,
			StartPos:       r.StartPos,
			EndPos:         r.EndPos,
			EventTimestamp: r.EventTimestamp.Format(tsFormat),
			GTID:           r.GTID,
			ConnectionID:   r.ConnectionID,
			SchemaName:     r.SchemaName,
			TableName:      r.TableName,
			EventType:      eventTypeName(r.EventType),
			PKValues:       r.PKValues,
			ChangedColumns: r.ChangedColumns,
			RowBefore:      r.RowBefore,
			RowAfter:       r.RowAfter,
			QueryText:      r.QueryText,
			QueryHash:      r.QueryHash,
			CommitTsUS:     r.CommitTsUS,
		}
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		return 0, fmt.Errorf("JSON encode failed: %w", err)
	}
	return len(rows), nil
}

// csvHeaders is the fixed column order for CSV output.
var csvHeaders = []string{
	"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
	"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
	"changed_columns", "row_before", "row_after", "query_text", "query_hash",
	"commit_ts_us",
}

func writeCSV(rows []ResultRow, w io.Writer) (int, error) {
	cw := csv.NewWriter(w)
	if err := cw.Write(csvHeaders); err != nil {
		return 0, err
	}
	for i := range rows {
		r := &rows[i]
		gtid := ""
		if r.GTID != nil {
			gtid = *r.GTID
		}
		connID := ""
		if r.ConnectionID != nil {
			connID = fmt.Sprintf("%d", *r.ConnectionID)
		}
		changed := ""
		if r.ChangedColumns != nil {
			b, _ := json.Marshal(r.ChangedColumns)
			changed = string(b)
		}
		before := ""
		if r.RowBefore != nil {
			b, _ := json.Marshal(r.RowBefore)
			before = string(b)
		}
		after := ""
		if r.RowAfter != nil {
			b, _ := json.Marshal(r.RowAfter)
			after = string(b)
		}
		queryText := ""
		if r.QueryText != nil {
			queryText = *r.QueryText
		}
		queryHash := ""
		if r.QueryHash != nil {
			queryHash = *r.QueryHash
		}
		commitTsUS := ""
		if r.CommitTsUS != nil {
			commitTsUS = fmt.Sprintf("%d", *r.CommitTsUS)
		}
		record := []string{
			fmt.Sprintf("%d", r.EventID),
			r.BinlogFile,
			fmt.Sprintf("%d", r.StartPos),
			fmt.Sprintf("%d", r.EndPos),
			r.EventTimestamp.Format(tsFormat),
			gtid,
			connID,
			r.SchemaName,
			r.TableName,
			eventTypeName(r.EventType),
			r.PKValues,
			changed,
			before,
			after,
			queryText,
			queryHash,
			commitTsUS,
		}
		if err := cw.Write(record); err != nil {
			return i, err
		}
	}
	cw.Flush()
	return len(rows), cw.Error()
}

// ─── Utility ─────────────────────────────────────────────────────────────────

func eventTypeName(et event.EventType) string {
	switch et {
	case event.EventInsert:
		return "INSERT"
	case event.EventUpdate:
		return "UPDATE"
	case event.EventDelete:
		return "DELETE"
	case event.EventSnapshot:
		return "SNAPSHOT"
	default:
		return "UNKNOWN"
	}
}

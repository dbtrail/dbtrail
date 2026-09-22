// Package event holds the source-agnostic row-event type that flows from a
// capture backend into the indexer and the whole downstream value stack (query,
// recover, reconstruct, shim, console). Today the MySQL/MariaDB binlog parser
// (internal/parser) produces it; a planned PostgreSQL WAL decoder is intended to
// produce the same Event without the read side linking any new capture library.
//
// It deliberately imports NO source driver (no go-mysql): everything downstream
// of an Event is source-neutral, so the read-side packages link no capture
// library — enforced by TestReadLayerDoesNotLinkGoMySQL. (Extracted from
// internal/parser — #528.)
package event

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// EventType represents the type of operation captured by a change event (DML or DDL).
type EventType uint8

const (
	EventInsert EventType = 1
	EventUpdate EventType = 2
	EventDelete EventType = 3
	EventDDL    EventType = 4
	EventGTID   EventType = 5 // GTID-only tracking event (no row data)
	// EventSnapshot is a synthetic event type emitted by query --include-snapshot
	// for rows read from a mydumper baseline Parquet file. No capture backend
	// produces this type — it exists so baseline rows can flow through the
	// same ResultRow pipeline as real change events.
	EventSnapshot EventType = 6
	// EventCommit marks a transaction commit boundary, emitted by the StreamParser
	// at an XID_EVENT (InnoDB DML) and — as a catch-all when the next transaction's
	// GTID_EVENT arrives — for transactions that carry a GTID but emit no XID and
	// aren't table DDL: implicitly-committed DDL/DCL (GRANT, CREATE DATABASE,
	// CREATE INDEX, ANALYZE TABLE, ...) and no-XID explicit terminators (XA COMMIT;
	// a COMMIT of a non-transactional transaction — a normal InnoDB COMMIT ends in
	// an XID_EVENT instead). It carries no row data, only the committed
	// transaction's GTID. The consumer advances the durable GTID checkpoint ONLY on
	// this event (and on EventDDL), never on the leading EventGTID, so a checkpoint
	// can never claim a half-streamed transaction (#491). The file parser does not
	// produce it.
	EventCommit EventType = 7
	// EventRelation carries a PostgreSQL relation's shape (column names, ordinals,
	// PK flags, type OIDs) in Relation, with no row data. The pgcapture decoder
	// emits one when it sees a pgoutput RelationMessage for an in-scope table; the
	// consumer persists it as a schema snapshot (metadata.WritePGSnapshot) and
	// stamps subsequent rows' SchemaVersion (#533). It is NEVER written to
	// binlog_events — the consumer handles it out-of-band — so it does not break the
	// "EventType numeric values are a persistence contract" rule for stored rows.
	EventRelation EventType = 8
)

// Event is a fully resolved change event with column names attached. It carries
// everything the indexer needs to write one row to binlog_events. DDL events
// (EventType=EventDDL) carry DDLQuery and DDLType instead of row data.
//
// Position fields are source-agnostic — the downstream stack treats them as
// opaque metadata (it never orders, compares, or computes on them). For a
// MySQL/MariaDB binlog source they hold the binlog coordinates: BinlogFile plus
// StartPos/EndPos byte offsets, and the GTID when enabled. A future PostgreSQL
// WAL decoder is expected to reuse these same fields for the LSN (so no field
// rename is needed to carry one); the exact LSN-encoding contract is that
// backend's to define (#530), not something this type fixes today.
type Event struct {
	BinlogFile   string // MySQL: binlog filename. (Wide enough to also hold a future Postgres LSN string.)
	StartPos     uint64 // MySQL: binlog byte offset. (uint64 also fits a Postgres LSN.)
	EndPos       uint64 // MySQL: binlog byte offset. (uint64 also fits a Postgres LSN.)
	Timestamp    time.Time
	GTID         string // empty when GTID is not enabled on the source
	ConnectionID uint32 // MySQL pseudo_thread_id from the transaction's QUERY(BEGIN) event; 0 = unknown
	// QueryText is the original SQL statement that produced this row event,
	// captured from the statement's ROWS_QUERY_EVENT (MySQL,
	// binlog_rows_query_log_events=ON) or ANNOTATE_ROWS event (MariaDB,
	// binlog_annotate_row_events=ON). Empty when the source does not log it —
	// capture is opt-in on the source (#699). Statement-scoped: a multi-statement
	// transaction stamps each statement's own text onto its rows.
	QueryText string
	// CommitTsUS is the transaction's commit time in MICROSECONDS since the
	// Unix epoch, read from the GTID event's immediate_commit_timestamp
	// (MySQL 8.0.1+). Zero when unavailable: a MariaDB source (neither its GTID
	// event nor its ANNOTATE_ROWS event carries one) or a MySQL older than
	// 8.0.1. GTIDs do NOT have to be enabled — 8.0 stamps the value on the
	// ANONYMOUS_GTID_EVENT it writes under gtid_mode=OFF as well, which the
	// integration test in internal/parser pins against a live server.
	//
	// The common-header timestamp every event already carries resolves to one
	// SECOND, which is far coarser than the rate at which a busy server
	// commits — inside one second, event order is knowable but event TIME is
	// not. Capturing the microsecond value keeps the fidelity the source
	// wrote down instead of discarding it at the parser; correlating an
	// indexed change against any other microsecond-stamped record (audit
	// logs, application traces) is impossible after it is lost.
	//
	// Transaction-scoped, exactly like ConnectionID: set from the GTID event
	// that opens the transaction and stamped on every row event inside it.
	CommitTsUS uint64
	Schema     string
	Table      string
	// EventType numeric values are a PERSISTENCE CONTRACT: stored as a number in
	// binlog_events.event_type and filtered by it. Never renumber existing values.
	EventType     EventType
	PKValues      string         // pipe-delimited PK values in ordinal order
	RowBefore     map[string]any // nil for INSERT
	RowAfter      map[string]any // nil for DELETE
	SchemaVersion uint32         // actual snapshot_id from schema_snapshots; updated by SwapResolver on DDL
	DDLQuery      string         // original DDL statement (EventDDL only)
	DDLType       DDLKind        // ALTER TABLE, CREATE TABLE, CREATE OR REPLACE TABLE, DROP TABLE, RENAME TABLE, TRUNCATE TABLE (EventDDL only)
	// DDLTables is every table a DROP or RENAME names, in the statement's
	// order and with repeats (a rename's pairs stay readable): both sides of
	// every rename pair, since the old name stops holding its rows and the new
	// one starts holding another table's. The first entry is Schema/Table.
	// Empty for the other kinds, which name one table (EventDDL only).
	DDLTables []DDLTable
	// Relation carries a PostgreSQL relation's shape (EventRelation only); the
	// consumer persists it as a schema snapshot and stamps subsequent rows'
	// SchemaVersion. nil for every other event type; never written to binlog_events.
	Relation *metadata.PGRelationSchema
	// StmtEnd is true on the row events of the ROWS_EVENT that carries STMT_END_F —
	// the last chunk of a statement. A statement larger than
	// binlog_row_event_max_size (8 KiB default) is split into several ROWS_EVENTs
	// under ONE TABLE_MAP, and only the last one sets STMT_END_F. The stream
	// consumer keys its POSITION-mode safe checkpoint off this flag so a resume
	// never lands on a byte offset BETWEEN two chunks — a mid-statement position
	// has no preceding TABLE_MAP for the live syncer and closes the stream with
	// "invalid table id, no corresponding table map event" (#775). Transient:
	// never written to binlog_events.
	StmtEnd bool
	// ReadAt is when the replication client delivered the binlog event this row
	// came out of — T1 in the availability-lag design (#1223). Transient: it is
	// in-memory only, never written to binlog_events and never to the archive
	// Parquet, so nothing about it is a persistence contract.
	//
	// It is stamped at the PARSER, deliberately, not where the stream consumer
	// receives the event off the channel: the channel is buffered, so a consumer
	// stamp would fold the queue wait into T1 and hide exactly the backlog that
	// T2−T1 exists to measure — when the indexer falls behind, that wait IS the
	// lag.
	//
	// ZERO on the file path (`bintrail index`) and on any consumer that builds
	// events itself: lag is meaningless for backfill re-indexing. Consumers must
	// SKIP the observation on zero rather than observing a 0-second latency — a
	// fabricated zero reads as "perfectly fresh", the most misleading value this
	// field could produce.
	ReadAt time.Time
}

// Filters controls which schemas and tables produce events.
// A nil map means "accept all" for that dimension.
type Filters struct {
	Schemas map[string]bool // keyed by schema name
	Tables  map[string]bool // keyed by "schema.table"
}

// Matches returns true when the schema+table passes both filter dimensions.
func (f *Filters) Matches(schema, table string) bool {
	if f.Schemas != nil && !f.Schemas[schema] {
		return false
	}
	if f.Tables != nil && !f.Tables[schema+"."+table] {
		return false
	}
	return true
}

// BuildPKValues produces a pipe-delimited string of PK values in ordinal order.
// Pipe (|) and backslash (\) inside values are escaped to prevent ambiguity.
// pkColumns must be in ordinal_position order (as returned by TableMeta.PKColumnMetas).
func BuildPKValues(pkColumns []metadata.ColumnMeta, row map[string]any) string {
	parts := make([]string, 0, len(pkColumns))
	for _, col := range pkColumns {
		parts = append(parts, EscapePKValue(formatPKValue(row[col.Name])))
	}
	return strings.Join(parts, "|")
}

// EscapePKValue applies the pipe/backslash escaping BuildPKValues uses for
// each individual PK column value. Exported so callers holding a raw,
// already-decoded single-column PK value (e.g. internal/shim, after
// MySQL-unescaping a SQL string literal) can re-encode it into the same
// at-rest form stored in binlog_events.pk_values before using it as a match
// filter — the string-literal unescaping and this pipe-delimiter escaping
// are two unrelated encodings, and neither implies the other.
func EscapePKValue(val string) string {
	val = strings.ReplaceAll(val, `\`, `\\`)
	val = strings.ReplaceAll(val, `|`, `\|`)
	return val
}

// MaxPKValuesLen is the character-count ceiling of binlog_events.pk_values
// (VARCHAR(512)). Measured in RUNES, not bytes: a multibyte utf8mb4 PK value
// (e.g. a CJK VARCHAR primary key) occupies one VARCHAR(512) character per
// rune regardless of how many UTF-8 bytes that rune takes, so a byte-length
// check would false-trip on perfectly legal values that fit the column.
//
// Deliberately NOT enforced inside BuildPKValues — see internal/indexer's
// insertBatch guard instead. BuildPKValues is also called by the BYOS
// Parquet path (internal/byos), which writes pk_values to customer-owned
// Parquet storage with no 512-character ceiling at all; a length check here
// would wrongly reject valid BYOS rows that the indexed-MySQL path never
// sees. The column is deliberately NOT widened either (see #944): moving
// VARCHAR(512) to VARCHAR(3072)+ crosses MySQL's 1-byte/2-byte
// length-prefix boundary, which is a full table rebuild, not an instant
// DDL — unacceptable to trigger silently on a routine CLI invocation
// against a large production binlog_events table.
const MaxPKValuesLen = 512

// hexPKPrefix marks a PK component that formatPKValue hex-encoded because its
// raw bytes are not storable in binlog_events.pk_values (#1132). "0x" is the
// spelling MySQL itself uses for a binary literal, so the stored form is
// reproducible from the source with SELECT CONCAT('0x', HEX(<pk_col>)) and can
// be pasted straight into `--pk`.
//
// Note that hex DOUBLES the component's length against the MaxPKValuesLen
// ceiling: a BINARY(16) UUID PK lands at 34 characters (fine), but a wide
// VARBINARY or a wide composite binary PK can now exceed 512 and trip
// indexer.checkPKValuesLength instead. That is the pre-existing wide-PK limit
// (#944), not a new failure introduced here — it is just reachable by a
// narrower set of PKs than the utf8mb4 rejection was.
const hexPKPrefix = "0x"

// formatPKValue renders a single PK value for BuildPKValues. []byte needs its
// own case: since #756, metadata.MapRow hands back a BINARY/VARBINARY value as
// []byte (routing it through marshalRow's base64-safe path instead of a raw Go
// string that json.Marshal could corrupt) — a real-world PK shape, e.g. a
// BINARY(16) UUID primary key. Without this case, "%v" on a []byte prints Go's
// bracketed decimal-byte representation (e.g. "[233 12 ...]") instead of the
// raw bytes, which would silently change pk_hash/pk_values for every row with
// such a PK. string(b) reproduces exactly what "%v" printed for that same
// value before #756 (when it was still a raw Go string of the same bytes).
//
// #1132: raw bytes that are not valid UTF-8 cannot be stored at all.
// binlog_events.pk_values is VARCHAR(512) with NO declared CHARACTER SET
// (internal/indexer/schema.go) — it inherits utf8mb4 from the MySQL 8.0+
// server default, and config.Connect sets no charset DSN parameter either, so
// the driver's utf8mb4 handshake collation applies on every index connection.
// MySQL therefore rejects the whole multi-row INSERT with error 1366, and
// because a batch flush failure is fail-loud by contract (internal/streamrun's
// flush, #652) ONE table with a BINARY/VARBINARY PK stops capture for every
// table in that source, not just the offending one: `bintrail stream` exits
// the process outright, while under `bintrail-console watch` that source
// crash-loops on backoff and then goes permanently failed (consoleapp/
// monitor.go). Those bytes are therefore hex-encoded (hexPKPrefix + uppercase
// hex, i.e. exactly what MySQL's own CONCAT('0x', HEX(col)) produces, so an
// operator can reproduce a stored pk_values straight from the source table
// and feed it back to --pk).
//
// The check is on CONTENT, not on the column's declared type. Two consequences
// worth being explicit about:
//
//   - It is BROADER than BINARY/VARBINARY. go-mysql delivers TEXT/BLOB as
//     []byte and coerceTextEncoding passes those through untouched, so a
//     latin1 TEXT prefix PK or a BLOB prefix PK lands here too — and also
//     stops killing capture. BINARY/VARBINARY is just the reported shape.
//   - It is what makes the change strictly additive for the indexed-MySQL
//     path: a pk_values already in binlog_events is necessarily valid UTF-8
//     (invalid bytes could never have been written), so no existing row's
//     pk_values — and therefore no existing pk_hash — changes spelling. Only
//     values that today stop capture get a new representation. A non-strict
//     sql_mode index server would instead have stored the value with its
//     invalid bytes replaced — the same pk_hash-over-a-mangled-value mechanic
//     checkPKValuesLength's comment documents for the LENGTH case (#944) — so
//     those rows were already unrecoverable, not working.
//
// BYOS IS OUTSIDE THAT INVARIANT, and deliberately so — the same index-vs-BYOS
// split MaxPKValuesLen's comment already draws. internal/byos and
// internal/buffer call BuildPKValues too, but write pk_values to customer-owned
// Parquet with no utf8mb4 column anywhere in the path, so a pure-BYOS agent
// (cliapp/agent.go permits BYOS with no --index-dsn) has been durably
// persisting the RAW spelling for binary PKs and never saw error 1366. For
// those keys the spelling — and therefore byos.PKHash, the metadata↔payload
// correlation key — changes at this boundary, so pre-fix payload Parquet stops
// correlating with post-fix metadata. Within a single event both sides are
// still stamped from the same value, so live correlation is unaffected; it is
// the cross-boundary lookups (internal/agent/handler.go's resolve_pk,
// buffer.ResolvePK) that silently miss rather than error. The compat read
// path is CanonicalPKValues (#1137): those lookups re-spell the stored value
// and also try the canonical spelling's hash, so pre-fix raw-spelling rows
// correlate with post-fix hashes again.
//
// Both halves are pinned at runtime by TestInsertBatch_binaryPrimaryKey, which
// checks the utf8mb4 premise against information_schema and asserts the raw
// form is still rejected. The charset is inherited, not declared, so it is not
// safe to assume.
//
// Residual, accepted ambiguity, same class as coerceTextEncoding's
// latin1-that-is-coincidentally-valid-UTF-8 case and marshalRow's
// looksLikeJSONContainer gate: a VARBINARY PK holding the literal ASCII text
// "0xDEAD" is valid UTF-8, so it is stored verbatim and collides with the
// encoding of the two raw bytes {0xDE,0xAD}. Distinguishing them would need a
// type-gated rule (always hex a BINARY/VARBINARY column), which would change
// the spelling of PK values that store and query correctly today — a real
// regression traded for a contrived collision. Content-gating is the narrower
// change and is the one taken.
func formatPKValue(v any) string {
	if b, ok := v.([]byte); ok {
		if !utf8.Valid(b) {
			return hexPKPrefix + strings.ToUpper(hex.EncodeToString(b))
		}
		return string(b)
	}
	return fmt.Sprintf("%v", v)
}

// splitPKValues splits a pipe-delimited pk_values string into its raw
// (unescaped) components — the exact inverse of the per-part EscapePKValue +
// "|" join that BuildPKValues performs. Byte-oriented on purpose: the
// delimiter and escape characters are ASCII, so they can never appear inside
// a multi-byte UTF-8 sequence, and a component's bytes come back exactly as
// formatPKValue produced them (including bytes that are not valid UTF-8).
// A trailing lone backslash (which EscapePKValue never produces) is kept
// literally rather than dropped.
func splitPKValues(s string) []string {
	parts := make([]string, 0, strings.Count(s, "|")+1)
	var cur strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\':
			if i+1 < len(s) {
				i++
				cur.WriteByte(s[i])
			} else {
				cur.WriteByte(c)
			}
		case '|':
			parts = append(parts, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	return append(parts, cur.String())
}

// CanonicalPKValues rewrites a pk_values string into the spelling
// BuildPKValues produces today for the same key: any component whose raw
// bytes are not valid UTF-8 is re-spelled as hexPKPrefix + uppercase hex,
// exactly as formatPKValue does since #1132; every other component is left
// untouched.
//
// This is the #1137 compat read path. Before #1132 the BYOS pipeline
// (internal/byos, internal/buffer) durably persisted the RAW spelling for a
// binary PK — customer-owned Parquet has no utf8mb4 column to reject it — so
// a hash over a pre-fix stored value no longer matches a hash computed over
// the post-fix spelling of the same key. The cross-boundary lookups (agent
// resolve_pk, buffer.ResolvePK, the recover pk_hash filter) call this on the
// stored value and, only when the spelling differs, also try the canonical
// spelling's hash.
//
// Properties callers rely on:
//   - Already-canonical input (the common case) is returned unchanged — the
//     SAME string, no allocation. The fast path is exact, not approximate:
//     escaping only inserts/removes an ASCII backslash adjacent to another
//     ASCII byte ('\' or '|'), and ASCII bytes can never sit inside a
//     multi-byte UTF-8 sequence, so the joined escaped string is valid UTF-8
//     if and only if every raw component is.
//   - Idempotent by construction: a hex spelling is ASCII, hence valid
//     UTF-8, and passes through untouched.
//
// Direction limitation: this closes only the stored-raw → post-fix-hash
// direction — a pre-#1132 stored spelling matched by a hash computed over the
// post-fix spelling. The symmetric case, a hash the control plane persisted
// PRE-fix over the raw spelling used to look up POST-fix rows, still silently
// misses: for a post-fix row canon == stored, so no alias is ever generated.
// The hex spelling is invertible, so a legacy de-canonicalization pass could
// close that direction too if it ever matters; it is deliberately not
// implemented.
func CanonicalPKValues(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	parts := splitPKValues(s)
	for i, p := range parts {
		if !utf8.ValidString(p) {
			p = hexPKPrefix + strings.ToUpper(hex.EncodeToString([]byte(p)))
		}
		parts[i] = EscapePKValue(p)
	}
	return strings.Join(parts, "|")
}

// ChangedColumns returns the sorted list of column names whose values differ
// between before and after images. Returns nil for INSERT/DELETE events where
// one image is nil.
func ChangedColumns(before, after map[string]any) []string {
	if before == nil || after == nil {
		return nil
	}
	var changed []string
	for key := range before {
		if !reflect.DeepEqual(before[key], after[key]) {
			changed = append(changed, key)
		}
	}
	sort.Strings(changed)
	return changed
}

// ─── Query-text sanitization (#699) ──────────────────────────────────────────

// MaxQueryTextBytes caps a captured statement's stored size. ROWS_QUERY/
// ANNOTATE events carry the FULL original statement — a bulk INSERT can run to
// megabytes — and every row event of that statement carries the text, so an
// uncapped statement would bloat the batch INSERT on the index path and the
// buffer/payload Parquet on the BYOS path. 16 KiB keeps any realistic
// hand-written or ORM statement intact; only bulk-statement tails are cut.
const MaxQueryTextBytes = 16 * 1024

// QueryTextTruncationMarker is appended to a capped statement so a forensics
// reader can tell a truncated statement from a complete one. (A truncated
// statement is deliberately NOT fed to STATEMENT_DIGEST — it usually ends
// mid-token and would not parse.)
const QueryTextTruncationMarker = " /* bintrail:truncated */"

// SanitizeQueryText prepares a captured statement for storage: it replaces
// invalid UTF-8 (a _binary'...' literal embeds raw bytes, which MySQL strict
// mode would reject with error 1366, aborting the whole batch INSERT) and
// caps the byte length at a rune boundary, appending
// QueryTextTruncationMarker when cut. Applied ONCE at the capture boundary
// (the parsers' ROWS_QUERY/ANNOTATE cases) so every downstream path — index
// batch INSERT, BYOS buffer accounting, payload Parquet — sees bounded, valid
// text; the indexer re-applies it as defense in depth.
func SanitizeQueryText(s string) string {
	if s == "" {
		return ""
	}
	s = strings.ToValidUTF8(s, "�")
	if len(s) <= MaxQueryTextBytes {
		return s
	}
	cut := MaxQueryTextBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + QueryTextTruncationMarker
}

// DDLTable is one table a DDL statement names.
type DDLTable struct {
	Schema, Table string
}

// DDLKind identifies the type of DDL statement detected in a binlog QUERY_EVENT.
// The string values are persisted in schema_changes.ddl_type; keep them stable.
type DDLKind string

const (
	DDLAlterTable    DDLKind = "ALTER TABLE"
	DDLCreateTable   DDLKind = "CREATE TABLE"
	DDLDropTable     DDLKind = "DROP TABLE"
	DDLRenameTable   DDLKind = "RENAME TABLE"
	DDLTruncateTable DDLKind = "TRUNCATE TABLE"
	// DDLReplaceTable is MariaDB's CREATE OR REPLACE TABLE: on an existing
	// table it is a DROP and a CREATE in one statement, so it refuses a
	// reconstruct over its window like a DROP (#1664).
	DDLReplaceTable DDLKind = "CREATE OR REPLACE TABLE"
)

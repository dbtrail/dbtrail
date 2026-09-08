// Package recovery generates reversal SQL from indexed binlog events.
// It reads events via the query engine and emits a transaction-wrapped SQL
// script that undoes each event in reverse chronological order.
package recovery

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// Dialect selects the SQL dialect for generated reversal SQL. The index is
// single-source (stream_state.flavor), so the dialect is decided ONCE at
// construction from that authoritative signal — never inferred per-row from
// resolver/type presence, which would silently emit the wrong dialect for a row
// whose snapshot failed to load (the all-columns fallback path).
type Dialect int

const (
	// MySQLDialect emits MySQL/MariaDB SQL: backtick identifiers, X'..' hex blobs,
	// backslash string escaping. The default, and the only dialect the MySQL-source
	// path — and the reconstruct mydumper writer, via the exported FormatSQLValue/
	// QuoteName — has ever produced.
	MySQLDialect Dialect = iota
	// PostgresDialect emits PostgreSQL SQL: double-quoted identifiers and
	// standard-conforming-string escaping ('' doubling, no backslash). Values
	// captured from pgoutput are already in PostgreSQL's canonical text form, so a
	// quoted literal coerces into the target column's type on INSERT/UPDATE/WHERE —
	// the dialect difference is identifier quoting + string escaping, not per-type
	// literal forms (#533).
	PostgresDialect
)

// Generator produces reversal SQL from indexed binlog events.
type Generator struct {
	db       *sql.DB
	resolver *metadata.Resolver            // default resolver (latest snapshot); may be nil
	cache    map[uint32]*metadata.Resolver // per-snapshot resolvers, loaded lazily
	dialect  Dialect
	// maxScriptBytes bounds the estimated row payload GenerateSQLFromRows will
	// render before it refuses (#654). 0 = unlimited. Defaults to
	// DefaultMaxScriptBytes in the constructors so every caller — CLI, the MCP
	// recover tool, the console, recover-cascade — is guarded with zero config;
	// the recover CLI overrides it via SetMaxScriptBytes (--max-script-bytes).
	maxScriptBytes int64
	// suppressTriggers pins session_replication_role = replica for the reversal
	// transaction on the PostgreSQL path (#1003). Opt-in — see
	// SetSuppressTriggers. Inert on the MySQL path (no such toggle exists).
	suppressTriggers bool
	// restoreAutoIncrement appends the commented-out AUTO_INCREMENT restore
	// checklist after COMMIT on the MySQL path (#1003). Opt-in — see
	// SetRestoreAutoIncrement. Inert on the PostgreSQL path.
	restoreAutoIncrement bool
}

// New creates a Generator emitting MySQL-dialect SQL. resolver may be nil — in that
// case, WHERE clauses for UPDATE and DELETE reversals will use ALL row columns
// instead of just PKs.
func New(db *sql.DB, resolver *metadata.Resolver) *Generator {
	return &Generator{db: db, resolver: resolver, dialect: MySQLDialect, maxScriptBytes: DefaultMaxScriptBytes}
}

// NewForDialect is New with an explicit SQL dialect. Callers that know the source
// flavor (e.g. `bintrail-pg recover` via DialectForIndex) pass PostgresDialect;
// everything else uses New (MySQL).
func NewForDialect(db *sql.DB, resolver *metadata.Resolver, dialect Dialect) *Generator {
	return &Generator{db: db, resolver: resolver, dialect: dialect, maxScriptBytes: DefaultMaxScriptBytes}
}

// DefaultMaxScriptBytes is the default ceiling on the estimated row payload of a
// reversal script. Above it, GenerateSQLFromRows refuses rather than buffering
// the whole script into RAM (#654): rendering builds a second full copy on top
// of the already-resident events, so a multi-gigabyte script roughly doubles
// peak memory. 2 GiB is deliberately generous — an ordinary recover renders
// kilobytes to megabytes, and a 2 GiB SQL script is already past being
// reviewable or appliable; only BLOB/TEXT-heavy windows approach it. The recover
// CLI exposes --max-script-bytes / BINTRAIL_RECOVER_MAX_BYTES to raise it or set
// 0 to disable. The bound is on the *rendered script*, not end-to-end memory:
// the events are fetched (row-count-bounded by --limit) before this runs, so it
// caps the render doubling, not the initial fetch.
const DefaultMaxScriptBytes int64 = 2 << 30

// SetMaxScriptBytes overrides the reversal-script payload budget enforced by
// GenerateSQLFromRows. n <= 0 disables the guard (unlimited). The recover CLI
// sets it from --max-script-bytes; other callers keep DefaultMaxScriptBytes.
func (g *Generator) SetMaxScriptBytes(n int64) { g.maxScriptBytes = n }

// SetSuppressTriggers turns on the PostgreSQL apply-side trigger suppression
// (#1003): the script pins `SET LOCAL session_replication_role = replica` right
// after BEGIN, so the reversal does not re-fire the target's ordinary (ENABLE)
// triggers and double-apply side effects the original triggers already logged as
// their own events. That rationale only holds when those sibling events are in
// the reversal's scope (#1121): under --table/--pk filters the trigger's derived
// rows were never fetched, so suppressing leaves the derived tables holding the
// bad state — neither re-fired nor reverted. The recover CLI wires it from
// --suppress-triggers.
//
// Precision on what `replica` guarantees (#1121): it skips ordinary (ENABLE)
// triggers only. ENABLE ALWAYS triggers still fire, and ENABLE REPLICA triggers
// fire ONLY under replica — the flag can cause trigger side effects a normal
// apply would not.
//
// It is OPT-IN, not the default, for two reasons. (1) Privilege: setting
// session_replication_role requires superuser (PostgreSQL ≤14) or an explicit
// GRANT SET ON PARAMETER (15+), so emitting it unconditionally would break every
// apply performed by an ordinary role — a regression on the default path, hit at
// the worst possible moment. (2) Semantics: `replica` also skips FOREIGN KEY
// constraint triggers, and a skipped check is never re-run — there is no
// deferred re-validation at COMMIT, so rows written in violation stay violating
// permanently while the constraint remains marked valid. A real behavior change
// the operator should choose.
//
// Inert on the MySQL path: MySQL has no session-level toggle to suppress
// triggers during an apply (a fundamental limitation, documented in
// docs/query-and-recovery.md — this does not change it). The recover CLI warns
// when the flag is set against a MySQL/MariaDB index.
func (g *Generator) SetSuppressTriggers(v bool) { g.suppressTriggers = v }

// SetRestoreAutoIncrement appends the AUTO_INCREMENT restore checklist after the
// reversal transaction on the MySQL path (#1003). The recover CLI wires it from
// --restore-auto-increment. Inert on the PostgreSQL path (its sequences are a
// different mechanism, not covered here).
//
// The emitted statements are COMMENTED OUT deliberately — see
// writeAutoIncrementSection for why N cannot be derived from the index and why
// the block sits after COMMIT.
func (g *Generator) SetRestoreAutoIncrement(v bool) { g.restoreAutoIncrement = v }

// CheckScriptBudget returns a *ScriptBudgetError when rendering rows would exceed
// the configured script-size budget (#654), or nil when within budget (or the
// budget is disabled). GenerateSQLFromRows calls it before rendering; callers
// that write their own preamble BEFORE GenerateSQLFromRows — e.g.
// cascaderecover.EmitSQL — call it first so a refusal writes nothing instead of
// leaving a dangling header (and, for cascade, a `SET FOREIGN_KEY_CHECKS=0`).
func (g *Generator) CheckScriptBudget(rows []query.ResultRow) error {
	if g.maxScriptBytes <= 0 {
		return nil
	}
	if est := EstimateScriptBytes(rows); est > g.maxScriptBytes {
		return &ScriptBudgetError{EstimatedBytes: est, Budget: g.maxScriptBytes}
	}
	return nil
}

// DialectForFlavor maps a stream_state.flavor value to the recovery SQL dialect.
// PostgreSQL gets its own dialect; MySQL, MariaDB, and an empty/unknown flavor all
// use MySQL (the established default — MariaDB recovery SQL is MySQL-dialect). It
// owns the canonical "postgres" flavor literal so callers don't re-derive it.
func DialectForFlavor(flavor string) Dialect {
	if flavor == "postgres" {
		return PostgresDialect
	}
	return MySQLDialect
}

// DialectForIndex returns the recovery dialect for an index database, read from the
// source flavor recorded in stream_state (the index is single-source). Best-effort:
// a nil db, or any read failure (no stream_state row on a file-indexed DB, very old
// schema), returns MySQLDialect and never blocks recovery. This is the authoritative
// selection every recover surface uses (cli/recover.go, console, MCP, agent). The nil
// guard lets a caller pass an as-yet-unopened handle (e.g. agent.IndexDB) directly.
// The stream_state read itself lives in query.SourceFlavor (shared with the
// reconstruct gap check, #593) — "" maps to MySQLDialect via DialectForFlavor.
func DialectForIndex(db *sql.DB) Dialect {
	return DialectForFlavor(query.SourceFlavor(db))
}

// resolverForRow returns the resolver matching the row's schema version.
// It loads resolvers lazily and caches them. Falls back to the default
// resolver for SchemaVersion=0 (pre-migration data) or on load failure.
func (g *Generator) resolverForRow(row query.ResultRow) *metadata.Resolver {
	if row.SchemaVersion == 0 || g.db == nil {
		return g.resolver
	}
	if g.resolver != nil && uint32(g.resolver.SnapshotID()) == row.SchemaVersion {
		return g.resolver
	}
	if g.cache != nil {
		if r, ok := g.cache[row.SchemaVersion]; ok {
			return r
		}
	}
	r, err := metadata.NewResolver(g.db, int(row.SchemaVersion))
	if g.cache == nil {
		g.cache = make(map[uint32]*metadata.Resolver)
	}
	if err != nil {
		// Without the snapshot the per-column generated/identity skip-sets are unknown,
		// so reversal SQL for an affected table may fail to apply (PostgreSQL rejects an
		// INSERT/UPDATE that writes a GENERATED ALWAYS identity or generated column). The
		// failure is loud at apply time inside the BEGIN/COMMIT wrapper, not silent.
		slog.Warn("failed to load schema snapshot for schema_version; using default resolver — reversal SQL for tables with generated or identity columns may fail to apply",
			"schema_version", row.SchemaVersion, "error", err)
		// Cache the fallback so we don't repeat the DB query and warning
		// for every row with this version.
		g.cache[row.SchemaVersion] = g.resolver
		return g.resolver
	}
	g.cache[row.SchemaVersion] = r
	return r
}

// GenerateSQL fetches events matching opts, reverses their order (most-recent
// first), and writes a BEGIN/COMMIT-wrapped SQL script to w.
// Returns the number of SQL statements written. A per-event generation error
// (e.g. a malformed/truncated stored row image) makes the whole call refuse
// up front — nothing is written and a non-nil error is returned (#784) — so a
// partial reversal is never silently blessed.
func (g *Generator) GenerateSQL(ctx context.Context, opts query.Options, w io.Writer) (int, error) {
	rows, err := query.New(g.db).Fetch(ctx, opts)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch events: %w", err)
	}
	return g.GenerateSQLFromRows(rows, w)
}

// GenerateSQLFromRows generates reversal SQL from pre-fetched rows. Use this
// when rows have already been fetched and merged from multiple sources (e.g.
// live MySQL + Parquet archives). The rows are reversed so the most-recent
// event is undone first.
func (g *Generator) GenerateSQLFromRows(rows []query.ResultRow, w io.Writer) (int, error) {
	if len(rows) == 0 {
		fmt.Fprintln(w, "-- No events matched the specified criteria.")
		return 0, nil
	}

	// Bound peak memory before rendering (#654). The whole script is buffered
	// into RAM (body, below) on top of the already-resident events, so a
	// pathological BLOB/TEXT window roughly doubles peak. Refuse up front rather
	// than emit a truncated script: a partial reversal applied to production is a
	// silently-wrong recovery — the same fail-loud stance as the schema-drift
	// refusal below. The bound is on the rendered payload, not the initial fetch
	// (the events are already loaded, row-count-bounded by --limit).
	if err := g.CheckScriptBudget(rows); err != nil {
		return 0, err
	}

	// Fail loud on a residual unchanged-TOAST marker (#592). Under the required
	// REPLICA IDENTITY FULL the PostgreSQL decoder resolves every unchanged
	// TOAST value at decode time, so the marker is never persisted for a
	// supported source — one in a fetched row means the capture invariant was
	// violated and the stored image does not carry the real value. The refusal
	// must be UP FRONT, before a single byte reaches w: a per-statement error in
	// the render loop below demotes to a SQL comment and the script keeps
	// rendering, and a reversal that writes the marker's JSON into a real column
	// is silent corruption — the same fail-loud stance as the schema-drift
	// refusal (#601) and the script-budget refusal (#654). Both images are
	// scanned because both are consumed (SET from row_before, WHERE from
	// row_after).
	for _, row := range rows {
		if err := event.CheckUnresolvedToast(row.SchemaName, row.TableName, row.PKValues,
			row.RowBefore, row.RowAfter); err != nil {
			return 0, err
		}
	}

	// Reverse so the most-recent event is undone first.
	// For multiple UPDATEs on the same row this yields the correct
	// rollback order automatically.
	slices.Reverse(rows)

	// Build the body into a buffer first. The whole script is one BEGIN/COMMIT
	// transaction, so if any statement references a column dropped/renamed after the
	// event (schema drift, #601) the entire transaction would roll back at apply time —
	// emitting part of it is worse than emitting nothing. Detecting drift up front and
	// refusing before a single byte reaches w mirrors #602's pre-write fail-loud.
	var body bytes.Buffer
	drift := map[string]map[string]bool{} // "schema.table" -> set of drifted columns
	var driftOrder []string               // table keys in first-seen order, for a stable message
	var genFailures []genFailure          // per-event generation errors, in first-seen order (#784)

	written := 0
	touched := map[string]bool{} // "schema\x00table" of every table the emitted SQL writes (#1003)
	for _, row := range rows {
		fmt.Fprintln(&body)

		gtidSuffix := ""
		if row.GTID != nil {
			gtidSuffix = " gtid=" + SanitizeForComment(*row.GTID)
		}
		// Every interpolated value is sanitized (#1120): a line break in the table
		// name or the PK would end this comment, and the remainder would begin a
		// new line executing inside the BEGIN/COMMIT below. This is the site that
		// ships today — a newline in a VARCHAR primary key is ordinary data.
		fmt.Fprintf(&body, "-- [%d] reverse %s on %s.%s pk=%s at %s%s\n",
			row.EventID,
			eventTypeName(row.EventType),
			SanitizeForComment(row.SchemaName), SanitizeForComment(row.TableName),
			SanitizeForComment(row.PKValues),
			row.EventTimestamp.Format("2006-01-02 15:04:05"),
			gtidSuffix,
		)

		stmt, cols, err := g.buildStatement(row)
		if err != nil {
			// Record the failure, then refuse the whole script below (#784).
			//
			// The "-- ERROR ..." line goes into the buffered body, which the
			// refusal below DISCARDS before anything reaches w — the diagnosis is
			// built from genFailures, not from this text, so this line can never
			// reach an operator. It is kept for readability while debugging a
			// rendered body, and sanitized (#1120) only as defense in depth: it
			// is not a live injection vector the way the header above is.
			//
			// Demoting the error to a
			// comment and returning success is a silent incomplete recovery: a
			// SQL comment has no apply-time effect, so a partial script commits
			// clean under BEGIN/COMMIT and the operator never learns an event
			// was skipped. Same fail-loud stance as the schema-drift (#601) and
			// script-budget (#654) refusals — refuse up front, write nothing.
			fmt.Fprintf(&body, "-- ERROR generating reversal for event %d: %s\n",
				row.EventID, SanitizeForComment(err.Error()))
			genFailures = append(genFailures, genFailure{
				eventID:    row.EventID,
				schemaName: row.SchemaName,
				tableName:  row.TableName,
				pkValues:   row.PKValues,
				err:        err,
			})
			continue
		}
		if d := g.driftedEmitted(row, cols); len(d) > 0 {
			key := row.SchemaName + "." + row.TableName
			if drift[key] == nil {
				drift[key] = map[string]bool{}
				driftOrder = append(driftOrder, key)
			}
			for _, c := range d {
				drift[key][c] = true
			}
		}
		fmt.Fprintln(&body, stmt+";")
		touched[row.SchemaName+"\x00"+row.TableName] = true
		written++
	}

	// Refuse before a single byte reaches w when any event failed to generate
	// (#784) — checked ahead of the drift refusal because a nil/malformed row
	// image is a more fundamental data-integrity problem than a since-renamed
	// column, and both are fail-loud refusals returning zero statements.
	if len(genFailures) > 0 {
		return 0, partialGenerationError(genFailures)
	}

	if len(driftOrder) > 0 {
		return 0, schemaDriftError(drift, driftOrder)
	}

	fmt.Fprintf(w, "-- Generated by bintrail recover at %s\n", time.Now().UTC().Format("2006-01-02 15:04:05 UTC"))
	fmt.Fprintf(w, "-- Events to reverse: %d\n", len(rows))
	fmt.Fprintln(w, "-- IMPORTANT: Review carefully before applying to production.")
	if g.dialect == PostgresDialect && g.suppressTriggers {
		// The trigger caveat below changes shape when the script suppresses them,
		// and the FK side effect of session_replication_role must not be silent.
		// Precision matters here (#1121): `replica` skips ordinary (ENABLE)
		// triggers only — ENABLE ALWAYS triggers still fire, and ENABLE REPLICA
		// triggers fire ONLY in this mode, so the flag can CAUSE trigger side
		// effects a normal apply would not. And a skipped FK constraint trigger is
		// never re-run: there is no deferred re-validation at COMMIT.
		fmt.Fprintln(w, "-- NOTE: this script pins session_replication_role = replica for its transaction")
		fmt.Fprintln(w, "--       (--suppress-triggers). Under replica, ordinary (ENABLE) triggers are skipped,")
		fmt.Fprintln(w, "--       but ENABLE ALWAYS triggers still fire, and ENABLE REPLICA triggers fire ONLY in")
		fmt.Fprintln(w, "--       this mode: the flag can cause trigger side effects a normal apply would not.")
		fmt.Fprintln(w, "--       FOREIGN KEY constraint triggers are also skipped, with NO re-validation at COMMIT:")
		fmt.Fprintln(w, "--       rows written in violation stay violating permanently while the constraint remains")
		fmt.Fprintln(w, "--       marked valid. Suppressing only helps when this script's scope also covers the")
		fmt.Fprintln(w, "--       tables the skipped triggers write; a --table/--pk-filtered reversal leaves their")
		fmt.Fprintln(w, "--       derived rows (audit rows, counters) unreverted either way. Sequences are NOT")
		fmt.Fprintln(w, "--       restored by this script. Apply this file as a whole, in one session (e.g. psql -f):")
		fmt.Fprintln(w, "--       outside a transaction block, SET LOCAL warns and does nothing; triggers would")
		fmt.Fprintln(w, "--       fire while appearing suppressed.")
		fmt.Fprintln(w, "--       See docs/query-and-recovery.md -> Restore limitations.")
	} else {
		fmt.Fprintln(w, "-- NOTE: applying this script fires the target's own triggers (e.g. AFTER INSERT/UPDATE),")
		fmt.Fprintln(w, "--       which can double-apply side effects the original triggers already logged as their")
		fmt.Fprintln(w, "--       own events (reversed above only if this script's filters cover the tables those")
		fmt.Fprintln(w, "--       triggers write). AUTO_INCREMENT/serial counters are NOT restored by this script.")
		fmt.Fprintln(w, "--       See docs/query-and-recovery.md -> Restore limitations.")
	}
	if g.dialect == MySQLDialect && g.restoreAutoIncrement {
		fmt.Fprintln(w, "--       An AUTO_INCREMENT restore checklist follows COMMIT (--restore-auto-increment).")
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "BEGIN;")
	if g.dialect == MySQLDialect {
		// TIMESTAMP/DATETIME literals in the statements below are rendered from
		// the captured (UTC, #757) value with no explicit zone marker. Pin the
		// session to UTC so a target with a non-UTC time_zone doesn't reinterpret
		// the literal and reintroduce the shift capture just fixed.
		fmt.Fprintln(w, "SET time_zone = '+00:00';")
		// EscapeString above renders string literals with backslash escapes
		// (`\\`, `\'`, `\0`); a target session with NO_BACKSLASH_ESCAPES would
		// misparse them and silently corrupt data. Zero-dates (0000-00-00)
		// captured from legacy data would also be rejected under NO_ZERO_DATE/
		// NO_ZERO_IN_DATE, aborting the whole apply. Pin a mode with neither
		// NO_BACKSLASH_ESCAPES nor the zero-date rules — mirroring how the PG
		// path defends its own escaping with standard_conforming_strings (#786).
		// STRICT_TRANS_TABLES is KEPT deliberately: with the zero-date rules
		// absent it still inserts 0000-00-00 cleanly, while truncation, out-of-
		// range and invalid values stay fail-loud during the apply. Dropping
		// strict entirely would silently coerce a captured value that no longer
		// fits a since-narrowed column — data corruption at the worst moment.
		fmt.Fprintln(w, "SET sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';")
	}
	if g.dialect == PostgresDialect {
		// escapePGString relies on standard_conforming_strings=on (PostgreSQL's
		// default), under which a backslash is literal. If the operator applies this
		// script in a session with it OFF, an unescaped backslash would be reinterpreted
		// (silent corruption). SET LOCAL pins it for this transaction only, so the
		// script defends its own escaping regardless of the target session's setting.
		fmt.Fprintln(w, "SET LOCAL standard_conforming_strings = on;")
		if g.suppressTriggers {
			// Suppress the target's ordinary (ENABLE) triggers for this apply
			// (#1003; matrix precision in #1121). Placed
			// inside the BEGIN/COMMIT the whole script already is: SET LOCAL is
			// transaction-scoped, so the applying session's setting is restored at
			// COMMIT/ROLLBACK and never leaks past the reversal. It is the FIRST
			// data-affecting-session statement, so a role without the privilege to
			// set it fails here with nothing written yet — the remedy (apply as a
			// superuser, or drop --suppress-triggers) is in the comment below.
			fmt.Fprintln(w, "-- Suppress the target's ordinary (ENABLE) triggers for this apply (--suppress-triggers);")
			fmt.Fprintln(w, "-- ENABLE ALWAYS triggers still fire, and ENABLE REPLICA triggers fire only in this mode.")
			fmt.Fprintln(w, "-- Requires superuser (PostgreSQL <= 14) or GRANT SET ON PARAMETER session_replication_role")
			fmt.Fprintln(w, "-- TO <role> (15+); without it this statement errors and the transaction applies nothing.")
			fmt.Fprintln(w, "-- It also skips FOREIGN KEY constraint triggers, and skipped checks are never re-run:")
			fmt.Fprintln(w, "-- rows this transaction writes in violation of a foreign key are NOT re-validated at")
			fmt.Fprintln(w, "-- COMMIT; they stay violating permanently while the constraint remains marked valid.")
			fmt.Fprintln(w, "SET LOCAL session_replication_role = replica;")
		}
	}
	if _, err := io.Copy(w, &body); err != nil {
		return 0, err
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "COMMIT;")
	if g.dialect == MySQLDialect && g.restoreAutoIncrement && len(touched) > 0 {
		g.writeAutoIncrementSection(w, touched)
	}
	return written, nil
}

// writeAutoIncrementSection appends the opt-in AUTO_INCREMENT restore checklist
// for every table the reversal writes (#1003, --restore-auto-increment).
//
// Three deliberate properties, all of them load-bearing:
//
//  1. It is emitted AFTER COMMIT. ALTER TABLE is DDL and implicitly commits in
//     MySQL, so placing it inside the reversal transaction would silently split
//     that transaction's atomicity — the very invariant GenerateSQLFromRows
//     buffers the whole body to protect.
//
//  2. The statements are COMMENTED OUT. N is not derivable from the index: the
//     schema snapshot records data types, PKs and generated/identity flags but
//     NOT which column is AUTO_INCREMENT (is_generated comes from
//     GENERATION_EXPRESSION, not EXTRA), and even with the column name the
//     correct N depends on operator intent (reuse the ids the reversal freed, or
//     never reuse them). The block therefore hands over an exact recipe rather
//     than guessing a value that would look plausible and be wrong.
//
//  3. No column name is guessed. Naming the "likely" AUTO_INCREMENT column from,
//     say, a single integer PK would point the operator's SELECT MAX() at the
//     wrong column whenever the guess misses, producing a wrong-but-credible N.
func (g *Generator) writeAutoIncrementSection(w io.Writer, touched map[string]bool) {
	keys := make([]string, 0, len(touched))
	for k := range touched {
		keys = append(keys, k)
	}
	sort.Strings(keys) // stable, reviewable output regardless of map order

	fmt.Fprintln(w)
	fmt.Fprintln(w, "-- ─── AUTO_INCREMENT restore (--restore-auto-increment) ─────────────────────")
	fmt.Fprintln(w, "-- Reversing an INSERT deletes the row but does NOT decrement the table's")
	fmt.Fprintln(w, "-- AUTO_INCREMENT counter; reversing a DELETE re-inserts an explicit id, which can")
	fmt.Fprintln(w, "-- bump it further. After the transaction above the rows are back as they were, but")
	fmt.Fprintln(w, "-- the NEXT generated id may not be.")
	fmt.Fprintln(w, "--")
	fmt.Fprintln(w, "-- The statements below are COMMENTED OUT on purpose: N cannot be derived from the")
	fmt.Fprintln(w, "-- index. The schema snapshot does not record which column is AUTO_INCREMENT, and the")
	fmt.Fprintln(w, "-- correct N depends on your intent (reuse the ids this reversal freed, or never reuse")
	fmt.Fprintln(w, "-- them). Substitute the column name, read the value with the SELECT, then apply the")
	fmt.Fprintln(w, "-- ALTER with the N you want.")
	fmt.Fprintln(w, "--")
	fmt.Fprintln(w, "-- They sit AFTER COMMIT because ALTER TABLE is DDL and implicitly commits in MySQL:")
	fmt.Fprintln(w, "-- inside the transaction above it would break that transaction's atomicity.")
	fmt.Fprintln(w, "-- InnoDB ignores a value at or below the id currently in use, so a too-low N is a")
	fmt.Fprintln(w, "-- no-op, never data loss.")
	for _, k := range keys {
		schema, table, _ := strings.Cut(k, "\x00")
		// Sanitized once here (#1120) because all three uses below are comment
		// lines: this block's ENTIRE safety mechanism is the "--" prefix, and it
		// sits after COMMIT, so a line-break-bearing identifier would run whatever
		// followed it as a standalone statement.
		//
		// Unlike the header above, this block has no executable sibling carrying
		// the exact bytes — the two commented statements ARE the deliverable, and
		// the operator is told to uncomment and run them. For such a table the
		// quoted rendering is not runnable as written, which is the intended
		// outcome: it stops at a syntax error the operator must read, rather than
		// naming a different table that might exist. See SanitizeForComment.
		qualified := SanitizeForComment(QuoteName(schema) + "." + QuoteName(table))
		fmt.Fprintln(w, "--")
		fmt.Fprintf(w, "-- %s:\n", qualified)
		fmt.Fprintf(w, "--   SELECT IFNULL(MAX(%s), 0) + 1 FROM %s;\n", QuoteName("<auto_increment_column>"), qualified)
		fmt.Fprintf(w, "--   ALTER TABLE %s AUTO_INCREMENT = <N>;\n", qualified)
	}
}

// genFailure records one event that could not be turned into reversal SQL, so
// partialGenerationError can name every failed event rather than only the first (#784).
type genFailure struct {
	eventID    uint64
	schemaName string
	tableName  string
	pkValues   string
	err        error
}

// partialGenerationError builds the fail-loud refusal for #784: one or more matched
// events could not be reversed (e.g. a nil before/after image from a malformed or
// truncated stored row), so emitting the remainder would silently commit an INCOMPLETE
// reversal. It names every failed event and its reason. Same fail-loud stance as the
// schema-drift (#601) and script-budget (#654) refusals — recover writes nothing and
// exits non-zero rather than blessing a partial script.
//
// Cascade-synthesized rows (cascaderecover, #835) never carry a real event_id — every
// synthesized victim is EventID==0, so "event 0: ..." is byte-identical across every
// failing victim and untraceable (and the remediation below, which tells the operator
// to narrow --pk/--pks, selects the RECOVERY ROOT, not the victim). Name those failures
// by schema.table + PK instead, using the fields the row already carries.
func partialGenerationError(failures []genFailure) error {
	parts := make([]string, 0, len(failures))
	for _, f := range failures {
		if f.eventID == 0 && f.schemaName != "" {
			parts = append(parts, fmt.Sprintf("%s.%s pk=%s: %v", f.schemaName, f.tableName, f.pkValues, f.err))
			continue
		}
		parts = append(parts, fmt.Sprintf("event %d: %v", f.eventID, f.err))
	}
	return fmt.Errorf("recover: refusing to emit reversal SQL — %d of the matched event(s) could not be "+
		"reversed (malformed or truncated stored row image), so the script would be a silently incomplete "+
		"recovery: %s. Investigate the named event(s); narrow the recovery window (--since/--until, --pk/--pks) "+
		"to exclude them if they are known-unrecoverable", len(failures), strings.Join(parts, "; "))
}

// schemaDriftError builds the fail-loud error for #601: the reversal SQL references
// columns the latest schema snapshot no longer carries. It names every affected
// schema.table and its drifted columns so the operator knows exactly what to reconcile.
//
// The remediation covers the case the accident-shaped ones miss. Drift is not
// necessarily a mistake: in a long-lived index the matched event can simply predate a
// DDL, so the reversal is correct for the shape that existed then and wrong for the
// shape now. Re-snapshotting does nothing there (the snapshot is already current), and
// the message used to offer only that and hand reconciliation.
//
// Only cause 1 is DIAGNOSABLE — the live table still having the column is observable.
// Past that, driftedColumns is in one state and the choice is about intent, not
// evidence, so the message must not present narrowing as strictly better. Narrowing
// EXCLUDES the drifted events, which then go unreversed, and nothing downstream reports
// that a PK lost coverage because the window moved (recover.go warns on elided archives
// and truncation only). This file refuses silently-incomplete recoveries everywhere
// else — partialGenerationError scopes the same advice "if they are known-unrecoverable"
// — so the cost is stated here rather than left for the operator to discover.
//
// The window advice is written "since/until", never "--since": this error crosses the
// CLI, MCP and console surfaces (GenerateSQLFromRows is shared, and neither the MCP nor
// the console rewrites it), and an MCP client handed a --flag will not find it. That is
// the #1114 rule, precedent in internal/mcptools/recover_cascade_test.go. The guard bans
// the specific CLI spellings rather than every "--", per #1271: operator-addressed
// remediation prose naming a shell command is deliberately NOT a leak.
func schemaDriftError(drift map[string]map[string]bool, order []string) error {
	parts := make([]string, 0, len(order))
	for _, key := range order {
		cols := make([]string, 0, len(drift[key]))
		for c := range drift[key] {
			cols = append(cols, c)
		}
		sort.Strings(cols)
		parts = append(parts, fmt.Sprintf("%s (%s)", key, strings.Join(cols, ", ")))
	}
	return fmt.Errorf("recover: refusing to emit reversal SQL — it references column(s) that the latest schema "+
		"snapshot no longer has (dropped or renamed after the event was captured), so the SQL would not apply to "+
		"the current table: %s. If the table does still have these columns the snapshot is stale, so "+
		"re-snapshot. Otherwise the matched event was captured under an earlier shape of this table than the "+
		"one the SQL would apply to, and there are two ways on. Narrowing the recovery window in time "+
		"(since/until) to events captured under the current shape emits normally, but the events you exclude "+
		"are then NOT reversed, so take it only when a current-shape event holds the state you want. If you "+
		"need the excluded events, reconcile the column(s) by hand", strings.Join(parts, "; "))
}

// ─── Script-size budget (#654) ─────────────────────────────────────────────────

// ScriptBudgetError is returned by GenerateSQLFromRows when the estimated row
// payload of the matched events exceeds the configured script-size budget
// (#654). It is a fail-loud refusal, not a truncation: a partial reversal script
// applied to a production database is a silently-wrong recovery, so recover
// refuses to emit anything. The recover CLI wraps it with command-specific
// guidance; the typed fields let other callers report it however they like.
type ScriptBudgetError struct {
	EstimatedBytes int64 // estimated row payload to render
	Budget         int64 // the configured ceiling that was exceeded
}

func (e *ScriptBudgetError) Error() string {
	return fmt.Sprintf("recover: refusing to generate the reversal script — the matched events hold ~%s "+
		"of row data, over the script-size budget of %s. Rendering the SQL would roughly double peak memory "+
		"(the events are already loaded). Narrow the recovery window, or raise/disable the budget (0 = unlimited)",
		humanizeBytes(e.EstimatedBytes), humanizeBytes(e.Budget))
}

// EstimateScriptBytes returns a cheap estimate of the row payload
// GenerateSQLFromRows will render into the in-memory script buffer (#654). It
// sums the resident before- and after-image bytes plus the PK across every row,
// walking the already-decoded maps (allocation-free). BOTH images are counted
// because the reversal can reference either, and a reverse UPDATE references
// both: DELETE→INSERT renders the before image; INSERT→DELETE renders the after
// image in its WHERE clause; UPDATE renders the before image in SET AND the after
// image in WHERE (see buildUpdate/buildDelete — the WHERE keys on row_after, the
// nil-resolver fallback using every after column). Summing both never
// under-counts the referenced payload, so the guard fails loud rather than
// letting an oversized script through. The rendered SQL adds escaping (binary →
// hex can roughly double a value) plus identifiers and comments, so this is a
// payload proxy, not byte-exact — adequate for the GB-scale pathological windows
// the guard targets. (A reverse DELETE of an INSERT whose PK is known renders
// only the key, so this is then a conservative over-estimate — the safe, fail-
// loud, recoverable direction.)
func EstimateScriptBytes(rows []query.ResultRow) int64 {
	var total int64
	for i := range rows {
		total += int64(len(rows[i].PKValues))
		total += estimateRowBytes(rows[i].RowBefore)
		total += estimateRowBytes(rows[i].RowAfter)
	}
	return total
}

func estimateRowBytes(m map[string]any) int64 {
	var t int64
	for k, v := range m {
		t += int64(len(k)) + estimateValueBytes(v)
	}
	return t
}

// estimateValueBytes approximates the in-memory footprint of a JSON-decoded
// value. Strings ([]byte base64 / TEXT / hex) and json.Number dominate the
// payload of fat rows, so their exact length is counted; scalars get a small
// constant. After json.Unmarshal into map[string]any, numbers are float64 (see
// the codebase's JSON round-trip note), with json.Number covered for UseNumber
// callers.
func estimateValueBytes(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case string:
		return int64(len(x))
	case []byte:
		return int64(len(x))
	case json.Number:
		return int64(len(x))
	case bool:
		return 1
	case float64:
		return 8
	case map[string]any:
		return estimateRowBytes(x)
	case []any:
		var t int64
		for _, e := range x {
			t += estimateValueBytes(e)
		}
		return t
	default:
		return 16
	}
}

// humanizeBytes formats a byte count with a binary KB/MB/GB suffix, matching the
// units cliutil.ParseByteSize accepts on the --max-script-bytes flag.
func humanizeBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2fGB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2fMB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2fKB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// ─── Statement generators ─────────────────────────────────────────────────────

// buildStatement is generateStatement plus the set of columns the emitted SQL actually
// references — schema-drift detection (#601) compares exactly those against the latest
// snapshot, so the check can never diverge from what is emitted. generateStatement is a
// thin wrapper kept for the test call sites that only need the SQL (no production caller
// uses it — GenerateSQLFromRows calls buildStatement directly).
func (g *Generator) buildStatement(row query.ResultRow) (string, []string, error) {
	switch row.EventType {
	case event.EventDelete:
		return g.buildInsert(row) // DELETE → INSERT (restore the deleted row)
	case event.EventUpdate:
		return g.buildUpdate(row) // UPDATE → reverse UPDATE (restore before state)
	case event.EventInsert:
		return g.buildDelete(row) // INSERT → DELETE (remove the inserted row)
	case event.EventSnapshot:
		// Snapshot rows are read-only baseline state, not change events, so
		// reversal SQL is undefined for them. Reject with a clear message
		// instead of falling through to the generic "unknown event type"
		// error — this path is only reachable if future code wires snapshots
		// into the recover pipeline.
		return "", nil, fmt.Errorf("cannot generate reversal SQL for SNAPSHOT event %d (baseline rows are read-only)", row.EventID)
	default:
		return "", nil, fmt.Errorf("unknown event type %d", row.EventType)
	}
}

func (g *Generator) generateStatement(row query.ResultRow) (string, error) {
	stmt, _, err := g.buildStatement(row)
	return stmt, err
}

// driftedEmitted returns the subset of emitted columns that existed in the event-time
// snapshot but are absent from the latest snapshot — columns dropped or renamed after
// the event was captured, whose presence in the reversal SQL would make it fail to apply
// against the current table (#601). Returns nil (detection off) whenever the schema
// knowledge for the comparison is unavailable: no latest-snapshot resolver, or the table
// is missing from either snapshot. The event-time membership gate is what prevents a
// false positive on a column ADDED after the event and not re-snapshotted — such a column
// is absent from the event-time snapshot too, so it is never flagged.
func (g *Generator) driftedEmitted(row query.ResultRow, emitted []string) []string {
	if g.resolver == nil || len(emitted) == 0 {
		return nil
	}
	evtR := g.resolverForRow(row)
	if evtR == nil {
		return nil
	}
	evtTM, err := evtR.Resolve(row.SchemaName, row.TableName)
	if err != nil {
		// Event-time table not in its snapshot — can't establish the event's columns,
		// so drift is undeterminable. Warn rather than go dark: every other resolver
		// path in this file logs on a resolve failure, and this detector's whole job is
		// to surface drift.
		slog.Warn("cannot resolve event-time schema for drift check; reversal SQL not checked for dropped/renamed columns",
			"schema", row.SchemaName, "table", row.TableName, "schema_version", row.SchemaVersion, "error", err)
		return nil
	}
	curTM, err := g.resolver.Resolve(row.SchemaName, row.TableName)
	if err != nil {
		// Table absent from the LATEST snapshot — usually a table dropped after the
		// event (its reversal would fail to apply), but also a legitimately scoped
		// --schemas/--tables snapshot. Ambiguous, so warn-and-continue rather than
		// refuse (mirrors pkWhereClause's all-columns fallback); apply-time stays loud.
		slog.Warn("cannot resolve current schema for drift check; reversal SQL not checked for dropped/renamed columns (table may have been dropped after the event)",
			"schema", row.SchemaName, "table", row.TableName, "error", err)
		return nil
	}
	return driftedColumns(emitted, evtTM, curTM)
}

// driftedColumns returns the columns in `emitted` that the event-time schema had but the
// current schema no longer has (dropped or renamed after the event). Pure and
// dialect-agnostic — the result preserves `emitted` order and is deduplicated. #601.
func driftedColumns(emitted []string, eventTime, current *metadata.TableMeta) []string {
	if eventTime == nil || current == nil {
		return nil
	}
	evtSet := columnNameSet(eventTime.Columns)
	curSet := columnNameSet(current.Columns)
	var drifted []string
	seen := map[string]bool{}
	for _, c := range emitted {
		if !seen[c] && evtSet[c] && !curSet[c] {
			drifted = append(drifted, c)
			seen[c] = true
		}
	}
	return drifted
}

func columnNameSet(cols []metadata.ColumnMeta) map[string]bool {
	s := make(map[string]bool, len(cols))
	for _, c := range cols {
		s[c.Name] = true
	}
	return s
}

// base64Cols maps each column of a table that ends up stored as []byte — the
// BLOB/TEXT families (both binlog type MYSQL_TYPE_BLOB, delivered as []byte by
// go-mysql) plus BINARY/VARBINARY (reinterpreted as []byte by metadata.MapRow,
// #756) — to whether it is binary. marshalRow base64-encodes those non-JSON
// []byte values into the stored JSON, so on recovery the value comes back as a
// base64 STRING that must be decoded before emission or the reversal SQL
// writes the base64 text verbatim (#653): binary → X'hex', text → a string
// literal.
//
// Returns nil (no coercion) for a non-MySQL dialect, a nil resolver, or an
// unresolvable table. In the last two cases BLOB/TEXT values are still emitted as
// base64 — recover without usable schema metadata cannot type its columns; the
// caller has already warned about the missing snapshot (resolverForRow /
// pkWhereClause), so this does not warn again.
func (g *Generator) base64Cols(r *metadata.Resolver, schema, table string) map[string]bool {
	if g.dialect != MySQLDialect || r == nil {
		return nil
	}
	tm, err := r.Resolve(schema, table)
	if err != nil {
		return nil
	}
	var m map[string]bool
	for _, c := range tm.Columns {
		binary, ok := base64StoredKind(c.DataType)
		if !ok {
			continue
		}
		if m == nil {
			m = make(map[string]bool)
		}
		m[c.Name] = binary
	}
	return m
}

// geometryCols maps each GEOMETRY-family column of a table to true, so buildInsert/
// buildUpdate render it via geometryLiteral (ST_GeomFromWKB) instead of the base64
// string path (#788). Returns nil (no coercion) for a non-MySQL dialect, a nil
// resolver, or an unresolvable table — the same gating as base64Cols. GEOMETRY is
// kept separate from base64Cols because its at-rest bytes are SRID+WKB, not a plain
// blob: routing it through decodeStoredBase64 would emit X'hex' of the whole SRID+WKB
// buffer, which a geometry column cannot load.
func (g *Generator) geometryCols(r *metadata.Resolver, schema, table string) map[string]bool {
	if g.dialect != MySQLDialect || r == nil {
		return nil
	}
	tm, err := r.Resolve(schema, table)
	if err != nil {
		return nil
	}
	var m map[string]bool
	for _, c := range tm.Columns {
		if !isSpatialType(c.DataType) {
			continue
		}
		if m == nil {
			m = make(map[string]bool)
		}
		m[c.Name] = true
	}
	return m
}

// isSpatialType reports whether a DataType is in the MySQL GEOMETRY family. The
// names match information_schema.COLUMNS.DATA_TYPE (MySQL 8.0.11+ reports a
// GEOMETRYCOLLECTION column as "geomcollection"; older MySQL and MariaDB as
// "geometrycollection" — both accepted), mirroring the spatial set already
// enumerated in internal/verify.
func isSpatialType(dataType string) bool {
	switch strings.ToLower(dataType) {
	case "geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		"geometrycollection", "geomcollection":
		return true
	default:
		return false
	}
}

// geometryLiteral renders a stored GEOMETRY value as ST_GeomFromWKB(X'<wkb hex>', <srid>)
// (#788). go-mysql delivers a geometry value as its at-rest MySQL bytes — SRID (4
// bytes, little-endian) followed by the WKB — and marshalRow base64-encodes that
// buffer into the stored JSON, so the value arrives here as a base64 STRING. We
// base64-decode it, split off the 4-byte SRID, and pass the remaining WKB to
// ST_GeomFromWKB with the SRID as its second argument (SRID 0 is emitted explicitly
// too, so a re-load preserves the exact SRID). Without this the base64 string would
// be emitted as a plain literal, which a geometry column cannot load — failing the
// whole BEGIN/COMMIT reversal over a single geometry column.
//
// Returns ("", false) for a nil, non-string, non-decodable, or too-short (<4 bytes)
// value so the caller falls back to ordinary formatting (nil → NULL); a malformed
// stored value is thus no worse off than before this fix.
func geometryLiteral(v any) (string, bool) {
	s, ok := v.(string)
	if !ok {
		return "", false
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil || len(raw) < 4 {
		return "", false
	}
	srid := binary.LittleEndian.Uint32(raw[:4])
	wkb := raw[4:]
	return fmt.Sprintf("ST_GeomFromWKB(X'%s', %d)", hex.EncodeToString(wkb), srid), true
}

// formatColumnValue renders one column's stored value as a MySQL literal, applying
// the type-specific decode the stored JSON needs: a GEOMETRY column →
// ST_GeomFromWKB(...) (#788), a BLOB/TEXT/BINARY/JSON column → base64-decoded then
// formatted (#653/#736/#756), everything else formatted as-is. geom and b64 are the
// per-table classification maps built by geometryCols/base64Cols (both nil when the
// table is untyped, in which case the value formats as-is — the schemaless fallback).
func (g *Generator) formatColumnValue(col string, v any, geom, b64 map[string]bool) string {
	if geom[col] {
		if lit, ok := geometryLiteral(v); ok {
			return lit
		}
		// nil or malformed geometry: fall through to ordinary formatting
		// (nil → NULL; anything else keeps its pre-#788 rendering).
	}
	if binary, ok := b64[col]; ok {
		v = decodeStoredBase64(v, binary)
	}
	return g.formatValue(v)
}

// base64StoredKind reports whether a column's DataType is in the BLOB or TEXT
// family — the ones go-mysql delivers as []byte so marshalRow base64-encodes them
// in storage — and if so whether it is binary (true → emit X'hex') or text
// (false → emit a string literal). go-mysql also delivers GEOMETRY as []byte, but
// it is NOT in this set: a bare X'hex' literal can't load a geometry column, so
// GEOMETRY is handled on its own path (geometryLiteral →
// ST_GeomFromWKB(X'<wkb>', <srid>), #788) because its at-rest form is SRID+WKB.
//
// "vector" is included (binary) since #1144: go-mysql delivers VECTOR via
// decodeBlob as the at-rest packed little-endian float32 bytes, and MySQL 9
// accepts exactly those bytes back as a hex literal — verified empirically
// against MySQL 9.7 that INSERT/UPDATE with X'<HEX(v)>' round-trips
// byte-identically into a VECTOR column, while a quoted string literal is
// rejected (ER 6136), which is why the pre-#1144 base64 emission failed the
// whole BEGIN/COMMIT script at apply time.
//
// "json" is included (non-binary) as a defense-in-depth companion to #736:
// marshalRow now only promotes a []byte to raw JSON when it looks like a
// JSON container ({ or [), so a JSON column whose top-level value is itself a
// bare scalar (rare, but legal) falls through to this same base64 path
// instead of failing to round-trip.
//
// "binary"/"varbinary" are included (binary) since #756: metadata.MapRow now
// reinterprets those two DataTypes as []byte (they arrive from go-mysql as a
// raw Go string with no charset, which json.Marshal could silently corrupt to
// U+FFFD), so they take the same []byte-to-base64 storage path as BLOB and
// must be decoded the same way on the way back out.
//
// Retroactive-reclassification risk (#756, accepted — unlike BLOB/TEXT, which
// were ALWAYS []byte-and-therefore-base64 from day one): a BINARY/VARBINARY
// event indexed BEFORE this fix shipped was stored as a PLAIN (non-base64)
// string, since go-mysql handed it to marshalRow as a Go string, not []byte.
// decodeStoredBase64 cannot tell "pre-fix plain string" from "post-fix base64
// string" — both are just strings in the stored JSON — so it now attempts to
// base64-decode old values too. It silently no-ops on a string that isn't
// valid base64 (see decodeStoredBase64), but a pre-fix value whose raw bytes
// happen to satisfy the base64 alphabet and padding (astronomically unlikely
// for genuinely random binary content, but plausible for a VARBINARY column
// storing ASCII-like data, e.g. a hex-encoded token) decodes to DIFFERENT,
// wrong bytes with no error. Fully closing this would need a per-event
// storage-format marker (there is none), which is out of proportion to
// #756's reported corruption class; this is the same class of accepted,
// documented historical-data ambiguity as the #736 nil-case gap below.
func base64StoredKind(dataType string) (binary, ok bool) {
	switch strings.ToLower(dataType) {
	case "blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary",
		"vector":
		return true, true
	case "text", "tinytext", "mediumtext", "longtext", "json":
		return false, true
	default:
		return false, false
	}
}

// decodeStoredBase64 reverses the storage-side base64 encoding of a BLOB/TEXT
// value (see base64Cols). binary selects the decoded Go type so FormatSQLValue
// emits X'hex' (binary) vs a quoted string (text). A value that is not a
// decodable base64 string is returned unchanged (defensive — NULL or
// pre-existing non-base64 data).
//
// bool/json.Number repair (#736): events indexed before marshalRow was fixed
// to gate on looksLikeJSONContainer may hold a BLOB/TEXT value that was
// mis-promoted to a bare JSON scalar — e.g. the literal string "false" stored
// as the JSON boolean false instead of the string "false". Decoding such a
// value yields a Go bool/json.Number instead of a string; since that value
// IS the column's original textual literal, it is restored directly rather
// than left corrupted. A value that decoded to Go nil (originally the
// string "null") is NOT repairable here — it is indistinguishable from a
// genuine SQL NULL and is left as nil (documented limitation, not guessed
// at). This nil case, and a bare JSON *string* scalar (bytes like `"YWJj"`,
// quotes included) that was mis-promoted the same way, are historical-only
// gaps: by the time this runs, the pre-#736 marshalRow had already parsed
// the outer quotes away as ordinary JSON-string syntax, so the value
// arriving here is the already-quote-stripped text (`YWJj`),
// indistinguishable from genuine base64 content and wrongly re-decoded on
// top of the original corruption — not repairable, a real fix belongs at
// the storage encoding, out of scope here. A genuine JSON column captured
// AFTER this fix with a bare string-scalar value does NOT hit this gap: it
// takes the ordinary []byte-to-base64 path (same as any TEXT/BLOB), and this
// function correctly reverses it to the original bytes, quotes included —
// which is exactly the text MySQL needs to re-parse the value back into
// that JSON column.
func decodeStoredBase64(v any, binary bool) any {
	var text string
	switch val := v.(type) {
	case string:
		b, err := base64.StdEncoding.DecodeString(val)
		if err != nil {
			return v
		}
		if binary {
			return b
		}
		return string(b)
	case bool:
		text = strconv.FormatBool(val)
	case json.Number:
		text = string(val)
	default:
		return v
	}
	if binary {
		return []byte(text)
	}
	return text
}

// generateInsert reverses a DELETE event: reconstruct the deleted row from
// row_before with a full INSERT, skipping STORED/VIRTUAL generated columns. On the
// PostgreSQL path it emits OVERRIDING SYSTEM VALUE so a GENERATED ALWAYS AS IDENTITY
// column accepts its restored value (#557); the clause is a harmless no-op on tables
// without such a column and on GENERATED BY DEFAULT identity (verified against live
// PG 14–17 by the integration suite), so it is emitted unconditionally rather than
// gated on identity metadata — keeping the highest-frequency recovery op robust.
// Identity columns are KEPT (the real id is the point of recovery); only generated
// columns are omitted.
func (g *Generator) generateInsert(row query.ResultRow) (string, error) {
	stmt, _, err := g.buildInsert(row)
	return stmt, err
}

func (g *Generator) buildInsert(row query.ResultRow) (string, []string, error) {
	if row.RowBefore == nil {
		return "", nil, fmt.Errorf("row_before is nil for DELETE event (event_id=%d)", row.EventID)
	}
	r := g.resolverForRow(row)
	genCols := generatedColsFromResolver(r, row.SchemaName, row.TableName)
	b64 := g.base64Cols(r, row.SchemaName, row.TableName)
	geom := g.geometryCols(r, row.SchemaName, row.TableName)
	var cols, colParts, valParts []string
	for _, col := range sortedKeys(row.RowBefore) {
		if genCols[col] {
			continue
		}
		cols = append(cols, col)
		colParts = append(colParts, g.quoteName(col))
		valParts = append(valParts, g.formatColumnValue(col, row.RowBefore[col], geom, b64))
	}
	overriding := ""
	if g.dialect == PostgresDialect {
		overriding = " OVERRIDING SYSTEM VALUE"
	}
	return fmt.Sprintf("INSERT INTO %s.%s (%s)%s VALUES (%s)",
		g.quoteName(row.SchemaName), g.quoteName(row.TableName),
		strings.Join(colParts, ", "),
		overriding,
		strings.Join(valParts, ", "),
	), cols, nil
}

// generateUpdate reverses an UPDATE event: SET all columns to row_before values
// (skipping generated and GENERATED ALWAYS identity columns, #557), WHERE identifies
// the row using row_after PK values.
func (g *Generator) generateUpdate(row query.ResultRow) (string, error) {
	stmt, _, err := g.buildUpdate(row)
	return stmt, err
}

func (g *Generator) buildUpdate(row query.ResultRow) (string, []string, error) {
	if row.RowBefore == nil {
		return "", nil, fmt.Errorf("row_before is nil for UPDATE event (event_id=%d)", row.EventID)
	}
	if row.RowAfter == nil {
		return "", nil, fmt.Errorf("row_after is nil for UPDATE event (event_id=%d)", row.EventID)
	}

	// SET clause: restore before-image values, omitting columns PostgreSQL forbids in
	// a SET — STORED/VIRTUAL generated columns AND GENERATED ALWAYS identity columns
	// (#557). PostgreSQL permits only SET <col> = DEFAULT on a GENERATED ALWAYS column,
	// never an explicit value, so a reverse-UPDATE (which has no OVERRIDING clause)
	// cannot restore its before-image regardless — omitting it is the only valid choice.
	// The WHERE clause still PK-targets the column.
	r := g.resolverForRow(row)
	skipCols := updateSetSkipCols(r, row.SchemaName, row.TableName)
	b64 := g.base64Cols(r, row.SchemaName, row.TableName)
	geom := g.geometryCols(r, row.SchemaName, row.TableName)
	var cols, setParts []string
	for _, col := range sortedKeys(row.RowBefore) {
		if skipCols[col] {
			continue
		}
		cols = append(cols, col)
		setParts = append(setParts, g.quoteName(col)+" = "+g.formatColumnValue(col, row.RowBefore[col], geom, b64))
	}

	// WHERE uses row_after (current state), so the UPDATE finds the right row
	// even if the PK itself was changed in the original UPDATE.
	whereParts, whereCols, allCols := g.pkWhereClause(r, row.SchemaName, row.TableName, row.RowAfter)
	cols = append(cols, whereCols...)

	stmt := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		g.quoteName(row.SchemaName), g.quoteName(row.TableName),
		strings.Join(setParts, ", "),
		strings.Join(whereParts, " AND "),
	)
	stmt += g.rowLimitSuffix(allCols)
	return g.allColsFallbackWarningComment(allCols) + stmt, cols, nil
}

// generateDelete reverses an INSERT event: delete the inserted row using its
// row_after PK values (the current DB state).
func (g *Generator) generateDelete(row query.ResultRow) (string, error) {
	stmt, _, err := g.buildDelete(row)
	return stmt, err
}

func (g *Generator) buildDelete(row query.ResultRow) (string, []string, error) {
	if row.RowAfter == nil {
		return "", nil, fmt.Errorf("row_after is nil for INSERT event (event_id=%d)", row.EventID)
	}
	r := g.resolverForRow(row)
	whereParts, whereCols, allCols := g.pkWhereClause(r, row.SchemaName, row.TableName, row.RowAfter)
	stmt := fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
		g.quoteName(row.SchemaName), g.quoteName(row.TableName),
		strings.Join(whereParts, " AND "),
	)
	stmt += g.rowLimitSuffix(allCols)
	return g.allColsFallbackWarningComment(allCols) + stmt, whereCols, nil
}

// rowLimitSuffix returns " LIMIT 1" for a reverse UPDATE/DELETE whose WHERE fell
// back to all columns. The original row event touched exactly ONE row, but an
// all-columns WHERE matches every byte-identical duplicate (PK-less table, no
// snapshot) — without the cap, reversing one INSERT deletes ALL copies (#789).
// MySQL-dialect only: PostgreSQL has no UPDATE/DELETE ... LIMIT, so the PG
// fallback stays unbounded (documented caveat in docs/query-and-recovery.md).
func (g *Generator) rowLimitSuffix(allColsFallback bool) string {
	if allColsFallback && g.dialect == MySQLDialect {
		return " LIMIT 1"
	}
	return ""
}

// allColsFallbackWarningComment returns a leading SQL comment line flagging that the
// statement below has no per-row cap, for the one dialect where rowLimitSuffix can't
// supply one. PostgreSQL has no UPDATE/DELETE ... LIMIT, so the all-columns fallback
// stays unbounded there (#789) with zero runtime signal otherwise — a PK-less table
// with duplicate rows silently over-applies (reversing one INSERT deletes every
// byte-identical copy) and the only prior warning was a docs paragraph. The comment
// is emitted (rather than only a slog.Warn) because it travels with the script into
// --dry-run/--output, which is where an operator actually reviews before applying.
func (g *Generator) allColsFallbackWarningComment(allColsFallback bool) string {
	if allColsFallback && g.dialect == PostgresDialect {
		return "-- WARNING: unbounded all-columns WHERE, no per-statement row cap on this dialect " +
			"- may affect every byte-identical duplicate row, not just the one this event touched\n"
	}
	return ""
}

// generatedColsFromResolver returns the set of STORED/VIRTUAL generated column
// names for a table, using the provided resolver. Returns nil when the resolver
// is absent or the table is not in the snapshot — callers treat nil as an empty set.
func generatedColsFromResolver(resolver *metadata.Resolver, schema, table string) map[string]bool {
	if resolver == nil {
		return nil
	}
	tm, err := resolver.Resolve(schema, table)
	if err != nil {
		slog.Warn("cannot determine generated columns; reversal INSERT may include generated columns",
			"schema", schema, "table", table, "error", err)
		return nil
	}
	var gen map[string]bool
	for _, c := range tm.Columns {
		if c.IsGenerated {
			if gen == nil {
				gen = make(map[string]bool)
			}
			gen[c.Name] = true
		}
	}
	return gen
}

// updateSetSkipCols returns the columns to omit from a reverse-UPDATE SET clause:
// STORED/VIRTUAL generated columns AND PostgreSQL GENERATED ALWAYS identity columns
// (#557) — PostgreSQL rejects `SET <col> = <value>` on either ("column can only be
// updated to DEFAULT"). A GENERATED ALWAYS column accepts only `SET <col> = DEFAULT`,
// never an explicit value, so a reverse-UPDATE can never restore its before-image and
// omitting it is the only valid choice. (A GENERATED BY DEFAULT identity is NOT
// skipped — PostgreSQL allows an explicit value there, which is required to reverse a
// PK-changing UPDATE.) Returns nil when the resolver is absent or the table is not in
// the snapshot.
func updateSetSkipCols(resolver *metadata.Resolver, schema, table string) map[string]bool {
	if resolver == nil {
		return nil
	}
	tm, err := resolver.Resolve(schema, table)
	if err != nil {
		slog.Warn("cannot determine generated/identity columns; reversal UPDATE may SET a generated or identity column",
			"schema", schema, "table", table, "error", err)
		return nil
	}
	var skip map[string]bool
	for _, c := range tm.Columns {
		if c.IsGenerated || c.IsIdentityAlways {
			if skip == nil {
				skip = make(map[string]bool)
			}
			skip[c.Name] = true
		}
	}
	return skip
}

// pkWhereClause builds "pk_col = val AND ..." from the given resolver, in the
// Generator's dialect. Falls back to ALL columns if the table cannot be resolved
// (e.g. table was dropped, or no snapshot was loaded). Note: on the PostgreSQL
// path the all-columns fallback can emit `"col" = '...'` for a json column, which
// has no `=` operator in PostgreSQL (jsonb does) — PK-scoped (the #533 norm) avoids
// it since PKs are scalars.
//
// It returns both the rendered clauses and the column names they reference, so
// schema-drift detection (#601) checks exactly the columns the WHERE emits — a PK-scoped
// WHERE whose PK still exists carries no drifted column even when some other column was
// dropped, so that recovery is (correctly) not refused. allColumns reports whether the
// all-columns fallback was taken — callers cap those statements with LIMIT 1 (#789),
// since an all-columns WHERE matches every byte-identical duplicate, not one row.
func (g *Generator) pkWhereClause(resolver *metadata.Resolver, schema, table string, row map[string]any) (clauses []string, cols []string, allColumns bool) {
	if resolver != nil {
		tm, err := resolver.Resolve(schema, table)
		if err != nil {
			slog.Warn("cannot resolve table for PK lookup; using all-columns WHERE",
				"schema", schema, "table", table, "error", err)
		} else {
			pkCols := tm.PKColumnMetas()
			if len(pkCols) > 0 {
				// A BLOB/TEXT column can be a PK with a prefix length, and its
				// row value is the same base64 string as elsewhere — decode it or
				// the WHERE matches zero rows (silent no-op recovery, #653).
				b64 := g.base64Cols(resolver, schema, table)
				parts := make([]string, 0, len(pkCols))
				names := make([]string, 0, len(pkCols))
				allFound := true
				for _, pk := range pkCols {
					v, ok := row[pk.Name]
					if !ok {
						allFound = false
						break
					}
					if binary, isB64 := b64[pk.Name]; isB64 {
						v = decodeStoredBase64(v, binary)
					}
					parts = append(parts, g.quoteName(pk.Name)+" = "+g.formatValue(v))
					names = append(names, pk.Name)
				}
				if allFound {
					return parts, names, false
				}
			}
		}
	}
	// Fallback: all columns — verbose, and NOT unique when byte-identical
	// duplicate rows exist (PK-less table, no snapshot). Callers bound the
	// statement with LIMIT 1 on MySQL so the reversal touches exactly one
	// row, like the original event did (#789).
	clauses, cols = g.allColsWhere(row)
	return clauses, cols, true
}

func (g *Generator) allColsWhere(row map[string]any) (clauses []string, cols []string) {
	cols = sortedKeys(row)
	parts := make([]string, len(cols))
	for i, col := range cols {
		v := row[col]
		if v == nil {
			// `col = NULL` is never true in SQL (NULL comparisons are UNKNOWN, not
			// TRUE) — a `col = NULL` fallback WHERE would match zero rows, silently
			// no-op'ing the reversal on any PK-less/unresolvable table with a NULL
			// column (#762). `IS NULL` is the correct predicate for both dialects.
			parts[i] = g.quoteName(col) + " IS NULL"
			continue
		}
		parts[i] = g.quoteName(col) + " = " + g.formatValue(v)
	}
	return parts, cols
}

// ─── Value formatting ─────────────────────────────────────────────────────────

// FormatSQLValue renders a Go value as a MySQL literal suitable for embedding
// in a generated SQL statement. Exported so other packages (notably the
// mydumper writer in internal/reconstruct, #187) can reuse the exact same
// formatting and escaping.
//
// Binlog-event values arrive here after a JSON round-trip — row_before/row_after
// are decoded via query.UnmarshalRowImage, so numeric values are json.Number
// (the exact literal, no float64 rounding — #496); the json.Number case emits
// them verbatim.
//
// DuckDB's database/sql driver (used by the full-table reconstruct path) returns
// int64 / float64 / time.Time / []byte natively — those cases are also handled
// here so the same function formats both JSON-round-tripped binlog values and
// direct DuckDB scan values from baseline Parquet rows. The float64 case is now
// reached only by DuckDB-origin DOUBLE/FLOAT columns, not binlog integers.
func FormatSQLValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case bool:
		if val {
			return "1"
		}
		return "0"

	case int64:
		return strconv.FormatInt(val, 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int:
		return strconv.FormatInt(int64(val), 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case uint32:
		return strconv.FormatUint(uint64(val), 10)

	case json.Number:
		// Row images read from binlog_events JSON come back as json.Number
		// (query.UnmarshalRowImage uses UseNumber), preserving the exact literal.
		// Emit it verbatim as a SQL numeric literal — integers above 2^53 survive
		// instead of being rounded through float64 (#496). JSON number syntax is a
		// valid SQL numeric literal (integer, decimal, or exponent).
		return string(val)

	case float64:
		// DuckDB-origin DOUBLE/FLOAT columns (baseline reconstruct path). Whole
		// numbers are emitted as integers, fractional ones as decimals. Binlog
		// integers no longer reach here — they arrive as json.Number, handled
		// above. math.Abs guard prevents int64 overflow for very large floats.
		if !math.IsInf(val, 0) && !math.IsNaN(val) &&
			val == math.Trunc(val) && math.Abs(val) < 1e15 {
			return strconv.FormatInt(int64(val), 10)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)

	case time.Time:
		// MySQL DATETIME literal with microsecond precision. UTC matches
		// the indexer's storage convention for event_timestamp.
		return "'" + val.UTC().Format("2006-01-02 15:04:05.000000") + "'"

	case []byte:
		// Binary/blob column. Emit as MySQL hex literal to survive
		// arbitrary non-UTF-8 bytes. Empty slices become X'' which MySQL
		// accepts as a zero-length BLOB.
		return "X'" + hex.EncodeToString(val) + "'"

	case string:
		return "'" + EscapeString(val) + "'"

	case map[string]any:
		// MySQL JSON column: re-serialise to JSON and store as a string literal.
		b, _ := json.Marshal(val)
		return "'" + EscapeString(string(b)) + "'"

	case []any:
		// JSON array column.
		b, _ := json.Marshal(val)
		return "'" + EscapeString(string(b)) + "'"

	case json.RawMessage:
		return "'" + EscapeString(string(val)) + "'"

	default:
		return "'" + EscapeString(fmt.Sprintf("%v", val)) + "'"
	}
}

// EscapeString escapes a string for safe embedding inside a MySQL
// single-quoted literal.
func EscapeString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	s = strings.ReplaceAll(s, "\x00", `\0`)
	return s
}

// QuoteName wraps a MySQL identifier (schema, table, column) in backticks,
// escaping any backticks in the name itself.
func QuoteName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// SanitizeForComment makes s safe to interpolate into a "--" SQL comment line
// (#1120). A "--" comment ends at the first line break; a value carrying one
// closes the comment, and what follows begins a NEW line that the parser reads
// as executable SQL.
//
// Both terminators are handled because this serves both dialects: MySQL ends the
// comment at LF only, PostgreSQL at LF *or* a bare CR (its lexer defines the
// comment body as [^\n\r]*). Nothing else terminates one — U+2028/U+2029 are not
// line breaks to either lexer.
//
// No existing escaper defends against this: event.EscapePKValue escapes only `\`
// and `|`, QuoteName only the backtick, quoteNamePG only the double quote — and a
// line break is legal inside a quoted identifier in either dialect, while inside
// a VARCHAR primary key it is ordinary data.
//
// A value holding a break is rendered with strconv.Quote, which is LOSSLESS
// (reversible via strconv.Unquote) and provably single-line — it escapes every
// non-printable, U+2028/U+2029 included. Flattening the break to a space is the
// obvious alternative and is worse: it makes "a\nb" and the perfectly ordinary
// value "a b" indistinguishable, so an operator who copies a mangled PK into a
// follow-up WHERE gets zero rows — and mid-incident, zero rows reads as "the row
// is already gone", the opposite of the truth. The surrounding quotes are
// themselves the signal that the rendering is escaped, so no separate marker is
// needed. Refusing outright would instead deny recovery to a table that is merely
// oddly named.
//
// A value with no break is returned unchanged, so ordinary scripts stay
// byte-identical and every golden-output test in this package still holds;
// TestSanitizeForComment_lineBreakForms pins that identity case.
//
// The quoted form is deliberately NOT valid SQL to uncomment. At the two sites
// where the commented text is itself the artifact — the AUTO_INCREMENT block
// below and reconstruct's binlog-only schema placeholder — that is the safer
// failure. MySQL has no escape for a line break inside a backtick identifier, so
// no rendering there can be both faithful and runnable, and a syntax error the
// operator must look at beats an identifier that silently reads as a different,
// possibly existing, table.
//
// Callers apply this where a value meets a "--", never to the value itself.
func SanitizeForComment(s string) string {
	if !strings.ContainsAny(s, "\r\n") {
		return s
	}
	return strconv.Quote(s)
}

// ─── Dialect dispatch (#533) ───────────────────────────────────────────────────

// quoteName quotes an identifier in the Generator's dialect.
func (g *Generator) quoteName(name string) string {
	if g.dialect == PostgresDialect {
		return quoteNamePG(name)
	}
	return QuoteName(name)
}

// formatValue renders a value as a literal in the Generator's dialect.
func (g *Generator) formatValue(v any) string {
	if g.dialect == PostgresDialect {
		return formatValuePG(v)
	}
	return FormatSQLValue(v)
}

// quoteNamePG wraps a PostgreSQL identifier in double quotes, doubling any embedded
// double quote.
func quoteNamePG(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// escapePGString escapes a string for a PostgreSQL single-quoted literal under
// standard_conforming_strings=on (PostgreSQL's default for 15+ years; the emitted
// SQL is portable under it): only the single quote is doubled. A backslash is a
// LITERAL backslash and must NOT be doubled — doubling it (as MySQL escaping does)
// would silently store two backslashes. PostgreSQL text cannot contain a NUL byte
// and pgoutput never delivers one, so none is handled.
func escapePGString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// formatValuePG renders a Go value as a PostgreSQL literal. Values captured from
// pgoutput arrive here as Go strings (pgoutput text mode) or nil — and a quoted,
// standard-conforming-escaped string coerces into the target column's type on
// INSERT/UPDATE/WHERE, so no per-type literal forms are needed (a numeric, uuid,
// bytea '\x..', jsonb, bool 't', timestamptz, etc. all coerce from their canonical
// text). The non-string cases are DEFENSIVE: they should not occur on the
// PostgreSQL path (which stores every value as text), but are handled so a stray
// value never emits invalid SQL.
func formatValuePG(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return "'" + escapePGString(val) + "'"
	case json.Number:
		// Defensive: PG values are stored as strings, not json.Number; if one
		// appears, emit it verbatim (a valid numeric literal, no float64 rounding).
		return string(val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case map[string]any, []any, json.RawMessage:
		// Defensive, mirroring FormatSQLValue: a structured value → JSON, quoted. On
		// the PG path the only structured value a row image can carry is the
		// unchanged-TOAST sentinel map — but a marker can no longer reach here through
		// the real path: GenerateSQLFromRows scans every row image and refuses up
		// front (#592, event.CheckUnresolvedToast), because the render loop demotes
		// per-statement errors to SQL comments and this function has no error return.
		// The JSON-marshal below is last-resort rendering for any OTHER stray
		// structured value, kept valid, collision-distinct SQL.
		b, _ := json.Marshal(val)
		return "'" + escapePGString(string(b)) + "'"
	default:
		// Defensive: any other Go type → its text form, quoted + escaped.
		return "'" + escapePGString(fmt.Sprintf("%v", val)) + "'"
	}
}

// FormatSetNullRestore emits an idempotent UPDATE that restores a foreign-key
// column an ON DELETE SET NULL cascade nulled (MySQL ≤8.x never logs it). It
// sets fkCol back to value, but ONLY for the row still in the nulled state
// (WHERE pk… AND fkCol IS NULL) — so a re-run, a manual fix, or a later re-point
// of the child is never clobbered (the cascade synthesis can't tell a
// re-pointed child from a still-nulled one, because the re-point event doesn't
// match the fk=parent scan that found the candidate). pkCols + row supply the
// PK predicate; value is the parent key (typed, so it renders as a numeric
// literal for an integer FK rather than a quoted string).
func FormatSetNullRestore(schema, table, fkCol string, value any, pkCols []metadata.ColumnMeta, row map[string]any) (string, error) {
	return FormatFKCascadeRestore(schema, table, fkCol, value, nil, pkCols, row)
}

// FormatFKCascadeRestore is the general form behind FormatSetNullRestore: it
// emits the idempotent UPDATE that puts a foreign-key column an InnoDB cascade
// rewrote below the binlog back to oldValue, guarded so it only touches rows
// still carrying what the cascade left there.
//
// cascadedValue is that residue and therefore the guard:
//
//	nil          → "AND fk IS NULL"  (ON DELETE SET NULL, ON UPDATE SET NULL, and
//	                                  the exotic ON UPDATE CASCADE to a NULL key)
//	non-nil      → "AND fk = <value>" (ON UPDATE CASCADE: the parent's new key)
//
// The guard is what makes a re-run, a manual fix, or a later re-point of the
// child safe: synthesis cannot tell a re-pointed child from an untouched one
// (the re-point event doesn't match the fk=old-key scan that found the
// candidate), so the statement must decline rather than clobber. pkCols + row
// supply the PK predicate; the values are typed, so an integer FK renders as a
// numeric literal rather than a quoted string.
//
// IDENTIFYING RELATIONSHIPS: when fkCol is itself part of the child's PRIMARY
// KEY (`PRIMARY KEY (pid, seq)` with pid REFERENCES parent(id) ON UPDATE
// CASCADE — one of the main reasons to declare the rule at all), the cascade
// moved the child's PK too. row is the child's PRE-cascade image, so naming
// that column from row AND appending the guard would emit the same column twice
// with contradictory values (`WHERE pid = 1 AND seq = 1 AND pid = 99`) — a
// predicate no row can satisfy, silently touching nothing while the caller
// reports a completed recovery. The PK predicate is therefore built from the
// POST-cascade identity: fkCol's PK term takes cascadedValue and doubles as the
// guard, so the single term `pid = 99` both identifies the row and keeps the
// statement idempotent. Substituting here rather than pre-substituting into row
// keeps row and the caller's pk_values (which the indexer also writes from the
// BEFORE image) consistent with each other.
func FormatFKCascadeRestore(schema, table, fkCol string, oldValue, cascadedValue any, pkCols []metadata.ColumnMeta, row map[string]any) (string, error) {
	if len(pkCols) == 0 {
		return "", fmt.Errorf("no PK columns for %s.%s foreign-key cascade restore", schema, table)
	}
	fkInPK := false
	where := make([]string, 0, len(pkCols)+1)
	for _, c := range pkCols {
		if c.Name == fkCol {
			if cascadedValue == nil {
				// The cascade would have had to NULL a PRIMARY KEY column, which
				// InnoDB cannot do (PK columns are NOT NULL) — so this is schema
				// drift between the snapshot's PK and the live table, the same
				// class the cascade engine flags as noref/norefupd. A `pk IS NULL`
				// predicate would match nothing and hand back a silent no-op, so
				// refuse instead: EmitSQL is all-or-nothing and the operator must
				// see why.
				return "", fmt.Errorf(
					"%s.%s: foreign-key column %q is part of the snapshot's PRIMARY KEY but the cascade left it NULL "+
						"(a PK column cannot be NULL — the snapshot's PK likely no longer matches the live table); "+
						"re-run `bintrail snapshot` and retry",
					schema, table, fkCol)
			}
			fkInPK = true
			where = append(where, QuoteName(c.Name)+" = "+FormatSQLValue(cascadedValue))
			continue
		}
		v, ok := row[c.Name]
		if !ok {
			return "", fmt.Errorf("PK column %q absent from %s.%s row for foreign-key cascade restore", c.Name, schema, table)
		}
		where = append(where, QuoteName(c.Name)+" = "+FormatSQLValue(v))
	}
	switch {
	case fkInPK:
		// Already emitted above as the PK term; a second copy would be redundant.
	case cascadedValue == nil:
		where = append(where, QuoteName(fkCol)+" IS NULL")
	default:
		where = append(where, QuoteName(fkCol)+" = "+FormatSQLValue(cascadedValue))
	}
	return fmt.Sprintf("UPDATE %s.%s SET %s = %s WHERE %s",
		QuoteName(schema), QuoteName(table), QuoteName(fkCol), FormatSQLValue(oldValue),
		strings.Join(where, " AND ")), nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func eventTypeName(et event.EventType) string {
	switch et {
	case event.EventInsert:
		return "INSERT"
	case event.EventUpdate:
		return "UPDATE"
	case event.EventDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

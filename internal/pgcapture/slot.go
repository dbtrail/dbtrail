package pgcapture

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/event"
)

// validatePublication checks that the pgoutput publication exists AND covers every
// table the capturer is asked to stream — validate-don't-create, mirroring how the
// MySQL path validates binlog_row_image=FULL rather than setting it. The publication
// defines the captured table set, which is an operator privilege/policy decision; a
// publication that exists but omits a requested table would emit ZERO events for it,
// silently and forever, so a coverage gap fails loud.
func validatePublication(ctx context.Context, conn *pgx.Conn, pubname string, filters event.Filters) error {
	var allTables, pubIns, pubUpd, pubDel bool
	err := conn.QueryRow(ctx, `SELECT puballtables, pubinsert, pubupdate, pubdelete FROM pg_publication WHERE pubname = $1`, pubname).Scan(&allTables, &pubIns, &pubUpd, &pubDel)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("pgcapture: publication %q does not exist — create it (CREATE PUBLICATION) covering the tables to capture", pubname)
	}
	if err != nil {
		return fmt.Errorf("pgcapture: checking publication %q: %w", pubname, err)
	}
	// Operation completeness applies to EVERY publication shape — even FOR ALL TABLES
	// can be created WITH (publish = 'insert'), which silently drops updates and deletes.
	// Check before the allTables short-circuit.
	if ops := missingPublishedOps(pubIns, pubUpd, pubDel); len(ops) > 0 {
		return fmt.Errorf("pgcapture: publication %q does not publish %s — those changes would be silently lost; recreate it WITH (publish = 'insert, update, delete') or leave publish unset for all operations",
			pubname, strings.Join(ops, ", "))
	}
	// FOR ALL TABLES covers everything and cannot carry per-table row filters or column
	// lists (PostgreSQL rejects WHERE/column-lists on FOR ALL TABLES), so it is unambiguously safe.
	if allTables {
		return nil
	}

	// A FOR TABLE publication MAY carry a row filter (PG15+ `... WHERE (...)`) or a column
	// list on a published table. pgoutput then emits only a SUBSET of changes — filtered
	// rows and unlisted columns are dropped, and updates crossing a row filter degrade to
	// spurious INSERT/DELETE. bintrail cannot honor a partial publication, so fail loud
	// regardless of the client --tables scope (the filter applies to whatever we stream).
	restricted, err := publicationRestrictedTables(ctx, conn, pubname)
	if err != nil {
		return err
	}
	if perr := restrictedPublicationError(pubname, restricted); perr != nil {
		return perr
	}

	// A nil table filter means "accept whatever the publication streams", so there is no
	// requested set to verify coverage against.
	if len(filters.Tables) == 0 {
		return nil
	}

	rows, err := conn.Query(ctx, `SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = $1`, pubname)
	if err != nil {
		return fmt.Errorf("pgcapture: listing tables of publication %q: %w", pubname, err)
	}
	defer rows.Close()
	published := make(map[string]bool)
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return fmt.Errorf("pgcapture: scanning tables of publication %q: %w", pubname, err)
		}
		published[t] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("pgcapture: listing tables of publication %q: %w", pubname, err)
	}

	var missing []string
	for t := range filters.Tables {
		if !published[t] {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("pgcapture: publication %q does not cover requested table(s) [%s] — their changes would be silently lost; add them to the publication",
			pubname, strings.Join(missing, ", "))
	}
	return nil
}

// restrictedTable names a published table that pgoutput would emit only partially,
// because the publication attached a row filter (PG15+ `WHERE (...)`) and/or a column
// list to it.
type restrictedTable struct {
	name       string // schema.table
	hasFilter  bool   // pg_publication_rel.prqual is non-null
	hasColList bool   // pg_publication_rel.prattrs is non-null (a partial column list)
}

// publicationRestrictedTables returns the published tables carrying a row filter or a
// column list. prqual and prattrs exist only on PG15+, so on older servers (where
// neither feature exists) it short-circuits to an empty result without touching the
// missing columns.
func publicationRestrictedTables(ctx context.Context, conn *pgx.Conn, pubname string) ([]restrictedTable, error) {
	var verNum int
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&verNum); err != nil {
		return nil, fmt.Errorf("pgcapture: reading server_version_num to check publication row filters: %w", err)
	}
	if verNum < 150000 {
		return nil, nil // pg_publication_rel.prqual/prattrs do not exist before PG15
	}

	rows, err := conn.Query(ctx, `
		SELECT n.nspname || '.' || c.relname, pr.prqual IS NOT NULL, pr.prattrs IS NOT NULL
		FROM pg_publication p
		JOIN pg_publication_rel pr ON pr.prpubid = p.oid
		JOIN pg_class c ON c.oid = pr.prrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE p.pubname = $1 AND (pr.prqual IS NOT NULL OR pr.prattrs IS NOT NULL)`, pubname)
	if err != nil {
		return nil, fmt.Errorf("pgcapture: checking row filters/column lists for publication %q: %w", pubname, err)
	}
	defer rows.Close()

	var restricted []restrictedTable
	for rows.Next() {
		var rt restrictedTable
		if err := rows.Scan(&rt.name, &rt.hasFilter, &rt.hasColList); err != nil {
			return nil, fmt.Errorf("pgcapture: scanning row filters/column lists for publication %q: %w", pubname, err)
		}
		restricted = append(restricted, rt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgcapture: checking row filters/column lists for publication %q: %w", pubname, err)
	}
	return restricted, nil
}

// restrictedPublicationError turns a non-empty restrictedTable list into a fatal,
// operator-actionable error (nil when the list is empty). Split out as a pure function
// so the decision/formatting is unit-testable without a live PostgreSQL server.
func restrictedPublicationError(pubname string, restricted []restrictedTable) error {
	if len(restricted) == 0 {
		return nil
	}
	parts := make([]string, 0, len(restricted))
	for _, rt := range restricted {
		var reason string
		switch {
		case rt.hasFilter && rt.hasColList:
			reason = "row filter + column list"
		case rt.hasFilter:
			reason = "row filter"
		default:
			reason = "column list"
		}
		parts = append(parts, fmt.Sprintf("%s (%s)", rt.name, reason))
	}
	sort.Strings(parts)
	return fmt.Errorf("pgcapture: publication %q applies a row filter or column list to table(s) [%s] — pgoutput would emit only a SUBSET of changes (filtered rows and unlisted columns are silently dropped, and updates crossing a row filter degrade to spurious INSERT/DELETE); bintrail cannot honor a partial publication — recreate it WITHOUT WHERE(...) / column lists so the full table is captured",
		pubname, strings.Join(parts, ", "))
}

// missingPublishedOps returns the row-changing operations (insert/update/delete) a
// publication does NOT publish. pgoutput emits nothing for an unpublished operation,
// so bintrail would silently miss those changes. TRUNCATE is excluded deliberately —
// bintrail does not rely on TRUNCATE row events. Pure, so the decision is unit-testable.
func missingPublishedOps(ins, upd, del bool) []string {
	var missing []string
	if !ins {
		missing = append(missing, "insert")
	}
	if !upd {
		missing = append(missing, "update")
	}
	if !del {
		missing = append(missing, "delete")
	}
	return missing
}

// validateReplicaIdentity is the PostgreSQL analog of metadata.ValidateBinlogRowImage,
// run once when capture starts. PostgreSQL's knob is per-table (REPLICA IDENTITY), not a
// global like MySQL's binlog_row_image.
//
//   - wal_level must be 'logical' (global) — logical replication is impossible
//     otherwise. This one is fatal.
//   - Every table in pg_publication_tables (which expands FOR ALL TABLES to the actual
//     set PostgreSQL will stream — NOT the narrower client-side Filters) should have
//     pg_class.relreplident = 'f' (FULL). Under any weaker identity ('d' default,
//     'i' using-index, 'n' nothing) an unchanged out-of-line TOAST value is GONE from
//     the before-image (proven in the spike, Part A). Those tables are RETURNED, not
//     refused: the decoder's cacheRelation skips the changes of every relation it is
//     handed below FULL (and records each one in the capture-skip ledger), at start
//     and mid-stream alike, so refusing here would stop every table over one. The
//     caller warns with the list.
//
// The report-only CheckReplicaIdentity (doctor, console preflight) still FAILS on
// the same list: there, it is the advice to fix before capture, not a stop.
func validateReplicaIdentity(ctx context.Context, conn *pgx.Conn, publication string) (notFull []string, err error) {
	if err := checkWALLevel(ctx, conn); err != nil {
		return nil, err
	}
	return replicaIdentityNotFull(ctx, conn, publication)
}

// ErrWALLevelNotLogical marks the "wal_level is readable but not 'logical'" case (a
// genuine misconfiguration) so a caller — the doctor — can distinguish it from a
// query failure (connection/permission/timeout) and give the right remediation
// (restart-with-config vs retry) without mislabeling a transient blip as a config bug.
var ErrWALLevelNotLogical = errors.New("wal_level is not 'logical'")

// ErrSlotLost and ErrSlotMissingOnResume mark the two fatal resume conditions where
// the WAL behind the saved checkpoint is irrecoverably gone — the slot was invalidated
// (wal_status=lost) or dropped. ensureSlot wraps them (preserving its descriptive
// message); the consumer (pgstreamrun.One) matches them with errors.Is to durably
// record the permanent loss so index-only `status` can show it after the process exits.
// Base strings are bare (no "pgcapture:" prefix) because ensureSlot wraps them with a
// "pgcapture: ...: %w" message; doubling the prefix would stutter, and the wrapped text
// is stored verbatim in stream_state.gap_lost_detail / shown in the status badge. Mirrors
// ErrWALLevelNotLogical's bare base string.
var (
	ErrSlotLost            = errors.New("replication slot invalidated (wal_status=lost)")
	ErrSlotMissingOnResume = errors.New("replication slot missing on resume")
)

// checkWALLevel verifies the (global) wal_level is 'logical' — logical replication
// is impossible otherwise. A value-wrong result wraps ErrWALLevelNotLogical; a query
// failure does not.
func checkWALLevel(ctx context.Context, conn *pgx.Conn) error {
	var walLevel string
	if err := conn.QueryRow(ctx, "SELECT current_setting('wal_level')").Scan(&walLevel); err != nil {
		return fmt.Errorf("pgcapture: checking wal_level: %w", err)
	}
	if walLevel != "logical" {
		return fmt.Errorf("pgcapture: wal_level is %q, must be 'logical' for logical replication: %w", walLevel, ErrWALLevelNotLogical)
	}
	return nil
}

// replicaIdentityNotFull returns the sorted "schema.table (relreplident=x)" of every
// published table NOT at REPLICA IDENTITY FULL. It is the single catalog-query source
// of truth shared by the capture-time validator (which warns with a non-empty list),
// the doctor check (which fails on one) and the report-only QueryReplicaIdentityNotFull (which surfaces the list
// for the console health panel). An empty result means every published table is FULL.
func replicaIdentityNotFull(ctx context.Context, conn *pgx.Conn, publication string) ([]string, error) {
	rows, err := conn.Query(ctx, `
		SELECT pt.schemaname, pt.tablename, c.relreplident::text
		FROM pg_publication_tables pt
		JOIN pg_namespace n ON n.nspname = pt.schemaname
		JOIN pg_class c ON c.relname = pt.tablename AND c.relnamespace = n.oid
		WHERE pt.pubname = $1`, publication)
	if err != nil {
		return nil, fmt.Errorf("pgcapture: checking replica identity for publication %q: %w", publication, err)
	}
	defer rows.Close()

	var notFull []string
	for rows.Next() {
		var schema, table, relreplident string
		if err := rows.Scan(&schema, &table, &relreplident); err != nil {
			return nil, fmt.Errorf("pgcapture: scanning replica identity for publication %q: %w", publication, err)
		}
		if relreplident != "f" {
			notFull = append(notFull, fmt.Sprintf("%s.%s (relreplident=%s)", schema, table, relreplident))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgcapture: checking replica identity for publication %q: %w", publication, err)
	}
	sort.Strings(notFull)
	return notFull, nil
}

// checkReplicaIdentityTables verifies every table the publication streams is at
// REPLICA IDENTITY FULL (the per-table loop; wal_level is checked separately).
func checkReplicaIdentityTables(ctx context.Context, conn *pgx.Conn, publication string) error {
	notFull, err := replicaIdentityNotFull(ctx, conn, publication)
	if err != nil {
		return err
	}
	if len(notFull) > 0 {
		return fmt.Errorf("pgcapture: table(s) not at REPLICA IDENTITY FULL [%s] — before-images would be partial (an unchanged out-of-line TOAST value is lost under a weaker identity, so recovery would be wrong); run ALTER TABLE <t> REPLICA IDENTITY FULL",
			strings.Join(notFull, ", "))
	}
	return nil
}

// The exported Check* functions are the report-only form of the capture-time
// validate* guards above, for bintrail-pg doctor. They are pure probes (no CREATE,
// no mutation) returning nil when healthy and a descriptive error otherwise; the
// doctor converts that error into a CheckResult. They live here so the single
// catalog-query source of truth (and its error wording) is shared with capture.

// CheckWALLevel verifies wal_level = 'logical' on the source.
func CheckWALLevel(ctx context.Context, conn *pgx.Conn) error {
	return checkWALLevel(ctx, conn)
}

// CheckPublication verifies the publication exists and covers the requested tables.
func CheckPublication(ctx context.Context, conn *pgx.Conn, publication string, filters event.Filters) error {
	return validatePublication(ctx, conn, publication, filters)
}

// CheckReplicaIdentity verifies every table the publication streams is at REPLICA
// IDENTITY FULL. It does NOT re-check wal_level (use CheckWALLevel for that), so the
// doctor can report the two distinctly.
func CheckReplicaIdentity(ctx context.Context, conn *pgx.Conn, publication string) error {
	return checkReplicaIdentityTables(ctx, conn, publication)
}

// QueryReplicaIdentityNotFull is the report-only sibling of CheckReplicaIdentity: it
// returns the published tables NOT at REPLICA IDENTITY FULL (empty = all FULL) instead
// of folding them into a fatal error. The streaming daemon polls it for the console
// health panel (#599) — a coverage signal the operator reads, not a startup gate.
func QueryReplicaIdentityNotFull(ctx context.Context, conn *pgx.Conn, publication string) ([]string, error) {
	return replicaIdentityNotFull(ctx, conn, publication)
}

// listUnloggedCaptureTables returns the "schema.table" of every UNLOGGED base table in
// the capture scope. UNLOGGED tables generate NO WAL, so logical decoding never sees
// their changes — they are captured as exactly nothing, silently; an operator may keep
// ephemeral data UNLOGGED on purpose, so this is surfaced as a WARNING: a coverage hole the operator should see now rather
// than discover during a failed recovery. (#555)
//
// CRUCIAL: an UNLOGGED table is NEVER in pg_publication_tables, so it must be found
// against pg_class directly. PostgreSQL refuses to add an UNLOGGED table to a FOR TABLE
// publication ("cannot add relation ... to publication ... unlogged"), and it EXCLUDES
// UNLOGGED tables from a FOR ALL TABLES publication's pg_publication_tables view
// (pg_relation_is_publishable gates on relpersistence). A query over pg_publication_
// tables therefore can never match an UNLOGGED row — it would be dead code.
//
// The dangerous cases are FOR ALL TABLES and FOR TABLES IN SCHEMA (PG 15+): the
// operator believes "everything" (or "everything in that schema") is captured, but the
// UNLOGGED tables silently are not — a schema-scoped publication accepts a schema
// containing UNLOGGED tables, and pg_publication_tables simply omits them. We scan user
// base tables for relpersistence = 'u' — all user schemas under FOR ALL TABLES, only the
// published schemas (pg_publication_namespace) under a schema-scoped publication — and
// report the ones in the client filter scope. For a FOR TABLE publication an UNLOGGED
// table cannot be in the publication at all, and a client --tables naming an unpublished
// table is already a fatal coverage gap in validatePublication — so there is nothing
// this guard could uniquely surface and it returns nil. (#1211 added the schema-scoped
// coverage.)
func listUnloggedCaptureTables(ctx context.Context, conn *pgx.Conn, publication string, filters event.Filters) ([]string, error) {
	var allTables bool
	err := conn.QueryRow(ctx, `SELECT puballtables FROM pg_publication WHERE pubname = $1`, publication).Scan(&allTables)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // publication absent — validatePublication reports that distinctly
	}
	if err != nil {
		return nil, fmt.Errorf("pgcapture: reading publication %q for the UNLOGGED check: %w", publication, err)
	}

	// nil = every user schema is in scope (FOR ALL TABLES); non-nil = only the
	// publication's schema members are (FOR TABLES IN SCHEMA, PG 15+).
	var schemaScope []string
	if !allTables {
		schemas, err := publicationSchemas(ctx, conn, publication)
		if err != nil {
			return nil, err
		}
		if len(schemas) == 0 {
			return nil, nil // FOR TABLE only: UNLOGGED tables cannot be published (see doc above)
		}
		schemaScope = schemas
	}

	rows, err := conn.Query(ctx, `
		SELECT n.nspname, c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r' AND c.relpersistence = 'u'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		  AND n.nspname NOT LIKE 'pg_temp%'
		  AND n.nspname NOT LIKE 'pg_toast_temp%'
		  AND ($1::text[] IS NULL OR n.nspname = ANY($1))`, schemaScope)
	if err != nil {
		return nil, fmt.Errorf("pgcapture: scanning for UNLOGGED tables: %w", err)
	}
	defer rows.Close()

	var unlogged []string
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, fmt.Errorf("pgcapture: scanning UNLOGGED tables: %w", err)
		}
		// The captured set is the publication scope narrowed by the client
		// schema/table filter (the zero Filters accepts all), so honor it here.
		if filters.Matches(schema, table) {
			unlogged = append(unlogged, schema+"."+table)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgcapture: scanning for UNLOGGED tables: %w", err)
	}
	sort.Strings(unlogged)
	return unlogged, nil
}

// publicationSchemas returns the schema names a FOR TABLES IN SCHEMA publication
// (PostgreSQL 15+) covers; empty when the publication has no schema members. On a
// server predating pg_publication_namespace (PG 14 and older, SQLSTATE 42P01
// undefined_table) it returns empty rather than an error: schema-scoped publications
// cannot exist there, so there is nothing to enumerate — an advisory guard must not
// break the preflight or the doctor on an older server. (#1211)
func publicationSchemas(ctx context.Context, conn *pgx.Conn, publication string) ([]string, error) {
	rows, err := conn.Query(ctx, `
		SELECT n.nspname
		FROM pg_publication_namespace pn
		JOIN pg_publication p ON p.oid = pn.pnpubid
		JOIN pg_namespace n ON n.oid = pn.pnnspid
		WHERE p.pubname = $1`, publication)
	if err != nil {
		if isUndefinedTable(err) {
			return nil, nil // pre-15 server: schema-scoped publications do not exist
		}
		return nil, fmt.Errorf("pgcapture: reading publication %q schema members for the UNLOGGED check: %w", publication, err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, fmt.Errorf("pgcapture: scanning publication %q schema members: %w", publication, err)
		}
		schemas = append(schemas, s)
	}
	if err := rows.Err(); err != nil {
		if isUndefinedTable(err) {
			return nil, nil // pre-15 server (pgx may defer the error to rows.Err)
		}
		return nil, fmt.Errorf("pgcapture: reading publication %q schema members for the UNLOGGED check: %w", publication, err)
	}
	return schemas, nil
}

// isUndefinedTable reports SQLSTATE 42P01 (undefined_table) — how a server answers a
// query over a catalog relation it does not have (pg_publication_namespace before 15).
func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

// ListUnloggedCaptureTables is the report-only form of the #555 UNLOGGED guard for
// bintrail-pg doctor (a pure probe; no mutation). nil/empty means no UNLOGGED tables
// in the capture scope.
func ListUnloggedCaptureTables(ctx context.Context, conn *pgx.Conn, publication string, filters event.Filters) ([]string, error) {
	return listUnloggedCaptureTables(ctx, conn, publication, filters)
}

// CascadeChild is one foreign-key cascade child whose PARENT the publication captures
// but the child itself it does NOT — so a delete on the parent fires an ON DELETE
// CASCADE / SET NULL / SET DEFAULT that rewrites the child, and that change is never
// captured. (#556)
type CascadeChild struct {
	Child  string // "schema.table" of the FK-bearing (child) table
	Parent string // "schema.table" of the referenced (parent) table
	Action string // human action, e.g. "ON DELETE CASCADE"
}

// listUncoveredCascadeChildren returns cascade children whose parent IS in the
// publication but the child is NOT. On PostgreSQL, FK ON DELETE CASCADE/SET NULL/SET
// DEFAULT is performed by real per-row WAL ops on the child (unlike MySQL ≤8.x, which
// does not log them) — so `recover` works directly IF the child is captured. When the
// parent is published but the child is not, a cascade delete on the parent silently
// rewrites rows we never indexed → unrecoverable, and invisible. This surfaces that as
// a WARNING. (#556)
//
// A FOR ALL TABLES publication covers every child by construction, so this finds
// nothing there; the gap is an explicit FOR TABLE publication that lists a parent but
// forgets its cascade child. A PUBLISHED child is already forced to RI FULL by
// checkReplicaIdentityTables, so only the not-published case needs surfacing here.
//
// Residual (documented, not covered here): a row that was deleted-by-cascade but never
// existed in our captured history (it predates capture) is recoverable only from a
// baseline — the PG baseline producer is a separate GA item (#593).
func listUncoveredCascadeChildren(ctx context.Context, conn *pgx.Conn, publication string) ([]CascadeChild, error) {
	rows, err := conn.Query(ctx, `
		SELECT child_ns.nspname, child.relname, parent_ns.nspname, parent.relname, con.confdeltype
		FROM pg_constraint con
		JOIN pg_class child ON child.oid = con.conrelid
		JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
		JOIN pg_class parent ON parent.oid = con.confrelid
		JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
		WHERE con.contype = 'f' AND con.confdeltype IN ('c', 'n', 'd')
		  AND EXISTS (SELECT 1 FROM pg_publication_tables pt
		              WHERE pt.pubname = $1 AND pt.schemaname = parent_ns.nspname AND pt.tablename = parent.relname)
		  AND NOT EXISTS (SELECT 1 FROM pg_publication_tables pt
		              WHERE pt.pubname = $1 AND pt.schemaname = child_ns.nspname AND pt.tablename = child.relname)`,
		publication)
	if err != nil {
		return nil, fmt.Errorf("pgcapture: checking FK cascade-child coverage for publication %q: %w", publication, err)
	}
	defer rows.Close()

	var out []CascadeChild
	for rows.Next() {
		var childSchema, childTable, parentSchema, parentTable, delType string
		if err := rows.Scan(&childSchema, &childTable, &parentSchema, &parentTable, &delType); err != nil {
			return nil, fmt.Errorf("pgcapture: scanning FK cascade-child coverage for publication %q: %w", publication, err)
		}
		out = append(out, CascadeChild{
			Child:  childSchema + "." + childTable,
			Parent: parentSchema + "." + parentTable,
			Action: cascadeAction(delType),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgcapture: checking FK cascade-child coverage for publication %q: %w", publication, err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Child < out[j].Child })
	return out, nil
}

// cascadeAction renders a pg_constraint.confdeltype code as its human FK action.
func cascadeAction(confdeltype string) string {
	switch confdeltype {
	case "c":
		return "ON DELETE CASCADE"
	case "n":
		return "ON DELETE SET NULL"
	case "d":
		return "ON DELETE SET DEFAULT"
	default:
		return "ON DELETE (" + confdeltype + ")"
	}
}

// ListUncoveredCascadeChildren is the report-only form of the #556 cascade-coverage
// guard for bintrail-pg doctor (a pure probe; no mutation). nil/empty means every
// cascade child of a published parent is itself published.
func ListUncoveredCascadeChildren(ctx context.Context, conn *pgx.Conn, publication string) ([]CascadeChild, error) {
	return listUncoveredCascadeChildren(ctx, conn, publication)
}

// ensureSlot returns the LSN to start replication from, creating the slot on first
// run. The first-run-vs-resume decision is gated on slot EXISTENCE in
// pg_replication_slots (NOT on savedLSN==0 — an empty saved checkpoint decodes to
// LSN 0, which would collide with a genuine first run).
//
//   - Absent: create a permanent pgoutput slot and start from its ConsistentPoint
//     (NOT IdentifySystem().XLogPos — that is past any pre-StartReplication DML and
//     yields an empty stream; a footgun hit in the spike).
//   - Present (resume): return savedLSN. PostgreSQL actually resumes from the slot's
//     own confirmed_flush_lsn (it clamps a lower client LSN forward and re-delivers —
//     at-least-once, never skipping), so savedLSN's load-bearing role is seeding
//     lastAcked, not the StartReplication argument.
//
// The slot is permanent (Temporary defaults false) so it survives restarts — required
// for resume. The flip side is source-side WAL retention if the consumer stalls
// (the PG analog of binlog retention); WAL-retention monitoring is #532.
//
// expectExisting is set by the consumer when it is resuming from a saved checkpoint
// (#534). In that case the slot MUST still exist and be valid: if it was dropped, or
// invalidated by max_slot_wal_keep_size (wal_status='lost' — the PG13+ feature the
// version floor is chosen to bound), the WAL since the checkpoint is gone, and
// silently creating a fresh slot from a new ConsistentPoint would SKIP that data.
// ensureSlot fails loud in that case rather than skip; the recovery policy
// (re-baseline) is #532.
func ensureSlot(ctx context.Context, replConn *pgconn.PgConn, queryConn *pgx.Conn, slotName string, savedLSN pglogrepl.LSN, expectExisting bool) (pglogrepl.LSN, error) {
	found, walStatus, err := querySlotState(ctx, queryConn, slotName)
	if err != nil {
		return 0, err
	}

	// A 'lost' slot is unusable regardless of mode — its WAL has been removed.
	if found && walStatus == WalStatusLost {
		return 0, fmt.Errorf("pgcapture: replication slot %q is invalidated (wal_status=lost; max_slot_wal_keep_size exceeded) — the WAL it needs is gone; re-baseline rather than resume: %w", slotName, ErrSlotLost)
	}

	if expectExisting {
		if !found {
			return 0, fmt.Errorf("pgcapture: resuming from a saved checkpoint but replication slot %q no longer exists — the WAL since the checkpoint is lost; re-baseline (creating a fresh slot would silently skip data): %w", slotName, ErrSlotMissingOnResume)
		}
		return savedLSN, nil
	}
	if found {
		// First run but the slot already exists (a prior run created it, or a TOCTOU).
		// Resume from it; PostgreSQL clamps savedLSN forward to the slot's own point.
		return savedLSN, nil
	}

	res, err := pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		// TOCTOU: another capturer or a restart created the slot between the check
		// and now (SQLSTATE 42710 = duplicate_object). Treat as a resume.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.SQLState() == "42710" {
			return savedLSN, nil
		}
		return 0, fmt.Errorf("pgcapture: creating replication slot %q: %w", slotName, err)
	}
	lsn, err := pglogrepl.ParseLSN(res.ConsistentPoint)
	if err != nil {
		return 0, fmt.Errorf("pgcapture: parsing consistent point %q for slot %q: %w", res.ConsistentPoint, slotName, err)
	}
	return lsn, nil
}

// EnsureSlotExists guarantees the named permanent pgoutput slot exists, creating
// it when absent. It is the baseline producer's (#593) slot seam: the baseline
// must ensure the slot BEFORE opening its snapshot transaction so the slot's
// consistent_point ≤ the baseline anchor LSN (overlap redelivery is harmless —
// reconstruct's merge is last-write-wins idempotent — but a slot created AFTER
// the anchor would silently skip the deltas in between).
//
// replConnect lazily opens a REPLICATION connection (replication=database) and
// is called ONLY when the slot must actually be created; pass nil when no
// replication DSN is available, in which case a missing slot is an actionable
// error rather than a silent skip. EnsureSlotExists owns the connection it
// opens through replConnect and closes it before returning — the caller never
// sees it.
//
// Slot identity is SCOPED, not name-only (review medium): pg_replication_slots
// is cluster-wide, so a same-named PHYSICAL slot, a non-pgoutput logical slot,
// or a logical slot for a DIFFERENT database would satisfy a bare name match
// while anchoring nothing — the baseline would publish with a void ordering
// guarantee. A name collision with anything that is not a pgoutput logical
// slot on the current database fails loud.
//
// It shares ensureSlot's other safety semantics: a wal_status=lost slot fails
// loud (wrapping ErrSlotLost), and a create racing another capturer (SQLSTATE
// 42710) re-checks the scoped state — success only if the raced slot is
// actually ours in kind. Returns created=true only when this call created the
// slot.
func EnsureSlotExists(ctx context.Context, queryConn *pgx.Conn, slotName string, replConnect func(context.Context) (*pgconn.PgConn, error)) (created bool, err error) {
	found, walStatus, err := queryScopedSlotState(ctx, queryConn, slotName)
	if err != nil {
		return false, err
	}
	if found && walStatus == WalStatusLost {
		return false, fmt.Errorf("pgcapture: replication slot %q is invalidated (wal_status=lost; max_slot_wal_keep_size exceeded) — drop and recreate it (the stream must also re-baseline): %w", slotName, ErrSlotLost)
	}
	if found {
		return false, nil
	}
	if replConnect == nil {
		return false, fmt.Errorf("pgcapture: replication slot %q does not exist and no replication connection is configured — provide --repl-dsn (replication=database) so the slot can be created, or create it first (e.g. by running `bintrail-pg stream`)", slotName)
	}
	replConn, err := replConnect(ctx)
	if err != nil {
		return false, fmt.Errorf("pgcapture: connecting to create replication slot %q: %w", slotName, err)
	}
	defer replConn.Close(ctx)
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		// TOCTOU: something created a slot with this name between the check and
		// now (SQLSTATE 42710 = duplicate_object). Re-check with the SCOPED
		// query: only a pgoutput logical slot on this database counts as "the
		// slot exists"; a raced foreign slot (physical / other plugin / other
		// database) is the collision error, not success.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.SQLState() == "42710" {
			raceFound, _, raceErr := queryScopedSlotState(ctx, queryConn, slotName)
			if raceErr != nil {
				return false, raceErr
			}
			if raceFound {
				return false, nil
			}
			return false, fmt.Errorf("pgcapture: replication slot %q was created concurrently but is not visible as a pgoutput logical slot on this database — retry, or resolve the name collision", slotName)
		}
		return false, fmt.Errorf("pgcapture: creating replication slot %q: %w", slotName, err)
	}
	return true, nil
}

// SlotFloorLSN returns a safe lower bound for replaying this slot's changes as
// deltas on top of a baseline snapshot — the fix for #771.
//
// It is deliberately NOT pg_current_wal_lsn() (a live read of "now"). Reading
// pg_current_wal_lsn() inside the baseline's snapshot transaction does not
// close the race between a concurrent transaction's WAL commit-record flush
// and its later removal from the procarray (the moment it becomes visible to
// new snapshots): a transaction can flush its commit record, and only
// afterwards be removed from the procarray, so a snapshot taken in that
// window correctly treats it as invisible while a same-transaction LSN read
// moments later can already be >= its commit LSN. Anchoring "deltas start
// here" on that live LSN can therefore silently exclude a transaction from
// BOTH the baseline (correctly, per MVCC) AND the delta window (incorrectly).
//
// SlotFloorLSN instead reports the slot's own confirmed_flush_lsn (falling
// back to restart_lsn when confirmed_flush_lsn is unset — a brand new slot
// that has never streamed) — call it BEFORE the caller's snapshot
// transaction begins. Logical decoding only advances these positions past
// transactions it has already resolved in full WAL order, so no transaction
// concurrent with (or later than) the caller's snapshot can have a commit
// LSN below what this returns; the value is therefore always <= any LSN read
// afterwards on the same connection (LSNs are monotonically non-decreasing),
// including the caller's own live pg_current_wal_lsn() anchor. Replaying
// deltas from this floor forward — rather than from the live anchor — may
// redeliver some already-visible-in-the-baseline changes; that overlap is
// harmless because the baseline+delta merge is last-write-wins over
// full-row images (same idempotency EnsureSlotExists's ordering invariant
// already relies on).
func SlotFloorLSN(ctx context.Context, conn *pgx.Conn, slotName string) (pglogrepl.LSN, error) {
	var restartText, confirmedText *string
	err := conn.QueryRow(ctx, `
		SELECT restart_lsn::text, confirmed_flush_lsn::text
		FROM pg_replication_slots WHERE slot_name = $1`, slotName).
		Scan(&restartText, &confirmedText)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("pgcapture: replication slot %q not found while reading its floor LSN", slotName)
	}
	if err != nil {
		return 0, fmt.Errorf("pgcapture: reading floor LSN for replication slot %q: %w", slotName, err)
	}
	var text string
	switch {
	case confirmedText != nil && *confirmedText != "":
		text = *confirmedText
	case restartText != nil && *restartText != "":
		text = *restartText
	default:
		return 0, fmt.Errorf("pgcapture: replication slot %q has neither confirmed_flush_lsn nor restart_lsn set — cannot compute a safe delta floor", slotName)
	}
	lsn, err := pglogrepl.ParseLSN(text)
	if err != nil {
		return 0, fmt.Errorf("pgcapture: parsing floor LSN %q for replication slot %q: %w", text, slotName, err)
	}
	return lsn, nil
}

// queryScopedSlotState is querySlotState with slot IDENTITY checks: it reports
// existence and wal_status only for a LOGICAL pgoutput slot belonging to the
// CURRENT database. A same-named slot of any other kind is a loud, actionable
// error — anchoring a baseline (or a resume decision) on a foreign slot would
// void every ordering guarantee while looking healthy.
func queryScopedSlotState(ctx context.Context, conn *pgx.Conn, slotName string) (found bool, walStatus string, err error) {
	var slotType, plugin, database, curDB string
	err = conn.QueryRow(ctx, `
		SELECT slot_type, coalesce(plugin, ''), coalesce(database, ''), coalesce(wal_status, ''), current_database()
		FROM pg_replication_slots WHERE slot_name = $1`, slotName).
		Scan(&slotType, &plugin, &database, &walStatus, &curDB)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("pgcapture: checking replication slot %q: %w", slotName, err)
	}
	if slotType != "logical" || plugin != "pgoutput" || database != curDB {
		return false, "", fmt.Errorf("pgcapture: replication slot %q exists but is not a pgoutput logical slot on database %q (slot_type=%q, plugin=%q, database=%q) — it cannot anchor this source; use a different --slot name or remove the conflicting slot", slotName, curDB, slotType, plugin, database)
	}
	return true, walStatus, nil
}

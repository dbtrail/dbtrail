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
		return fmt.Errorf("pgcapture: publication %q does not publish %s — those changes would be silently lost; run %s (it keeps the publication's tables and other options), or recreate the publication leaving publish unset",
			pubname, strings.Join(ops, ", "), TruncateFixStatement(pubname))
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
// so bintrail would silently miss those changes. TRUNCATE is not in this fatal set:
// without it every row change is still captured and only the record of a TRUNCATE
// (the decoder's marker, #827) is lost, so it is a warning of its own
// (pubtruncate.go, #3108). Pure, so the decision is unit-testable.
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
//     set PostgreSQL will stream — NOT the narrower client-side Filters) that is not
//     at REPLICA IDENTITY FULL (pg_class.relreplident 'd' default, 'i' using-index,
//     'n' nothing) is RETURNED, never refused. Such a table is captured: PostgreSQL
//     sends at most the key of the row an UPDATE or a DELETE changed, the decoder
//     stores that earlier row in part, and only undo needs it whole. The caller
//     warns with the list. Only the tables filters accepts are returned: the
//     decoder neither warns about nor captures the others.
//
// The one table below FULL that is NOT captured, a replica identity that leaves a
// primary-key column out (TableIdentity.MissesKey), is skipped by the decoder at each
// description of the table (cacheRelation); here it is only named.
func validateReplicaIdentity(ctx context.Context, conn *pgx.Conn, publication string, filters event.Filters) ([]TableIdentity, error) {
	if err := checkWALLevel(ctx, conn); err != nil {
		return nil, err
	}
	all, err := QueryTableIdentities(ctx, conn, publication)
	if err != nil {
		return nil, err
	}
	var kept []TableIdentity
	for _, t := range all {
		if filters.Matches(t.Schema, t.Table) {
			kept = append(kept, t)
		}
	}
	return kept, nil
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

// TableIdentity is one published table whose UPDATE and DELETE cannot be
// undone, as the catalog has it now: a table that is not at REPLICA IDENTITY
// FULL, or a partitioned one whose partitions are not all at FULL.
type TableIdentity struct {
	Schema, Table string
	// Identity is pg_class.relreplident: "d" default, "i" using index, "n"
	// nothing.
	Identity string
	// NoIdentity reports that PostgreSQL has no identity to send for the table:
	// NOTHING, DEFAULT with no usable primary key, or USING INDEX on an index
	// that is gone. PostgreSQL itself then refuses UPDATE and DELETE on the
	// table while a publication that publishes them holds it.
	NoIdentity bool
	// MissesKey names the primary-key columns the identity leaves out; empty
	// when it holds them all (always, under DEFAULT with an ordinary key). A
	// table with any is not captured: see SkipReplicaIdentityIndexWithoutPrimaryKey.
	MissesKey []string
	// PartitionsBelowFull names, each quoted and schema-qualified, the
	// partitions under a partitioned table that are not at FULL themselves.
	// REPLICA IDENTITY is set per partition, and the old row of a change is
	// what its partition logs, so the table's changes are undoable only when
	// this is empty AND the table itself is at FULL. A partitioned table at
	// FULL with any is listed for that reason alone (Identity is then "f").
	PartitionsBelowFull []string
	// PartitionsMissKey names, the same way, the partitions whose own replica
	// identity is an index that leaves a primary-key column out. A
	// partitioned table published through its root with any is not captured,
	// as a table with such an identity is not.
	PartitionsMissKey []string
}

// Captured reports whether capture takes the table's changes, as far as its
// replica identity says.
func (t TableIdentity) Captured() bool {
	return len(t.MissesKey) == 0 && len(t.PartitionsMissKey) == 0
}

// tableIdentitiesQuery lists the tables of publication $1 below REPLICA
// IDENTITY FULL, and the partitioned ones with a partition below it (read
// through pg_inherits, which takes no lock on any partition: see
// partitionsQuery), with the index PostgreSQL takes their identity from: the
// primary key under DEFAULT, the chosen index under USING INDEX, and only one
// that is valid and not deferrable (relcache's own rule, RelationGetIndexList).
// The identity is that index's KEY columns (not its INCLUDE columns); the
// primary-key columns are read the way the capture reads them (pkColumnsQuery).
const tableIdentitiesQuery = `
	SELECT pt.schemaname, pt.tablename, c.relreplident::text,
	       c.relreplident <> 'f' AND ri.indexrelid IS NULL,
	       COALESCE((SELECT array_agg(a.attname::text ORDER BY a.attnum)
	                 FROM pg_index pk
	                 JOIN pg_attribute a ON a.attrelid = pk.indrelid AND a.attnum = ANY(pk.indkey)
	                 WHERE pk.indrelid = c.oid AND pk.indisprimary AND ri.indexrelid IS NOT NULL
	                   AND NOT EXISTS (SELECT 1 FROM unnest(ri.indkey::int2[]) WITH ORDINALITY AS k(att, ord)
	                                   WHERE k.ord <= ri.indnkeyatts AND k.att = a.attnum)), '{}')::text[],
	       parts.below, parts.misses
	FROM pg_publication_tables pt
	JOIN pg_namespace n ON n.nspname = pt.schemaname
	JOIN pg_class c ON c.relname = pt.tablename AND c.relnamespace = n.oid
	LEFT JOIN pg_index ri ON ri.indrelid = c.oid AND ri.indisvalid AND ri.indimmediate
	     AND ((c.relreplident = 'd' AND ri.indisprimary) OR (c.relreplident = 'i' AND ri.indisreplident))
	CROSS JOIN LATERAL (
	     WITH RECURSIVE tree AS (
	          SELECT c.oid AS oid, c.relkind AS relkind, c.relreplident AS ident
	          UNION ALL
	          SELECT ch.oid, ch.relkind, ch.relreplident
	          FROM tree t
	          JOIN pg_inherits i ON i.inhparent = t.oid
	          JOIN pg_class ch ON ch.oid = i.inhrelid
	          WHERE t.relkind = 'p'
	     ), leaves AS (
	          SELECT t.oid, t.ident,
	                 '"' || replace(ln.nspname, '"', '""') || '"."' || replace(lc.relname, '"', '""') || '"' AS quoted
	          FROM tree t
	          JOIN pg_class lc ON lc.oid = t.oid
	          JOIN pg_namespace ln ON ln.oid = lc.relnamespace
	          WHERE t.relkind <> 'p' AND t.oid <> c.oid
	     )
	     SELECT COALESCE((SELECT array_agg(l.quoted ORDER BY l.quoted) FROM leaves l WHERE l.ident <> 'f'), '{}')::text[] AS below,
	            COALESCE((SELECT array_agg(l.quoted ORDER BY l.quoted)
	                      FROM leaves l
	                      JOIN pg_index lri ON lri.indrelid = l.oid AND lri.indisreplident AND lri.indisvalid AND lri.indimmediate
	                      WHERE l.ident = 'i'
	                        AND EXISTS (SELECT 1 FROM pg_index lpk, unnest(lpk.indkey::int2[]) AS a(att)
	                                    WHERE lpk.indrelid = l.oid AND lpk.indisprimary AND a.att > 0
	                                      AND NOT EXISTS (SELECT 1 FROM unnest(lri.indkey::int2[]) WITH ORDINALITY AS k(att, ord)
	                                                      WHERE k.ord <= lri.indnkeyatts AND k.att = a.att))), '{}')::text[] AS misses
	) parts
	WHERE pt.pubname = $1 AND (c.relreplident <> 'f' OR cardinality(parts.below) > 0)
	ORDER BY 1, 2`

// QueryTableIdentities returns the published tables that are not at REPLICA
// IDENTITY FULL, with what the doctor and the capture's startup warnings say
// about each; empty when every table is at FULL. Report-only: capture decides
// from what PostgreSQL sends (cacheRelation), which is the same rule.
func QueryTableIdentities(ctx context.Context, conn *pgx.Conn, publication string) ([]TableIdentity, error) {
	rows, err := conn.Query(ctx, tableIdentitiesQuery, publication)
	if err != nil {
		return nil, fmt.Errorf("pgcapture: checking replica identity for publication %q: %w", publication, err)
	}
	defer rows.Close()
	var out []TableIdentity
	for rows.Next() {
		var t TableIdentity
		if err := rows.Scan(&t.Schema, &t.Table, &t.Identity, &t.NoIdentity, &t.MissesKey, &t.PartitionsBelowFull, &t.PartitionsMissKey); err != nil {
			return nil, fmt.Errorf("pgcapture: scanning replica identity for publication %q: %w", publication, err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgcapture: checking replica identity for publication %q: %w", publication, err)
	}
	return out, nil
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
// forgets its cascade child. A PUBLISHED child is captured whatever its replica
// identity, so only the not-published case needs surfacing here.
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
	// By its identity, not its name alone: a physical slot, a slot of
	// another plugin or one of another database that happens to be called
	// this is not the capture's, and resuming "on" it would look healthy
	// while reading a stream nobody positioned (queryScopedSlotState, as
	// EnsureSlotExists has always asked). Catalog columns only, so a standby
	// source answers it like a primary.
	found, walStatus, err := queryScopedSlotState(ctx, queryConn, slotName)
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

// Name is the table as "schema.table (relreplident=x)", the form the health
// panel's list uses, with how many of its partitions are below FULL when
// some are.
func (t TableIdentity) Name() string {
	if n := len(t.PartitionsBelowFull); n > 0 {
		return fmt.Sprintf("%s.%s (relreplident=%s, %d partition(s) not FULL)", t.Schema, t.Table, t.Identity, n)
	}
	return fmt.Sprintf("%s.%s (relreplident=%s)", t.Schema, t.Table, t.Identity)
}

// SplitTableIdentities sorts the tables below REPLICA IDENTITY FULL by what
// it means for each: captured without undo; not captured because the
// identity leaves the primary key out; and with no identity at all, where
// PostgreSQL itself refuses UPDATE and DELETE. A table is in one list only.
func SplitTableIdentities(all []TableIdentity) (withoutUndo, missesKey, noIdentity []TableIdentity) {
	for _, t := range all {
		switch {
		case t.NoIdentity:
			noIdentity = append(noIdentity, t)
		case !t.Captured():
			missesKey = append(missesKey, t)
		default:
			withoutUndo = append(withoutUndo, t)
		}
	}
	return withoutUndo, missesKey, noIdentity
}

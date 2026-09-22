package console

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/query"
)

// GET /api/schema-changes — the DDL history view (#1443).
//
// The index records every CREATE / ALTER / DROP / RENAME / TRUNCATE the stream
// sees in schema_changes, the MCP tool list_schema_changes serves it, and the
// CLI can query it; the console had no surface for it, so "what ALTERs ran
// last week?" was answerable everywhere except the browser. This endpoint
// reads the selected server's schema_changes table with the same filters the
// MCP tool takes (schema, table, ddl_type prefix-matched, since/until) and the
// Events caps model (default 100, max 1000, one probe row for has_more).
//
// Ordering is `detected_at DESC, binlog_file DESC, binlog_pos DESC, id DESC`.
// detected_at is second-granular and DDL arrives in same-second bursts (a
// migration runs dozens of statements inside one second); with detected_at
// alone the order within a second is whatever the storage engine returns, so
// a CREATE can list before the DROP that followed it and the story reads
// backwards exactly where it is densest. The binlog coordinate IS the true
// per-source order, with id as the deterministic tail. Sibling issue #1441
// applies the same tiebreak to the MCP tool; the two queries are written
// separately on purpose (this package does not reach into internal/mcptools).
//
// Scope: the resolved deny/allow table scope (startup floor + session profile
// + policy restrictions, #1449) is pushed into the WHERE clause with the same
// two comparisons buildQuery uses for row events, so a session that cannot
// read a table never sees its DDL either — and the cap stays exact, which a
// post-fetch filter over the page could not promise.
//
// What the scope does NOT do, and what covers the rest: the WHERE scopes the
// row's schema_name/table_name, and every row carries its statement verbatim.
// A DROP or RENAME has a row for each table it names, each scoped by its own
// rules, but the text of `DROP TABLE users, secrets` on the users row still
// names secrets, and an ALTER can name a denied table in a REFERENCES clause.
// That is why, under an active access
// profile (opts.ProfileActive: a named profile, direct session restrictions,
// or the startup --profile), the statement text is WITHHELD — the same posture
// /api/events takes for query_text/query_hash (#699/#838): DDL text can carry
// literals (`ADD COLUMN c VARCHAR(16) DEFAULT '<value>'`) and other tables'
// names, and no per-column redaction can reach inside it. Time, table, type
// and binlog position stay. The response says so (statement_withheld plus a
// warning), and it also announces the table scoping the way /api/events
// announces its own (#1311): a shorter list must never read as "nothing else
// happened".
//
// Open-core: this is the free query_explorer surface. No attribution fields
// (no connection_id-style columns) belong here.
//
// Deliberately NOT audited, like list_schema_changes: ext.Record fires from
// surfaces that serve historical ROW DATA, and a DDL statement carries none.
const (
	schemaChangesDefaultLimit = eventsDefaultLimit
	schemaChangesMaxLimit     = eventsMaxLimit
)

// schemaChangeDDLTypes is the ddl_type filter vocabulary, matched as a PREFIX
// of the stored ddl_type ("ALTER" matches "ALTER TABLE") exactly like the MCP
// tool, so the two surfaces answer the same question the same way.
var schemaChangeDDLTypes = []string{"CREATE", "ALTER", "DROP", "RENAME", "TRUNCATE"}

// schemaChangeDTO is one DDL detection on the wire. Field names follow the
// MCP tool's output so a reader moving between the two surfaces sees one
// vocabulary.
type schemaChangeDTO struct {
	ID         int64  `json:"id"`
	DetectedAt string `json:"detected_at"`
	Schema     string `json:"schema_name"`
	Table      string `json:"table_name"`
	DDLType    string `json:"ddl_type"`
	Statement  string `json:"statement"`
	BinlogFile string `json:"binlog_file"`
	BinlogPos  uint64 `json:"binlog_pos"`
}

// schemaChangesResponse is the GET /api/schema-changes body. Count and Limit
// describe the page, never the probe row (the same contract as eventsResponse).
type schemaChangesResponse struct {
	Changes []schemaChangeDTO `json:"changes"`
	Count   int               `json:"count"`
	Limit   int               `json:"limit"`
	// HasMore reports that at least one further change matched beyond the
	// cap. One probe row, never a COUNT(*).
	HasMore bool `json:"has_more"`
	// StatementWithheld is true when every Statement in this page is empty
	// because an access profile is active (see the file comment). A flag
	// rather than an omitted field, so a client can tell "withheld" from an
	// index that stored an empty statement.
	StatementWithheld bool `json:"statement_withheld,omitempty"`
	// Warnings announce what this listing does not include: tables outside
	// the session's access, and withheld statement text. Same register as
	// eventsResponse.Warnings; the UI renders them above the list.
	Warnings []string `json:"warnings,omitempty"`
}

// Scoping notices, worded for the person reading the list.
const (
	schemaChangesScopeWarning = "Your access policy limits which tables you can read, so DDL recorded for other " +
		"tables is not listed here. A DROP or RENAME that names several tables is listed under each of them."
	schemaChangesWithheldWarning = "Statement text is withheld while an access profile is active, because DDL text " +
		"can carry values and name other tables. Time, table, type and binlog position are shown."
)

// schemaChangesFilter is the parsed, validated request.
type schemaChangesFilter struct {
	Schema  string
	Table   string
	DDLType string // upper-cased, one of schemaChangeDDLTypes, or ""
	Since   *time.Time
	Until   *time.Time
	// Deny and Allow are the resolved table scope (see buildSchemaChangesQuery).
	Deny  []query.SchemaTable
	Allow []query.SchemaTable
	// Fetch is the row count asked of the database: the page cap plus the
	// probe row.
	Fetch int
}

// buildSchemaChangesQuery renders the SELECT for f. Pure, so the shape —
// clauses, argument order and the ordering tiebreak — is testable without a
// database.
func buildSchemaChangesQuery(f schemaChangesFilter) (string, []any) {
	var where []string
	var args []any
	if f.Schema != "" {
		where = append(where, "schema_name = ?")
		args = append(args, f.Schema)
	}
	if f.Table != "" {
		where = append(where, "table_name = ?")
		args = append(args, f.Table)
	}
	if f.DDLType != "" {
		where = append(where, "ddl_type LIKE ?")
		args = append(args, f.DDLType+"%")
	}
	if f.Since != nil {
		where = append(where, "detected_at >= ?")
		args = append(args, *f.Since)
	}
	if f.Until != nil {
		where = append(where, "detected_at <= ?")
		args = append(args, *f.Until)
	}
	// The scope clauses mirror internal/query's buildQuery: allow matches
	// with BINARY (a case-insensitive allow fails open on a
	// lower_case_table_names=0 host), deny stays on the column collation
	// (case-insensitive there withholds MORE, the safe direction). Deny
	// composes over allow via AND, so deny always wins.
	//
	// One thing binlog_events never has: schema_name can be EMPTY here — on
	// rows indexed before #1435, which recorded unqualified DDL (`USE app;
	// TRUNCATE TABLE secrets`) with no schema. New rows carry the session's
	// default database, so under an allow-list profile an unqualified DDL
	// row goes from invisible-to-everyone to visible-to-users-allowed on
	// that schema+table — the correct attribution, not a widening. The
	// empty-row handling stays for the historical rows: a deny keyed on the
	// pair would let them through, so the deny also withholds an unqualified
	// row whose TABLE matches (it may be the denied table, and the safe
	// direction is to withhold), and the allow list already excludes them
	// (BINARY '' never matches a named schema).
	if len(f.Allow) > 0 {
		ors := make([]string, len(f.Allow))
		for i, at := range f.Allow {
			ors[i] = "(BINARY schema_name = ? AND BINARY table_name = ?)"
			args = append(args, at.Schema, at.Table)
		}
		where = append(where, "("+strings.Join(ors, " OR ")+")")
	}
	for _, dt := range f.Deny {
		where = append(where, "NOT (table_name = ? AND (schema_name = ? OR schema_name = ''))")
		args = append(args, dt.Table, dt.Schema)
	}
	q := "SELECT id, detected_at, schema_name, table_name, ddl_type, ddl_query, binlog_file, binlog_pos FROM schema_changes"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY detected_at DESC, binlog_file DESC, binlog_pos DESC, id DESC LIMIT ?"
	args = append(args, f.Fetch)
	return q, args
}

// handleSchemaChanges serves GET /api/schema-changes.
func (s *Server) handleSchemaChanges(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	q := r.URL.Query()
	ddlType := strings.ToUpper(strings.TrimSpace(q.Get("ddl_type")))
	if ddlType != "" && !slices.Contains(schemaChangeDDLTypes, ddlType) {
		writeJSONError(w, http.StatusBadRequest,
			fmt.Sprintf("invalid ddl_type %q; must be one of %s", q.Get("ddl_type"), strings.Join(schemaChangeDDLTypes, ", ")))
		return
	}
	since, err := cliutil.ParseTime(q.Get("since"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid since: "+err.Error())
		return
	}
	until, err := cliutil.ParseTime(q.Get("until"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid until: "+err.Error())
		return
	}
	limit := clampLimit(atoiDefault(q.Get("limit"), 0), schemaChangesDefaultLimit, schemaChangesMaxLimit)

	// The same scope resolution handleSchemas runs for its name listings:
	// startup floor, then the session's profile and direct restrictions.
	opts, err := s.applySessionProfile(r.Context(), r, b, query.Options{
		DenyTables:    s.denyTables,
		RedactColumns: s.redactCols,
		ProfileActive: s.profileActive,
	})
	if err != nil {
		writeSessionProfileError(w, r, err)
		return
	}

	sqlText, args := buildSchemaChangesQuery(schemaChangesFilter{
		Schema:  q.Get("schema"),
		Table:   q.Get("table"),
		DDLType: ddlType,
		Since:   since,
		Until:   until,
		Deny:    opts.DenyTables,
		Allow:   opts.AllowTables,
		Fetch:   limit + 1,
	})
	rows, err := b.db.QueryContext(r.Context(), sqlText, args...)
	if err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1146 {
			// ER_NO_SUCH_TABLE: an index provisioned before DDL tracking
			// existed. Actionable rather than a bare server error — the
			// table is created by init, which is safe to re-run.
			writeJSONError(w, http.StatusUnprocessableEntity,
				"This index has no schema_changes table, so no DDL history was recorded for it. "+
					"Re-run init against the index to add the table (CLI: bintrail init); "+
					"DDL that ran before that cannot be back-filled.")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	// Under an active profile the statement column is dropped from every row
	// (see the file comment); the flag and the warning below say so.
	withheld := opts.ProfileActive
	changes := []schemaChangeDTO{} // never null on the wire
	for rows.Next() {
		var c schemaChangeDTO
		var detectedAt time.Time
		var stmt sql.NullString
		if err := rows.Scan(&c.ID, &detectedAt, &c.Schema, &c.Table, &c.DDLType, &stmt, &c.BinlogFile, &c.BinlogPos); err != nil {
			writeJSONError(w, http.StatusInternalServerError, "scan schema_changes: "+err.Error())
			return
		}
		c.DetectedAt = detectedAt.UTC().Format(consoleTSFormat)
		if !withheld {
			c.Statement = stmt.String
		}
		changes = append(changes, c)
	}
	if err := rows.Err(); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "read schema_changes: "+err.Error())
		return
	}
	hasMore := len(changes) > limit
	if hasMore {
		changes = changes[:limit]
	}
	var warnings []string
	if withheld {
		warnings = []string{schemaChangesScopeWarning, schemaChangesWithheldWarning}
	}
	writeJSON(w, http.StatusOK, schemaChangesResponse{
		Changes:           changes,
		Count:             len(changes),
		Limit:             limit,
		HasMore:           hasMore,
		StatementWithheld: withheld,
		Warnings:          warnings,
	})
}

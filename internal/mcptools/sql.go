package mcptools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// ─── sql: analytical SQL on the copy (#2456) ─────────────────────────────────
//
// The other tools read the change index: what happened to rows. This one runs
// a read-only statement on the copy (the Parquet tables the console's SQL page
// and MySQL port answer from), so a client can ask what the data looks like
// now: totals, rankings, trends. The engine is the surface's own (the console
// binds Target.RunSQL to the same path /api/sql runs); this file owns what is
// MCP-specific: the caps sized for a model, the statement echoed back, the
// access-rule refusal and the audit record.

const (
	// SQLDefaultMaxRows is the row cap of one sql call, and the most max_rows
	// may ask for. Lower than the SQL page's: the answer is read by a model.
	SQLDefaultMaxRows = 500
	// SQLMaxResultBytes is the most one answer may weigh, as the JSON the
	// client reads: the rows plus the statement echo, the columns and the
	// notice. Rows past it are left out and the answer says so, rather than
	// handing the client a payload it may refuse (#1438).
	SQLMaxResultBytes = 1 << 20
	// SQLMaxStatementBytes is the longest statement the tool takes. Every
	// answer echoes it and the audit record stores it, so it must leave the
	// rows most of the answer's budget.
	SQLMaxStatementBytes = 64 << 10
)

// SQLAnswer is what a surface's RunSQL returns for one statement.
type SQLAnswer struct {
	Result sqlsandbox.Result
	// CopyUpdatedAt is the snapshot the tables answered from; nil when the
	// copy has no snapshot and only the change log was read.
	CopyUpdatedAt *time.Time
	// OlderCopy says why an earlier copy than the newest answered (#2210),
	// "" when the newest did.
	OlderCopy string
}

// SQLRunFunc runs one statement on a Target's copy. schema is where
// unqualified names resolve ("" for the default); maxRows is the row cap. The
// error's text is shown to the client as it is.
type SQLRunFunc func(ctx context.Context, statement, schema string, maxRows int) (SQLAnswer, error)

// SQLArgs are the sql tool's parameters.
type SQLArgs struct {
	Statement string `json:"statement" jsonschema:"One read-only SQL statement (DuckDB dialect), e.g. SELECT status, count(*) FROM shop.orders GROUP BY status. Tables are named as on the source database (schema.table). The events table holds the history of row changes."`
	Schema    string `json:"schema,omitempty" jsonschema:"Schema that unqualified table names resolve in, like USE on MySQL. Omit it and qualify names as schema.table."`
	MaxRows   int    `json:"max_rows,omitempty" jsonschema:"Most rows to return, 1 to 500. Default 500."`
}

type routedSQLArgs struct {
	SQLArgs
	Server string `json:"server,omitempty" jsonschema:"Which monitored server answers this call: a server name or id from the web interface's server list (this tool's description lists them). Omit it to use this connection's server."`
}

func (w routedSQLArgs) toolArgs() SQLArgs { return w.SQLArgs }
func (w routedSQLArgs) serverArg() string { return w.Server }

// sqlToolResult is the sql tool's answer.
type sqlToolResult struct {
	// Statement is the statement that ran, byte for byte as it was sent: the
	// tool never rewrites it (the caps cut rows after the run), so this is
	// what the user re-runs elsewhere to get every row.
	Statement      string              `json:"statement"`
	Schema         string              `json:"schema,omitempty"`
	Columns        []sqlsandbox.Column `json:"columns"`
	Rows           [][]any             `json:"rows"`
	RowCount       int                 `json:"row_count"`
	Truncated      bool                `json:"truncated"`
	Notice         string              `json:"notice,omitempty"`
	TruncatedCells int                 `json:"truncated_cells,omitempty"`
	ElapsedMS      int64               `json:"elapsed_ms"`
	CopyUpdatedAt  *time.Time          `json:"copy_updated_at"`
	OlderCopy      string              `json:"older_copy,omitempty"`
}

// sqlToolDescription is the sql tool's description.
const sqlToolDescription = "Run one read-only SQL statement on the copy of the monitored database: its tables as they are now, " +
	"for questions about the current data (totals, rankings, trends, joins across tables). " +
	"For who changed which row and when, use query, recover or reconstruct instead: they read the history of changes. " +
	"The answer carries the statement that ran (statement) and the time of the copy that answered (copy_updated_at): " +
	"show the user both with the result, so they can check what was asked and how current the numbers are. " +
	"At most 500 rows and 1 MiB come back; past that, truncated is true and notice says what was left out. " +
	"Prefer aggregating in SQL (GROUP BY, ORDER BY ... LIMIT) over pulling raw rows. " +
	sqlCatalogHint

// sqlCatalogHint tells the model how to learn the tables before guessing
// names (#2456). The copy answers its own catalog, so this is a sentence, not
// a tool; it names the traps measured on a real worker: SHOW TABLES without
// the schema parameter searches a schema that holds no table and returns
// nothing (with it, it lists that schema's tables); and the events table is
// not in SHOW ALL TABLES (its view is installed only for a statement that
// names it) although it can be queried, and only exists where the change log
// is on the console's machine.
const sqlCatalogHint = "To learn the tables before writing a query, run SHOW ALL TABLES (every table with its columns and types) " +
	"or DESCRIBE schema.table; SHOW TABLES lists nothing unless the schema parameter is set. " +
	"The events table, the history of row changes, exists when the change log is on this server's machine: " +
	"it is not listed by SHOW ALL TABLES but can be queried, and DESCRIBE events shows its columns."

// SQLCatalogHint is the sentence of the sql tool's description that sends a
// model to the copy's own catalog, exported so the console's real-worker test
// can run each statement it names.
func SQLCatalogHint() string { return sqlCatalogHint }

// errSQLAccessRules is the per-call refusal under access rules.
var errSQLAccessRules = errors.New("SQL on the copy is unavailable on this server while access rules are active " +
	"(a data profile, denied tables or redacted columns): those rules withhold tables and columns on every other read, " +
	"and free SQL reads the copy's files directly, which they cannot filter. The query, recover and reconstruct tools still answer")

// MakeSQLTool returns the sql tool handler.
func MakeSQLTool(cfg Config) func(context.Context, *mcp.CallToolRequest, SQLArgs) (*mcp.CallToolResult, any, error) {
	return func(ctx context.Context, req *mcp.CallToolRequest, args SQLArgs) (*mcp.CallToolResult, any, error) {
		if strings.TrimSpace(args.Statement) == "" {
			return ErrorResult(errors.New("statement is empty: pass one SQL statement")), nil, nil
		}
		if len(args.Statement) > SQLMaxStatementBytes {
			return ErrorResult(fmt.Errorf("statement is %d KiB, longer than the %d KiB this tool takes", len(args.Statement)>>10, SQLMaxStatementBytes>>10)), nil, nil
		}
		if args.MaxRows < 0 || args.MaxRows > SQLDefaultMaxRows {
			return ErrorResult(fmt.Errorf("max_rows must be between 1 and %d, or left out", SQLDefaultMaxRows)), nil, nil
		}
		maxRows := args.MaxRows
		if maxRows == 0 {
			maxRows = SQLDefaultMaxRows
		}
		schema := strings.TrimSpace(args.Schema)

		t, err := cfg.Resolve(ctx, "")
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		defer t.release()
		// Before anything runs: free SQL cannot honor a rule that withholds
		// tables or columns, the same refusal /api/sql makes.
		if t.ProfileActive || len(t.DenyTables) > 0 || len(t.RedactColumns) > 0 {
			return ErrorResult(errSQLAccessRules), nil, nil
		}
		if t.RunSQL == nil {
			return ErrorResult(errors.New("SQL on the copy is not available on this server")), nil, nil
		}

		ans, err := t.RunSQL(ctx, args.Statement, schema, maxRows)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		res := ans.Result
		out := sqlToolResult{
			Statement:      args.Statement,
			Schema:         schema,
			Columns:        res.Columns,
			Truncated:      res.Truncated,
			TruncatedCells: res.TruncatedCells,
			ElapsedMS:      res.Elapsed.Milliseconds(),
			CopyUpdatedAt:  ans.CopyUpdatedAt,
			OlderCopy:      ans.OlderCopy,
		}
		if out.Columns == nil {
			out.Columns = []sqlsandbox.Column{}
		}
		// The rows get what the rest of the answer leaves of the budget. The
		// rest is measured with the longest notice it can carry, so adding
		// the notice afterwards cannot push the answer past it.
		out.Rows, out.Notice = [][]any{}, sqlSizeNotice(len(res.Rows))
		frame, err := json.Marshal(out)
		if err != nil {
			return ErrorResult(fmt.Errorf("encode sql result: %w", err)), nil, nil
		}
		kept := fitSQLRows(res.Rows, SQLMaxResultBytes-len(frame))
		out.Rows, out.Notice = res.Rows[:kept], ""
		if out.Rows == nil {
			out.Rows = [][]any{}
		}
		out.RowCount = kept
		switch {
		case kept < len(res.Rows):
			out.Truncated = true
			out.Notice = sqlSizeNotice(kept)
		case res.Truncated:
			out.Notice = sqlRowsNotice(kept)
		}

		// Compact, unlike the other tools' indented answers: indenting puts
		// every cell of every row on a line of its own, which multiplies what
		// a model reads for a 500-row answer.
		payload, err := json.Marshal(out)
		if err != nil {
			return ErrorResult(fmt.Errorf("encode sql result: %w", err)), nil, nil
		}

		ext.Record(ctx, ext.AuditEvent{
			Surface: cfg.auditSurface(),
			Action:  "sql.run",
			Actor:   ext.ProcessActor(""),
			Detail: t.auditDetail(map[string]string{
				"sql":        args.Statement,
				"rows":       strconv.Itoa(kept),
				"truncated":  strconv.FormatBool(out.Truncated),
				"elapsed_ms": strconv.FormatInt(out.ElapsedMS, 10),
			}),
		})

		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(payload)}},
		}, nil, nil
	}
}

// fitSQLRows is how many leading rows fit in budget bytes of JSON.
func fitSQLRows(rows [][]any, budget int) int {
	used := 0
	for i, r := range rows {
		b, err := json.Marshal(r)
		if err != nil {
			return i
		}
		used += len(b) + 1 // the separator between rows
		if used > budget {
			return i
		}
	}
	return len(rows)
}

// sqlRerunHint is how the user gets the rows a capped answer left out.
const sqlRerunHint = "To see every row, the user can run the statement above on the SQL page of the web interface " +
	"or on DBTrail's MySQL port, which return more rows; or narrow it with WHERE, GROUP BY or LIMIT."

func sqlRowsNotice(kept int) string {
	return fmt.Sprintf("Only the first %d rows are here: the statement returns more. %s", kept, sqlRerunHint)
}

func sqlSizeNotice(kept int) string {
	if kept == 0 {
		return fmt.Sprintf("No row is here: the first one alone does not fit in the %d KiB this tool returns at once. "+
			"Select fewer or narrower columns.", SQLMaxResultBytes>>10)
	}
	return fmt.Sprintf("Only the first %d rows are here: the next one would pass the %d KiB this tool returns at once. %s",
		kept, SQLMaxResultBytes>>10, sqlRerunHint)
}

package console

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/modelcontextprotocol/go-sdk/auth"

	"github.com/dbtrail/dbtrail/internal/mcptools"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The console half of the MCP sql tool (#2456): the tool (mcptools.MakeSQLTool)
// owns the caps, the echoed statement, the access-rule refusal and the audit
// record; this binds it to the same path POST /api/sql runs (runSQL: the
// local-copy gates, the slot, the per-statement views) and turns that path's
// errors into text an MCP client can be shown.

// mcpSQLRunner binds the sql tool to one server's bundle.
func (s *Server) mcpSQLRunner(b *bundle) mcptools.SQLRunFunc {
	return func(ctx context.Context, statement, schema string, maxRows int) (mcptools.SQLAnswer, error) {
		if b.noArchive {
			return mcptools.SQLAnswer{}, errors.New("archive access is disabled for this server, so its copy cannot be read")
		}
		out, err := s.runSQL(ctx, b, mcpSQLUser(ctx), statement, schema, maxRows, sqlsandbox.Session{})
		if err != nil {
			return mcptools.SQLAnswer{}, mcpSQLError(err)
		}
		return mcptools.SQLAnswer{Result: out.Result, CopyUpdatedAt: out.CopyUpdatedAt, OlderCopy: out.OlderCopyNote}, nil
	}
}

// mcpSQLUser keys the one-query-at-a-time slot for an MCP call: the token's
// identity, so an agent calling in a loop waits its turn behind its own last
// statement like a person on the SQL page does, and two different tokens do
// not wait on each other.
func mcpSQLUser(ctx context.Context) string {
	if ti := auth.TokenInfoFromContext(ctx); ti != nil && ti.UserID != "" {
		return "mcp:" + ti.UserID
	}
	return "mcp"
}

// sqlEventsInS3MCPMessage is sqlEventsInS3Message for an MCP client, which
// has no Events page but has the history tools.
const sqlEventsInS3MCPMessage = "the change history for this server is on S3, so events cannot be read with SQL here; the tables can. " +
	"For the history of rows, use the query or reconstruct tools"

// mcpSQLError maps runSQL's errors to what an MCP client is shown. The
// runner's typed errors already read as sentences and pass through; a worker
// failure, whose text can carry host paths, and anything unknown are logged
// and replaced, as writeSQLError does for the browser.
func mcpSQLError(err error) error {
	var refusal *sqlRefusal
	var refused *sqlsandbox.RefusedError
	var qerr *sqlsandbox.QueryError
	var timeout *sqlsandbox.TimeoutError
	var busy *sqlsandbox.BusyError
	var werr *sqlsandbox.WorkerError
	switch {
	case errors.As(err, &refusal):
		switch refusal.Message {
		case sqlCopyNotLocalMessage:
			return errors.New(sqlCopyNotLocalPortMessage)
		case sqlEventsInS3Message:
			return errors.New(sqlEventsInS3MCPMessage)
		}
		return errors.New(refusal.Message)
	case errors.Is(err, sqlsandbox.ErrCopyNotLocal):
		return errors.New(sqlCopyNotLocalPortMessage)
	case errors.As(err, &timeout):
		return fmt.Errorf("%w; narrow it (a WHERE on a table, or a smaller window on events)", timeout)
	case errors.As(err, &refused), errors.As(err, &qerr), errors.As(err, &busy),
		errors.Is(err, sqlsandbox.ErrBusy), errors.Is(err, sqlsandbox.ErrResultTooLarge),
		errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.As(err, &werr):
		slog.Error("console: the SQL worker failed", "error", err, "surface", "mcp")
		return errors.New(sqlWorkerFailedMessage)
	default:
		slog.Error("console: sql query failed", "error", err, "surface", "mcp")
		return errors.New("the query could not be run; DBTrail's log has the details")
	}
}

package console

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// FlashbackTarget is the go-mysql-free resolution of a flashback connection's
// target server (issue #996): the per-source index handle, baseline source, and
// default schema the MySQL-protocol serving layer needs to build a shim handler.
//
// It lives here — but the serving code does NOT — because internal/console is a
// read-layer package barred from linking go-mysql (#528,
// TestReadLayerDoesNotLinkGoMySQL). cmd/bintrail-console owns the protocol
// server and consumes this plain struct; nothing here imports the protocol or
// capture libraries.
type FlashbackTarget struct {
	// ID is the canonical registry id the selector resolved to (the boot id
	// for the boot entry) — the key the routing tally and metric use, so a
	// client connecting by display name and one by id count together.
	ID string
	// IndexDB is the open connection to the server's per-source index. It is
	// owned by the connManager — the serving layer must not Close it.
	IndexDB *sql.DB
	// IndexDBName is the schema where binlog_events lives (the query planner
	// scopes information_schema.PARTITIONS to it).
	IndexDBName string
	// BaselineDir / BaselineS3 are the resolved _snapshot baseline source, split
	// by scheme (dir-preferred, mirroring the console's Time-travel tab). The
	// #766 local→S3 fallback the console bundle also carries is intentionally
	// dropped — a documented single-source-parity edge for the embedded port
	// (see docs/time-travel-sql.md).
	BaselineDir string
	BaselineS3  string
	// NoArchive disables archive auto-discovery for this server.
	NoArchive bool
	// DefaultSchema is the source database name (from the registry SourceDSN),
	// seeded so USE-less `_flashback.<table>` queries resolve; empty for the
	// boot entry (no registry SourceDSN).
	DefaultSchema string
	// SQL runs free read-only SQL on this server's Parquet copy, the same
	// way POST /api/sql does for the browser (same sandbox, same views, same
	// caps); nil when the port cannot offer it, and SQLUnavailable says why
	// in the words the client is shown. Whether the copy is local is decided
	// per statement, inside Run, so the reason stays exact.
	SQL            *SQLOnCopy
	SQLUnavailable string
	// ForwardDSN is the DSN (credentials included) the serving layer's read
	// router (#2038) opens its ONE upstream connection with: everything the
	// router does on the source (EXPLAIN, forwarded statements, prepared
	// statements, USE) runs on that connection. It is the server's
	// forwarding account when one is set (#2079; ForwardSeparate is then
	// true), else its source DSN; empty for the boot entry and for a server
	// with no source.
	ForwardDSN      string
	ForwardSeparate bool
	// SourceSSL is how the connection to the source uses TLS: the entry's
	// ssl_* fields (ServerEntry.SourceSSL), the value capture connects
	// with. The read router's connection must decide TLS from it, for the
	// forwarding account as for the source account, or it would cross the
	// network in clear where capture is encrypted.
	SourceSSL config.SSL
}

// SQLOnCopy is the free-SQL executor the embedded port hands each connection.
type SQLOnCopy struct {
	s    *Server
	b    *bundle
	user string
}

// Run runs one statement; schema is where unqualified names resolve (the
// connection's USE), empty for DuckDB's default; sess is what the connection
// set for itself (its time zone and sql_select_limit). The error is one the
// client can be shown: the runner's typed errors pass through, the route's
// own refusals (the copy is not queryable here, whatever the statement)
// become a sqlsandbox.UnavailableError with wording that does not name the
// browser, and a worker failure, whose text can carry host paths, is logged
// here and replaced.
func (q *SQLOnCopy) Run(ctx context.Context, statement, schema string, sess sqlsandbox.Session) (sqlsandbox.Result, error) {
	out, err := q.s.runSQL(ctx, q.b, q.user, statement, schema, 0, sess)
	if err != nil {
		var werr *sqlsandbox.WorkerError
		var refusal *sqlRefusal
		switch {
		case errors.As(err, &werr):
			slog.Error("console: the SQL worker failed", "error", err, "surface", "flashback port")
			return sqlsandbox.Result{}, errors.New(sqlWorkerFailedMessage)
		case errors.As(err, &refusal):
			msg := refusal.Message
			switch msg {
			case sqlCopyNotLocalMessage:
				msg = sqlCopyNotLocalPortMessage
			case sqlEventsInS3Message:
				msg = sqlEventsInS3PortMessage
			}
			return sqlsandbox.Result{}, &sqlsandbox.UnavailableError{Reason: msg}
		case errors.Is(err, sqlsandbox.ErrCopyNotLocal):
			return sqlsandbox.Result{}, &sqlsandbox.UnavailableError{Reason: sqlCopyNotLocalPortMessage}
		}
		return sqlsandbox.Result{}, err
	}
	return out.Result, nil
}

// sqlCopyNotLocalPortMessage is sqlCopyNotLocalMessage for a MySQL client,
// which is not in a browser.
const sqlCopyNotLocalPortMessage = "the copy for this server is only on S3; SQL on the copy needs a local copy"

// sqlEventsInS3PortMessage is sqlEventsInS3Message for a MySQL client: the
// row history it can ask for on this same connection is _diff, which reads
// the archives in S3. Kept short: clients cut an error message at 512 bytes.
const sqlEventsInS3PortMessage = "the change history for this server is on S3, so events cannot be read on this port; the tables can. " +
	"For one row's history: SELECT * FROM _diff.<table> BETWEEN '<from>' AND '<to>' WHERE <pk> = <value>. " +
	"To query the whole history, run your own DuckDB on the copy in S3 (views.sql with the change log included)"

// CopyUpdatedAt is the snapshot time the copy's tables answer from, zero
// when there is none or it cannot be read: the router's freshness input.
// One state-only view build (snapshot discovery on disk; no index read).
func (q *SQLOnCopy) CopyUpdatedAt(ctx context.Context) time.Time {
	in, err := q.s.buildViewsInput(ctx, q.b, viewsRequest{PinSnapshot: true, OmitEvents: true, StateOnly: true, ForStatement: true})
	if err != nil {
		// The router forwards on a zero time; the operator still has to
		// learn why the copy never answers.
		slog.Warn("read routing: cannot read the copy's snapshot time; statements are forwarded", "server", q.user, "err", err)
		return time.Time{}
	}
	return in.BaselineSnapshot
}

// sqlOnCopyFor decides, once per connection, whether the port can offer
// free SQL on this server: the console has a sandbox runner, and archive
// access is on. The same two gates POST /api/sql applies before it looks at
// the copy, minus the data-profile one, which a token-authenticated port has
// no profile to apply.
func (s *Server) sqlOnCopyFor(b *bundle, id string) (*SQLOnCopy, string) {
	switch {
	case s.sqlRunner == nil:
		return nil, "SQL on the copy is not enabled on this console"
	case b.noArchive:
		return nil, "archive access is disabled for this server, so its copy cannot be read"
	}
	return &SQLOnCopy{s: s, b: b, user: "server:" + id}, ""
}

// ResolveFlashback maps a flashback connection username to its target server's
// per-source index + baseline, opening the connection lazily via connManager.
// The username selects the server by registry ID, display Name, or "default"
// (the boot entry). Returns ErrUnknownServer when the selector matches no
// selectable server — the serving layer turns that into a MySQL "no such
// database" error on the client's first query. The registry is read live, so
// servers added in the console mid-session are reachable without a restart.
func (s *Server) ResolveFlashback(ctx context.Context, selector string) (FlashbackTarget, error) {
	id, ok := s.flashbackTarget(selector)
	if !ok {
		return FlashbackTarget{}, ErrUnknownServer
	}
	b, err := s.cm.Resolve(ctx, id)
	if err != nil {
		return FlashbackTarget{}, err
	}
	dir, s3 := splitBaselineSource(b.baselineSrc)
	sqlOnCopy, sqlWhyNot := s.sqlOnCopyFor(b, id)
	forwardDSN, forwardSeparate := s.flashbackForwardDSN(id)
	return FlashbackTarget{
		ID:              id,
		IndexDB:         b.db,
		IndexDBName:     b.dbName,
		BaselineDir:     dir,
		BaselineS3:      s3,
		NoArchive:       b.noArchive,
		DefaultSchema:   s.flashbackDefaultSchema(id),
		SQL:             sqlOnCopy,
		SQLUnavailable:  sqlWhyNot,
		ForwardDSN:      forwardDSN,
		ForwardSeparate: forwardSeparate,
		SourceSSL:       s.flashbackSourceSSL(id),
	}, nil
}

// flashbackTarget maps a connection username to a canonical server id: a
// registry ID, a registry display Name, or "default" for the boot entry. The
// registry is read live so servers added in the UI mid-session are reachable
// without restarting the port.
func (s *Server) flashbackTarget(selector string) (string, bool) {
	if selector == "" {
		return "", false
	}
	if _, ok := s.cm.reg.Get(selector); ok {
		return selector, true // matched by id
	}
	for _, e := range s.cm.reg.List() {
		if e.Name == selector {
			return e.ID, true // matched by display name
		}
	}
	if selector == bootServerID && s.cm.bootSelectable() {
		return bootServerID, true
	}
	return "", false
}

// flashbackSourceSSL is the TLS the entry's source connection uses (its ssl_*
// fields, an empty mode meaning the default), the same value capture reads.
// An unknown id gets the default mode: nothing routes there anyway.
func (s *Server) flashbackSourceSSL(id string) config.SSL {
	entry, _ := s.cm.reg.Get(id)
	return entry.SourceSSL()
}

// flashbackDefaultSchema derives the source database name for a target server
// from its registry SourceDSN, for `USE`-less fully qualified queries. Empty
// for the boot entry (no registry SourceDSN) or an unparseable/absent DSN.
func (s *Server) flashbackDefaultSchema(id string) string {
	entry, ok := s.cm.reg.Get(id)
	if !ok || entry.SourceDSN == "" {
		return ""
	}
	cfg, err := drivermysql.ParseDSN(entry.SourceDSN)
	if err != nil {
		return ""
	}
	return cfg.DBName
}

// splitBaselineSource maps a resolved baseline source (the console bundle's
// already dir-preferred baselineSrc) onto the shim's dir/S3 config fields by
// scheme. The #766 local→S3 fallback the console bundle also carries is
// deliberately NOT represented — a documented single-source-parity limitation
// for the embedded port: a server with BOTH a local dir and an S3 copy reads
// `_snapshot` only from the local dir here (see docs/time-travel-sql.md).
func splitBaselineSource(src string) (dir, s3 string) {
	if strings.HasPrefix(src, "s3://") {
		return "", src
	}
	if src != "" {
		return src, ""
	}
	return "", ""
}

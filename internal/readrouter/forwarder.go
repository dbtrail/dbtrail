package readrouter

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	drivermysql "github.com/go-sql-driver/mysql"
)

// CodeUpstreamLost is the MySQL client error code a Forwarder answers with
// once its connection to the source is gone (CR_SERVER_GONE_ERROR, "MySQL
// server has gone away"). It is what a client would see from MySQL itself,
// and every driver knows to reconnect on it.
const CodeUpstreamLost uint16 = 2006

// RowSink receives a forwarded resultset as it arrives from the source, so a
// large result is never held whole in this process. Header is called once,
// before the first row; Row once per row with the cell values (nil for NULL;
// []byte cells alias the packet buffer and are valid only during the call).
// An error from either aborts the statement: the rest of the resultset is
// discarded and the connection to the source is dropped (it cannot be
// resynchronised mid-resultset).
type RowSink interface {
	Header(fields []*mysql.Field) error
	Row(values []any) error
}

// Forwarder is one MySQL-protocol connection to the source, opened lazily and
// held for the life of one client connection on the port: a client's
// transaction, its USE and its session settings all live on this one
// upstream connection, exactly as if the client had connected to MySQL
// itself. It also runs the EXPLAIN the decision reads.
//
// The credentials are the registry's source DSN (the operator's), not the
// client's: the port authenticates on the console token, so a forwarded
// statement has the operator's grants, the same way the copy has no grants
// at all.
//
// The connection is never replaced behind the client's back. Once it is lost
// (a network error, the source closing an idle connection, the query
// deadline), every later statement fails with CodeUpstreamLost until the
// client reconnects: a transparent reconnect would be a NEW session, with
// the transaction rolled back, the settings gone and the database reset,
// while the client believes nothing happened.
type Forwarder struct {
	addr, user, pass string
	tls              *tls.Config
	policy           Policy
	connectTimeout   time.Duration
	// queryTimeout bounds each round trip on the upstream socket (read and
	// write deadline), the same deadline the port applies to a copy
	// statement; a statement past it loses the connection.
	queryTimeout time.Duration

	mu   sync.Mutex
	db   string
	conn *client.Conn
	dead error
}

// NewForwarder parses a go-sql-driver DSN (the registry's source DSN) and
// returns a Forwarder that connects on first use. queryTimeout is the
// deadline for each statement on the upstream connection; 0 means none.
func NewForwarder(sourceDSN string, policy Policy, queryTimeout time.Duration) (*Forwarder, error) {
	cfg, err := drivermysql.ParseDSN(sourceDSN)
	if err != nil {
		return nil, fmt.Errorf("source DSN: %w", err)
	}
	if cfg.Net != "tcp" && cfg.Net != "" {
		return nil, fmt.Errorf("source DSN: only tcp addresses are forwarded, not %q", cfg.Net)
	}
	addr := cfg.Addr
	if _, _, err := net.SplitHostPort(addr); err != nil {
		addr = net.JoinHostPort(addr, "3306")
	}
	return &Forwarder{
		addr: addr, user: cfg.User, pass: cfg.Passwd, db: cfg.DBName,
		tls:            cfg.TLS,
		policy:         policy,
		connectTimeout: 10 * time.Second,
		queryTimeout:   queryTimeout,
	}, nil
}

// get returns the upstream connection, opening it on first use. A connection
// that could not be opened, or was lost, stays lost (see Forwarder).
func (f *Forwarder) get(ctx context.Context) (*client.Conn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dead != nil {
		return nil, f.dead
	}
	if f.conn != nil {
		return f.conn, nil
	}
	c, err := client.ConnectWithContext(ctx, f.addr, f.user, f.pass, f.db, f.connectTimeout, func(c *client.Conn) error {
		c.ReadTimeout = f.queryTimeout
		c.WriteTimeout = f.queryTimeout
		if f.tls != nil {
			// The DSN asked for TLS (tls=true, skip-verify, preferred or a
			// registered config): forwarding honours it, or the live rows
			// would cross the network in clear where the operator asked
			// for encryption.
			c.SetTLSConfig(f.tls)
		}
		return nil
	})
	if err != nil {
		f.dead = lostError(fmt.Errorf("connect to the source: %w", err))
		return nil, f.dead
	}
	f.conn = c
	return c, nil
}

// lose records that the upstream connection is gone, closes it, and makes
// every later statement fail with CodeUpstreamLost naming the cause.
func (f *Forwarder) lose(cause error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn != nil {
		_ = f.conn.Close()
		f.conn = nil
	}
	if f.dead == nil {
		f.dead = lostError(cause)
	}
}

func lostError(cause error) error {
	return mysql.NewError(CodeUpstreamLost, fmt.Sprintf("MySQL server has gone away (the port's connection to the source was lost: %v); reconnect to continue", cause))
}

// IsLost reports whether err is the Forwarder's "connection to the source
// was lost" error.
func IsLost(err error) bool {
	var me *mysql.MyError
	return errors.As(err, &me) && me.Code == CodeUpstreamLost
}

// watch ties the upstream socket to ctx for the duration of one statement:
// when ctx ends (the query deadline, the client connection closing, daemon
// shutdown) the socket is closed, which is the only way to make go-mysql's
// blocking read return. The returned stop must be called when the statement
// is done.
func (f *Forwarder) watch(ctx context.Context) func() bool {
	return context.AfterFunc(ctx, func() { f.lose(ctx.Err()) })
}

// Decide runs EXPLAIN FORMAT=JSON on the source and applies the policy. A
// failure to EXPLAIN (a statement MySQL itself rejects, a lost connection) is
// returned as an error; the caller forwards the statement, so MySQL's own
// answer, error included, reaches the client.
func (f *Forwarder) Decide(ctx context.Context, stmt string) (toCopy bool, reason string, err error) {
	c, err := f.get(ctx)
	if err != nil {
		return false, "", err
	}
	defer f.watch(ctx)()
	res, err := c.Execute("EXPLAIN FORMAT=JSON " + stmt)
	if err != nil {
		if !isMySQLError(err) {
			f.lose(err)
		}
		return false, "", fmt.Errorf("explain: %w", err)
	}
	defer res.Close()
	if res.Resultset == nil || res.RowNumber() == 0 {
		return false, "", errors.New("explain: no plan returned")
	}
	raw, err := res.GetString(0, 0)
	if err != nil {
		return false, "", fmt.Errorf("explain: %w", err)
	}
	plan, err := ParsePlan([]byte(raw))
	if err != nil {
		return false, "", err
	}
	toCopy, reason = f.policy.Decide(plan)
	return toCopy, reason, nil
}

// sinkError marks an error raised by the caller's RowSink (the client went
// away mid-resultset), as opposed to one from the source.
type sinkError struct{ err error }

func (e sinkError) Error() string { return "forwarding rows to the client: " + e.err.Error() }
func (e sinkError) Unwrap() error { return e.err }

// Forward runs the statement on the source. A resultset is streamed through
// sink row by row and the returned Result carries only its tail (fields,
// status, warning count) with Streaming/StreamingDone set, so go-mysql's
// server writes just the trailing EOF; an OK packet (a write, SET, USE,
// BEGIN) comes back whole with its affected rows. MySQL's own error is
// returned as is, for the port to write back to the client byte for byte.
func (f *Forwarder) Forward(ctx context.Context, stmt string, sink RowSink) (*mysql.Result, error) {
	c, err := f.get(ctx)
	if err != nil {
		return nil, err
	}
	defer f.watch(ctx)()
	var res mysql.Result
	var cells []any
	err = c.ExecuteSelectStreaming(stmt, &res,
		func(row []mysql.FieldValue) error {
			if cap(cells) < len(row) {
				cells = make([]any, len(row))
			}
			cells = cells[:len(row)]
			for i := range row {
				cells[i] = row[i].Value()
			}
			if err := sink.Row(cells); err != nil {
				return sinkError{err}
			}
			return nil
		},
		func(r *mysql.Result) error {
			if err := sink.Header(r.Fields); err != nil {
				return sinkError{err}
			}
			return nil
		})
	if err != nil {
		var se sinkError
		switch {
		case errors.As(err, &se):
			// Unread rows are still on the wire: the connection cannot
			// be reused.
			f.lose(se)
			return nil, se.err
		case isMySQLError(err):
			// The source answered with an error packet (before or between
			// rows); the connection is in sync and stays usable.
			return nil, err
		default:
			f.lose(err)
			return nil, lostError(err)
		}
	}
	if res.Resultset != nil && len(res.Fields) == 0 {
		// An OK packet: the client library leaves an empty Resultset on
		// it, which the server would mistake for a resultset.
		res.Resultset = nil
	}
	return &res, nil
}

// UseDB selects the database on the upstream connection, so unqualified
// names in forwarded statements resolve where the client expects. Before the
// connection exists it only records the name for the connect (the handshake
// and the schema seed call it; opening the source there would charge every
// connection a round trip it may never use).
func (f *Forwarder) UseDB(ctx context.Context, db string) error {
	f.mu.Lock()
	if f.dead != nil {
		f.mu.Unlock()
		return f.dead
	}
	if f.conn == nil {
		f.db = db
		f.mu.Unlock()
		return nil
	}
	c := f.conn
	f.mu.Unlock()
	defer f.watch(ctx)()
	if err := c.UseDB(db); err != nil {
		if !isMySQLError(err) {
			f.lose(err)
			return lostError(err)
		}
		return err
	}
	f.mu.Lock()
	f.db = db
	f.mu.Unlock()
	return nil
}

// InTransaction reports whether the upstream connection is inside an
// explicit transaction, from the status flags MySQL sends with every answer:
// the authoritative signal, where parsing BEGIN/COMMIT text is a guess.
func (f *Forwarder) InTransaction() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.conn != nil && f.conn.IsInTransaction()
}

// Close drops the upstream connection; the Forwarder is not reused after.
func (f *Forwarder) Close() {
	f.mu.Lock()
	if f.conn != nil {
		_ = f.conn.Close()
		f.conn = nil
	}
	f.mu.Unlock()
}

func isMySQLError(err error) bool {
	var me *mysql.MyError
	return errors.As(err, &me)
}

// BufferSink collects a forwarded resultset in memory, for callers that
// have no connection to stream to (tests). Production callers stream.
type BufferSink struct {
	Fields []*mysql.Field
	Rows   [][]any
}

func (b *BufferSink) Header(fields []*mysql.Field) error { b.Fields = fields; return nil }

// Row copies the cells: []byte values alias the packet buffer.
func (b *BufferSink) Row(values []any) error {
	row := make([]any, len(values))
	for i, v := range values {
		if bs, ok := v.([]byte); ok {
			v = bytes.Clone(bs)
		}
		row[i] = v
	}
	b.Rows = append(b.Rows, row)
	return nil
}

// Package querykill stops, on the MySQL server, a read whose caller stopped
// waiting for it.
//
// go-sql-driver/mysql cancels a statement by closing the client socket and
// nothing else: it never sends KILL QUERY. The server only notices the
// missing client when it next writes to it, which for a scan that returns its
// first row only at the end (ORDER BY ... DESC LIMIT 8 over millions of rows,
// a GROUP BY) is when the whole scan is done. A web page that gives up after
// 20 s and asks again therefore stacks one more full scan on the index server
// with every retry, each running to its end with nobody listening.
//
// The Connector here wraps a driver.Connector. Each physical connection reads
// its own CONNECTION_ID() once, when it is opened. When a SELECT's context
// ends before the statement is over, the wrapper sends KILL QUERY <that id> on
// a separate, freshly dialed connection with a short budget of its own.
//
// What it does NOT touch, on purpose:
//   - Statements run with ExecContext (writes, DDL). A write that times out on
//     the client generally commits on the server, and callers rely on that
//     (the stream restart after an index write deadline replays from its last
//     checkpoint and drops what the slow batch left behind); killing it would
//     change that contract.
//   - Statements whose context cannot end (context.Background and friends):
//     there is no abandonment to react to.
//   - Work that runs on its own context: only the context the statement was
//     started with is watched, so a background job keeps running when a
//     request that asked for it goes away, and is stopped only when its own
//     context ends.
//
// A KILL never reaches another statement on the same server. The KILL is
// sent while the statement still holds the connection: closing the rows waits
// for an in-flight KILL, and database/sql cannot hand the connection to anyone
// else until that close returns. A connection that had a KILL sent on its
// behalf is then reported as bad (IsValid false, ResetSession ErrBadConn), so
// the pool discards it: its thread id can only ever have belonged to our
// statement, or to nothing.
//
// Nor on another server: thread ids are only unique per server, and the
// connection that carries the KILL is dialed anew, so behind a DNS name with
// several addresses or a load balancer it can land elsewhere. Each connection
// therefore records the server it reached (@@hostname, @@port, @@server_id),
// and the KILL connection checks it reached the same one before sending
// anything. What this cannot see is a proxy that multiplexes client sessions
// over backend connections: there CONNECTION_ID() names whatever the proxy
// answers for, and the guard is only as good as the proxy's own KILL.
package querykill

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
)

// DefaultKillTimeout bounds the dial and the KILL QUERY together. Short on
// purpose: it is paid by a request whose client is already gone.
const DefaultKillTimeout = 5 * time.Second

// erNoSuchThread is MySQL's "Unknown thread id": the connection is already
// gone on the server, which is what a KILL wanted.
const erNoSuchThread = 1094

// warnEvery spaces out the KILL-failure warnings: a refused KILL fails the
// same way on every abandoned request, and the console retries every few
// seconds.
const warnEvery = time.Minute

// Thread names one connection on one server: its CONNECTION_ID() and the
// server it was opened on.
type Thread struct {
	ID     uint64
	Server string // "hostname:port/server_id" as the server reports it
}

// errOtherServer is the KILL connection reaching a server other than the
// statement's.
var errOtherServer = errors.New("the connection opened to send KILL QUERY reached a different server than the statement's; nothing was killed")

// KillFunc sends KILL QUERY for t. It is the test seam; the default dials a
// new connection from the wrapped connector and checks it reached t.Server.
type KillFunc func(ctx context.Context, t Thread) error

// Connector is a driver.Connector whose connections kill their own abandoned
// reads on the server. Build it with New.
type Connector struct {
	inner   driver.Connector
	kill    KillFunc
	timeout time.Duration
	log     *slog.Logger

	killWarn limiter // KILL failures
	idWarn   limiter // CONNECTION_ID() failures
}

// limiter lets one warning through per warnEvery; the rest go to Debug.
type limiter struct{ last atomic.Int64 }

func (l *limiter) log(lg *slog.Logger, msg string, args ...any) {
	now := time.Now().UnixNano()
	last := l.last.Load()
	if (last != 0 && now-last < int64(warnEvery)) || !l.last.CompareAndSwap(last, now) {
		lg.Debug(msg, args...)
		return
	}
	lg.Warn(msg, args...)
}

// Option tunes a Connector.
type Option func(*Connector)

// WithKillFunc replaces how KILL QUERY is sent (tests).
func WithKillFunc(f KillFunc) Option { return func(c *Connector) { c.kill = f } }

// WithKillTimeout replaces DefaultKillTimeout.
func WithKillTimeout(d time.Duration) Option { return func(c *Connector) { c.timeout = d } }

// WithLogger replaces slog.Default().
func WithLogger(l *slog.Logger) Option { return func(c *Connector) { c.log = l } }

// New wraps inner. With no KillFunc, KILL QUERY is sent on a connection dialed
// from inner itself: same server, same account. An account may always kill
// its own threads, so no PROCESS or CONNECTION_ADMIN privilege is needed.
func New(inner driver.Connector, opts ...Option) *Connector {
	c := &Connector{inner: inner, timeout: DefaultKillTimeout}
	for _, o := range opts {
		o(c)
	}
	if c.log == nil {
		c.log = slog.Default()
	}
	if c.kill == nil {
		c.kill = c.dialAndKill
	}
	return c
}

// Driver implements driver.Connector.
func (c *Connector) Driver() driver.Driver { return c.inner.Driver() }

// Close closes the wrapped connector when it has a Close (sql.DB.Close calls
// it).
func (c *Connector) Close() error {
	if cl, ok := c.inner.(io.Closer); ok {
		return cl.Close()
	}
	return nil
}

// Connect implements driver.Connector. A connection whose CONNECTION_ID()
// cannot be read is still returned, unguarded: the guard is a cleanup, and a
// console that cannot read the index because of it would be worse.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	inner, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	cn := &conn{inner: inner, c: c}
	t, err := readThread(ctx, inner)
	if err != nil {
		if ctx.Err() != nil {
			// The caller gave up while the connection was being set up: the
			// driver has closed it. Not a reason to warn about the guard.
			inner.Close()
			return nil, ctx.Err()
		}
		c.idWarn.log(c.log, "index: could not read CONNECTION_ID(); a read abandoned by its caller on this connection will keep running on the server until it ends",
			"error", err)
		return cn, nil
	}
	cn.thread, cn.hasID = t, true
	return cn, nil
}

// threadQuery reads the connection's id and the server it reached. Every
// part exists on MySQL 5.7+ and MariaDB.
const threadQuery = "SELECT CONNECTION_ID(), @@hostname, @@port, @@server_id"

func readThread(ctx context.Context, c driver.Conn) (Thread, error) {
	q, ok := c.(driver.QueryerContext)
	if !ok {
		return Thread{}, errors.New("driver connection cannot run a query")
	}
	rows, err := q.QueryContext(ctx, threadQuery, nil)
	if err != nil {
		return Thread{}, err
	}
	defer rows.Close()
	dest := make([]driver.Value, len(rows.Columns()))
	if len(dest) != 4 {
		return Thread{}, fmt.Errorf("%s returned %d columns", threadQuery, len(dest))
	}
	if err := rows.Next(dest); err != nil {
		return Thread{}, fmt.Errorf("%s: %w", threadQuery, err)
	}
	id, err := positiveID(dest[0])
	if err != nil {
		return Thread{}, err
	}
	return Thread{ID: id, Server: fmt.Sprintf("%s:%s/%s", text(dest[1]), text(dest[2]), text(dest[3]))}, nil
}

func text(v driver.Value) string {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return fmt.Sprint(v)
}

func positiveID(v driver.Value) (uint64, error) {
	switch v := v.(type) {
	case int64:
		if v > 0 {
			return uint64(v), nil
		}
	case uint64:
		if v > 0 {
			return v, nil
		}
	case []byte:
		if n, err := strconv.ParseUint(string(v), 10, 64); err == nil && n > 0 {
			return n, nil
		}
	case string:
		if n, err := strconv.ParseUint(v, 10, 64); err == nil && n > 0 {
			return n, nil
		}
	}
	return 0, fmt.Errorf("CONNECTION_ID() returned %v (%T)", v, v)
}

func (c *Connector) dialAndKill(ctx context.Context, t Thread) error {
	k, err := c.inner.Connect(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer k.Close()
	here, err := readThread(ctx, k)
	if err != nil {
		return fmt.Errorf("identify the server: %w", err)
	}
	if here.Server != t.Server {
		return fmt.Errorf("%w (statement on %s, KILL connection on %s)", errOtherServer, t.Server, here.Server)
	}
	ex, ok := k.(driver.ExecerContext)
	if !ok {
		return errors.New("driver connection cannot run a statement")
	}
	_, err = ex.ExecContext(ctx, "KILL QUERY "+strconv.FormatUint(t.ID, 10), nil)
	return err
}

// killQuery sends the KILL for t within the connector's budget. It never
// returns an error: the read it cleans up after has already failed for its
// caller, and a KILL the server refuses must not turn that into a crash or a
// hang. Failures are logged, one warning per warnEvery.
func (c *Connector) killQuery(t Thread) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	err := c.kill(ctx, t)
	if err == nil {
		return
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) && me.Number == erNoSuchThread {
		return
	}
	c.killWarn.log(c.log, "index: could not stop an abandoned read on the server (KILL QUERY failed); it keeps running until it ends",
		"connection_id", t.ID, "error", err)
}

// conn wraps one physical connection.
type conn struct {
	inner  driver.Conn
	c      *Connector
	thread Thread
	hasID  bool
	// killed is set once a KILL was sent on this connection's behalf: from
	// then on the pool must not reuse it (see the package doc).
	killed atomic.Bool
}

var (
	_ driver.Conn               = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.SessionResetter    = (*conn)(nil)
	_ driver.Validator          = (*conn)(nil)
	_ driver.NamedValueChecker  = (*conn)(nil)
)

func (cn *conn) Prepare(query string) (driver.Stmt, error) {
	s, err := cn.inner.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &stmt{inner: s, cn: cn}, nil
}

func (cn *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	var s driver.Stmt
	var err error
	if p, ok := cn.inner.(driver.ConnPrepareContext); ok {
		s, err = p.PrepareContext(ctx, query)
	} else {
		s, err = cn.inner.Prepare(query)
	}
	if err != nil {
		return nil, err
	}
	return &stmt{inner: s, cn: cn}, nil
}

func (cn *conn) Close() error { return cn.inner.Close() }

//nolint:staticcheck // driver.Conn requires Begin.
func (cn *conn) Begin() (driver.Tx, error) { return cn.inner.Begin() }

func (cn *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if b, ok := cn.inner.(driver.ConnBeginTx); ok {
		return b.BeginTx(ctx, opts)
	}
	if opts.Isolation != driver.IsolationLevel(0) || opts.ReadOnly {
		return nil, errors.New("querykill: driver does not support non-default transaction options")
	}
	return cn.inner.Begin() //nolint:staticcheck // fallback for drivers without BeginTx
}

func (cn *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	q, ok := cn.inner.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return cn.guard(ctx, func() (driver.Rows, error) { return q.QueryContext(ctx, query, args) })
}

func (cn *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if e, ok := cn.inner.(driver.ExecerContext); ok {
		return e.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (cn *conn) Ping(ctx context.Context) error {
	if p, ok := cn.inner.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

func (cn *conn) ResetSession(ctx context.Context) error {
	if cn.killed.Load() {
		return driver.ErrBadConn
	}
	if r, ok := cn.inner.(driver.SessionResetter); ok {
		return r.ResetSession(ctx)
	}
	return nil
}

func (cn *conn) IsValid() bool {
	if cn.killed.Load() {
		return false
	}
	if v, ok := cn.inner.(driver.Validator); ok {
		return v.IsValid()
	}
	return true
}

func (cn *conn) CheckNamedValue(nv *driver.NamedValue) error {
	if c, ok := cn.inner.(driver.NamedValueChecker); ok {
		return c.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}

func (cn *conn) kill() {
	cn.killed.Store(true)
	cn.c.killQuery(cn.thread)
}

// guard runs one read under watch. run is the wrapped driver's own call.
func (cn *conn) guard(ctx context.Context, run func() (driver.Rows, error)) (driver.Rows, error) {
	if !cn.hasID || ctx.Done() == nil {
		return run()
	}
	if err := ctx.Err(); err != nil {
		return nil, err // nothing sent, nothing to kill
	}
	st := &statement{cn: cn, ctx: ctx, stop: make(chan struct{}), exited: make(chan struct{})}
	go st.watch()
	rows, err := run()
	if errors.Is(err, driver.ErrSkip) || errors.Is(err, driver.ErrBadConn) {
		// The driver sent nothing: ErrSkip declines bind args without
		// client-side interpolation (database/sql retries as a prepared
		// statement, guarded on its own), and go-sql-driver returns
		// ErrBadConn only when nothing was written. A KILL now could only
		// hit an earlier statement still running on this thread, such as
		// an abandoned write inside the same transaction.
		st.end(true)
		return nil, err
	}
	if err != nil {
		st.end(false)
		return nil, err
	}
	return &rowsGuard{inner: rows, st: st}, nil
}

// statement is the watch over one running read.
type statement struct {
	cn  *conn
	ctx context.Context

	mu   sync.Mutex
	over bool // the statement ended, or its KILL was sent

	stopOnce sync.Once
	stop     chan struct{}
	exited   chan struct{}
}

func (st *statement) watch() {
	defer close(st.exited)
	select {
	case <-st.stop:
		return
	case <-st.ctx.Done():
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.over {
		return
	}
	st.over = true
	st.cn.kill()
}

// end marks the statement over. complete means the server is known to be done
// with it (its rows were closed cleanly, which drains them, or the driver declined to send it). Otherwise, with
// the context already ended, the server may still be running it (the driver
// closed the socket and returned) and end sends the KILL itself rather than
// leave it to a race with the watcher. Taking mu waits for a KILL the watcher
// has in flight: the connection stays ours until that is sent. end returns
// only after the watcher goroutine has exited.
func (st *statement) end(complete bool) {
	st.mu.Lock()
	if !st.over {
		st.over = true
		if !complete && st.ctx.Err() != nil {
			st.cn.kill()
		}
	}
	st.mu.Unlock()
	st.stopOnce.Do(func() { close(st.stop) })
	<-st.exited
}

// rowsGuard ends the statement's watch when its rows are done.
type rowsGuard struct {
	inner driver.Rows
	st    *statement
}

var (
	_ driver.Rows                           = (*rowsGuard)(nil)
	_ driver.RowsNextResultSet              = (*rowsGuard)(nil)
	_ driver.RowsColumnTypeScanType         = (*rowsGuard)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*rowsGuard)(nil)
	_ driver.RowsColumnTypeLength           = (*rowsGuard)(nil)
	_ driver.RowsColumnTypeNullable         = (*rowsGuard)(nil)
	_ driver.RowsColumnTypePrecisionScale   = (*rowsGuard)(nil)
)

func (r *rowsGuard) Columns() []string { return r.inner.Columns() }

func (r *rowsGuard) Next(dest []driver.Value) error { return r.inner.Next(dest) }

// Close drains through the wrapped rows first: the statement is still the
// server's until that returns, and a context that ends during the drain must
// still be able to kill it. A clean drain means the server is done.
func (r *rowsGuard) Close() error {
	err := r.inner.Close()
	r.st.end(err == nil)
	return err
}

func (r *rowsGuard) HasNextResultSet() bool {
	if n, ok := r.inner.(driver.RowsNextResultSet); ok {
		return n.HasNextResultSet()
	}
	return false
}

func (r *rowsGuard) NextResultSet() error {
	if n, ok := r.inner.(driver.RowsNextResultSet); ok {
		return n.NextResultSet()
	}
	return io.EOF
}

var anyType = reflect.TypeOf((*any)(nil)).Elem()

func (r *rowsGuard) ColumnTypeScanType(i int) reflect.Type {
	if t, ok := r.inner.(driver.RowsColumnTypeScanType); ok {
		return t.ColumnTypeScanType(i)
	}
	return anyType
}

func (r *rowsGuard) ColumnTypeDatabaseTypeName(i int) string {
	if t, ok := r.inner.(driver.RowsColumnTypeDatabaseTypeName); ok {
		return t.ColumnTypeDatabaseTypeName(i)
	}
	return ""
}

func (r *rowsGuard) ColumnTypeLength(i int) (int64, bool) {
	if t, ok := r.inner.(driver.RowsColumnTypeLength); ok {
		return t.ColumnTypeLength(i)
	}
	return 0, false
}

func (r *rowsGuard) ColumnTypeNullable(i int) (bool, bool) {
	if t, ok := r.inner.(driver.RowsColumnTypeNullable); ok {
		return t.ColumnTypeNullable(i)
	}
	return false, false
}

func (r *rowsGuard) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	if t, ok := r.inner.(driver.RowsColumnTypePrecisionScale); ok {
		return t.ColumnTypePrecisionScale(i)
	}
	return 0, 0, false
}

// stmt wraps a prepared statement. database/sql runs every read that has bind
// arguments through here (the driver declines them on the connection path
// unless interpolateParams is set), so this is the path the console's own
// filtered reads take.
type stmt struct {
	inner driver.Stmt
	cn    *conn
}

var (
	_ driver.Stmt              = (*stmt)(nil)
	_ driver.StmtQueryContext  = (*stmt)(nil)
	_ driver.StmtExecContext   = (*stmt)(nil)
	_ driver.NamedValueChecker = (*stmt)(nil)
	_ driver.ColumnConverter   = (*stmt)(nil)
)

func (s *stmt) Close() error  { return s.inner.Close() }
func (s *stmt) NumInput() int { return s.inner.NumInput() }

//nolint:staticcheck // driver.Stmt requires Exec.
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) { return s.inner.Exec(args) }

//nolint:staticcheck // driver.Stmt requires Query.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) { return s.inner.Query(args) }

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	q, ok := s.inner.(driver.StmtQueryContext)
	if !ok {
		vals, err := namedToValues(args)
		if err != nil {
			return nil, err
		}
		return s.cn.guard(ctx, func() (driver.Rows, error) { return s.inner.Query(vals) }) //nolint:staticcheck // fallback
	}
	return s.cn.guard(ctx, func() (driver.Rows, error) { return q.QueryContext(ctx, args) })
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if e, ok := s.inner.(driver.StmtExecContext); ok {
		return e.ExecContext(ctx, args)
	}
	vals, err := namedToValues(args)
	if err != nil {
		return nil, err
	}
	return s.inner.Exec(vals) //nolint:staticcheck // fallback
}

func (s *stmt) CheckNamedValue(nv *driver.NamedValue) error {
	if c, ok := s.inner.(driver.NamedValueChecker); ok {
		return c.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}

func (s *stmt) ColumnConverter(idx int) driver.ValueConverter {
	if c, ok := s.inner.(driver.ColumnConverter); ok { //nolint:staticcheck // forwarded as-is
		return c.ColumnConverter(idx)
	}
	return driver.DefaultParameterConverter
}

func namedToValues(args []driver.NamedValue) ([]driver.Value, error) {
	out := make([]driver.Value, len(args))
	for i, a := range args {
		if a.Name != "" {
			return nil, errors.New("querykill: driver does not support named parameters")
		}
		out[i] = a.Value
	}
	return out, nil
}

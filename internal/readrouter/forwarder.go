package readrouter

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
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
// The credentials are the operator's, not the client's: the server's
// forwarding account when the registry holds one, else its source DSN. The
// port authenticates on the console token, so a forwarded statement has that
// account's grants, the same way the copy has no grants at all.
//
// The connection is never replaced behind the client's back. Once it is lost
// (a network error, the source closing an idle connection, the query
// deadline), every later statement fails with CodeUpstreamLost until the
// client reconnects: a transparent reconnect would be a NEW session, with
// the transaction rolled back, the settings gone and the database reset,
// while the client believes nothing happened.
type Forwarder struct {
	dsn              string
	addr, user, pass string
	// tls is the DSN's own tls= setting, tlsInDSN whether it has one. A
	// DSN that sets it wins over ssl, as it does for capture's connections.
	tls      *tls.Config
	tlsInDSN bool
	// ssl is how the server's source connection uses TLS (the registry
	// entry's ssl_* fields): the same value capture connects with.
	ssl config.SSL
	// OnCleartext, when set, is called once the connection has been opened
	// WITHOUT encryption because the mode is "preferred" and the source
	// offers no TLS, with the error that proved it: the caller's chance to
	// say so in the log, as capture does for its own connection.
	OnCleartext func(error)
	// OnConnect, when set, is told how the one attempt to open the
	// connection ended: nil when it opened, else the error as the source or
	// the network gave it (before it is turned into CodeUpstreamLost for
	// the client). It is how a caller learns WHY a source could not be
	// reached, an account the source refuses among the reasons
	// (AccountRefused).
	OnConnect      func(error)
	policy         Policy
	connectTimeout time.Duration
	// queryTimeout bounds each round trip on the upstream socket (read and
	// write deadline), the same deadline the port applies to a copy
	// statement; a statement past it loses the connection.
	queryTimeout time.Duration

	mu   sync.Mutex
	db   string
	conn *client.Conn
	// raw is conn's socket (the TLS connection when it is encrypted), kept
	// so that another goroutine can interrupt a statement in flight by
	// closing IT. A net.Conn may be closed from any goroutine; the client
	// library's Conn may not (its Close writes packet state the statement's
	// goroutine is writing too). See interrupt.
	raw net.Conn
	// threadID is the source's id for this connection (CONNECTION_ID()),
	// from the handshake: what a KILL names.
	threadID uint32
	dead     error
}

// NewForwarder parses a go-sql-driver DSN (the registry's forwarding or
// source DSN) and
// returns a Forwarder that connects on first use. ssl is the TLS the server's
// source connection uses, the value capture connects to the same server
// with: the upstream connection is encrypted, or refused, or falls back to
// cleartext by the same rule (config.ConnectSSLWith). A setting that cannot
// be used (an unknown mode, an unreadable CA file) is an error here, before
// anything dials. queryTimeout is the deadline for each statement on the
// upstream connection; 0 means none.
func NewForwarder(sourceDSN string, ssl config.SSL, policy Policy, queryTimeout time.Duration) (*Forwarder, error) {
	cfg, err := drivermysql.ParseDSN(sourceDSN)
	if err != nil {
		return nil, fmt.Errorf("source DSN: %w", err)
	}
	if _, err := config.BuildTLSConfig(ssl.Mode, ssl.CA, ssl.Cert, ssl.Key, config.DSNHost(sourceDSN)); err != nil {
		var se *config.TLSSettingsError
		if errors.As(err, &se) {
			// Worded without the command-line flags: this value comes from
			// a registry entry's ssl_* fields.
			return nil, fmt.Errorf("source TLS settings: %s", se.Problem)
		}
		return nil, fmt.Errorf("source TLS settings: %w", err)
	}
	if cfg.Net != "tcp" && cfg.Net != "" {
		return nil, fmt.Errorf("source DSN: only tcp addresses are forwarded, not %q", cfg.Net)
	}
	addr := cfg.Addr
	if _, _, err := net.SplitHostPort(addr); err != nil {
		addr = net.JoinHostPort(addr, "3306")
	}
	dsnTLS, tlsInDSN := cfg.TLS, cfg.TLS != nil || cfg.TLSConfig != ""
	if strings.EqualFold(cfg.TLSConfig, "preferred") {
		// tls=preferred in the DSN is the MODE preferred, written in the
		// DSN: encrypted when the source offers TLS, and when it offers
		// none, the one fallback to an unencrypted connection, reported
		// through OnCleartext. The driver capture connects with does that
		// fallback itself for such a DSN; the client used here has none, so
		// taking the DSN's tls.Config as it stands would refuse a source
		// that capture reaches. Like every tls= in a DSN it wins over the
		// server's mode, so the mode's CA and certificate are not used.
		ssl = config.SSL{Mode: "preferred"}
		dsnTLS, tlsInDSN = nil, false
		plain := cfg.Clone()
		plain.TLSConfig, plain.TLS = "", nil
		sourceDSN = plain.FormatDSN()
	}
	return &Forwarder{
		dsn:  sourceDSN,
		addr: addr, user: cfg.User, pass: cfg.Passwd, db: cfg.DBName,
		tls:            dsnTLS,
		tlsInDSN:       tlsInDSN,
		ssl:            ssl,
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
	// TLS is decided by capture's own rule for this server
	// (config.ConnectSSLWith): the mode's tls.Config first; a retry in
	// cleartext only for "preferred" against a source that offers no TLS at
	// all; never for required or the verify modes.
	c, err := config.ConnectSSLWith(f.dsn, f.ssl, f.OnCleartext, func(_ string, modeTLS *tls.Config) (*client.Conn, error) {
		return client.ConnectWithContext(ctx, f.addr, f.user, f.pass, f.db, f.connectTimeout, func(c *client.Conn) error {
			c.ReadTimeout = f.queryTimeout
			c.WriteTimeout = f.queryTimeout
			switch {
			case f.tlsInDSN:
				// A tls= inside the DSN wins over the mode (tls=false
				// included), as it does for capture (config.applyTLS).
				if f.tls != nil {
					c.SetTLSConfig(f.tls)
				}
			case modeTLS != nil:
				c.SetTLSConfig(modeTLS)
			}
			return nil
		})
	})
	var se *config.TLSSettingsError
	if errors.As(err, &se) {
		// A setting that was usable when the forwarder was made and is not
		// now (a CA file removed since). This text reaches the port's
		// client and the console's pages: worded from the server's setting,
		// not from the command-line flag the settings error names.
		err = fmt.Errorf("this server's TLS settings cannot be used: %s", se.Problem)
	}
	if f.OnConnect != nil {
		f.OnConnect(err)
	}
	if err != nil {
		f.dead = lostError(fmt.Errorf("connect to the source: %w", err))
		return nil, f.dead
	}
	f.conn = c
	f.raw = c.Conn.Conn
	f.threadID = c.GetConnectionID()
	return c, nil
}

// lose records that the upstream connection is gone, closes it, and makes
// every later statement fail with CodeUpstreamLost naming the cause. It is
// called by the goroutine that runs the connection's statements, never by
// another one: see interrupt for that.
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

// lost returns the error every statement gets once the connection is gone,
// with the cause that was recorded first.
func (f *Forwarder) lost() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dead
}

// interrupt ends the upstream connection from ANOTHER goroutine than the one
// running its statements: it marks the connection lost and closes the socket
// under it, which makes a blocked read or write in the client library return
// with an error; the statement's goroutine then does the library's own close
// (lose, or Close). The library's Conn is not touched here: its Close resets
// the packet sequence, which a statement being written is updating at the
// same moment.
func (f *Forwarder) interrupt(cause error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dead == nil {
		f.dead = lostError(cause)
	}
	if f.raw != nil {
		_ = f.raw.Close()
	}
}

// ThreadID is the source's id for the upstream connection (what a KILL
// names), or 0 when none was opened.
func (f *Forwarder) ThreadID() uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.threadID
}

func lostError(cause error) error {
	return mysql.NewError(CodeUpstreamLost, fmt.Sprintf("MySQL server has gone away (the port's connection to the source was lost: %v); reconnect to continue", cause))
}

// AccountRefused reports whether err is the source turning the ACCOUNT away
// when the connection was opened (a wrong password, a host the account may
// not connect from, a locked or expired account, a database it may not use),
// as opposed to the source being unreachable. code is MySQL's error number.
func AccountRefused(err error) (code uint16, refused bool) {
	var me *mysql.MyError
	if !errors.As(err, &me) {
		return 0, false
	}
	switch me.Code {
	case mysql.ER_ACCESS_DENIED_ERROR, // 1045: wrong user or password
		mysql.ER_DBACCESS_DENIED_ERROR, // 1044: no access to the DSN's database
		mysql.ER_HOST_NOT_PRIVILEGED,   // 1130: host not allowed to connect
		mysql.ER_MUST_CHANGE_PASSWORD,  // 1820
		1698,                           // access denied (no password, or the auth plugin)
		1862,                           // the password has expired
		3118,                           // the account is locked (MySQL)
		4151:                           // the account is locked (MariaDB)
		return me.Code, true
	}
	return 0, false
}

// KillThreads ends, on the source, the connections with the given thread
// ids: one connection of its own, opened with dsn by the rule every
// Forwarder connects with (ssl), sends KILL for each. It is how the
// statements of client connections that were just dropped are stopped on the
// source, where they would otherwise run to their end. An account may kill
// its own threads, so dsn is the account those connections used.
//
// An id the source does not know is a connection that already ended: not an
// error. The error of a login the source refuses is the source's own (see
// AccountRefused); any other KILL the source would not do is reported after
// all of them were sent.
func KillThreads(ctx context.Context, dsn string, ssl config.SSL, ids []uint32, timeout time.Duration) error {
	if len(ids) == 0 {
		return nil
	}
	f, err := NewForwarder(dsn, ssl, Policy{}, timeout)
	if err != nil {
		return err
	}
	defer f.Close()
	var connectErr error
	f.OnConnect = func(err error) { connectErr = err }
	err = killEach(ids, func(q string) error {
		_, err := f.Forward(ctx, q, &BufferSink{})
		return err
	})
	if connectErr != nil {
		return connectErr
	}
	return err
}

// killEach sends one KILL per id through run and gathers what failed,
// leaving out the source's "no such thread".
func killEach(ids []uint32, run func(string) error) error {
	var failed []error
	for _, id := range ids {
		err := run(fmt.Sprintf("KILL %d", id))
		var me *mysql.MyError
		if err == nil || errors.As(err, &me) && me.Code == mysql.ER_NO_SUCH_THREAD {
			continue
		}
		failed = append(failed, fmt.Errorf("thread %d: %w", id, err))
		if IsLost(err) {
			break // nothing more can be sent on this connection
		}
	}
	return errors.Join(failed...)
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
// blocking read return. That happens on another goroutine than the
// statement's, hence interrupt and not lose. The returned stop must be called
// when the statement is done.
func (f *Forwarder) watch(ctx context.Context) func() bool {
	return context.AfterFunc(ctx, func() { f.interrupt(ctx.Err()) })
}

// Decide applies the policy to the statement: the one shape that needs no
// plan (Policy.Prejudge: SELECT ... FROM one table LIMIT a few) is decided
// here without touching the source; everything else runs EXPLAIN
// FORMAT=JSON on the source and goes through Policy.DecideStatement. A
// failure to EXPLAIN (a statement MySQL itself rejects, a lost connection)
// is returned as an error; the caller forwards the statement, so MySQL's
// own answer, error included, reaches the client.
func (f *Forwarder) Decide(ctx context.Context, stmt string) (Decision, error) {
	if d, ok := f.policy.Prejudge(stmt); ok {
		return d, nil
	}
	c, err := f.get(ctx)
	if err != nil {
		return Decision{}, err
	}
	defer f.watch(ctx)()
	res, err := c.Execute("EXPLAIN FORMAT=JSON " + stmt)
	return f.decideFromExplain(stmt, res, err)
}

// decideFromExplain reads the plan out of an EXPLAIN FORMAT=JSON answer and
// applies the policy.
func (f *Forwarder) decideFromExplain(stmt string, res *mysql.Result, err error) (Decision, error) {
	if err != nil {
		if !isMySQLError(err) {
			f.lose(err)
		}
		return Decision{}, fmt.Errorf("explain: %w", err)
	}
	plan, err := planFromExplain(res)
	if err != nil {
		return Decision{}, err
	}
	return f.policy.DecideStatement(stmt, plan), nil
}

// planFromExplain reads the plan out of an EXPLAIN FORMAT=JSON answer.
//
// The result is NOT closed. Close returns its Resultset to go-mysql's
// process-wide pool, and a pooled Resultset keeps its column definitions:
// BuildSimpleTextResultset reuses a non-nil Field it finds there instead of
// making one, so the next result built anywhere in this process (a copy
// answer, a time-travel answer, on any connection) would go out with its
// first column named EXPLAIN and typed as the plan was.
func planFromExplain(res *mysql.Result) (Plan, error) {
	if res == nil || res.Resultset == nil || res.RowNumber() == 0 {
		return Plan{}, errors.New("explain: no plan returned")
	}
	raw, err := res.GetString(0, 0)
	if err != nil {
		return Plan{}, fmt.Errorf("explain: %w", err)
	}
	return ParsePlan([]byte(raw))
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
	return f.stream(sink, func(res *mysql.Result, perRow client.SelectPerRowCallback, perRes client.SelectPerResultCallback) error {
		return c.ExecuteSelectStreaming(stmt, res, perRow, perRes)
	})
}

// stream runs one statement through run (the text or the prepared form of
// go-mysql's streaming read) and hands its rows to sink; see Forward for
// what comes back.
func (f *Forwarder) stream(sink RowSink, run func(*mysql.Result, client.SelectPerRowCallback, client.SelectPerResultCallback) error) (*mysql.Result, error) {
	var res mysql.Result
	var cells []any
	err := run(&res,
		func(row []mysql.FieldValue) error {
			if cap(cells) < len(row) {
				cells = make([]any, len(row))
			}
			cells = cells[:len(row)]
			for i := range row {
				cells[i] = row[i].Value()
				// An empty string arrives from go-mysql's row parser as a
				// nil byte slice, which a sink reads as NULL. The cell's
				// own type is what says it is a string.
				if b, isBytes := cells[i].([]byte); isBytes && b == nil && row[i].Type == mysql.FieldValueTypeString {
					cells[i] = []byte{}
				}
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
			return nil, unwrapMySQLError(err)
		default:
			// When the statement was interrupted (its context ended), the
			// error here is only the closed socket; the client is told
			// the first cause, the one interrupt recorded.
			f.lose(err)
			return nil, f.lost()
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

// SessionStatusFlags are the MySQL status flags that describe a session and
// outlive the statement that set them: autocommit, in a transaction, in a
// read-only transaction, NO_BACKSLASH_ESCAPES. The other flags are about one
// statement or one result.
const SessionStatusFlags = mysql.SERVER_STATUS_IN_TRANS | mysql.SERVER_STATUS_AUTOCOMMIT |
	mysql.SERVER_STATUS_NO_BACKSLASH_ESCAPED | mysql.SERVER_STATUS_IN_TRANS_READONLY

// Status is the state of the session on the source as the source last
// reported it, in MySQL's status flags (SessionStatusFlags, and no other
// flag). known is false while there is no session to speak of: before the
// connection is opened, and once it is lost.
//
// It is read from the connection, which keeps the status of the last packet
// the source sent that carried one: the OK that ended the login (so a source
// that opens its sessions with NO_BACKSLASH_ESCAPES, or with autocommit off,
// is reported from the first answer on), every statement's answer, the
// EXPLAIN of a decision and a USE. MySQL's error packet carries no status, so
// after a statement the source refused the flags are those of the packet
// before it, exactly what a client connected to the source would hold.
func (f *Forwarder) Status() (status uint16, known bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// A connection being interrupted (dead set, the socket closed under it)
	// is still held until its statement returns: its session is gone.
	if f.conn == nil || f.dead != nil {
		return 0, false
	}
	return connSessionStatus(f.conn), true
}

// connSessionStatus reads the session flags the client library holds for c.
// The library exposes autocommit and in-transaction as methods and the whole
// status only as text (StatusString: the flag's name, or "(<value>)" for one
// it has no name for), so the other two flags are read from that text.
// TestForwarder_Status pins both spellings against the library.
func connSessionStatus(c *client.Conn) (status uint16) {
	if c.IsAutoCommit() {
		status |= mysql.SERVER_STATUS_AUTOCOMMIT
	}
	if c.IsInTransaction() {
		status |= mysql.SERVER_STATUS_IN_TRANS
	}
	for flag := range strings.SplitSeq(c.StatusString(), "|") {
		switch flag {
		case "SERVER_STATUS_NO_BACKSLASH_ESCAPED":
			status |= mysql.SERVER_STATUS_NO_BACKSLASH_ESCAPED
		case "SERVER_STATUS_IN_TRANS_READONLY", fmt.Sprintf("(%d)", mysql.SERVER_STATUS_IN_TRANS_READONLY):
			status |= mysql.SERVER_STATUS_IN_TRANS_READONLY
		}
	}
	return status
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

// Stmt is a statement prepared on the source for one client connection: the
// source keeps it, binds the arguments itself and answers in the binary
// protocol, exactly as it would for a client connected to it. Nothing is
// rebuilt from text, so no argument is ever escaped here.
type Stmt interface {
	// Params and Columns are the counts the source answered PREPARE with;
	// ParamFields and ColumnFields its raw definitions, to relay as they are.
	Params() int
	Columns() int
	ParamFields() [][]byte
	ColumnFields() [][]byte
	// Decide applies the policy to this execution: the plan comes from
	// EXPLAIN on the same statement with the same arguments.
	Decide(ctx context.Context, args []any) (Decision, error)
	// Execute runs the statement on the source with args bound; rows and
	// the returned Result are as in Forwarder.Forward.
	Execute(ctx context.Context, args []any, sink RowSink) (*mysql.Result, error)
	// Close frees the statement on the source.
	Close()
}

// prepared is the Forwarder's Stmt.
type prepared struct {
	f     *Forwarder
	c     *client.Conn
	query string
	st    *client.Stmt
	// explain is the prepared EXPLAIN of the same statement, made on the
	// first execution that needs a plan; explainErr remembers that the
	// source would not prepare one (so it is not asked again).
	explain    *client.Stmt
	explainErr error
}

// Prepare prepares the statement on the source. MySQL's own refusal (a
// syntax error, a statement that cannot be prepared) is returned as is.
func (f *Forwarder) Prepare(ctx context.Context, query string) (Stmt, error) {
	c, err := f.get(ctx)
	if err != nil {
		return nil, err
	}
	defer f.watch(ctx)()
	st, err := c.Prepare(query)
	if err != nil {
		if isMySQLError(err) {
			return nil, unwrapMySQLError(err)
		}
		f.lose(err)
		return nil, lostError(err)
	}
	return &prepared{f: f, c: c, query: query, st: st}, nil
}

// unwrapMySQLError returns the *mysql.MyError inside err, so the port
// writes the source's code and message back rather than a wrapped 1105.
func unwrapMySQLError(err error) error {
	var me *mysql.MyError
	if errors.As(err, &me) {
		return me
	}
	return err
}

func (p *prepared) Params() int            { return p.st.ParamNum() }
func (p *prepared) Columns() int           { return p.st.ColumnNum() }
func (p *prepared) ParamFields() [][]byte  { return p.st.RawParamFields }
func (p *prepared) ColumnFields() [][]byte { return p.st.RawColumnFields }

// live returns the connection the statement was prepared on, or the error
// every statement gets once that connection is lost: a statement does not
// survive its connection.
func (p *prepared) live(ctx context.Context) error {
	c, err := p.f.get(ctx)
	if err != nil {
		return err
	}
	if c != p.c {
		return lostError(errors.New("the statement was prepared on a connection that is gone"))
	}
	return nil
}

func (p *prepared) Decide(ctx context.Context, args []any) (Decision, error) {
	if d, ok := p.f.policy.Prejudge(p.query); ok {
		return d, nil
	}
	if p.explainErr != nil {
		return Decision{}, p.explainErr
	}
	if err := p.live(ctx); err != nil {
		return Decision{}, err
	}
	defer p.f.watch(ctx)()
	if p.explain == nil {
		st, err := p.c.Prepare("EXPLAIN FORMAT=JSON " + p.query)
		if err != nil {
			if !isMySQLError(err) {
				p.f.lose(err)
				return Decision{}, fmt.Errorf("explain: %w", err)
			}
			p.explainErr = fmt.Errorf("explain: %w", err)
			return Decision{}, p.explainErr
		}
		p.explain = st
	}
	res, err := p.explain.Execute(args...)
	return p.f.decideFromExplain(p.query, res, err)
}

func (p *prepared) Execute(ctx context.Context, args []any, sink RowSink) (*mysql.Result, error) {
	if err := p.live(ctx); err != nil {
		return nil, err
	}
	defer p.f.watch(ctx)()
	return p.f.stream(sink, func(res *mysql.Result, perRow client.SelectPerRowCallback, perRes client.SelectPerResultCallback) error {
		return p.st.ExecuteSelectStreaming(res, perRow, perRes, args...)
	})
}

// Close frees the statement (and its EXPLAIN) on the source. On a lost
// connection there is nothing to free.
func (p *prepared) Close() {
	p.f.mu.Lock()
	alive := p.f.conn == p.c && p.f.dead == nil
	p.f.mu.Unlock()
	if !alive {
		return
	}
	for _, st := range []*client.Stmt{p.st, p.explain} {
		if st == nil {
			continue
		}
		if err := st.Close(); err != nil {
			p.f.lose(err)
			return
		}
	}
}

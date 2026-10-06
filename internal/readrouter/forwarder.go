package readrouter

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
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
	OnConnect func(error)
	// OnCollation, when set, is told that a new connection's collation
	// could not be settled (settleCollation): the collation the source gave
	// the session, and the source's refusal to change it. The connection
	// works; the copy does not answer on it.
	OnCollation func(collation string, err error)
	// OnSession, when set, is told the state the source opened the session
	// in (SessionStatusFlags), at most once: with the source's first answer
	// to a statement, because that is the first packet that shows what an
	// init_connect did (it runs after the login is answered). It is how a
	// caller learns that the source's sessions do not start as the port's
	// handshake announced. When the first statement is itself a SET of
	// autocommit or of sql_mode, the flag that statement sets is the
	// client's doing and is reported as the default (autocommit on, no
	// NO_BACKSLASH_ESCAPES); the other flag is still the source's.
	OnSession func(status uint16)
	// TrackSession asks the source to say, with each answer, that one of the
	// session settings the copy has to reproduce was changed
	// (SessionTrackedVariables), so that a change no reader of the
	// statement's text sees (a stored function that runs SET inside a
	// SELECT) is heard (#2127). Set before the first statement. See
	// trackSession for what it costs and when the source does not do it.
	TrackSession bool
	// OnUntracked, when set, is told that the source does not report session
	// changes on this connection though TrackSession asked for it, and why:
	// at most once when the connection opens, and once more if the session
	// stops reporting later (TrackSessionAgain). The connection works; a
	// setting changed inside a stored function is not seen on it.
	//
	// unusable says more: asking for tracking is what broke a connection to
	// this source (see open, lose). The caller should not ask on the next
	// connections to it (leave TrackSession unset); this one was opened
	// again without asking, or is lost.
	OnUntracked func(why string, unusable bool)
	// capabilities reads what the handshake settled on; a test's seam.
	capabilities   func(*client.Conn) string
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
	// sessionUntold: OnSession has not been told yet about this session.
	sessionUntold bool
	// hadSession: the source accepted the login at least once, so there was
	// a session (and maybe a transaction) to lose. See Lost.
	hadSession bool
	// tracked: the source reports changes to SessionTrackedVariables on this
	// session (trackSession).
	tracked bool
	// asked: the connection that is open asked its handshake for session
	// tracking (it was not opened again without asking: see get).
	asked bool
	// sessionChanged: since TakeSessionChanged was last called, an answer
	// from the source said a tracked setting changed, or the source answered
	// a tracked session with an error (which says nothing either way). See
	// heard.
	sessionChanged bool
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
	c, tracked, err := f.open(ctx, f.TrackSession)
	f.asked = f.TrackSession
	if err != nil && f.TrackSession && protocolError(err) && ctx.Err() == nil {
		// The source and this connection do not speak the same protocol
		// once session tracking is asked for (see open). One more attempt,
		// without asking: what worked before tracking existed still works.
		// When the second attempt fails too, tracking was not the cause, and
		// its error is the one reported.
		first := err
		f.asked = false
		if c, _, err = f.open(ctx, false); err == nil {
			f.unusable(first)
		}
	}
	f.tracked = tracked && err == nil
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
	f.sessionUntold = f.OnSession != nil
	f.hadSession = true
	return c, nil
}

// open makes one attempt at the upstream connection: the login, the
// collation (settleCollation) and, when track is set, session tracking
// (trackSession). It reports whether the session is tracked. Called with f.mu
// held.
//
// Asking for session tracking changes what the client library reads: from
// the answer to the login on, it decodes the session-state data of every OK
// packet, and it returns an error for data it does not understand (a tracker
// type it does not know, a length that does not add up). A source, or
// something between the port and it (a proxy, a router), that agrees to
// CLIENT_SESSION_TRACK and then writes such data would fail every connection
// of the port where it worked before tracking was asked for. get tells that
// case from the others by the error (protocolError) and opens the connection
// once more without asking.
func (f *Forwarder) open(ctx context.Context, track bool) (*client.Conn, bool, error) {
	// TLS is decided by capture's own rule for this server
	// (config.ConnectSSLWith): the mode's tls.Config first; a retry in
	// cleartext only for "preferred" against a source that offers no TLS at
	// all; never for required or the verify modes.
	c, err := config.ConnectSSLWith(f.dsn, f.ssl, f.OnCleartext, func(_ string, modeTLS *tls.Config) (*client.Conn, error) {
		return client.ConnectWithContext(ctx, f.addr, f.user, f.pass, f.db, f.connectTimeout, func(c *client.Conn) error {
			c.ReadTimeout = f.queryTimeout
			c.WriteTimeout = f.queryTimeout
			if track {
				// Only asked for: what the source agreed to is read after
				// the handshake (trackSession). The one error this returns
				// is for a capability the library does not know.
				_ = c.SetCapability(mysql.CLIENT_SESSION_TRACK)
			}
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
	if err == nil {
		// Part of opening the connection: one that breaks here did not open.
		if serr := f.settleCollation(ctx, c); serr != nil {
			_ = c.Close()
			err = fmt.Errorf("settle the session's collation: %w", serr)
		}
	}
	tracked := false
	if err == nil && track {
		var terr error
		if tracked, terr = f.trackSession(ctx, c); terr != nil {
			_ = c.Close()
			err = fmt.Errorf("ask the source to report session changes: %w", terr)
		}
	}
	if err != nil {
		return nil, false, err
	}
	return c, tracked, nil
}

// lose records that the upstream connection is gone, closes it, and makes
// every later statement fail with CodeUpstreamLost naming the cause. It is
// called by the goroutine that runs the connection's statements, never by
// another one: see interrupt for that.
// settledCollation is what settleCollation names on a connection whose
// source did not know the collation the handshake asked for. Both MySQL and
// MariaDB have it, and it compares text the way the copy does.
const settledCollation = "utf8mb4_unicode_ci"

// settleCollation runs on a connection the source has just accepted, before
// anything of the client's. The handshake asks for utf8mb4_0900_ai_ci
// (mysql.DEFAULT_COLLATION_NAME), which is MySQL's own. A MariaDB that does
// not have it (10.11) does not refuse: it gives the session its default for
// utf8mb4, utf8mb4_general_ci, a collation no client asked for and under
// which the copy cannot give MySQL's answers, so the copy would never answer
// on a default connection. Such a session is given utf8mb4_unicode_ci
// instead.
//
// What decides is the collation the source says the session has, never a
// version number. Whether to ask at all is read from the greeting: only a
// source that greets as MariaDB is asked, since MySQL has had the collation
// for as long as this port supports it. MySQL pays nothing, MariaDB one
// statement per connection, and a MariaDB that did not know the collation a
// second one. A MariaDB behind a proxy that greets as MySQL is not asked; its
// session stays as the source gave it and the copy does not answer there.
//
// These are the port's statements, not the client's. They go around Forward,
// so the session's first-statement report (OnSession) still waits for the
// client's own, and the read-only mode, which judges the client's
// statements, does not see them. They leave no warning; what FOUND_ROWS()
// and ROW_COUNT() say before the client's first statement is the one thing
// of theirs a client could read. A client that later sets a collation of its
// own gets it.
//
// A source that answers either statement with an error and keeps the
// session (stillInSession) leaves the connection as it is: the client still
// gets its answers from MySQL, and OnCollation is told. Anything else (a
// broken connection, a session the source ended, a client that left: ctx)
// is a connection that did not open.
//
// The result is not closed: that would hand it back to go-mysql's pool with
// its column still set, for the next resultset built in this process to
// inherit (see planFromExplain).
func (f *Forwarder) settleCollation(ctx context.Context, c *client.Conn) error {
	if !strings.Contains(strings.ToLower(c.GetServerVersion()), "mariadb") {
		return nil
	}
	// f.mu is held and f.raw is not set yet, so watch cannot reach this
	// connection: a client that leaves closes it here.
	defer context.AfterFunc(ctx, func() { _ = c.Conn.Conn.Close() })()
	refused := func(collation string, err error) error {
		if !stillInSession(err) {
			if cause := ctx.Err(); cause != nil {
				return cause
			}
			return err
		}
		if f.OnCollation != nil {
			f.OnCollation(collation, err)
		}
		return nil
	}
	res, err := c.Execute("SELECT @@collation_connection")
	if err != nil {
		return refused("", err)
	}
	cell, err := res.GetString(0, 0)
	if err != nil {
		return refused("", &mysql.MyError{Code: mysql.ER_UNKNOWN_ERROR, Message: "the source did not say the session's collation: " + err.Error()})
	}
	got := strings.Clone(cell) // cell is a view of the packet's bytes
	if strings.EqualFold(got, mysql.DEFAULT_COLLATION_NAME) {
		return nil
	}
	if _, err := c.Execute("SET NAMES utf8mb4 COLLATE " + settledCollation); err != nil {
		return refused(got, err)
	}
	return nil
}

// SessionTrackedVariables is what the source is asked to report changes of
// (session_track_system_variables): every session variable the port reads
// back before the copy answers (shim's sessionReadBackSQL; a test there holds
// the two together), and three more.
//
// collation_connection, which the read-back reads by what it does (its
// collation probe). max_join_size, because setting it is one of the ways
// sql_big_selects changes. session_track_system_variables itself, so that
// the SET that asks for the list is itself a change the source reports: its
// answer proves that the source's marks reach this connection (trackSession).
//
// A client can replace the list (a connector that asks for the variables it
// follows). The port reads the list with the session and, when a name of its
// own is gone (TracksSession), adds its names back (TrackSessionAgain).
//
// Measured on MySQL 8.0 and 8.4 and MariaDB 10.11, 11.4, 11.8 and 12.3: each
// of these, set inside a stored function a SELECT calls, is reported with
// that SELECT's answer; so is one set to the value it already had. All of
// them exist on every one of those servers, which matters: MariaDB refuses
// the whole list for one name it does not know (MySQL only warns).
const SessionTrackedVariables = "time_zone,sql_mode,sql_select_limit,lc_time_names,div_precision_increment,sql_auto_is_null," +
	"sql_big_selects,max_join_size,character_set_results,character_set_connection,collation_connection,session_track_system_variables"

// trackSessionMark is the entry of SessionTrackedVariables that makes the SET
// of the list a change the source reports (trackSession).
const trackSessionMark = "session_track_system_variables"

// trackSessionAliases are the names of SessionTrackedVariables that MariaDB
// 11.4 and later leave out when they print the list back, while still
// reporting their changes (measured): there they are other variables' names.
var trackSessionAliases = map[string]bool{"collation_connection": true, "sql_big_selects": true}

// TracksSession reports whether a session whose session_track_system_variables
// is list still reports everything the port asked for: every name of
// SessionTrackedVariables is in it (in any order and case, as MariaDB prints
// the list sorted), but for the two a server may leave out of what it prints
// (trackSessionAliases); or the list is "*", which is everything. A list that
// kept the port's mark and lost a setting is not that: a change to the
// setting would not be reported.
func TracksSession(list string) bool {
	has := map[string]bool{}
	for name := range strings.SplitSeq(list, ",") {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "*" {
			return true
		}
		has[name] = true
	}
	for name := range strings.SplitSeq(SessionTrackedVariables, ",") {
		if !has[name] && !trackSessionAliases[name] {
			return false
		}
	}
	return true
}

// trackSessionSQL sets the session's tracked list to SessionTrackedVariables
// added to what the session tracks now (has), each name once: MySQL refuses a
// list that names a variable twice.
func trackSessionSQL(has string) string {
	var names []string
	seen := map[string]bool{}
	for _, list := range []string{has, SessionTrackedVariables} {
		for name := range strings.SplitSeq(list, ",") {
			name = strings.TrimSpace(name)
			if key := strings.ToLower(name); name != "" && !seen[key] && !strings.ContainsAny(name, "'\\") {
				seen[key] = true
				names = append(names, name)
			}
		}
	}
	return "SET SESSION session_track_system_variables = '" + strings.Join(names, ",") + "'"
}

// trackSession runs on a connection the source has just accepted, after
// settleCollation and before anything of the client's, when TrackSession is
// set. It asks the source to report changes to SessionTrackedVariables: one
// statement per connection, on every source. From then on the source marks
// the answer to any statement that changed one of them (the
// SERVER_SESSION_STATE_CHANGED status flag, on a packet it sends anyway), and
// a statement that changed nothing costs nothing more.
//
// It reports whether the session is tracked. It is not, and the connection
// opens all the same (OnUntracked is told why), when:
//
//   - the source did not agree to CLIENT_SESSION_TRACK in the handshake (a
//     server or a proxy without session tracking), or to CLIENT_DEPRECATE_EOF.
//     Without the second, a resultset ends in an EOF packet, and MySQL does
//     not mark that one: it keeps the change for the next OK packet, which
//     may be many statements later (measured on 8.0 and 8.4; MariaDB marks
//     the EOF too);
//   - the source refuses the SET and keeps the session;
//   - the source takes the SET and does not mark its answer. The list names
//     itself (trackSessionMark), so a source that tracks reports the SET as
//     a change to a tracked setting, in the very answer to it (measured on
//     all six). An answer without the mark is from a source whose marks do
//     not reach this connection, a proxy that answers the SET itself or
//     drops the flag among them: every connection proves the whole path
//     once, with the statement it sends anyway.
//
// Anything else (a broken connection, a client that left: ctx) is a
// connection that did not open, as in settleCollation. No privilege is
// needed: an account granted SELECT, INSERT and EXECUTE on one schema sets
// it (measured on all six).
//
// Like settleCollation's, the statement goes around Forward.
func (f *Forwarder) trackSession(ctx context.Context, c *client.Conn) (bool, error) {
	capabilities := f.capabilities
	if capabilities == nil {
		capabilities = (*client.Conn).CapabilityString
	}
	agreed := "|" + capabilities(c) + "|"
	for _, need := range []string{"CLIENT_SESSION_TRACK", "CLIENT_DEPRECATE_EOF"} {
		if !strings.Contains(agreed, "|"+need+"|") {
			f.untracked("the source did not agree to " + need + " when the connection opened")
			return false, nil
		}
	}
	// f.mu is held and f.raw is not set yet: see settleCollation.
	defer context.AfterFunc(ctx, func() { _ = c.Conn.Conn.Close() })()
	res, err := c.Execute(trackSessionSQL(""))
	if err != nil {
		if !stillInSession(err) {
			if cause := ctx.Err(); cause != nil {
				return false, cause
			}
			return false, err
		}
		f.untracked("the source refused to: " + err.Error())
		return false, nil
	}
	if !markedChanged(res) {
		f.untracked(notMarked)
		return false, nil
	}
	return true, nil
}

// notMarked is why a source that took the tracked list is not tracked.
const notMarked = "the source took the list of settings to report and did not report that change itself"

// markedChanged reports whether the source marked an answer as having
// changed a tracked setting.
func markedChanged(res *mysql.Result) bool {
	return res != nil && res.Status&mysql.SERVER_SESSION_STATE_CHANGED != 0
}

// untracked tells OnUntracked. Called with f.mu held.
func (f *Forwarder) untracked(why string) {
	if f.OnUntracked != nil {
		f.OnUntracked(why, false)
	}
}

// unusable tells OnUntracked that asking this source for session tracking
// broke a connection, with the error that showed it. Called with f.mu held.
func (f *Forwarder) unusable(cause error) {
	if f.OnUntracked != nil {
		f.OnUntracked("with session tracking asked for, the source's answers could not be read: "+cause.Error(), true)
	}
}

// protocolError reports whether err says the two ends of the connection do
// not speak the same protocol: an error the client library made up while
// reading a packet. It is not an error packet from the source (a refused
// login, a refused statement), not a connection that could not be made or
// that broke (a dial error, a timeout, a closed socket, an end of file), not
// a client that left (the context), and not a TLS setting.
func protocolError(err error) bool {
	if err == nil || isMySQLError(err) {
		return false
	}
	for _, not := range []error{mysql.ErrBadConn, io.EOF, io.ErrUnexpectedEOF, net.ErrClosed, context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, not) {
			return false
		}
	}
	var ne net.Error
	var se *config.TLSSettingsError
	return !errors.As(err, &ne) && !errors.As(err, &se)
}

// SessionTracked reports whether the source says, with its answers, that a
// setting of SessionTrackedVariables changed on this session. False before
// the connection is opened.
func (f *Forwarder) SessionTracked() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tracked && f.conn != nil
}

// TrackSessionAgain puts SessionTrackedVariables back in the session's
// tracked list, keeping what the list has now (has, as the source printed
// it): for a session whose list a statement of the client replaced
// (TracksSession). One statement, and it never opens a connection. When the
// source refuses it and keeps the session, or takes it without marking its
// answer (trackSession), the session is no longer tracked (SessionTracked,
// OnUntracked) and an error is returned; any other failure loses the
// connection.
func (f *Forwarder) TrackSessionAgain(ctx context.Context, has string) error {
	f.mu.Lock()
	if f.dead != nil {
		f.mu.Unlock()
		return f.dead
	}
	c := f.conn
	f.mu.Unlock()
	if c == nil {
		return nil
	}
	defer f.watch(ctx)()
	res, err := c.Execute(trackSessionSQL(has))
	if err != nil && !stillInSession(err) {
		f.lose(err)
		return f.lost()
	}
	why := ""
	switch {
	case err != nil:
		why = "the source refused to report them again after the session's list was replaced: " + err.Error()
	case !markedChanged(res):
		why, err = "after the session's list was replaced, "+notMarked, errors.New(notMarked)
	default:
		return nil
	}
	f.mu.Lock()
	f.tracked = false
	f.untracked(why)
	f.mu.Unlock()
	return err
}

// heard takes in one answer from the source: its status flags, or the error
// it answered with. It is how a change to the session reaches
// TakeSessionChanged.
//
//   - A status with SERVER_SESSION_STATE_CHANGED: on a tracked session, the
//     statement changed a tracked setting. It is the flag that is read, never
//     the list of changes: for a resultset the library keeps the status of
//     the closing packet and drops the rest of it.
//   - An error packet carries no status, so it cannot say. A statement that
//     changed a setting and then failed (a function that runs SET, then
//     SIGNAL) is reported by MySQL with the NEXT answer, whatever statement
//     that is, and by MariaDB never (measured). So on a tracked session an
//     error counts as a change. On one that is not tracked it does not: a
//     function that succeeds is not seen there either, and that connection
//     behaves as it did before tracking was asked for.
//
// The answer to every statement goes through here (Forward, a prepared
// statement's execution), and so does the EXPLAIN of a decision: MariaDB
// runs a deterministic function with constant arguments while it plans, and
// MySQL a scalar subquery with an aggregate (measured), so an EXPLAIN can
// change the session as its statement would. The answers that run nothing of
// the client's do not: a PREPARE (measured: neither server runs a function
// while it prepares), a USE, a PING, and the port's own SETs.
func (f *Forwarder) heard(status uint16, err error) {
	marked, failed := status&mysql.SERVER_SESSION_STATE_CHANGED != 0, isMySQLError(err)
	if !marked && !failed {
		return
	}
	f.mu.Lock()
	if marked || f.tracked {
		f.sessionChanged = true
	}
	f.mu.Unlock()
}

// TakeSessionChanged reports whether, since it was last called, the source
// said that a tracked session setting changed, or (on a tracked session)
// answered a statement with an error; and forgets it. A caller that reads the session back afterwards
// calls it once more when that is done: the read-back saw everything
// reported up to its own answer.
func (f *Forwarder) TakeSessionChanged() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	changed := f.sessionChanged
	f.sessionChanged = false
	return changed
}

func (f *Forwarder) lose(cause error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.asked && protocolError(cause) {
		// A packet the library could not read, on a connection that asked
		// for session tracking. It cannot be put right in place: the
		// connection is lost like any other. The caller is told, so that
		// the client's next connection does not ask.
		f.unusable(cause)
	}
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
		if !stillInSession(err) {
			f.lose(err)
		}
		f.heard(0, err)
		return Decision{}, fmt.Errorf("explain: %w", err)
	}
	if res != nil {
		f.heard(res.Status, nil)
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
	res, err := f.stream(sink, func(res *mysql.Result, perRow client.SelectPerRowCallback, perRes client.SelectPerResultCallback) error {
		return c.ExecuteSelectStreaming(stmt, res, perRow, perRes)
	})
	if err == nil {
		f.tellSession(stmt)
	}
	return res, err
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
		case stillInSession(err):
			// The source answered with an error packet (before or between
			// rows); the connection is in sync and stays usable. An error
			// packet that ends the session (sessionEnded) falls through to
			// the loss below instead.
			f.heard(0, err)
			return nil, unwrapMySQLError(err)
		default:
			// When the statement was interrupted (its context ended), the
			// error here is only the closed socket; the client is told
			// the first cause, the one interrupt recorded.
			f.lose(err)
			return nil, f.lost()
		}
	}
	f.heard(res.Status, nil)
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
		if !stillInSession(err) {
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
// connection is opened, and once it is lost and let go of.
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
	// A connection interrupted from another goroutine (dead set, the socket
	// closed under it) is still held here until its statement's goroutine
	// lets go of it, and still answers: when the interrupt lands just as a
	// statement succeeded, that statement's answer must carry the flags the
	// source sent with it, not those of a session that never ran anything.
	// Every command AFTER it is refused (Lost), so nothing else is stamped
	// from a session that is gone.
	if f.conn == nil {
		return 0, false
	}
	return connSessionStatus(f.conn), true
}

// tellSession reports the session's opening state to OnSession after the
// source's first answer, to the statement stmt (see OnSession).
func (f *Forwarder) tellSession(stmt string) {
	f.mu.Lock()
	if !f.sessionUntold || f.conn == nil {
		f.mu.Unlock()
		return
	}
	f.sessionUntold = false
	status := connSessionStatus(f.conn)
	f.mu.Unlock()
	if Classify(stmt) == KindSet {
		low := strings.ToLower(stmt)
		if strings.Contains(low, "autocommit") {
			status |= mysql.SERVER_STATUS_AUTOCOMMIT
		}
		if strings.Contains(low, "sql_mode") {
			status &^= mysql.SERVER_STATUS_NO_BACKSLASH_ESCAPED
		}
	}
	f.OnSession(status)
}

// Lost returns the error every command gets once a session this connection
// had on the source is gone (CodeUpstreamLost), nil while it is not. It turns
// non-nil the moment the connection is interrupted, before the statement in
// flight has returned.
//
// A source that never let this connection in (unreachable, the login
// refused) is not that case and answers nil: no session existed, so there is
// no transaction the client could wrongly believe it still has, and what the
// port answers without the source (time travel) must keep working while the
// source is down. Forwarded statements and Ping still answer the lost error
// there.
func (f *Forwarder) Lost() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.hadSession {
		return nil
	}
	return f.dead
}

// Ping sends COM_PING to the source on the session this connection holds, so
// the answer tells whether that session is still there and carries its state
// as it is now (the source's OK updates what Status reports). It never opens
// a connection: with none opened yet there is no session to ask about, and
// nil is returned without anything being sent. A lost connection answers the
// lost error; a ping that fails loses it. The round trip is bounded like a
// statement's (the query deadline, and ctx).
func (f *Forwarder) Ping(ctx context.Context) error {
	f.mu.Lock()
	if f.dead != nil {
		f.mu.Unlock()
		return f.dead
	}
	c := f.conn
	f.mu.Unlock()
	if c == nil {
		return nil
	}
	defer f.watch(ctx)()
	if err := c.Ping(); err != nil {
		// Any error: a server whose session is alive never refuses a PING
		// (what MySQL answers with after an idle timeout is an error
		// packet, and the session is gone with it).
		f.lose(err)
		return f.lost()
	}
	return nil
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

// stillInSession reports whether err is an error packet from a source whose
// session goes on: the statement failed, the connection is in sync and stays
// usable. An error packet that says the source ENDED the session is not
// that (sessionEnded), and neither is anything that is not an error packet.
func stillInSession(err error) bool { return isMySQLError(err) && !sessionEnded(err) }

// codeIdleTimeout is MySQL's ER_CLIENT_INTERACTION_TIMEOUT (8.0.24 and
// later): the answer to the first command sent after wait_timeout, followed
// by the socket closing.
const codeIdleTimeout = 4031

// sessionEnded reports whether err is the source saying it has ended this
// session. The number alone is not trusted: MariaDB numbers its own errors
// from 4000 up, so 4031 can be an unrelated error there, and MySQL's wording
// is matched too. A server that closes an idle connection without any answer
// needs nothing here: the next read sees a broken socket, which is a loss.
func sessionEnded(err error) bool {
	var me *mysql.MyError
	return errors.As(err, &me) && me.Code == codeIdleTimeout && strings.Contains(me.Message, "disconnected by the server")
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
		if stillInSession(err) {
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
			if !stillInSession(err) {
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
	res, err := p.f.stream(sink, func(res *mysql.Result, perRow client.SelectPerRowCallback, perRes client.SelectPerResultCallback) error {
		return p.st.ExecuteSelectStreaming(res, perRow, perRes, args...)
	})
	if err == nil {
		p.f.tellSession(p.query)
	}
	return res, err
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

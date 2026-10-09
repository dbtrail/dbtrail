package shim

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/readrouter"
)

// The status flags the port puts on the wire (#2110).
//
// MySQL sends two bytes of status with its handshake and with every OK and
// EOF packet: whether the session is in autocommit mode, whether it is inside
// a transaction, and a few more. Drivers act on them. PyMySQL sends
// SET AUTOCOMMIT only when the last status says the mode differs from the one
// it wants; Connector/J skips a COMMIT the flags say is not needed
// (useLocalTransactionState) and reads NO_BACKSLASH_ESCAPES to decide how to
// escape a string; the C library's mysql_real_escape_string does the same. A
// flag that is wrong is therefore not cosmetic: the port used to announce
// status 0 in its handshake, PyMySQL took that for "autocommit is off" and
// never turned it off on the source, and a rollback() then undid nothing.
//
// The rule: the flags describe the session the client really has.
//
//   - Under read routing that is the session on the source, as the source
//     last reported it (Router.Status), whoever answered the statement: a
//     SELECT the copy served inside a transaction on the source still says
//     "in a transaction".
//   - On a connection with no router there is no transaction to be in:
//     autocommit, always.
//   - The handshake and the OK that ends authentication are written before
//     the port knows which server the client wants, so they announce what a
//     new MySQL session is: autocommit, no transaction. (A source configured
//     to open sessions otherwise, with autocommit off or with
//     NO_BACKSLASH_ESCAPES in its sql_mode, is the case they cannot cover;
//     the first answer after the port opens its connection to the source
//     corrects the flags. See docs/time-travel-sql.md.)
//
// Session writes them (stampStatus), once per command, so no answer leaves
// with the flags of an earlier one.

// sessionStatusFlags are the flags that describe the session and outlive the
// statement that set them.
const sessionStatusFlags = readrouter.SessionStatusFlags

// statementStatusFlags are the flags about the statement just run that the
// port passes on from the source's answer. Every other flag promises the
// client something this port does not deliver, and is dropped:
// MORE_RESULTS_EXISTS and PS_OUT_PARAMS (another result set follows: the port
// relays one), CURSOR_EXISTS and LAST_ROW_SENT (a cursor: the port refuses
// them), SESSION_STATE_CHANGED (session-state data in the OK packet: the
// port relays none) and METADATA_CHANGED (the client may reuse cached column
// definitions: the port always sends them).
const statementStatusFlags = mysql.SERVER_STATUS_NO_GOOD_INDEX_USED | mysql.SERVER_STATUS_NO_INDEX_USED |
	mysql.SERVER_STATUS_DB_DROPPED | mysql.SERVER_QUERY_WAS_SLOW

// newSessionStatus is what a session that has run nothing reports.
const newSessionStatus = mysql.SERVER_STATUS_AUTOCOMMIT

// sessionStatuser is the part of a Handler the Session asks for the flags;
// the console's routing proxy implements it by asking the handler it bound.
type sessionStatuser interface{ SessionStatus() uint16 }

// sourceSession is the part of a Handler the Session asks about the session
// on the source that a routed connection holds; the console's routing proxy
// implements it the same way. A handler without it has no such session.
type sourceSession interface {
	// SourceLost is the error to answer a command with when the connection
	// to the source is gone, nil when it is not (or there is none to lose).
	// statement says the command is a statement the client sent, which is
	// counted as one nobody answered.
	SourceLost(statement bool) error
	// PingSource answers COM_PING.
	PingSource() error
}

// SourceLost: once a routed connection has lost the session it had on the
// source, every command on it is answered with that loss (error 2006), the ones the
// port would answer itself included: a time-travel statement, SHOW WARNINGS,
// USE, PING. The client's session on the source is gone, and its transaction
// with it; an OK from the port, which can only say "autocommit, no
// transaction" then, would tell a driver that follows the flags that there
// is nothing to commit, and a pool that validates with PING that the
// connection is healthy. The client must reconnect, and 2006 is what makes
// every driver do so.
//
// A source that never let the connection in is not that case (Router.Lost):
// forwarded statements answer 2006, and time travel keeps working, which is
// what the port is for when the source is down.
func (h *Handler) SourceLost(statement bool) error {
	if h.router == nil {
		return nil
	}
	err := h.router.Lost()
	if err == nil {
		return nil
	}
	if statement {
		h.observeRoute(RouteMySQL, RouteReasonUpstreamLost)
	}
	h.routeWarn("lost", "read routing: the connection to the source was lost; this client connection answers 2006 until it reconnects", err)
	return err
}

// PingSource answers COM_PING. On a routed connection with a session on the
// source the PING is the source's to answer: only the source knows whether
// that session is still there, and its OK carries the session's state as it
// is now, where the port only remembers the last status it was sent (an
// INSERT refused with autocommit off leaves a transaction open that no
// packet announced). Before the first forwarded statement there is no session
// on the source, nothing is sent and none is opened: the answer is the
// port's own OK, "autocommit, no transaction", which is what a client that
// has run nothing holds. Without a router the port answers.
func (h *Handler) PingSource() error {
	if h.router == nil {
		return nil
	}
	ctx, cancel := h.queryContext()
	defer cancel()
	return h.router.Ping(ctx)
}

// SessionStatus is the state of the client's session in MySQL's status flags:
// the source session's under read routing, as the source last reported it,
// and "autocommit, no transaction" otherwise (no router, or no session on the
// source yet, or the one there was is lost and its transaction with it).
func (h *Handler) SessionStatus() uint16 {
	if h.router != nil {
		if status, known := h.router.Status(); known {
			return status & sessionStatusFlags
		}
	}
	return newSessionStatus
}

// stampStatus puts the session's flags on the connection, which go-mysql
// reads for every EOF and ORs into every OK, and on the answer v when it is a
// result: the session's flags replace whatever the handler left there, and
// the flags about the statement that the source sent are kept.
func (s *Session) stampStatus(v any) {
	status := newSessionStatus
	if h, ok := s.h.(sessionStatuser); ok {
		status = h.SessionStatus() & sessionStatusFlags
	}
	if r, ok := v.(*mysql.Result); ok && r != nil {
		r.Status = status | r.Status&statementStatusFlags
		status = r.Status
	}
	s.conn.UnsetStatus(^uint16(0))
	s.conn.SetStatus(status)
}

// NewConn runs the server side of the MySQL handshake on conn and returns the
// connection, as server.Server.NewCustomizedConn does, with one difference:
// the handshake and the OK that ends authentication announce autocommit, as
// MySQL and MariaDB do, instead of status 0.
//
// go-mysql writes both from the connection's status, creates the connection
// inside NewCustomizedConn and writes the handshake before anything can set
// that status, so there is no option to pass. The OK is fixed through the
// library's own hook (OnAuthSuccess runs before it is written). The handshake
// has no hook: its two status bytes are set in the packet as it is written
// (handshakeConn). TestStatus_handshakeAnnouncesAutocommit pins the result
// against the library, so an upgrade that changes how the handshake is
// written fails there and not in a driver; a handshake that is not
// recognised at run time is logged.
func NewConn(conn net.Conn, srv *server.Server, auth server.AuthenticationHandler, h server.Handler) (*server.Conn, error) {
	return srv.NewCustomizedConn(&handshakeConn{Conn: conn}, announceStatus{auth}, h)
}

// announceStatus sets the new session's status on the connection once the
// client is authenticated, before the library writes its OK.
type announceStatus struct{ server.AuthenticationHandler }

func (a announceStatus) OnAuthSuccess(c *server.Conn) error {
	c.SetStatus(newSessionStatus)
	return a.AuthenticationHandler.OnAuthSuccess(c)
}

// handshakeConn sets the status announced by the server's handshake, the
// first packet written on the connection (before any TLS upgrade). Every
// later write passes through untouched.
//
// It also gives every write a time limit (portWriteTimeout): the library
// sets none on a server connection.
type handshakeConn struct {
	net.Conn
	written bool
	// stalled says the write timeout was already logged for this connection.
	stalled bool
}

// portWriteTimeout is how long one write to a client may block, in
// nanoseconds: what MySQL calls net_write_timeout, and the 60 seconds this
// port announces for it (sysvars.go). A write blocks only while the client
// reads nothing, so a client that takes long over a large answer is not cut
// as long as it keeps reading; one that asked and stopped reading is, and
// its connection ends, which gives back the result the port held for it
// (ResultBudget, #2241). An atomic so a test can lower it.
var portWriteTimeout atomic.Int64

func init() { portWriteTimeout.Store(int64(60 * time.Second)) }

func (c *handshakeConn) Write(p []byte) (int, error) {
	if d := time.Duration(portWriteTimeout.Load()); d > 0 {
		_ = c.Conn.SetWriteDeadline(time.Now().Add(d))
	}
	if c.written {
		n, err := c.Conn.Write(p)
		var ne net.Error
		if err != nil && errors.As(err, &ne) && ne.Timeout() && !c.stalled {
			c.stalled = true
			slog.Warn("mysql port: a client read nothing of its answer for the whole write timeout; its connection is closed",
				"remote", c.RemoteAddr(), "timeout", time.Duration(portWriteTimeout.Load()))
		}
		return n, err
	}
	c.written = true
	out, ok := setHandshakeStatus(p, newSessionStatus)
	if !ok && len(p) > 4 && p[4] != mysql.ERR_HEADER {
		// The first packet is neither the handshake this knows nor an error
		// sent in its place: the status it announces is whatever the library
		// wrote, most likely 0, which a driver reads as "autocommit off".
		handshakeNotSet.Do(func() { slog.Warn(handshakeNotSetWarning) })
	}
	return c.Conn.Write(out)
}

// handshakeNotSet makes the warning above a one-time one: the cause is the
// build (a library that writes its handshake differently), not a connection.
var handshakeNotSet sync.Once

const handshakeNotSetWarning = "mysql port: the server handshake was not recognised, so its status flags were left as written; " +
	"a driver that trusts them may believe autocommit is off (logged once)"

// setHandshakeStatus returns a copy of the protocol 10 handshake packet p
// (with its 4-byte header) announcing status. Anything that is not exactly
// one such packet is returned as it is, with ok false.
func setHandshakeStatus(p []byte, status uint16) (out []byte, ok bool) {
	// Header: 3 bytes of payload length, the sequence number (0 for the
	// handshake). Payload: protocol version 10, the server version ending in
	// NUL, connection id (4), salt (8), filler (1), capabilities (2),
	// charset (1), then the status.
	if len(p) < 5 || p[3] != 0 || p[4] != 10 {
		return p, false
	}
	if int(p[0])|int(p[1])<<8|int(p[2])<<16 != len(p)-4 {
		return p, false
	}
	end := bytes.IndexByte(p[5:], 0)
	if end < 0 {
		return p, false
	}
	at := 5 + end + 1 + 4 + 8 + 1 + 2 + 1
	if len(p) < at+2 {
		return p, false
	}
	out = bytes.Clone(p)
	binary.LittleEndian.PutUint16(out[at:], status)
	return out, true
}

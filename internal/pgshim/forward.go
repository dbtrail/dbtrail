package pgshim

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// Forwarding to the source (#2771): a statement the copy is not to answer is
// sent, as the client wrote it, to the server's own PostgreSQL, and the
// source's answer goes back to the client message by message, its errors
// with their codes and its notices as they come.
//
// A client connection has at most one connection to the source, opened the
// first time a statement needs it. It is held while something of the
// client's lives there (a transaction block, an extended-protocol batch, an
// open portal) and given back to the port's pool of connections to the
// sources as soon as nothing does: a client of a connection pool that opened
// one block an hour ago must not hold a slot for good. What a later
// connection needs is brought back to it: the session's settings are set
// again, and a prepared statement is parsed again the first time it runs
// there.
//
// A connection that is lost, or that cannot be trusted to be read-only any
// more, is never replaced behind the client's back: every later statement
// that needs the source is refused until the client reconnects. A
// transaction block open on it is gone, and a new connection would be a new
// session while the client believes nothing happened.
//
// It logs in as the server's forwarding account (Forward.DSN), never as the
// account DBTrail captures with (#2317), and is read-only (forward_screen.go).
//
// What goes to the source here: a transaction block the client opens (its
// BEGIN, every statement in it, its COMMIT or ROLLBACK), and a statement on
// the system catalog this port does not answer from the copy. Everything else
// is answered as before.

// Forward is how a connection reaches its server's source.
type Forward struct {
	// DSN is the forwarding account's postgres:// URL; "" when the server has
	// none the port may use, and Refused then says why.
	DSN     string
	Refused string
	// CaptureAccount: DSN is the account DBTrail captures with (#3054), so
	// every statement sent with it must pass the capture gate first
	// (capture_gate.go).
	CaptureAccount bool
	// Acquire takes one of the connections to sources the port may hold
	// (Limit of them); release gives it back. When none is free it waits for
	// one, as a statement waits in line for the copy, for as long as the host
	// allows and no longer than ctx, which ends when the client leaves or
	// the statement's statement_timeout passes. ok false: none came free.
	Acquire func(ctx context.Context) (release func(), ok bool)
	Limit   int
	// Timeout bounds each wait for the source's answer, as the port's own
	// cap bounds a statement on the copy; 0 for none.
	Timeout time.Duration
	// Barred, when set, is asked before every statement: a reason it
	// returns sends nothing to the source, as if the server had no
	// forwarding account. It is how a host keeps what its views hide (rows,
	// tables or columns a server's rules hide) from being read on the
	// source, where those rules do not apply.
	Barred func() string
	// Cost is when a statement the copy answers as PostgreSQL does is sent
	// there rather than to the source, by the source's plan (route_cost.go);
	// nil for DefaultCostPolicy.
	Cost *CostPolicy
	// Park, when set, keeps the client's connection to the source open
	// between its statements, as the MySQL port keeps its own (#3046): it
	// is offered to the port's pool with reclaim, which the pool calls, from
	// any goroutine, when another client waits for a connection and none is
	// free; reclaim ends the connection and gives its slot back. unpark
	// takes it back for the client's next statement, and reports false when
	// it was reclaimed first. Nil: the connection is ended after every
	// answer, as before.
	Park func(reclaim func()) (unpark func() bool)
}

// barred is why nothing may be sent to the source now, "" when it may.
func (f *Forward) barred() string {
	if f == nil || f.Barred == nil {
		return ""
	}
	return f.Barred()
}

// Timeouts toward the source. Variables so a test can shorten them.
var (
	// sourceConnectTimeout bounds opening the connection, its login included.
	sourceConnectTimeout = 15 * time.Second
	// sourceCancelGrace is how long a statement that was cancelled may take
	// to say so before its connection is closed.
	sourceCancelGrace = 10 * time.Second
	// sourceWriteTimeout bounds sending one batch of messages.
	sourceWriteTimeout = 30 * time.Second
	// sourceSlotWait bounds the wait for a connection to the source when
	// the port holds as many as it opens: the copy's line
	// (sqlsandbox.DefaultMaxWait).
	sourceSlotWait = sqlsandbox.DefaultMaxWait
)

// maxSourceMessage is the largest message the port relays from the source:
// a row, typically. PostgreSQL's own ceiling on one value is 1 GB; this one
// keeps one client's row from taking as much memory in this process.
const maxSourceMessage = 256 << 20

// internalStatement is the name of the prepared statement and portal the port
// uses for its own statements on the source in the middle of a client's
// extended-protocol batch, out of the client's way.
const internalStatement = "dbtrail_internal"

// upstream is the connection to the source.
type upstream struct {
	conn    net.Conn
	fe      *pgproto3.Frontend
	pid     uint32
	key     []byte
	dial    pgconn.DialFunc
	release func()
	logger  *slog.Logger
	// ctx, when set, is the statement whose answer is being read: a cancel
	// request for it, its statement_timeout and the client leaving stop
	// the source's work on it. Unset, each read has a context of its own.
	ctx context.Context
	// tx is the transaction status the source last reported (0 before it
	// reported one); inTx is set while the client's transaction block is
	// open there.
	tx   byte
	inTx bool
	// inCycle is set while extended-protocol messages were sent since the
	// last Sync: the source expects a Sync before the next simple query.
	inCycle bool
	// cycle counts the batches ended there (syncCycle): a check of the
	// capture gate made in one batch holds for that batch only.
	cycle uint64
	// failed is set when the source answered an error in this cycle: it
	// discards everything up to the Sync.
	failed bool
	// sent is each setting as the source has it now (syncSettings); nil
	// when it is not known, and every setting is given again.
	sent map[string]string
	// stmts are the client's prepared statements parsed on the source, by
	// name; portals the client's portals bound there.
	stmts   map[string]*preparedStatement
	portals map[string]bool
	// unsafe is set when the source reported default_transaction_read_only
	// off, or said it is closing the connection: it is dropped after the
	// statement.
	unsafe    bool
	unsafeWhy string
	// cancelled is set while a cancel request was sent for the statement
	// running.
	cancelled atomic.Bool
	// unpark, when set, is the connection kept idle between the client's
	// statements (parkIdleSource): calling it takes it back from the
	// port's pool, and reports whether it was still this client's.
	unpark func() bool
}

// forwarding reports whether this connection can send statements to its
// source.
func (s *session) forwarding() bool {
	return s.hasForwardAccount() && s.bound.Forward.barred() == ""
}

// hasForwardAccount reports whether the server has a forwarding account,
// whether or not anything may be sent with it now.
func (s *session) hasForwardAccount() bool {
	return s.bound != nil && s.bound.Forward != nil && s.bound.Forward.DSN != ""
}

// leaveBarredSource drops the connection to the source when nothing may be
// sent there any more (Forward.Barred: a rule saved while it was open): a
// block open on it fails, and only its ROLLBACK is answered.
func (s *session) leaveBarredSource() {
	if s.up == nil || !s.hasForwardAccount() {
		return
	}
	if why := s.bound.Forward.barred(); why != "" {
		s.loseSource(why)
	}
}

// pinned reports whether the client's transaction block lives on the source.
func (s *session) pinned() bool { return s.up != nil && s.up.inTx }

// source returns the connection to the source, opening it when there is
// none.
func (s *session) source() (*upstream, *pgErr) {
	// Every path to the source comes through here: whatever way a statement
	// took, nothing reaches a source the host bars (Forward.Barred), and a
	// connection open on it when that changed is dropped.
	if why := s.bound.Forward.barred(); why != "" {
		s.loseSource(why)
		return nil, &pgErr{"0A000", "this statement is answered by the source, and nothing is sent there: " + why}
	}
	if s.up != nil {
		return s.up, nil
	}
	if s.upLost != "" {
		return nil, &pgErr{"08006", "the connection to the source was lost (" + s.upLost + "); reconnect"}
	}
	f := s.bound.Forward
	if f == nil || f.DSN == "" {
		why := "the server has no forwarding account"
		if f != nil && f.Refused != "" {
			why = f.Refused
		}
		return nil, &pgErr{"0A000", "this statement is answered by the source, and it cannot be sent there: " + why}
	}
	release := func() {}
	if f.Acquire != nil {
		// Every statement may need one now (route_cost.go asks the source for
		// the plan of most), so a client waits for a slot a statement gives
		// back, a few milliseconds away, instead of being refused while it is
		// taken (#2985).
		// Within the statement's own context (its statement_timeout and a
		// cancel request, already counting what it spent before), and never
		// longer than sourceSlotWait, or slotWaitCap when one is set.
		base, finish := s.statementContext()
		d := sourceSlotWait
		if c := s.slotWaitCap; c > 0 && c < d {
			d = c
		}
		ctx, cancel := context.WithTimeout(base, d)
		r, ok := f.Acquire(ctx)
		cancel()
		stopped := stoppedError(base)
		finish()
		if !ok && stopped != nil {
			// The statement's own end (its statement_timeout, a cancel
			// request) stopped the wait: that is its answer, not the limit.
			return nil, stopped
		}
		if !ok {
			return nil, &pgErr{"53300", fmt.Sprintf("the port already holds %d connections to sources, the most it opens (--max-source-connections), and none was given back while this statement waited; try again in a moment", f.Limit)}
		}
		release = r
	}
	up, perr := s.openSource(f.DSN)
	if perr != nil {
		release()
		return nil, perr
	}
	up.release = release
	s.up = up
	return up, nil
}

// openSource logs in to the source as the forwarding account.
func (s *session) openSource(dsn string) (*upstream, *pgErr) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		// Not echoed: the text can carry the password.
		return nil, &pgErr{"08001", "the server's forwarding account cannot be read; set it again"}
	}
	if cfg.ConnectTimeout == 0 || cfg.ConnectTimeout > sourceConnectTimeout {
		cfg.ConnectTimeout = sourceConnectTimeout
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	// Read-only, whatever a statement holds; and the formats this port told
	// the client it writes in, so a row the source writes reads as the
	// client expects.
	for k, v := range map[string]string{
		"default_transaction_read_only": "on",
		"application_name":              "DBTrail pg-router",
		"client_encoding":               "UTF8",
		"DateStyle":                     "ISO, MDY",
		"IntervalStyle":                 "postgres",
		"bytea_output":                  "hex",
		"standard_conforming_strings":   "on",
	} {
		cfg.RuntimeParams[k] = v
	}
	ctx, cancel := context.WithTimeout(s.connContext(), sourceConnectTimeout)
	defer cancel()
	pc, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			// The source refused the account. Not passed with its own code: a
			// 28P01 would tell the client its password to the port is wrong,
			// and it is not.
			s.logger.Warn("pgshim: the source refused the forwarding account", "code", pe.Code, "err", pe.Message)
			return nil, &pgErr{"08004", fmt.Sprintf("the source refused the server's forwarding account (%s: %s); fix the forwarding account", pe.Code, pe.Message)}
		}
		s.logger.Warn("pgshim: cannot connect to the source", "err", err)
		return nil, &pgErr{"08001", "cannot connect to the source: " + err.Error()}
	}
	if v := pc.ParameterStatus("default_transaction_read_only"); v != "on" {
		_ = pc.Close(context.Background())
		return nil, &pgErr{"0A000", "the source did not confirm that the connection is read-only (PostgreSQL 14 and later say so), so nothing is sent to it from this port"}
	}
	hc, err := pc.Hijack()
	if err != nil {
		_ = pc.Close(context.Background())
		return nil, &pgErr{"08001", "cannot use the connection to the source: " + err.Error()}
	}
	hc.Frontend.SetMaxBodyLen(maxSourceMessage)
	return &upstream{conn: hc.Conn, fe: hc.Frontend, pid: hc.PID, key: hc.SecretKey, dial: cfg.DialFunc, logger: s.logger,
		tx: hc.TxStatus, stmts: map[string]*preparedStatement{}, portals: map[string]bool{}}, nil
}

// closeSource ends the connection to the source and gives its slot back.
func (s *session) closeSource() {
	s.claimSource()
	up := s.up
	if up == nil {
		return
	}
	s.up = nil
	up.terminate()
}

// terminate ends the connection and gives its slot back. It touches nothing
// of the session's: the port's pool calls it from another goroutine for a
// parked connection (parkIdleSource).
func (up *upstream) terminate() {
	up.fe.Send(&pgproto3.Terminate{})
	_ = up.conn.SetWriteDeadline(time.Now().Add(time.Second))
	_ = up.fe.Flush()
	_ = up.conn.Close()
	up.release()
}

// parkIdleSource keeps the connection to the source open past the answer
// just given when nothing of the client's lives there, for the client's
// next statement (#3046): the MySQL port keeps its connection to the source
// for the client's whole life, and opening one costs a login and the
// settings each time, several times the statement a key lookup is. Unlike
// MySQL's it is not held from other clients: the port's pool takes it back
// (Forward.Park) when another client waits for a connection and none is
// free. Without a pool that can, it is ended (releaseIdleSource). Called
// before every ReadyForQuery; claimSource takes it back on the client's next
// message, before anything can be sent on it.
func (s *session) parkIdleSource() {
	up := s.up
	if up == nil || up.unpark != nil || up.inTx || up.inCycle || len(up.portals) > 0 || up.tx != 'I' {
		return
	}
	if up.unsafe || s.bound == nil || s.bound.Forward == nil || s.bound.Forward.Park == nil {
		s.closeSource()
		return
	}
	up.unpark = s.bound.Forward.Park(up.terminate)
}

// claimSource takes back a parked connection to the source (parkIdleSource)
// before anything of the client's next message is done: what the pool
// reclaimed meanwhile, or the source ended while it was idle, is dropped,
// and the next statement that needs the source opens one again, as it did
// before connections were kept. Nothing of the client's lived on it.
func (s *session) claimSource() {
	up := s.up
	if up == nil || up.unpark == nil {
		return
	}
	unpark := up.unpark
	up.unpark = nil
	if !unpark() {
		// Ended by the pool, on its own goroutine: not touched here.
		s.up = nil
		return
	}
	if why := up.idleEnded(); why != "" {
		s.logger.Debug("pgshim: the connection to the source kept between statements was ended while idle; another is opened", "why", why)
		s.up = nil
		up.terminate()
	}
}

// idleEnded says why a connection kept idle can no longer be used, "" when
// it can: an idle PostgreSQL connection sends nothing, so anything to read
// (a FATAL for idle_session_timeout, a closed socket) means it is over. The
// socket is looked at without waiting and without taking anything from it
// (peekIdle); where that cannot be done, a read waits up to a millisecond.
func (up *upstream) idleEnded() string {
	c := up.conn
	if t, ok := c.(*tls.Conn); ok {
		c = t.NetConn()
	}
	if sc, ok := c.(syscall.Conn); ok {
		if why, ok := peekIdle(sc); ok {
			return why
		}
	}
	if err := up.conn.SetReadDeadline(time.Now().Add(time.Millisecond)); err != nil {
		return err.Error()
	}
	var b [1]byte
	n, err := up.conn.Read(b[:])
	_ = up.conn.SetReadDeadline(time.Time{})
	var ne net.Error
	switch {
	case n > 0:
		return idleSentSomething
	case errors.As(err, &ne) && ne.Timeout():
		return ""
	case err != nil:
		return err.Error()
	}
	return "the connection read nothing and no error"
}

// idleSentSomething is why a kept connection the source wrote to while it
// was idle is not used.
const idleSentSomething = "the source sent something while the connection was idle"

// markSourceRead notes that the source just sent the client a row or a
// statement's end: from now on the copy may not answer this connection with
// less of the tables it read (waitForCopy), and the position known for it is
// from before this answer until the next one is read. Rows before an error
// count too. Which tables: the ones the source confirms for the statement
// being proven (session.proving), and every table for any other.
func (s *session) markSourceRead() {
	s.knownValid = false
	s.sourceAnsweredAt = time.Now()
	if s.src.all {
		return
	}
	p := s.proving
	if p == nil {
		s.src.markAll()
		return
	}
	if !p.answered {
		p.answered = true
		// Not confirmed yet: every table counts as read until it is.
		s.src.pending = append(s.src.pending, p)
	}
}

// readKnownPosition reads the answer of the position query sent behind a
// statement (forwardSimple) or after a batch (syncOnSource): kept as the
// connection's known position when the source gave one. Only a lost
// connection is returned; any other failure leaves no known position, and
// the copy then asks for one or the source answers.
func (s *session) readKnownPosition(up *upstream) *errSourceLost {
	cell, perr, lost := s.readCell(up)
	if lost != nil {
		return lost
	}
	if perr != nil {
		s.logger.Debug("pgshim: the source did not say its WAL position after an answer", "code", perr.code, "err", perr.msg)
		return nil
	}
	if lsn, ok := parseLSN(string(cell)); ok {
		s.knownLSN, s.knownValid = lsn, true
	}
	return nil
}

// releaseIdleSource gives the connection to the source back when nothing of
// the client's lives there: no block, no batch, no portal. Called before
// every ReadyForQuery.
func (s *session) releaseIdleSource() {
	up := s.up
	if up == nil || up.inTx || up.inCycle || len(up.portals) > 0 || up.tx != 'I' {
		return
	}
	s.closeSource()
}

// loseSource drops a connection to the source that can no longer be trusted
// to be in step with the client. A client's transaction block on it is gone:
// the block fails here, and only its ROLLBACK is answered.
func (s *session) loseSource(why string) {
	if s.up == nil {
		return
	}
	pinned := s.up.inTx
	if len(s.src.pending) > 0 {
		// What the source sent on it will never be confirmed.
		s.src.markAll()
	}
	s.logger.Warn("pgshim: the connection to the source is closed", "reason", why)
	s.closeSource()
	s.upLost = why
	s.savepoints = nil
	if pinned && s.txStatus() == 'T' {
		s.markFailed()
	}
}

// sendCancel asks the source to stop the statement this connection runs, the
// way a PostgreSQL client does: a new connection carrying its key.
func (up *upstream) sendCancel() {
	addr := up.conn.RemoteAddr()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dial := up.dial
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	c, err := dial(ctx, addr.Network(), addr.String())
	if err != nil {
		up.logger.Warn("pgshim: a cancel request could not reach the source", "err", err)
		return
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 12+len(up.key))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(buf)))
	binary.BigEndian.PutUint32(buf[4:8], 80877102)
	binary.BigEndian.PutUint32(buf[8:12], up.pid)
	copy(buf[12:], up.key)
	if _, err := c.Write(buf); err != nil {
		up.logger.Warn("pgshim: a cancel request could not be sent to the source", "err", err)
		return
	}
	// The source closes it once it read the request.
	_, _ = c.Read(make([]byte, 1))
}

// watch sends the source a cancel request when ctx ends before the returned
// function is called, and closes the connection if the source has not
// answered sourceCancelGrace later. The returned function waits for a
// cancel request it sent to be delivered: one that arrived late would stop
// the connection's NEXT statement.
func (up *upstream) watch(ctx context.Context) func() {
	var mu sync.Mutex
	finished := false
	var delivered chan struct{}
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		defer mu.Unlock()
		if finished {
			return
		}
		up.cancelled.Store(true)
		delivered = make(chan struct{})
		go func() {
			defer close(delivered)
			up.sendCancel()
		}()
		_ = up.conn.SetReadDeadline(time.Now().Add(sourceCancelGrace))
	})
	return func() {
		stop()
		mu.Lock()
		finished = true
		ch := delivered
		mu.Unlock()
		if ch != nil {
			<-ch
		}
		_ = up.conn.SetReadDeadline(time.Time{})
		up.cancelled.Store(false)
	}
}

// send writes messages to the source and flushes them.
func (up *upstream) send(msgs ...pgproto3.FrontendMessage) error {
	for _, m := range msgs {
		up.fe.Send(m)
	}
	_ = up.conn.SetWriteDeadline(time.Now().Add(sourceWriteTimeout))
	err := up.fe.Flush()
	_ = up.conn.SetWriteDeadline(time.Time{})
	return err
}

// relayOut is what a read of the source's answer does with what it reads.
type relayOut int

const (
	// relayClient sends the answer to the client.
	relayClient relayOut = iota
	// relayQuiet keeps it: the port's own statements, whose error is
	// returned.
	relayQuiet
	// relayErrors sends the client the source's errors and notices only:
	// the answer to a message the client already got the port's reply to
	// (a Parse, a Bind).
	relayErrors
	// relayCapture keeps it, as relayQuiet does, and hands the done
	// function every DataRow too: the port's own statements that read a
	// row (routeByCost's EXPLAIN).
	relayCapture
)

// quiet reports whether nothing read goes to the client.
func (o relayOut) quiet() bool { return o == relayQuiet || o == relayCapture }

// errSourceLost ends a read of the source that cannot go on.
type errSourceLost struct{ why string }

func (e *errSourceLost) Error() string { return e.why }

// relay reads the source's answer until done says it ended, sending it to
// the client or keeping it (out). It returns the source's error, if it
// answered one, as a *pgErr (already sent to the client unless out is
// relayQuiet), and a non-nil lost when the connection cannot be used again.
//
// Every read is bounded: by the statement's context when one is set
// (up.ctx), else by a context of its own that a cancel request and
// statement_timeout end, and by the port's cap (Forward.Timeout). When it
// ends, the source is asked to stop, and the connection is closed if it does
// not.
func (s *session) relay(up *upstream, out relayOut, done func(pgproto3.BackendMessage) bool) (perr *pgErr, lost *errSourceLost) {
	ctx := up.ctx
	if ctx == nil {
		c, finish := s.statementContext()
		defer finish()
		ctx = c
	}
	if f := s.bound.Forward; f != nil && f.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, f.Timeout)
		defer cancel()
	}
	stopWatch := up.watch(ctx)
	defer stopWatch()
	sent := 0
	copyOut := false
	for {
		msg, err := up.fe.Receive()
		if err != nil {
			why := "it did not answer: " + err.Error()
			if up.cancelled.Load() {
				why = "it did not stop a cancelled statement in time"
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() && !up.cancelled.Load() {
				why = "it did not answer in time"
			}
			var big *pgproto3.ExceededMaxBodyLenErr
			if errors.As(err, &big) {
				why = fmt.Sprintf("it sent a message of %d bytes, more than the %d MB this port relays", big.ActualBodyLen, maxSourceMessage>>20)
			}
			if up.unsafeWhy != "" {
				why = up.unsafeWhy
			}
			return perr, &errSourceLost{why}
		}
		switch m := msg.(type) {
		case *pgproto3.ParameterStatus:
			if m.Name == "default_transaction_read_only" && m.Value != "on" {
				up.unsafe = true
				up.unsafeWhy = "it reported default_transaction_read_only off, which this port does not forward under"
			}
			// The source's reports are not the client's: the port reports
			// its own settings.
			continue
		case *pgproto3.ErrorResponse:
			e := *m
			if e.Severity == "FATAL" || e.Severity == "PANIC" {
				// The source is closing this connection, not the client's.
				e.Severity, e.SeverityUnlocalized = "ERROR", "ERROR"
				up.unsafe = true
				up.unsafeWhy = "the source closed it: " + e.Message
			}
			perr = &pgErr{e.Code, e.Message}
			if !out.quiet() {
				s.be.Send(&e)
			}
		case *pgproto3.NoticeResponse, *pgproto3.NotificationResponse:
			if !out.quiet() {
				s.be.Send(m)
			}
			continue
		case *pgproto3.CopyInResponse:
			// The screen lets no COPY through; should one reach the source
			// all the same, it gets no data.
			if err := up.send(&pgproto3.CopyFail{Message: "COPY is not served through this port"}); err != nil {
				return perr, &errSourceLost{"it could not be told to stop a COPY: " + err.Error()}
			}
			continue
		case *pgproto3.CopyOutResponse:
			copyOut = true
			continue
		case *pgproto3.CopyData, *pgproto3.CopyDone:
			if copyOut {
				continue
			}
			return perr, &errSourceLost{fmt.Sprintf("it sent %T outside a COPY", m)}
		case *pgproto3.CopyBothResponse:
			return perr, &errSourceLost{"it started a replication stream"}
		case *pgproto3.CommandComplete:
			if copyOut {
				copyOut = false
				perr = &pgErr{"0A000", "COPY is not served on this port: it reads rows with SELECT"}
				if !out.quiet() {
					s.errorResponse(perr.code, perr.msg)
				}
				msg = &pgproto3.ErrorResponse{}
				break
			}
			if out == relayClient {
				s.be.Send(m)
				s.markSourceRead()
			}
		case *pgproto3.DataRow:
			if out == relayCapture && done(msg) {
				return perr, nil
			}
			if out == relayClient {
				s.be.Send(m)
				s.markSourceRead()
				for _, v := range m.Values {
					sent += len(v)
				}
				if sent > flushAfterBytes {
					// A large answer goes out as it comes, not held whole
					// here.
					sent = 0
					if err := s.be.Flush(); err != nil {
						return perr, &errSourceLost{"the client stopped reading its answer"}
					}
				}
			}
			continue
		case *pgproto3.RowDescription, *pgproto3.EmptyQueryResponse, *pgproto3.ParameterDescription,
			*pgproto3.NoData, *pgproto3.PortalSuspended, *pgproto3.BindComplete, *pgproto3.ParseComplete,
			*pgproto3.CloseComplete:
			if out == relayClient {
				s.be.Send(m)
			}
		case *pgproto3.ReadyForQuery:
			up.tx = m.TxStatus
		default:
			return perr, &errSourceLost{fmt.Sprintf("it sent %T, which this port does not relay", m)}
		}
		if done(msg) {
			return perr, nil
		}
	}
}

// statusKeeper ends a relay on ReadyForQuery and keeps the transaction
// status it reports.
type statusKeeper struct{ tx byte }

func (k *statusKeeper) done(m pgproto3.BackendMessage) bool {
	if r, ok := m.(*pgproto3.ReadyForQuery); ok {
		k.tx = r.TxStatus
		return true
	}
	return false
}

// syncCycle ends the extended-protocol batch on the source, if one is open,
// with a Sync, and returns the transaction status it reports (0 when none
// was open). out says whether the source's notices of it go to the client.
func (s *session) syncCycle(up *upstream, out relayOut) (byte, *errSourceLost) {
	if !up.inCycle {
		return 0, nil
	}
	if err := up.send(&pgproto3.Sync{}); err != nil {
		return 0, &errSourceLost{"it could not be written to: " + err.Error()}
	}
	k := &statusKeeper{}
	if _, lost := s.relay(up, out, k.done); lost != nil {
		return 0, lost
	}
	up.inCycle, up.failed = false, false
	up.cycle++
	if k.tx == 'I' {
		// The implicit transaction ended, and with it every portal.
		clear(up.portals)
	}
	return k.tx, nil
}

// internal runs one of the port's own statements on the source (a setting, a
// transaction's BEGIN or end) and returns its error, if any, and its command
// tag. Outside an extended batch it is a simple query, after which up.tx is
// the source's status; inside one it is a statement of the port's own name,
// so the client's unnamed statement and portal are left as they are.
func (s *session) internal(up *upstream, sql string) (tag string, perr *pgErr, lost *errSourceLost) {
	tag, _, perr, lost = s.internalCell(up, sql)
	return tag, perr, lost
}

// internalCell is internal with the first cell of the last row the statement
// answered, nil when it answered none.
func (s *session) internalCell(up *upstream, sql string) (tag string, cell []byte, perr *pgErr, lost *errSourceLost) {
	keepTag := func(m pgproto3.BackendMessage) {
		if c, ok := m.(*pgproto3.CommandComplete); ok {
			tag = string(c.CommandTag)
		}
	}
	// keepRow keeps a row's first cell and says whether m was a row.
	keepRow := func(m pgproto3.BackendMessage) bool {
		r, ok := m.(*pgproto3.DataRow)
		if ok && len(r.Values) > 0 {
			// The message's buffer is reused by the next one.
			cell = append([]byte{}, r.Values[0]...)
		}
		return ok
	}
	if !up.inCycle {
		if err := up.send(&pgproto3.Query{String: sql}); err != nil {
			return "", nil, nil, &errSourceLost{"it could not be written to: " + err.Error()}
		}
		// A simple query ends the source's unnamed statement.
		delete(up.stmts, "")
		k := &statusKeeper{}
		perr, lost = s.relay(up, relayCapture, func(m pgproto3.BackendMessage) bool {
			if keepRow(m) {
				return false
			}
			keepTag(m)
			return k.done(m)
		})
		if lost == nil && k.tx == 'I' {
			clear(up.portals)
		}
		return tag, cell, perr, lost
	}
	if up.failed {
		return "", nil, &pgErr{"25P02", "current transaction is aborted, commands ignored until end of transaction block"}, nil
	}
	// Closed first: one left by an earlier error (the Close after it was
	// discarded with the rest of that batch) would refuse this Parse, and the
	// portal of an earlier statement of the port's in this batch, which lives
	// as long as the batch's transaction, would refuse this Bind.
	// Closing one that does not exist is not an error.
	if err := up.send(
		&pgproto3.Close{ObjectType: 'S', Name: internalStatement},
		&pgproto3.Close{ObjectType: 'P', Name: internalStatement},
		&pgproto3.Parse{Name: internalStatement, Query: sql},
		&pgproto3.Bind{DestinationPortal: internalStatement, PreparedStatement: internalStatement},
		&pgproto3.Execute{Portal: internalStatement},
		&pgproto3.Close{ObjectType: 'S', Name: internalStatement},
		&pgproto3.Flush{},
	); err != nil {
		return "", nil, nil, &errSourceLost{"it could not be written to: " + err.Error()}
	}
	n := 0
	perr, lost = s.relay(up, relayCapture, func(m pgproto3.BackendMessage) bool {
		if _, ok := m.(*pgproto3.ErrorResponse); ok {
			return true
		}
		if keepRow(m) {
			return false
		}
		keepTag(m)
		n++
		return n == 6 // CloseComplete twice, ParseComplete, BindComplete, CommandComplete, CloseComplete
	})
	if perr != nil {
		up.failed = true
	}
	return tag, cell, perr, lost
}

// beginReadOnly opens the client's transaction block on the source with the
// port's own BEGIN, and has the source say that the block is read-only before
// anything of the client's runs in it. A block that is not is never used: the
// connection is closed, which rolls it back.
func (s *session) beginReadOnly(up *upstream, begin string) (perr *pgErr, lost *errSourceLost) {
	const ask = "SHOW transaction_read_only"
	var cell []byte
	if !up.inCycle {
		// One write and one answer: the question rides behind the BEGIN.
		_, cell, perr, lost = s.internalCell(up, begin+"; "+ask)
	} else if _, perr, lost = s.internal(up, begin); perr == nil && lost == nil {
		if _, cell, perr, lost = s.internalCell(up, ask); perr != nil && lost == nil {
			// The BEGIN was taken and the question was not answered: a block
			// is open there that nothing vouches for.
			return nil, &errSourceLost{"it did not say whether the transaction block is read-only: " + perr.msg}
		}
	}
	if lost != nil || perr != nil {
		return perr, lost
	}
	if string(cell) != "on" {
		return nil, &errSourceLost{"it opened a transaction block that is not read-only"}
	}
	return nil, nil
}

// sourceSettings are the settings the source must have for this session, as
// SET statements write them: what the client set on the port that changes
// how a statement runs or how its answer is written.
func (s *session) sourceSettings() [][2]string {
	st := s.set
	return [][2]string{
		{"TimeZone", quoteLiteral(st.zone.name)},
		{"search_path", showSearchPath(st.searchPath)},
		{"extra_float_digits", strconv.Itoa(st.extraFloat)},
		{"statement_timeout", strconv.FormatInt(st.stmtTimeout.Milliseconds(), 10)},
		{"lock_timeout", strconv.FormatInt(st.lockTimeout.Milliseconds(), 10)},
		{"client_min_messages", st.minMessages},
		{"default_transaction_isolation", quoteLiteral(st.defaultIso)},
	}
}

// quoteLiteral writes v as a string literal.
func quoteLiteral(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }

// syncSettings gives the source the settings the session has now, the ones
// it does not have already, before a statement is sent to it.
func (s *session) syncSettings(up *upstream) (perr *pgErr, lost *errSourceLost) {
	if up.failed {
		return nil, nil
	}
	if up.sent == nil {
		up.sent = map[string]string{}
	}
	var stmts []string
	want := s.sourceSettings()
	for _, kv := range want {
		if up.sent[kv[0]] != kv[1] {
			stmts = append(stmts, "SET "+kv[0]+" = "+kv[1])
		}
	}
	if len(stmts) == 0 {
		return nil, nil
	}
	if !up.inCycle {
		_, perr, lost = s.internal(up, strings.Join(stmts, "; "))
	} else {
		for _, stmt := range stmts {
			if _, perr, lost = s.internal(up, stmt); perr != nil || lost != nil {
				break
			}
		}
	}
	if lost != nil {
		return nil, lost
	}
	if perr != nil {
		up.sent = nil
		return &pgErr{perr.code, "the source refused a setting of this session: " + perr.msg}, nil
	}
	for _, kv := range want {
		up.sent[kv[0]] = kv[1]
	}
	return nil, nil
}

// prepareSource gets the connection and the session's settings onto it, for
// a statement about to be sent.
func (s *session) prepareSource() (*upstream, *pgErr) {
	up, perr := s.source()
	if perr != nil {
		return nil, perr
	}
	perr, lost := s.syncSettings(up)
	if lost != nil {
		s.loseSource(lost.why)
		return nil, s.lostError(lost)
	}
	return up, perr
}

// lostError is the client's error for a connection to the source lost in the
// middle of a statement.
func (s *session) lostError(lost *errSourceLost) *pgErr {
	return &pgErr{"08006", "the connection to the source was lost (" + lost.why + "); reconnect"}
}

// afterSource drops a connection that reported it is no longer read-only,
// or that the source said it is closing, and tells the client so in the
// answer it is getting: the block it was in fails with it.
func (s *session) afterSource(up *upstream) {
	if !up.unsafe {
		return
	}
	why := up.unsafeWhy
	s.loseSource(why)
	s.errorResponse("08006", "the connection to the source is closed ("+why+"); reconnect")
	s.ext.skip = s.ext.skip || up.inCycle
}

// forwardSimple answers a simple query from the source. Every outcome is sent
// to the client here; the caller sends ReadyForQuery. ok is false when the
// client was sent an error.
func (s *session) forwardSimple(qstr string) (ok bool) {
	if perr := forwardRefusal(qstr); perr != nil {
		s.fail(perr)
		return false
	}
	up, perr := s.prepareSource()
	if perr != nil {
		s.fail(perr)
		return false
	}
	if _, lost := s.syncCycle(up, relayClient); lost != nil {
		s.loseSource(lost.why)
		s.fail(s.lostError(lost))
		return false
	}
	// With the capture account, nothing goes there that the capture gate
	// does not let through (#3054), asked before anything of it is sent.
	if perr := s.captureRefusal(up, qstr); perr != nil {
		s.fail(perr)
		return false
	}
	ctx, done := s.statementContext()
	defer done()
	up.ctx = ctx
	defer func() { up.ctx = nil }()
	// The source's WAL position goes in the same write, behind the client's
	// statement: the source runs it once that one has ended, so it is at or
	// past every commit the answer could show, and costs no round trip of
	// its own (#2985, knownLSN).
	msgs := []pgproto3.FrontendMessage{&pgproto3.Query{String: qstr}}
	position := s.bound.CopyWait != nil
	if position {
		msgs = append(msgs, &pgproto3.Query{String: sourceWALPositionSQL})
	}
	// And behind it, which tables the statement read, when its text lets
	// them be told (source_tables.go): the source resolves the names as it
	// just did for the statement.
	proof := s.proveReads(qstr)
	if proof != nil {
		msgs = append(msgs, &pgproto3.Query{String: proofSQL(proof.reads)})
	}
	s.proving = proof
	defer func() { s.proving = nil }()
	if err := up.send(msgs...); err != nil {
		s.loseSource("it could not be written to: " + err.Error())
		s.fail(s.lostError(&errSourceLost{"it could not be written to"}))
		return false
	}
	delete(up.stmts, "")
	k := &statusKeeper{}
	srcErr, lost := s.relay(up, relayClient, k.done)
	if lost == nil && position {
		lost = s.readKnownPosition(up)
	}
	if lost == nil && proof != nil {
		var cell []byte
		var perr *pgErr
		if cell, perr, lost = s.readCell(up); lost == nil {
			s.confirmProof(proof, cell, perr)
		}
	}
	if lost != nil {
		s.loseSource(lost.why)
		if srcErr != nil {
			// The source's error was sent: one answer, not two.
			s.markFailed()
			return false
		}
		s.fail(s.lostError(lost))
		return false
	}
	if srcErr == nil && k.tx == 'T' {
		s.tookSavepointStmt(qstr)
	}
	s.followSourceStatus(up, k.tx)
	unsafe := up.unsafe
	s.afterSource(up)
	return srcErr == nil && !unsafe
}

// savepoint is what the session's settings were when a savepoint of the
// client's block was made on the source, which a ROLLBACK TO it goes back
// to, as PostgreSQL's does.
type savepoint struct {
	name              string
	set, sessionLevel settings
}

// tookSavepointStmt follows a SAVEPOINT, RELEASE or ROLLBACK TO the source
// took: the settings at a savepoint are kept, a release lets it and the ones
// after it go, and a rollback to one brings its settings back and the block
// back from its error.
func (s *session) tookSavepointStmt(qstr string) {
	if !s.pinned() {
		return
	}
	kind, name := savepointStmt(qstr)
	at := -1
	for i := len(s.savepoints) - 1; i >= 0; i-- {
		if s.savepoints[i].name == name {
			at = i
			break
		}
	}
	switch kind {
	case "savepoint":
		s.savepoints = append(s.savepoints, savepoint{name: name, set: s.set, sessionLevel: s.sessionLevel})
	case "release":
		if at >= 0 {
			s.savepoints = s.savepoints[:at]
		}
	case "rollback":
		if at >= 0 {
			s.set, s.sessionLevel = s.savepoints[at].set, s.savepoints[at].sessionLevel
			s.savepoints = s.savepoints[:at+1]
		} else {
			// The source went back to a savepoint this session did not see
			// made (one made inside a longer text): what the settings were
			// there is not known, and the ones of now are given again.
			s.logger.Warn("pgshim: the source rolled back to a savepoint this connection did not follow; the session's settings stay as they are now", "savepoint", name)
		}
		// The source went back to its own settings at the savepoint: they
		// are given again before the next statement.
		s.up.sent = nil
		if s.txStatus() == 'E' {
			s.tx = 'T'
		}
	}
}

// followSourceStatus takes the transaction status the source reported after
// a statement of the client's transaction block.
func (s *session) followSourceStatus(up *upstream, tx byte) {
	if tx == 'I' {
		clear(up.portals)
	}
	if !up.inTx {
		return
	}
	switch tx {
	case 'E':
		if s.txStatus() == 'T' {
			s.markFailed()
		}
	case 'I':
		// The block ended on the source (a statement that ended it): ended
		// here too.
		up.inTx, up.sent = false, nil
		s.tx = 'I'
		s.savepoints = nil
		s.dropPortals()
	}
}

// forwardTxControl sends the source the BEGIN, SET TRANSACTION, COMMIT or
// ROLLBACK the client asked for, before the port moves its own transaction
// status: a block the client opens lives on the source until it ends. What
// is sent is the port's own text, written from what it read of the client's
// (sessionCmd.sourceText), never the client's: nothing it did not read goes
// along. tag, when not empty, is the command tag the source answered, which
// the client is given.
func (s *session) forwardTxControl(cmd *sessionCmd) (tag string, perr *pgErr) {
	s.leaveBarredSource()
	if !s.forwarding() {
		return "", nil
	}
	switch cmd.kind {
	case cmdBegin:
		if s.txStatus() != 'I' {
			return "", nil // a warning, here and on the source alike
		}
		up, perr := s.prepareSource()
		if perr != nil {
			return "", perr
		}
		if !up.inCycle && up.tx != 'I' {
			// The source is in a block the client does not know of.
			s.loseSource("it was in a transaction block the client had not opened")
			return "", &pgErr{"08006", "the connection to the source was out of step with this session and is closed; reconnect"}
		}
		begin, perr := cmd.sourceText()
		if perr != nil {
			return "", perr
		}
		srcErr, lost := s.beginReadOnly(up, begin)
		if lost != nil {
			s.loseSource(lost.why)
			return "", s.lostError(lost)
		}
		if srcErr != nil {
			if !up.inCycle && up.tx != 'I' {
				// The BEGIN was taken and what followed it was not.
				s.loseSource("it could not confirm that the transaction block is read-only")
			}
			return "", srcErr
		}
		up.inTx = true
		return "", nil
	case cmdSetTx:
		// The block's isolation level, set on the block where it runs.
		if !s.pinned() || s.txStatus() != 'T' {
			return "", nil
		}
		up, perr := s.prepareSource()
		if perr != nil {
			return "", perr
		}
		stmt, perr := cmd.sourceText()
		if perr != nil {
			return "", perr
		}
		_, srcErr, lost := s.internal(up, stmt)
		if lost != nil {
			s.loseSource(lost.why)
			return "", s.lostError(lost)
		}
		return "", srcErr
	case cmdCommit, cmdRollback:
		up := s.up
		if up == nil || !up.inTx {
			return "", nil
		}
		stmt, perr := cmd.sourceText()
		if perr != nil {
			return "", perr
		}
		if s.txStatus() == 'E' || up.failed {
			// A failed block is rolled back, whatever ends it.
			stmt = "ROLLBACK"
		}
		if up.failed {
			// The source discards everything up to a Sync: the batch is
			// ended first, then the block.
			if _, lost := s.syncCycle(up, relayQuiet); lost != nil {
				up.inTx = false
				s.loseSource(lost.why)
				return "", nil
			}
		}
		srcTag, srcErr, lost := s.internal(up, stmt)
		up.inTx, up.sent = false, nil
		if lost != nil {
			s.loseSource(lost.why)
			return "", s.lostError(lost)
		}
		if srcErr != nil {
			// A COMMIT the source could not make (a serialization failure)
			// rolls the block back there: here too.
			s.tx = 'I'
			s.set, s.sessionLevel = s.txSaved, s.txSaved
			s.savepoints = nil
			s.dropPortals()
			if !up.inCycle && up.tx != 'I' {
				s.loseSource("it did not end the transaction block")
			}
			return "", srcErr
		}
		if !up.inCycle && up.tx != 'I' {
			// The block did not end there: the connection is not in step.
			s.loseSource("it did not end the transaction block")
			return "", &pgErr{"08006", "the source did not end the transaction block, and the connection to it is closed; reconnect"}
		}
		return srcTag, nil
	}
	return "", nil
}

package pgcapture

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"

	"github.com/jackc/pgx/v5/pgconn"
)

// SourceUnreachableError marks a capture run that ended because the PostgreSQL
// source could not be reached: nothing about the source's configuration is
// known to be wrong, and the same run can succeed once the source is back. A
// supervisor keeps retrying it without limit (#3117), as the MySQL binlog
// syncer does for a dropped connection (MaxReconnectAttempts: 0).
//
// Only Capturer.Run produces it, so a failure of the index database, which can
// carry the same network errors, is never reported as the source being down.
type SourceUnreachableError struct {
	Err error
	// Resumed is set when this run resumed from a saved position, so this
	// source has been captured before: an address that stopped answering is
	// an outage. A source never captured may just have a wrong address.
	Resumed bool
}

func (e *SourceUnreachableError) Error() string { return e.Err.Error() }
func (e *SourceUnreachableError) Unwrap() error { return e.Err }

// IsSourceUnreachable reports whether err is a capture run that ended because
// the source could not be reached.
func IsSourceUnreachable(err error) bool {
	var u *SourceUnreachableError
	return errors.As(err, &u)
}

// decodeError is a failure while decoding the stream. The decoder also reads
// the index (LastChange), so a network error inside it says nothing certain
// about the source: it is left to the supervisor's ordinary policy. If the
// source really is gone, the next run fails to connect and is classified then.
type decodeError struct{ err error }

func (e *decodeError) Error() string { return e.err.Error() }
func (e *decodeError) Unwrap() error { return e.err }

// SourceWasCaptured reports whether err is an unreachable source that this
// index has a saved position for.
func SourceWasCaptured(err error) bool {
	var u *SourceUnreachableError
	return errors.As(err, &u) && u.Resumed
}

// markSourceUnreachable wraps err in a SourceUnreachableError when it says the
// source could not be reached, and returns it unchanged otherwise.
func markSourceUnreachable(err error, resumed bool) error {
	if err == nil || !sourceUnreachable(err) {
		return err
	}
	return &SourceUnreachableError{Err: err, Resumed: resumed}
}

// sourceUnreachable is an allowlist on purpose: an error it does not know
// keeps the give-up, so a failure that never heals cannot retry for ever
// looking like a network problem.
func sourceUnreachable(err error) bool {
	var dec *decodeError
	if errors.As(err, &dec) {
		return false
	}
	if errors.Is(err, ErrSlotLost) || errors.Is(err, ErrSlotMissingOnResume) || errors.Is(err, ErrSlotNotReady) {
		return false
	}
	// An answer from the server decides by itself, and is looked at first:
	// pgconn.ConnectError also carries a refused password or a missing database.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.SQLState() {
		case "08000", // connection_exception
			"08001", // sqlclient_unable_to_establish_sqlconnection
			"08003", // connection_does_not_exist
			"08006", // connection_failure
			"57P01", // admin_shutdown: the server is stopping
			"57P02", // crash_shutdown: another backend crashed
			"57P03": // cannot_connect_now: starting up or in recovery
			return true
		}
		// Credentials, a missing database, a publication or slot problem, and
		// also too_many_connections (53300), which is what an exhausted
		// max_wal_senders says: a setting that does not change by waiting.
		return false
	}
	// A deadline is unreachable only while connecting. Later it is a statement
	// the source did not finish in time (a slot creation waiting on an open
	// transaction, say), and that source is reachable.
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		var netErr net.Error
		if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) {
			return true
		}
	} else if errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// A TLS failure arrives as a *net.OpError too, and needs a configuration
	// change, so only these specific network conditions count.
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	for _, target := range []error{
		io.EOF, io.ErrUnexpectedEOF, net.ErrClosed, pgconn.ErrConnClosed,
		syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.ECONNABORTED, syscall.EPIPE,
		syscall.EHOSTUNREACH, syscall.EHOSTDOWN, syscall.ENETUNREACH, syscall.ENETDOWN, syscall.ETIMEDOUT,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

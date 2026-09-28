package telemetry

import (
	"context"
	"database/sql/driver"
	"errors"
	"io/fs"
	"net"

	"github.com/go-sql-driver/mysql"
)

// Error classes. This is the complete taxonomy — ClassifyError never returns
// anything else, and in particular never returns err.Error(): bintrail error
// strings routinely carry DSNs, hostnames, schema and table names, and file
// paths.
//
// Every class here has at least one producer in the tree. Three that never
// had one (binlog_parse, flag_invalid, network) were dropped in #1503: a
// documented class no code path can emit reads as coverage that does not
// exist, and the only way to produce them would have been to match on
// message text, which this package refuses to do.
const (
	ClassDBConnection   = "db_connection"
	ClassDBPermission   = "db_permission"
	ClassBinlogNotFound = "binlog_not_found"
	ClassSchemaMismatch = "schema_mismatch"
	ClassConfigInvalid  = "config_invalid"
	ClassStorageIO      = "storage_io"
	ClassNotFound       = "not_found"
	ClassInternal       = "internal"
	ClassUnknown        = "unknown"
)

// classes is the set ClassifyError and SetError may emit. Anything outside it
// is coerced to ClassUnknown rather than trusted onto the wire.
var classes = map[string]bool{
	ClassDBConnection:   true,
	ClassDBPermission:   true,
	ClassBinlogNotFound: true,
	ClassSchemaMismatch: true,
	ClassConfigInvalid:  true,
	ClassStorageIO:      true,
	ClassNotFound:       true,
	ClassInternal:       true,
	ClassUnknown:        true,
}

// Classed is implemented by errors that know their own telemetry class. The
// packages that produce a failure worth distinguishing implement it on their
// sentinel or typed error, which lets ClassifyError bucket them without this
// package importing any of them. That direction is forced, not chosen:
// telemetry is imported by internal/console, a read-layer package whose
// depguard (internal/event) forbids linking the capture stack, and a leaf
// package importing its own producers invites cycles. The method returns a
// class NAME, never a message; a value outside the taxonomy is coerced to
// "unknown" by normalizeClass. This package's tests deliberately do not
// import the producers either, so each producer package carries a wiring
// test that asserts the exact class against its real error.
type Classed interface {
	TelemetryClass() string
}

// MySQLNumbered is implemented by errors that carry a MySQL server error
// number from a client library this package must not link. The replication
// client (go-mysql) is the one that sees a binlog-side failure such as 1236,
// but it is a capture library, and telemetry sits under the read layer too
// (internal/event's depguard test forbids the console from linking it), so
// the capture side wraps its error (parser.ReplicationError) and this package
// reads the number through the interface. The number is all that is read.
type MySQLNumbered interface {
	MySQLErrorNumber() uint16
}

// MySQL server error numbers worth distinguishing. Only the number is read —
// the Message field of either error type can contain schema, table and user
// names.
const (
	erDBAccessDenied     = 1044
	erAccessDenied       = 1045
	erHostNotPrivileged  = 1130
	erTableAccessDenied  = 1142
	erColumnAccessDenied = 1143
	erSpecificAccess     = 1227
	erBadDB              = 1049
	erNoSuchTable        = 1146
	// ER_MASTER_FATAL_ERROR_READING_BINLOG: the source cannot serve the
	// requested position — the binlog was purged, or the GTID set names
	// transactions it no longer has. The replication client is the only path
	// that ever sees it, which is why it arrives through MySQLNumbered and not
	// as the driver's *MySQLError.
	erMasterFatalReadingBinlog = 1236

	// What a server that is reachable but unwell answers to a write (#1630).
	// A capture daemon that ran for minutes and then died on one of these used
	// to report unknown, which hid the failures an operator can act on.
	//
	// ER_DISK_FULL and ER_RECORD_FILE_FULL ("The table is full"): the server
	// ran out of room for the write.
	erDiskFull       = 1021
	erRecordFileFull = 1114
	// ER_CON_COUNT_ERROR and ER_TOO_MANY_USER_CONNECTIONS: the server was
	// reached and turned the connection away.
	erConCountError          = 1040
	erTooManyUserConnections = 1203
	// ER_LOCK_WAIT_TIMEOUT and ER_LOCK_DEADLOCK: the server gave up on the
	// statement because of contention. See classifyMySQLNumber for why these
	// share a class with the connection failures.
	erLockWaitTimeout = 1205
	erLockDeadlock    = 1213
	// ER_NO_PARTITION_FOR_GIVEN_VALUE: the partition the row belongs in does
	// not exist.
	erNoPartitionForGivenValue = 1526
	// ER_NET_PACKET_TOO_LARGE: the statement is larger than the server's
	// max_allowed_packet.
	erNetPacketTooLarge = 1153
	// CR_SERVER_GONE_ERROR and CR_SERVER_LOST are CLIENT error numbers. The
	// query driver reports the same condition as driver.ErrBadConn or
	// ErrInvalidConn (handled in ClassifyError); the numbers are here for a
	// proxy or a client library that relays them as a number.
	crServerGoneError = 2006
	crServerLost      = 2013
)

// classifyMySQLNumber maps a server error number to a class, shared by the
// two client libraries that can surface one. A number with no bucket is
// ClassUnknown, never a connectivity class: the server ANSWERED, so the
// failure is specific and simply not one we have a bucket for.
//
// The class names the KIND of failure, not which server had it. The same
// number means the same thing whether the index or the source sent it (a
// source out of connections is as much a db_connection failure as an index
// out of connections), and nothing on the wire says which side it was.
//
// db_connection covers a server that did not serve the request: unreachable,
// refusing connections, or too contended to finish the statement. Lock wait
// timeout and deadlock sit there because the class set is closed and that is
// the one class that reads "the database was not available for this write",
// the same reading the client-side write deadline already has. Telling
// "contended" from "unreachable" needs a class of its own, and a new class
// has to reach the receiving side before anything here may emit it.
func classifyMySQLNumber(number uint16) string {
	switch number {
	case erAccessDenied, erDBAccessDenied, erHostNotPrivileged, erTableAccessDenied, erColumnAccessDenied, erSpecificAccess:
		return ClassDBPermission
	case erBadDB, erNoSuchTable, erNoPartitionForGivenValue:
		return ClassNotFound
	case erMasterFatalReadingBinlog:
		return ClassBinlogNotFound
	case erDiskFull, erRecordFileFull:
		return ClassStorageIO
	case erConCountError, erTooManyUserConnections,
		erLockWaitTimeout, erLockDeadlock,
		crServerGoneError, crServerLost:
		return ClassDBConnection
	case erNetPacketTooLarge:
		return ClassConfigInvalid
	}
	return ClassUnknown
}

// ClassifyError maps an error to a bounded class using structural checks
// (errors.Is/errors.As) only — never string matching, which would couple the
// taxonomy to message wording and tempt someone into shipping the message.
//
// Deliberately conservative: callers that know more about the failure should
// pass a precise class to Span.SetError instead of relying on this. An honest
// "unknown" beats a confidently wrong bucket.
func ClassifyError(err error) string {
	if err == nil {
		return ""
	}

	// A producer that declared its own class wins: it knows more about the
	// failure than any structural probe below can infer.
	var classed Classed
	if errors.As(err, &classed) {
		return normalizeClass(classed.TelemetryClass())
	}

	// The query driver (go-sql-driver) and the replication client each wrap a
	// server error packet in their own type; a binlog-side failure only ever
	// arrives through the latter, which reaches here as a MySQLNumbered.
	var drvErr *mysql.MySQLError
	if errors.As(err, &drvErr) {
		return classifyMySQLNumber(drvErr.Number)
	}
	var numbered MySQLNumbered
	if errors.As(err, &numbered) {
		return classifyMySQLNumber(numbered.MySQLErrorNumber())
	}

	// A driver-level failure (bad host, refused, TLS) surfaces as a net error
	// wrapped by the driver rather than a MySQLError.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return ClassDBConnection
	}
	// driver.ErrBadConn is what database/sql hands back once its own retries
	// on a dead connection are spent: the driver's spelling of "server has
	// gone away" and "lost connection during query".
	if errors.Is(err, mysql.ErrInvalidConn) || errors.Is(err, driver.ErrBadConn) || errors.Is(err, context.DeadlineExceeded) {
		return ClassDBConnection
	}

	if errors.Is(err, fs.ErrNotExist) {
		return ClassNotFound
	}
	if errors.Is(err, fs.ErrPermission) {
		return ClassStorageIO
	}
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return ClassStorageIO
	}

	return ClassUnknown
}

// normalizeClass coerces an arbitrary caller-supplied class to the taxonomy,
// so a typo or a future refactor cannot smuggle free text onto the wire.
func normalizeClass(class string) string {
	if classes[class] {
		return class
	}
	return ClassUnknown
}

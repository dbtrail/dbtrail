package doctor

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
)

// A check's Kind names WHAT went wrong in a fixed word a screen can switch on
// (#1803), where Detail and Remediation stay prose written for a person. The
// set is closed: every kind is a constant below, never built from input, so a
// kind cannot carry a hostname or a password past the scrubber that runs over
// Detail. An empty kind means "not one of these", and the prose is all there
// is. Kinds are set on findings only (fail or warn); the status still says how
// bad it is.
const (
	// KindHostUnreachable: the name did not resolve, or no route reached it.
	KindHostUnreachable = "host_unreachable"
	// KindPortClosed: the host answered, and nothing listens on the port.
	KindPortClosed = "port_closed"
	// KindTimeout: nothing answered within the connect budget.
	KindTimeout = "timeout"
	// KindAccessDenied: a database answered and refused the user or password.
	KindAccessDenied = "access_denied"
	// KindLoopbackInContainer: the typed address is this machine's own
	// (localhost, 127.0.0.1) and nothing answered there, while the host's
	// address from inside a container did. Only claimed on that proof.
	KindLoopbackInContainer = "loopback_in_container"
	// KindMissingPrivilege: the user lacks a privilege; Subjects names each.
	KindMissingPrivilege = "missing_privilege"
	// KindBinlogSettings: a binary log setting is wrong; Subjects names the
	// variable.
	KindBinlogSettings = "binlog_settings"
	// KindNoPrimaryKey: tables with no primary key; Subjects names every one
	// as schema.table, and Statements carries a statement per table.
	KindNoPrimaryKey = "no_primary_key"
)

// Kinds lists every kind, for tests and for a screen that wants to know what
// it may be sent.
func Kinds() []string {
	return []string{
		KindHostUnreachable, KindPortClosed, KindTimeout, KindAccessDenied,
		KindLoopbackInContainer, KindMissingPrivilege, KindBinlogSettings, KindNoPrimaryKey,
	}
}

// ClassifyConnectError names why opening a connection failed, or "" when the
// error is none of the connection kinds. It looks through every wrap
// (config.Connect wraps the driver's error), never at the message text.
func ClassifyConnectError(err error) string {
	if err == nil {
		return ""
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case 1045, 1698: // ER_ACCESS_DENIED_ERROR, ER_ACCESS_DENIED_NO_PASSWORD_ERROR
			return KindAccessDenied
		}
		return ""
	}
	// Before the timeout test: a resolver that timed out still produced no
	// address, and the name is what to check.
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return KindHostUnreachable
	}
	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return KindPortClosed
	case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		return KindHostUnreachable
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded):
		return KindTimeout
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return KindTimeout
	}
	return ""
}

// isLoopbackHost reports whether host names this machine itself, which inside
// a container is the container, not the machine the person typed it on.
func isLoopbackHost(host string) bool {
	h := strings.ToLower(strings.TrimSpace(host))
	h = strings.TrimSuffix(h, ".")
	if strings.HasPrefix(h, "[") && strings.HasSuffix(h, "]") {
		h = h[1 : len(h)-1]
	}
	if h == "localhost" {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && (ip.IsLoopback() || ip.IsUnspecified())
}

// loopbackRetryAfter reports whether a failure of this kind on a loopback
// address is worth one retry at the container host's address. Access denied
// means a database answered, so the address was right.
func loopbackRetryAfter(kind string) bool {
	switch kind {
	case KindPortClosed, KindTimeout, KindHostUnreachable:
		return true
	}
	return false
}

// DockerHostRetry is the retry address the web interface uses: the same port
// on host.docker.internal, the name a container reaches its host by (Docker
// Desktop sets it; the compose file maps it on Linux).
func DockerHostRetry(_ string, port string) string {
	port = strings.TrimSpace(port)
	if port == "" {
		port = "3306"
	}
	return net.JoinHostPort("host.docker.internal", port)
}

// WithLoopbackRetry lets a failed connection to a loopback address be retried
// once at retry(host, port), with the same user and password. Only the web
// interface passes it: it runs where a container is likely, and the command
// line reaches the host it was given and nothing else. The retry proves the
// container case, so no container detection is needed.
func WithLoopbackRetry(retry func(host, port string) string) BuildOption {
	return func(c *buildConfig) { c.loopbackRetry = retry }
}

// loopbackRetryTimeout bounds the retry. It runs only after a failure, so it
// adds to a wait the person is already in.
const loopbackRetryTimeout = 3 * time.Second

// proveLoopback retries a failed loopback connection at the retry address and
// reports the address when a database answered there, with or without letting
// the user in. "" when the host is not loopback, the kind does not qualify, or
// nothing answered.
func proveLoopback(sourceDSN, kind string, retry func(host, port string) string) string {
	if retry == nil || !loopbackRetryAfter(kind) {
		return ""
	}
	cfg, err := mysql.ParseDSN(sourceDSN)
	if err != nil {
		return ""
	}
	host, port, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		host, port = cfg.Addr, ""
	}
	if !isLoopbackHost(host) {
		return ""
	}
	alt := retry(host, port)
	if alt == "" {
		return ""
	}
	cfg.Addr = alt
	if cfg.Timeout == 0 || cfg.Timeout > loopbackRetryTimeout {
		cfg.Timeout = loopbackRetryTimeout
	}
	db, err := config.Connect(cfg.FormatDSN())
	if err == nil {
		db.Close()
		return alt
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return alt
	}
	return ""
}

// primaryKeyStatements builds one statement per table that adds a surrogate
// key, the same one the check's remediation shows. names are schema.table as
// the snapshot's classifier reports them. A name with more than one dot is
// ambiguous (schema a.b, table c, or schema a, table b.c) and gets no
// statement, since a guess would alter the wrong table; it is still named in
// Subjects.
func primaryKeyStatements(names []string) []string {
	var out []string
	for _, n := range names {
		schema, table, ok := strings.Cut(n, ".")
		if !ok || schema == "" || table == "" || strings.Contains(table, ".") {
			continue
		}
		out = append(out, "ALTER TABLE "+quoteIdent(schema)+"."+quoteIdent(table)+
			" ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;")
	}
	return out
}

// quoteIdent quotes a MySQL identifier: backticks, with a backtick inside
// doubled.
func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// portSuffix returns ":port" of a host:port address, or "" when it has none.
func portSuffix(addr string) string {
	if _, port, err := net.SplitHostPort(addr); err == nil {
		return ":" + port
	}
	return ""
}

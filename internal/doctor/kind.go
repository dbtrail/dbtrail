package doctor

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/status"
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
	// KindNotInnoDB: tables not on InnoDB, with or without a key; Subjects
	// names every one as schema.table, and Statements carries the Overview
	// card's statement for each (engine only, or engine and key).
	KindNotInnoDB = "not_innodb"
	// KindNoPrimaryKey: tables with no primary key; Subjects names every one
	// as schema.table, and Statements carries a statement per table.
	KindNoPrimaryKey = "no_primary_key"
)

// Kinds lists every kind, for tests and for a screen that wants to know what
// it may be sent.
func Kinds() []string {
	return []string{
		KindHostUnreachable, KindPortClosed, KindTimeout, KindAccessDenied,
		KindLoopbackInContainer, KindMissingPrivilege, KindBinlogSettings, KindNoPrimaryKey, KindNotInnoDB,
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
	}
	// One test covers every timeout: context.DeadlineExceeded,
	// os.ErrDeadlineExceeded (a dial or read deadline) and any dialer's own
	// error all implement net.Error with Timeout() true. A separate check for
	// the two sentinels was written here too; a mutation deleting it kept
	// every test green, because this line already answered for both.
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

// WithLoopbackRetry lets a failed connection to a loopback address be followed
// by one look at retry(host, port): a connect that reads the server's greeting
// and sends nothing — never the user, never the password (see proveLoopback).
// Only the web interface passes it: it runs where a container is likely, and
// the command line contacts the host it was given and nothing else. A greeting
// there proves the container case, so no container detection is needed.
func WithLoopbackRetry(retry func(host, port string) string) BuildOption {
	return func(c *buildConfig) { c.loopbackRetry = retry }
}

// loopbackRetryTimeout bounds the probe, name lookup included. It runs only
// after a failure, so it adds to a wait the person is already in.
const loopbackRetryTimeout = 3 * time.Second

// proveLoopback looks for a database at the retry address after a loopback
// address failed, and reports that address when one is there. "" when the
// host is not loopback, the kind does not qualify, or nothing that speaks
// MySQL answered.
//
// It sends NOTHING: no user, no password, not a single byte (#1803). The
// retry address is a name nobody typed. Docker Desktop and our compose file
// map it to the machine itself, but on a plain install nothing does, and a
// resolver that answers for it anyway (a search domain is enough) points it
// at some other machine. A login there would hand that machine the password;
// with caching_sha2 over plain TCP the driver even fetches the server's key
// and encrypts the password TO it. And the login proves nothing the greeting
// does not: a MySQL server speaks first, before any credentials, so reading
// its greeting is the whole proof. The finding then SUGGESTS the address; the
// person decides whether to use it.
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
	ctx, cancel := context.WithTimeout(context.Background(), loopbackRetryTimeout)
	defer cancel()
	if greetingAt(ctx, alt) {
		return alt
	}
	return ""
}

// loopbackRemediation says what the loopback probe KNOWS and no more: nothing
// answered at the typed address, and something that speaks MySQL answered at
// alt. Whether DBTrail runs in a container is NOT known — on a plain install a
// DNS search domain can send host.docker.internal to another machine's MySQL —
// so the advice to use alt is conditional on it, with the other case said too.
func loopbackRemediation(sourceDSN, alt string) string {
	typed := "the address you typed"
	if cfg, err := mysql.ParseDSN(sourceDSN); err == nil && cfg.Addr != "" {
		typed = cfg.Addr
	}
	return "Nothing answered at " + typed + ", but a MySQL server answered at " + alt + ".\n\n" +
		"If DBTrail runs in a container, localhost is the container itself, and that server is your machine. Use this as the host:\n\n" +
		"  " + strings.TrimSuffix(alt, portSuffix(alt)) + "\n\n" +
		"If DBTrail does not run in a container, that address belongs to another machine. Check the address of your own database instead."
}

// maxGreetingLen bounds the one packet the probe reads. A real greeting is
// about 80 bytes; anything claiming more than this is not one, and must not
// make the probe allocate what a stranger asks for.
const maxGreetingLen = 1024

// greetingAt connects to addr, reads the first packet the server sends, and
// reports whether it is a MySQL or MariaDB greeting. It writes nothing and
// closes. ctx bounds the name lookup, the connect and the read.
func greetingAt(ctx context.Context, addr string) bool {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	defer conn.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetReadDeadline(dl)
	}
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return false
	}
	n := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	if hdr[3] != 0 || n < 1 || n > maxGreetingLen {
		return false
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return false
	}
	return isMySQLGreeting(payload)
}

// isMySQLGreeting recognises the first packet a MySQL or MariaDB server
// sends. Two shapes count:
//   - a handshake, protocol 10: a printable version string ended by a zero
//     byte, then at least the connection id, the first part of the salt and
//     its filler (13 bytes);
//   - an error packet (0xff and a two-byte error code), which is how a
//     server refuses a client's HOST before any login ("Host ... is not
//     allowed to connect", 1130). It is still a MySQL server answering.
func isMySQLGreeting(p []byte) bool {
	switch p[0] {
	case 0x0a:
		end := -1
		for i := 1; i < len(p); i++ {
			if p[i] == 0 {
				end = i
				break
			}
			if p[i] < 0x20 || p[i] > 0x7e {
				return false
			}
		}
		return end > 1 && len(p)-end-1 >= 13
	case 0xff:
		if len(p) < 3 {
			return false
		}
		code := int(p[1]) | int(p[2])<<8
		return code >= 1000 && code < 6000
	}
	return false
}

// primaryKeyStatement is the statement that makes a refused table capturable
// (a key, the InnoDB engine, or both, by its reason).
// It is the Overview card's own FixSQL (#1802), fed the same reason and the
// same column name the snapshot would record, so the setup check and the card
// can never hand somebody two different statements for one table. In
// particular it never guesses `id`: a key-less table very often already HAS a
// plain `id` column, and a statement that dies with ERROR 1060 is worse than
// none. The column name is metadata.SuggestPKColumn's, over the table's own
// columns.
func primaryKeyStatement(rt metadata.RefusedTable) string {
	return status.UncapturedTable{Schema: rt.Schema, Table: rt.Table, Reason: rt.Reason, PKColumn: rt.PKColumn}.FixSQL()
}

// portSuffix returns ":port" of a host:port address, or "" when it has none.
func portSuffix(addr string) string {
	if _, port, err := net.SplitHostPort(addr); err == nil {
		return ":" + port
	}
	return ""
}

package config

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// CurrentBinlogPosition returns the current binlog file name and position
// from the source server. It tries SHOW BINARY LOG STATUS first (MySQL 8.4+)
// and falls back to SHOW MASTER STATUS for 5.7 / 8.0 / Percona <8.4 where the
// new statement does not exist. When both fail, the wrapped error includes
// both diagnostics so the operator does not chase the wrong syntax.
//
// When binary logging is disabled on the source (log_bin=OFF), the statement
// that exists on this server version returns an empty resultset (sql.ErrNoRows
// from Scan), while the statement that doesn't exist returns syntax error 1064
// — never both ErrNoRows. The two real-world shapes are:
//
//   - 5.7/8.0/Percona<8.4 + log_bin=OFF: SHOW BINARY LOG STATUS → 1064,
//     SHOW MASTER STATUS → ErrNoRows
//   - 8.4+ + log_bin=OFF: SHOW BINARY LOG STATUS → ErrNoRows,
//     SHOW MASTER STATUS → 1064 (removed in 8.4.0)
//
// So when EITHER branch returns ErrNoRows we know log_bin=OFF and emit a
// domain-specific error. Privileges missing surfaces as 1227 (ER_SPECIFIC_
// ACCESS_DENIED_ERROR), not ErrNoRows, so false-positive risk is essentially
// nil.
func CurrentBinlogPosition(db *sql.DB) (file string, pos uint32, err error) {
	file, pos, err = scanBinlogStatus(db, "SHOW BINARY LOG STATUS")
	if err == nil {
		return file, pos, nil
	}
	firstErr := err
	file, pos, err = scanBinlogStatus(db, "SHOW MASTER STATUS")
	if err != nil {
		if errors.Is(firstErr, sql.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
			return "", 0, fmt.Errorf("current binlog position empty — log_bin appears to be OFF on the source server (run \"SHOW VARIABLES LIKE 'log_bin'\" to confirm)")
		}
		return "", 0, fmt.Errorf("SHOW BINARY LOG STATUS / SHOW MASTER STATUS: %w (fallback: %w)", firstErr, err)
	}
	return file, pos, nil
}

// scanBinlogStatus runs a SHOW … STATUS statement and extracts the binlog file
// and position from its first two columns. The remaining columns are discarded,
// which makes the read tolerant to the column-count difference across flavors:
// MySQL 5.7/8.0 and 8.4 return five columns (… Executed_Gtid_Set), MariaDB
// returns four (no Executed_Gtid_Set). Only File and Position are needed.
//
// An empty resultset (log_bin=OFF) surfaces as sql.ErrNoRows so the caller's
// OFF-detection keeps working unchanged; statement-not-found surfaces as the
// driver's syntax error (1064).
func scanBinlogStatus(db *sql.DB, stmt string) (file string, pos uint32, err error) {
	rows, err := db.Query(stmt)
	if err != nil {
		return "", 0, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", 0, err
	}
	if len(cols) < 2 {
		return "", 0, fmt.Errorf("%s returned %d column(s), expected at least File and Position", stmt, len(cols))
	}
	if !rows.Next() {
		if rerr := rows.Err(); rerr != nil {
			return "", 0, rerr
		}
		return "", 0, sql.ErrNoRows
	}

	dest := make([]any, len(cols))
	dest[0] = &file
	dest[1] = &pos
	for i := 2; i < len(cols); i++ {
		dest[i] = new(sql.RawBytes)
	}
	if err := rows.Scan(dest...); err != nil {
		return "", 0, err
	}
	return file, pos, nil
}

// CurrentGTIDExecuted returns the source server's executed GTID set
// (@@GLOBAL.gtid_executed) when GTID mode is fully enabled, for first-run
// start-position auto-discovery (the GTID sibling of CurrentBinlogPosition).
//
// It returns "" (no error) in two cases the caller must fall back to
// position-based discovery for:
//
//   - @@GLOBAL.gtid_mode is anything other than "ON" (OFF, OFF_PERMISSIVE,
//     ON_PERMISSIVE): the executed set is absent or may not cover every
//     transaction, so it cannot anchor GTID replication. Note gtid_executed
//     can be non-empty even with gtid_mode=OFF (a server that once ran with
//     GTIDs on), which is why gtid_mode gates the read.
//   - the executed set is empty (fresh server, zero transactions): starting
//     GTID replication from an empty set replays the binlog from the very
//     beginning, which is NOT the "start from now" contract of first-run
//     auto-discovery.
//
// MySQL formats gtid_executed with '\n' between UUID blocks; all whitespace
// is stripped so the value is usable as a replication start set as-is.
//
// A server without the gtid_mode variable at all — Error 1193
// ER_UNKNOWN_SYSTEM_VARIABLE, which is what MariaDB returns — also yields
// ("", nil): such a server structurally cannot supply a MySQL GTID set, so
// falling back to position discovery is correct. This matters because a
// MariaDB source can reach this call under the DEFAULT mysql flavor
// (streamrun gates on the configured flavor, and e.g. `bintrail-console
// watch` exposes no --source-flavor for its main source); a hard failure
// here would crash-loop that daemon at startup. Genuine connection/query
// failures remain errors — the caller treats them as fatal by design.
func CurrentGTIDExecuted(db *sql.DB) (string, error) {
	var gtidMode string
	if err := db.QueryRow("SELECT @@GLOBAL.gtid_mode").Scan(&gtidMode); err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1193 { // ER_UNKNOWN_SYSTEM_VARIABLE
			return "", nil
		}
		return "", fmt.Errorf("SELECT @@GLOBAL.gtid_mode: %w", err)
	}
	if !strings.EqualFold(gtidMode, "ON") {
		return "", nil
	}
	var executed string
	if err := db.QueryRow("SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
		return "", fmt.Errorf("SELECT @@GLOBAL.gtid_executed: %w", err)
	}
	// strings.Fields splits on any whitespace (spaces, '\n', tabs); joining
	// with "" removes it all.
	set := strings.Join(strings.Fields(executed), "")
	if set == "" {
		// The "" return sends the caller down position-mode discovery; on a
		// GTID-enabled source that has a user-visible consequence worth naming.
		slog.Warn("source is gtid_mode=ON but @@GLOBAL.gtid_executed is empty; " +
			"starting in position mode — live-source verify will stay inconclusive " +
			"until the stream is restarted with --start-gtid")
	}
	return set, nil
}

// defaultTimeout is the TCP connect timeout applied when the DSN does not
// specify one. Prevents indefinite hangs when MySQL is unreachable.
const defaultTimeout = 10 * time.Second

// Connect opens and verifies a MySQL connection using the given DSN.
// parseTime=true is always injected so DATETIME columns scan into time.Time.
// A 10-second TCP connect timeout is applied when the DSN does not specify one.
// The caller is responsible for closing the returned *sql.DB.
func Connect(dsn string) (*sql.DB, error) {
	cfg, err := normalizeDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}
	if err := pingBounded(db, cfg.Timeout); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}
	return db, nil
}

// pingBounded verifies a freshly opened connection within the DSN's own connect
// budget (cfg.Timeout, defaultTimeout when the DSN sets none).
//
// A bare db.Ping() is UNBOUNDED, and cfg.Timeout is the DIAL timeout only —
// normalizeDSN sets no ReadTimeout or WriteTimeout anywhere. So against a peer
// that completes the TCP handshake and then never speaks MySQL (a frozen VM, an
// idle-dropped NLB) the dial succeeds and the driver then blocks in
// readHandshakePacket forever. That made opening a connection the one step of a
// stream restart that could never return, so a supervisor could neither count
// the attempt nor give up: the daemon stayed alive with capture dead and
// /api/healthz green (#1482). The driver installs its context watcher BEFORE the
// handshake read (connector.Connect), so a deadline here really does cut it.
//
// Scoped to the connect phase deliberately. It never becomes a read deadline on
// a pooled connection, so a legitimately long query — a partition scan, a schema
// migration — is untouched, which is why this is not a DSN-level ReadTimeout.
// Operators who need a longer handshake budget already have the knob: timeout=
// in the DSN.
func pingBounded(db *sql.DB, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return db.PingContext(ctx)
}

// driverDefaultMaxAllowedPacket is the client-side max_allowed_packet the
// go-sql-driver assigns when a DSN omits the parameter. Read from the driver
// itself (via NewConfig) so it tracks any change to that default instead of
// hardcoding 64 MiB.
var driverDefaultMaxAllowedPacket = mysql.NewConfig().MaxAllowedPacket

// buildDSN applies bintrail's connection invariants to a user DSN and returns
// the normalized DSN string. Split out from Connect so the invariants are unit
// testable without a live server. Invariants: parseTime=true (DATETIME scans
// into time.Time), Loc=UTC, a default connect timeout, and aligning the
// client's max_allowed_packet with the server's.
func buildDSN(dsn string) (string, error) {
	cfg, err := normalizeDSN(dsn)
	if err != nil {
		return "", err
	}
	return cfg.FormatDSN(), nil
}

// normalizeDSN parses a user DSN and applies the same invariants as buildDSN,
// returning the *mysql.Config so callers that need to attach a programmatic
// *tls.Config (ConnectWithTLS) share the exact invariants without a DSN-string
// round-trip.
// ForbidLocalFiles turns the driver's local-file access (LOAD DATA LOCAL
// INFILE) off on cfg, whatever the DSN asked for. bintrail never loads a local
// file into MySQL, so a DSN that allows it is never a need, and every MySQL
// connection bintrail opens goes through this package to get it applied.
func ForbidLocalFiles(cfg *mysql.Config) { cfg.AllowAllFiles = false }

// OpenMySQL opens cfg with local-file access forced off and nothing else
// changed, for the callers that must not take Connect's other invariants:
// ProxySQL's admin interface (no @@max_allowed_packet to probe), and the
// server-level connections that create the index database. cfg itself is
// left as the caller passed it.
func OpenMySQL(cfg *mysql.Config) (*sql.DB, error) {
	return sql.Open("mysql", openDSN(cfg))
}

func openDSN(cfg *mysql.Config) string {
	c := cfg.Clone()
	ForbidLocalFiles(c)
	return c.FormatDSN()
}

func normalizeDSN(dsn string) (*mysql.Config, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN: %w", err)
	}
	ForbidLocalFiles(cfg)
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}
	// Align the client's max_allowed_packet with the server's instead of the
	// go-sql-driver's fixed 64 MiB default. binlog_events stores full
	// before/after row images as JSON, and a large BLOB/JSON value
	// (base64-inflated ~1.33×) can exceed 64 MiB. The rejection #652 reproduced
	// is server-side (Error 1105 "… mysql_send_long_data() … longer than
	// 'max_allowed_packet'"), so raising the *server* limit (docker-compose
	// --max-allowed-packet=1G) is the load-bearing change; maxAllowedPacket=0
	// makes the driver fetch @@max_allowed_packet from the server and size to it
	// (bundled index MySQL: 1 GiB; a BYO index: whatever it set) so the client
	// tracks the server rather than imposing its own cap.
	//
	// This is a ceiling raise, not a fix: events larger than the server's
	// max_allowed_packet are still rejected, and that rejection must — but does
	// not yet (#652) — fail loud rather than drop silently.
	//
	// Precedence: an explicit non-default maxAllowedPacket in the DSN is
	// preserved. We compare against the driver's own default rather than
	// string-matching the DSN, so a value the case-sensitive driver ignores
	// (e.g. a mis-cased param) falls through to the safe server-honoring 0
	// instead of being silently left at the 64 MiB cap.
	if cfg.MaxAllowedPacket == driverDefaultMaxAllowedPacket {
		cfg.MaxAllowedPacket = 0
	}
	return cfg, nil
}

// ConnectWithTLS opens and verifies a MySQL connection like Connect, but also
// attaches a programmatic TLS configuration to the connection. tlsCfg == nil
// means no TLS (plaintext), identical to Connect.
//
// For the programmatic tlsCfg path the connection FAILS (it never silently falls
// back to plaintext) if the server cannot satisfy the requested TLS — this
// function never sets the driver's own AllowFallbackToPlaintext. A caller that
// wants an opportunistic fallback must detect the failure and retry with a nil
// tlsCfg itself, so any cleartext downgrade is an explicit, loggable decision
// (#946/#947). (An operator who puts tls=preferred in the DSN itself opts into
// the driver's own silent fallback — the DSN-precedence rule below hands that
// case to the driver by design.)
//
// An explicit tls= parameter already present in the DSN takes precedence: if the
// parsed config already carries any TLS setting, tlsCfg is ignored so an
// operator's own DSN choice always wins.
//
// Used by the stream path (internal/streamrun) so --ssl-mode protects the index
// write connection (full row images = PII, plus index credentials) and the
// source helper connection, not only the binlog replication stream (#946).
func ConnectWithTLS(dsn string, tlsCfg *tls.Config) (*sql.DB, error) {
	cfg, err := normalizeDSN(dsn)
	if err != nil {
		return nil, err
	}
	applyTLS(cfg, tlsCfg)

	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create MySQL connector: %w", err)
	}
	db := sql.OpenDB(connector)
	if err := pingBounded(db, cfg.Timeout); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}
	return db, nil
}

// applyTLS attaches tlsCfg to cfg unless the DSN already specified TLS (either a
// *tls.Config or a tls= name/mode), in which case the operator's choice wins.
// Split out so the precedence rule is unit-testable without a live server.
func applyTLS(cfg *mysql.Config, tlsCfg *tls.Config) {
	if tlsCfg == nil || cfg.TLS != nil || cfg.TLSConfig != "" {
		return
	}
	cfg.TLS = tlsCfg
}

// DSNHost returns the TCP host of a MySQL DSN, for use as a TLS ServerName
// (verify-identity only). It is best-effort and never errors: an unparseable
// DSN or a unix-socket DSN yields "" (hostname verification is meaningless over
// a socket), and the subsequent connect surfaces any real DSN error with proper
// context. Unlike ParseSourceDSN it imposes no TCP/port requirement, so it is
// safe to call on an index DSN that may legitimately use a socket.
func DSNHost(dsn string) string {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || strings.EqualFold(cfg.Net, "unix") {
		return ""
	}
	if h, _, err := net.SplitHostPort(cfg.Addr); err == nil {
		return h
	}
	return cfg.Addr
}

// DSNHasExplicitTLS reports whether the DSN sets its own tls= parameter (any
// value). Used to warn when a DSN's own TLS choice silently overrides a stronger
// --ssl-mode on the same connection (#946).
func DSNHasExplicitTLS(dsn string) bool {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return false
	}
	return cfg.TLSConfig != "" || cfg.TLS != nil
}

// ParseSourceDSN decomposes a go-sql-driver DSN into host, port, user, and
// password. It requires a TCP address and rejects unix-socket DSNs — binlog
// replication (BinlogSyncerConfig) and remote dumps both need a host:port.
func ParseSourceDSN(dsn string) (host string, port uint16, user, password string, err error) {
	cfg, parseErr := mysql.ParseDSN(dsn)
	if parseErr != nil {
		return "", 0, "", "", fmt.Errorf("invalid --source-dsn: %w", parseErr)
	}
	if strings.EqualFold(cfg.Net, "unix") {
		return "", 0, "", "", fmt.Errorf("--source-dsn uses a unix socket; binlog replication requires a TCP address")
	}
	h, p, splitErr := net.SplitHostPort(cfg.Addr)
	if splitErr != nil {
		return "", 0, "", "", fmt.Errorf("invalid address in --source-dsn %q: %w", cfg.Addr, splitErr)
	}
	portN, convErr := strconv.ParseUint(p, 10, 16)
	if convErr != nil {
		return "", 0, "", "", fmt.Errorf("invalid port in --source-dsn: %w", convErr)
	}
	return h, uint16(portN), cfg.User, cfg.Passwd, nil
}

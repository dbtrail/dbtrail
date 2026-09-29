package config

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// TestConnect_locUTCApplied verifies that Connect injects Loc=UTC, overriding
// any loc the user may have specified in their DSN.
func TestConnect_locUTCApplied(t *testing.T) {
	// Simulate the path Connect takes for a DSN with loc=Local.
	cfg, err := mysql.ParseDSN("root:pass@tcp(127.0.0.1:3306)/test?loc=Local")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC // Connect always forces this

	if cfg.Loc != time.UTC {
		t.Errorf("expected Loc=UTC, got %v", cfg.Loc)
	}
}

// TestBuildDSN_honorsServerMaxAllowedPacket verifies that buildDSN sets
// MaxAllowedPacket=0 (fetch the limit from the server) instead of leaving the
// driver's 64 MiB client-side default, so large row images up to the index
// server's configured ceiling can be written. It re-parses the output to assert
// on the resolved config. ParseTime and Timeout are meaningful checks here
// (ParseDSN defaults them to false / 0); the Loc override is verified
// separately in TestBuildDSN_forcesLocUTC (a plain DSN can't prove it because
// ParseDSN already defaults Loc to UTC).
func TestBuildDSN_honorsServerMaxAllowedPacket(t *testing.T) {
	out, err := buildDSN("root:pass@tcp(127.0.0.1:3306)/bintrail_index")
	if err != nil {
		t.Fatalf("buildDSN: %v", err)
	}
	cfg, err := mysql.ParseDSN(out)
	if err != nil {
		t.Fatalf("re-parse buildDSN output %q: %v", out, err)
	}
	if cfg.MaxAllowedPacket != 0 {
		t.Errorf("expected MaxAllowedPacket=0 (honor server limit), got %d", cfg.MaxAllowedPacket)
	}
	if !cfg.ParseTime {
		t.Error("expected ParseTime=true preserved")
	}
	if cfg.Timeout != defaultTimeout {
		t.Errorf("expected default Timeout=%v applied, got %v", defaultTimeout, cfg.Timeout)
	}
}

// TestBuildDSN_forcesLocUTC verifies buildDSN overrides a user-supplied loc
// with UTC. The input sets loc=Local (which survives a FormatDSN round-trip
// unless overridden), so the assertion would fail if the cfg.Loc=UTC line were
// removed — unlike a plain DSN, where ParseDSN already defaults Loc to UTC.
func TestBuildDSN_forcesLocUTC(t *testing.T) {
	out, err := buildDSN("root:pass@tcp(127.0.0.1:3306)/db?loc=Local")
	if err != nil {
		t.Fatalf("buildDSN: %v", err)
	}
	cfg, err := mysql.ParseDSN(out)
	if err != nil {
		t.Fatalf("re-parse buildDSN output %q: %v", out, err)
	}
	if cfg.Loc != time.UTC {
		t.Errorf("expected Loc forced to UTC, got %v", cfg.Loc)
	}
}

// TestBuildDSN_explicitMaxAllowedPacketWins verifies precedence: an explicit,
// non-default maxAllowedPacket in the user's DSN is not overridden by the
// server-honoring default. Matches the project's CLI flag > env > default
// precedence ethos. 33554432 (32 MiB) differs from both the driver default
// (67108864) and the server-honoring 0, so it genuinely discriminates.
func TestBuildDSN_explicitMaxAllowedPacketWins(t *testing.T) {
	out, err := buildDSN("root:pass@tcp(127.0.0.1:3306)/db?maxAllowedPacket=33554432")
	if err != nil {
		t.Fatalf("buildDSN: %v", err)
	}
	cfg, err := mysql.ParseDSN(out)
	if err != nil {
		t.Fatalf("re-parse buildDSN output %q: %v", out, err)
	}
	if cfg.MaxAllowedPacket != 33554432 {
		t.Errorf("explicit DSN maxAllowedPacket should be preserved, got %d", cfg.MaxAllowedPacket)
	}
}

// TestBuildDSN_miscasedParamHonorsServer guards the precedence logic against the
// casing class: a mis-cased maxAllowedPacket param is NOT recognized by the
// case-sensitive go-sql-driver (it leaves MaxAllowedPacket at the default), so
// buildDSN must fall through to the safe server-honoring 0 rather than silently
// leave the client capped at 64 MiB. A substring/case-insensitive guard would
// regress this.
func TestBuildDSN_miscasedParamHonorsServer(t *testing.T) {
	out, err := buildDSN("root:pass@tcp(127.0.0.1:3306)/db?MaxAllowedPacket=33554432")
	if err != nil {
		t.Fatalf("buildDSN: %v", err)
	}
	cfg, err := mysql.ParseDSN(out)
	if err != nil {
		t.Fatalf("re-parse buildDSN output %q: %v", out, err)
	}
	if cfg.MaxAllowedPacket != 0 {
		t.Errorf("mis-cased param (ignored by the driver) should honor the server (0), got %d", cfg.MaxAllowedPacket)
	}
}

// TestBuildDSN_credentialSubstringHonorsServer guards against a regression to
// substring-matching the whole DSN: a password that contains the literal
// "maxAllowedPacket" must NOT suppress the server-honoring default.
func TestBuildDSN_credentialSubstringHonorsServer(t *testing.T) {
	out, err := buildDSN("root:maxAllowedPacketX@tcp(127.0.0.1:3306)/db")
	if err != nil {
		t.Fatalf("buildDSN: %v", err)
	}
	cfg, err := mysql.ParseDSN(out)
	if err != nil {
		t.Fatalf("re-parse buildDSN output %q: %v", out, err)
	}
	if cfg.MaxAllowedPacket != 0 {
		t.Errorf("a credential containing the param name must not suppress server-honoring (0), got %d", cfg.MaxAllowedPacket)
	}
}

// TestBuildDSN_invalid surfaces a parse error rather than panicking.
func TestBuildDSN_invalid(t *testing.T) {
	if _, err := buildDSN("not-a-valid-dsn::::"); err == nil {
		t.Error("expected error for invalid DSN, got nil")
	}
}

// binlogStatusColumns matches the layout SHOW BINARY LOG STATUS /
// SHOW MASTER STATUS returns: File, Position, Binlog_Do_DB,
// Binlog_Ignore_DB, Executed_Gtid_Set. CurrentBinlogPosition scans
// the first two into typed targets and the rest into string sinks.
var binlogStatusColumns = []string{
	"File",
	"Position",
	"Binlog_Do_DB",
	"Binlog_Ignore_DB",
	"Executed_Gtid_Set",
}

// TestCurrentBinlogPosition_NewStatementHappyPath exercises the
// MySQL 8.4 path where SHOW BINARY LOG STATUS returns a row.
func TestCurrentBinlogPosition_NewStatementHappyPath(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnRows(
		sqlmock.NewRows(binlogStatusColumns).
			AddRow("binlog.000042", uint32(12345), "", "", ""))

	file, pos, err := CurrentBinlogPosition(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if file != "binlog.000042" {
		t.Errorf("file = %q, want %q", file, "binlog.000042")
	}
	if pos != 12345 {
		t.Errorf("pos = %d, want 12345", pos)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestCurrentBinlogPosition_FallbackHappyPath exercises the pre-8.4
// path: the new statement returns a syntax error (1064), then
// SHOW MASTER STATUS returns a row.
func TestCurrentBinlogPosition_FallbackHappyPath(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnError(&mysql.MySQLError{
		Number:  1064,
		Message: "You have an error in your SQL syntax",
	})
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(
		sqlmock.NewRows(binlogStatusColumns).
			AddRow("mysql-bin.000007", uint32(987), "", "", ""))

	file, pos, err := CurrentBinlogPosition(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if file != "mysql-bin.000007" {
		t.Errorf("file = %q, want %q", file, "mysql-bin.000007")
	}
	if pos != 987 {
		t.Errorf("pos = %d, want 987", pos)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestCurrentBinlogPosition_MariaDBFourColumns covers a MariaDB source:
// SHOW BINARY LOG STATUS does not exist there (syntax error 1064), and
// SHOW MASTER STATUS returns only FOUR columns — File, Position, Binlog_Do_DB,
// Binlog_Ignore_DB — because MariaDB has no Executed_Gtid_Set column. The scan
// must tolerate the differing column count and still extract File+Position.
// Reproduces the alpha smoke-test failure "expected 4 destination arguments in
// Scan, not 5" against real MariaDB 11.4.
func TestCurrentBinlogPosition_MariaDBFourColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnError(&mysql.MySQLError{
		Number:  1064,
		Message: "You have an error in your SQL syntax near 'LOG STATUS'",
	})
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
			AddRow("mariadb-bin.000002", uint32(1483), "", ""))

	file, pos, err := CurrentBinlogPosition(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if file != "mariadb-bin.000002" {
		t.Errorf("file = %q, want %q", file, "mariadb-bin.000002")
	}
	if pos != 1483 {
		t.Errorf("pos = %d, want 1483", pos)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestCurrentBinlogPosition_LogBinOff covers the real-world shape of
// log_bin=OFF on every supported MySQL/Percona line. The crash-loop
// in #325 surfaced as an asymmetric error pair — exactly one branch
// returns ErrNoRows (empty resultset because log_bin=OFF) and the
// other returns 1064 (statement doesn't exist on that version):
//
//   - 5.7 / 8.0 / Percona <8.4: SHOW BINARY LOG STATUS → 1064,
//     SHOW MASTER STATUS → ErrNoRows.
//   - 8.4+: SHOW BINARY LOG STATUS → ErrNoRows, SHOW MASTER STATUS →
//     1064 (removed in 8.4.0).
//
// Plus the degenerate sanity case where sqlmock simulates both empty.
// All three must hit the domain error.
func TestCurrentBinlogPosition_LogBinOff(t *testing.T) {
	cases := []struct {
		name      string
		firstErr  error
		firstRows *sqlmock.Rows
		fbErr     error
		fbRows    *sqlmock.Rows
	}{
		{
			name:     "pre_8.4 with log_bin=OFF: new statement is 1064, old returns empty",
			firstErr: &mysql.MySQLError{Number: 1064, Message: "syntax error near 'BINARY LOG STATUS'"},
			fbRows:   sqlmock.NewRows(binlogStatusColumns),
		},
		{
			name:      "8.4+ with log_bin=OFF: new returns empty, old is 1064",
			firstRows: sqlmock.NewRows(binlogStatusColumns),
			fbErr:     &mysql.MySQLError{Number: 1064, Message: "syntax error near 'MASTER STATUS'"},
		},
		{
			name:      "both empty (sqlmock degenerate sanity)",
			firstRows: sqlmock.NewRows(binlogStatusColumns),
			fbRows:    sqlmock.NewRows(binlogStatusColumns),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			firstExp := mock.ExpectQuery("SHOW BINARY LOG STATUS")
			if tc.firstErr != nil {
				firstExp.WillReturnError(tc.firstErr)
			} else {
				firstExp.WillReturnRows(tc.firstRows)
			}
			fbExp := mock.ExpectQuery("SHOW MASTER STATUS")
			if tc.fbErr != nil {
				fbExp.WillReturnError(tc.fbErr)
			} else {
				fbExp.WillReturnRows(tc.fbRows)
			}

			_, _, err = CurrentBinlogPosition(db)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			msg := err.Error()
			if !strings.Contains(msg, "log_bin") {
				t.Errorf("error message does not mention log_bin: %q", msg)
			}
			if !strings.Contains(msg, "SHOW VARIABLES") {
				t.Errorf("error message does not point at the remediation query: %q", msg)
			}
			// Domain error must not leak the generic fallback wrap or
			// raw ErrNoRows — operators would chase those fruitlessly.
			if strings.Contains(msg, "fallback:") {
				t.Errorf("domain error should not include 'fallback:' wrap: %q", msg)
			}
			if strings.Contains(msg, "no rows in result set") {
				t.Errorf("domain error should not leak raw sql.ErrNoRows: %q", msg)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

// TestCurrentBinlogPosition_BothNonErrNoRowsFail is the scope-creep
// guard: if both statements fail with non-ErrNoRows errors (e.g.
// connection drop, permission denied), the caller should still get
// the existing combined-error diagnostic so they don't lose the
// underlying detail.
func TestCurrentBinlogPosition_BothNonErrNoRowsFail(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	firstErr := errors.New("connection reset by peer")
	secondErr := errors.New("permission denied")

	mock.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnError(firstErr)
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnError(secondErr)

	_, _, err = CurrentBinlogPosition(db)
	if err == nil {
		t.Fatal("expected error when both statements fail, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "SHOW BINARY LOG STATUS / SHOW MASTER STATUS") {
		t.Errorf("error message missing combined diagnostic header: %q", msg)
	}
	if !strings.Contains(msg, "connection reset by peer") {
		t.Errorf("error message lost first diagnostic: %q", msg)
	}
	if !strings.Contains(msg, "permission denied") {
		t.Errorf("error message lost fallback diagnostic: %q", msg)
	}
	if strings.Contains(msg, "log_bin") {
		t.Errorf("error message should not mention log_bin for non-ErrNoRows failures: %q", msg)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// ─── ParseSourceDSN ───────────────────────────────────────────────────────────

func TestParseSourceDSN_tcp(t *testing.T) {
	dsn := "root:secret@tcp(db.example.com:3306)/mydb"
	host, port, user, pass, err := ParseSourceDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if host != "db.example.com" {
		t.Errorf("host: expected db.example.com, got %q", host)
	}
	if port != 3306 {
		t.Errorf("port: expected 3306, got %d", port)
	}
	if user != "root" {
		t.Errorf("user: expected root, got %q", user)
	}
	if pass != "secret" {
		t.Errorf("password: expected secret, got %q", pass)
	}
}

func TestParseSourceDSN_noPassword(t *testing.T) {
	dsn := "repl@tcp(127.0.0.1:13306)/"
	host, port, user, pass, err := ParseSourceDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if host != "127.0.0.1" {
		t.Errorf("host: expected 127.0.0.1, got %q", host)
	}
	if port != 13306 {
		t.Errorf("port: expected 13306, got %d", port)
	}
	if user != "repl" {
		t.Errorf("user: expected repl, got %q", user)
	}
	if pass != "" {
		t.Errorf("password: expected empty, got %q", pass)
	}
}

func TestParseSourceDSN_unixSocket(t *testing.T) {
	dsn := "root@unix(/var/run/mysqld/mysqld.sock)/test"
	_, _, _, _, err := ParseSourceDSN(dsn)
	if err == nil {
		t.Error("expected error for unix socket DSN, got nil")
	}
}

func TestParseSourceDSN_invalid(t *testing.T) {
	_, _, _, _, err := ParseSourceDSN("not-a-valid-dsn::::")
	if err == nil {
		t.Error("expected error for invalid DSN, got nil")
	}
}

// TestParseSourceDSN_ipv6 verifies IPv6 addresses are parsed correctly.
func TestParseSourceDSN_ipv6(t *testing.T) {
	dsn := "root:pw@tcp([::1]:3306)/db"
	host, port, _, _, err := ParseSourceDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error for IPv6 DSN: %v", err)
	}
	if host != "::1" {
		t.Errorf("host: expected ::1, got %q", host)
	}
	if port != 3306 {
		t.Errorf("port: expected 3306, got %d", port)
	}
}

// TestParseSourceDSN_portOutOfRange verifies that a port above the uint16 max
// (65535) is rejected. go-mysql-driver accepts it syntactically, but
// ParseSourceDSN uses strconv.ParseUint with bitSize=16 to catch it.
func TestParseSourceDSN_portOutOfRange(t *testing.T) {
	dsn := "root@tcp(localhost:65536)/"
	_, _, _, _, err := ParseSourceDSN(dsn)
	if err == nil {
		t.Error("expected error for port 65536 (exceeds uint16 max), got nil")
	}
	if !strings.Contains(err.Error(), "port") {
		t.Errorf("expected 'port' in error message, got: %v", err)
	}
}

// ─── CurrentGTIDExecuted ────────────────────────────────────────────────────

// TestCurrentGTIDExecuted_onStripsWhitespace verifies the happy path:
// gtid_mode=ON returns @@GLOBAL.gtid_executed with ALL whitespace stripped —
// MySQL formats the variable with '\n' between UUID blocks, and the value
// must be usable as a replication start set as-is.
func TestCurrentGTIDExecuted_onStripsWhitespace(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT @@GLOBAL.gtid_mode`).WillReturnRows(
		sqlmock.NewRows([]string{"@@GLOBAL.gtid_mode"}).AddRow("ON"))
	mock.ExpectQuery(`SELECT @@GLOBAL.gtid_executed`).WillReturnRows(
		sqlmock.NewRows([]string{"@@GLOBAL.gtid_executed"}).
			AddRow("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77,\n8f9e146f-0000-1111-2222-333344445555:1-5"))

	got, err := CurrentGTIDExecuted(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77,8f9e146f-0000-1111-2222-333344445555:1-5"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestCurrentGTIDExecuted_gtidModeNotOn verifies every non-ON gtid_mode
// (OFF, OFF_PERMISSIVE, ON_PERMISSIVE) returns "" with NO error and never
// queries gtid_executed. gtid_executed can be non-empty even with
// gtid_mode=OFF (a server that once ran with GTIDs on), so gating on
// gtid_mode — not on the set being non-empty — is load-bearing.
func TestCurrentGTIDExecuted_gtidModeNotOn(t *testing.T) {
	for _, modeVal := range []string{"OFF", "OFF_PERMISSIVE", "ON_PERMISSIVE"} {
		t.Run(modeVal, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			mock.ExpectQuery(`SELECT @@GLOBAL.gtid_mode`).WillReturnRows(
				sqlmock.NewRows([]string{"@@GLOBAL.gtid_mode"}).AddRow(modeVal))

			got, err := CurrentGTIDExecuted(db)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != "" {
				t.Errorf("got %q, want empty for gtid_mode=%s", got, modeVal)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("gtid_executed must not be queried when gtid_mode is not ON: %v", err)
			}
		})
	}
}

// TestCurrentGTIDExecuted_emptyExecutedSet verifies a fresh server
// (gtid_mode=ON, zero transactions) returns "" with no error, so the caller
// falls back to position discovery instead of replaying the binlog from the
// beginning.
func TestCurrentGTIDExecuted_emptyExecutedSet(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT @@GLOBAL.gtid_mode`).WillReturnRows(
		sqlmock.NewRows([]string{"@@GLOBAL.gtid_mode"}).AddRow("ON"))
	mock.ExpectQuery(`SELECT @@GLOBAL.gtid_executed`).WillReturnRows(
		sqlmock.NewRows([]string{"@@GLOBAL.gtid_executed"}).AddRow(""))

	got, err := CurrentGTIDExecuted(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "" {
		t.Errorf("got %q, want empty for an empty executed set", got)
	}
}

// TestCurrentGTIDExecuted_unknownVariableIsNotAnError covers a server without
// the gtid_mode variable at all — Error 1193 ER_UNKNOWN_SYSTEM_VARIABLE, which
// is what MariaDB returns. Such a server structurally cannot supply a MySQL
// GTID set, so the result is ("", nil): position fallback, not a fatal error.
// Capture now detects the flavor before this runs, but the function must stay
// safe on its own: a declared mysql flavor accepted after a failed detection
// can still point it at a MariaDB, and a hard failure here would crash-loop
// that daemon at startup.
func TestCurrentGTIDExecuted_unknownVariableIsNotAnError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT @@GLOBAL.gtid_mode`).WillReturnError(&mysql.MySQLError{
		Number:  1193,
		Message: "Unknown system variable 'gtid_mode'",
	})

	got, err := CurrentGTIDExecuted(db)
	if err != nil {
		t.Fatalf("expected 1193 to map to a clean position fallback, got error: %v", err)
	}
	if got != "" {
		t.Errorf("got %q, want empty for a server without gtid_mode", got)
	}
}

// TestCurrentGTIDExecuted_queryErrorPropagates verifies a query failure is a
// real error (the streamrun caller treats it as fatal — it must not silently
// degrade a GTID source to position mode).
func TestCurrentGTIDExecuted_queryErrorPropagates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT @@GLOBAL.gtid_mode`).WillReturnError(
		errors.New("connection reset"))

	if _, err := CurrentGTIDExecuted(db); err == nil {
		t.Fatal("expected error, got nil")
	}
}

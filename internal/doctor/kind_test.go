package doctor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
	"syscall"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
)

// The kinds are what a screen switches on (#1803), so every one of them is
// pinned against a REAL failure where one can be produced on any machine, and
// against a wrapped synthetic one where it cannot. A classifier tested only on
// errors built by hand passes while the driver wraps the real one somewhere
// errors.As cannot see.

// closedPort returns a loopback port nothing listens on: bound, then closed.
func closedPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(l.Addr().String())
	l.Close()
	return port
}

// silentPort returns a loopback port that accepts connections and never
// speaks: the MySQL handshake read waits until the connect budget runs out.
func silentPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var held []net.Conn
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			held = append(held, c)
		}
	}()
	t.Cleanup(func() {
		l.Close()
		for _, c := range held {
			c.Close()
		}
	})
	_, port, _ := net.SplitHostPort(l.Addr().String())
	return port
}

func connectErr(t *testing.T, dsn string) error {
	t.Helper()
	db, err := config.Connect(dsn)
	if err == nil {
		db.Close()
		t.Fatalf("connecting to %s succeeded; the test needs it to fail", dsn)
	}
	return err
}

func TestClassifyConnectError_realFailures(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
		want string
	}{
		{"nothing listening", "u:p@tcp(127.0.0.1:" + closedPort(t) + ")/?timeout=2s", KindPortClosed},
		// .invalid is reserved (RFC 2606): it never resolves anywhere.
		{"name does not resolve", "u:p@tcp(no-such-host.invalid:3306)/?timeout=2s", KindHostUnreachable},
		{"accepts and never answers", "u:p@tcp(127.0.0.1:" + silentPort(t) + ")/?timeout=1s", KindTimeout},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := connectErr(t, c.dsn)
			if got := ClassifyConnectError(err); got != c.want {
				t.Errorf("ClassifyConnectError(%v) = %q, want %q", err, got, c.want)
			}
		})
	}
}

func TestClassifyConnectError_wrappedShapes(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("failed to ping MySQL: %w", err) }
	dial := func(errno syscall.Errno) error {
		return &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", errno)}
	}
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"unrelated", errors.New("boom"), ""},
		{"refused", wrap(dial(syscall.ECONNREFUSED)), KindPortClosed},
		{"no route to host", wrap(dial(syscall.EHOSTUNREACH)), KindHostUnreachable},
		{"network unreachable", wrap(dial(syscall.ENETUNREACH)), KindHostUnreachable},
		{"dns not found", wrap(&net.OpError{Op: "dial", Err: &net.DNSError{Err: "no such host", Name: "db", IsNotFound: true}}), KindHostUnreachable},
		// A resolver that timed out still did not produce an address: the
		// name is the thing to check, not the port or a firewall.
		{"dns timeout", wrap(&net.DNSError{Err: "i/o timeout", Name: "db", IsTimeout: true}), KindHostUnreachable},
		{"context deadline", wrap(context.DeadlineExceeded), KindTimeout},
		{"os deadline", wrap(&net.OpError{Op: "read", Err: os.ErrDeadlineExceeded}), KindTimeout},
		{"access denied", wrap(&mysql.MySQLError{Number: 1045, Message: "Access denied"}), KindAccessDenied},
		{"auth plugin refused", wrap(&mysql.MySQLError{Number: 1698, Message: "Access denied"}), KindAccessDenied},
		// Unknown database is not a connection problem this list names.
		{"unknown database", wrap(&mysql.MySQLError{Number: 1049}), ""},
		{"other server error", wrap(&mysql.MySQLError{Number: 1040, Message: "Too many connections"}), ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ClassifyConnectError(c.err); got != c.want {
				t.Errorf("ClassifyConnectError(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}
}

func TestIsLoopbackHost(t *testing.T) {
	yes := []string{"localhost", "LOCALHOST", " localhost ", "localhost.", "127.0.0.1", "127.1.2.3", "::1", "[::1]", "0.0.0.0", "::"}
	no := []string{"", "  ", "db", "localhost.example.com", "mylocalhost", "127.0.0.1.nip.io", "10.0.0.1", "host.docker.internal", "::2", "[::1"}
	for _, h := range yes {
		if !isLoopbackHost(h) {
			t.Errorf("isLoopbackHost(%q) = false, want true", h)
		}
	}
	for _, h := range no {
		if isLoopbackHost(h) {
			t.Errorf("isLoopbackHost(%q) = true, want false", h)
		}
	}
}

// buildConnect runs Build far enough to see the connection check only: it
// fails at the connection, so nothing else runs.
func buildConnect(t *testing.T, dsn string, opts ...BuildOption) CheckResult {
	t.Helper()
	r := Build(context.Background(), dsn, "", "", 0, opts...)
	if len(r.Checks) != 1 || r.Checks[0].Name != SourceConnectionCheckName || r.Checks[0].Status != StatusFail {
		t.Fatalf("want exactly one failed connection check, got %+v", r.Checks)
	}
	return r.Checks[0]
}

func TestBuild_connectionFailureCarriesItsKind(t *testing.T) {
	got := buildConnect(t, "u:p@tcp(127.0.0.1:"+closedPort(t)+")/?timeout=2s")
	if got.Kind != KindPortClosed {
		t.Errorf("kind = %q, want %q", got.Kind, KindPortClosed)
	}
}

// Without the option (the CLI), a loopback failure is never retried: the
// command line reaches the host it was given and nothing else.
func TestBuild_loopbackRetryOnlyWithTheOption(t *testing.T) {
	calls := 0
	retry := func(host, port string) string { calls++; return net.JoinHostPort("127.0.0.1", closedPort(t)) }

	buildConnect(t, "u:p@tcp(127.0.0.1:"+closedPort(t)+")/?timeout=2s")
	if calls != 0 {
		t.Fatalf("retried without the option")
	}

	// Not a loopback host: never retried, even with the option.
	buildConnect(t, "u:p@tcp(no-such-host.invalid:3306)/?timeout=2s", WithLoopbackRetry(retry))
	if calls != 0 {
		t.Errorf("retried a host that is not loopback")
	}

	// Loopback, retry also fails: the finding stays what it was. The class
	// is claimed only when the other address PROVES a database is there.
	got := buildConnect(t, "u:p@tcp(localhost:"+closedPort(t)+")/?timeout=2s", WithLoopbackRetry(retry))
	if calls != 1 {
		t.Errorf("loopback failure retried %d times, want 1", calls)
	}
	if got.Kind != KindPortClosed {
		t.Errorf("kind = %q after a failed retry, want %q unchanged", got.Kind, KindPortClosed)
	}
}

// A loopback address that answered with access denied reached a database:
// that is not the container case, and there is nothing to retry.
func TestLoopbackRetryAfter(t *testing.T) {
	for kind, want := range map[string]bool{
		KindPortClosed: true, KindTimeout: true, KindHostUnreachable: true,
		KindAccessDenied: false, "": false,
	} {
		if got := loopbackRetryAfter(kind); got != want {
			t.Errorf("loopbackRetryAfter(%q) = %v, want %v", kind, got, want)
		}
	}
}

func TestDockerHostRetry(t *testing.T) {
	cases := map[[2]string]string{
		{"localhost", "3306"}:   "host.docker.internal:3306",
		{"127.0.0.1", "3307"}:   "host.docker.internal:3307",
		{"::1", "3306"}:         "host.docker.internal:3306",
		{"localhost", ""}:       "host.docker.internal:3306",
		{" localhost ", "3306"}: "host.docker.internal:3306",
	}
	for in, want := range cases {
		if got := DockerHostRetry(in[0], in[1]); got != want {
			t.Errorf("DockerHostRetry(%q, %q) = %q, want %q", in[0], in[1], got, want)
		}
	}
}

func TestCheckPrimaryKeys_namesEveryTableAndItsStatement(t *testing.T) {
	db, mock, done := pkDB(t)
	defer done()
	var cols, tabs [][3]string
	var want []string
	for i := 0; i < pkNameLimit+2; i++ {
		name := fmt.Sprintf("t%02d", i)
		cols = append(cols, [3]string{"shop", name, ""})
		tabs = append(tabs, [3]string{"shop", name, "BASE TABLE"})
		want = append(want, "shop."+name)
	}
	// A backtick inside an identifier is doubled when quoted.
	cols = append(cols, [3]string{"shop", "we`ird", ""})
	tabs = append(tabs, [3]string{"shop", "we`ird", "BASE TABLE"})
	want = append(want, "shop.we`ird")
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(colRows(cols...))
	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(tabRows(tabs...))

	got := checkPrimaryKeys(db, nil, snapshotPending)
	if got.Status != StatusFail || got.Kind != KindNoPrimaryKey {
		t.Fatalf("status/kind = %v/%q, want FAIL/%q", got.Status, got.Kind, KindNoPrimaryKey)
	}
	slices.Sort(want)
	if !slices.Equal(got.Subjects, want) {
		t.Errorf("subjects = %v, want every table (the text caps at %d, the list must not): %v", got.Subjects, pkNameLimit, want)
	}
	if len(got.Statements) != len(want) {
		t.Fatalf("statements = %d, want one per table (%d)", len(got.Statements), len(want))
	}
	for _, s := range got.Statements {
		if !strings.HasPrefix(s, "ALTER TABLE `shop`.`") || !strings.HasSuffix(s, "` ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;") {
			t.Errorf("statement not in the expected shape: %s", s)
		}
	}
	if !slices.Contains(got.Statements, "ALTER TABLE `shop`.`we``ird` ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;") {
		t.Errorf("the backtick in we`ird was not doubled: %v", got.Statements)
	}
}

// "a.b.c" could be schema a, table b.c or schema a.b, table c: a statement
// that guesses runs against the wrong table. It is named, with no statement.
func TestPrimaryKeyStatements_ambiguousNameGetsNone(t *testing.T) {
	got := primaryKeyStatements([]string{"a.b.c", "shop.orders", "nodot", ".x", "x."})
	want := []string{"ALTER TABLE `shop`.`orders` ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;"}
	if !slices.Equal(got, want) {
		t.Errorf("primaryKeyStatements = %v, want %v", got, want)
	}
}

// A snapshot already taken makes the finding a warning, not a refusal, but it
// is the same kind: the screen grades by status, and names by kind.
func TestCheckPrimaryKeys_warningKeepsItsKind(t *testing.T) {
	db, mock, done := pkDB(t)
	defer done()
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(colRows([3]string{"shop", "nopk", ""}))
	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(tabRows([3]string{"shop", "nopk", "BASE TABLE"}))
	got := checkPrimaryKeys(db, nil, snapshotTaken)
	if got.Status != StatusWarn || got.Kind != KindNoPrimaryKey || !slices.Equal(got.Subjects, []string{"shop.nopk"}) {
		t.Errorf("got %v/%q/%v, want WARN/%q/[shop.nopk]", got.Status, got.Kind, got.Subjects, KindNoPrimaryKey)
	}
}

// A query that failed proves nothing about keys: no kind, no tables.
func TestCheckPrimaryKeys_aFailedQueryHasNoKind(t *testing.T) {
	db, mock, done := pkDB(t)
	defer done()
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnError(errPKProbe)
	if got := checkPrimaryKeys(db, nil, snapshotPending); got.Kind != "" || got.Subjects != nil {
		t.Errorf("a failed probe claims kind %q subjects %v", got.Kind, got.Subjects)
	}
}

func grantsDB(t *testing.T, grants ...string) CheckResult {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows := sqlmock.NewRows([]string{"Grants"})
	for _, g := range grants {
		rows.AddRow(g)
	}
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(rows)
	return checkReplicationGrants(context.Background(), db)
}

func TestCheckReplicationGrants_namesTheMissingPrivilege(t *testing.T) {
	got := grantsDB(t, "GRANT REPLICATION SLAVE ON *.* TO `dbtrail`@`%`")
	if got.Kind != KindMissingPrivilege || !slices.Equal(got.Subjects, []string{"REPLICATION CLIENT"}) {
		t.Errorf("got kind %q subjects %v, want %q [REPLICATION CLIENT]", got.Kind, got.Subjects, KindMissingPrivilege)
	}
	got = grantsDB(t, "GRANT USAGE ON *.* TO `dbtrail`@`%`")
	if !slices.Equal(got.Subjects, []string{"REPLICATION SLAVE", "REPLICATION CLIENT"}) {
		t.Errorf("subjects = %v, want both", got.Subjects)
	}
	if got := grantsDB(t, "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `dbtrail`@`%`"); got.Kind != "" || got.Subjects != nil {
		t.Errorf("a pass carries kind %q subjects %v", got.Kind, got.Subjects)
	}
}

func TestCheckReplicationGrants_aFailedQueryHasNoKind(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SHOW GRANTS").WillReturnError(errors.New("Error 2013: Lost connection"))
	if got := checkReplicationGrants(context.Background(), db); got.Kind != "" {
		t.Errorf("a failed SHOW GRANTS claims kind %q: it proves no privilege is missing", got.Kind)
	}
}

func TestBinlogChecks_kindOnlyForAWrongSetting(t *testing.T) {
	run := func(t *testing.T, fn func(context.Context, *sql.DB) CheckResult, setup func(sqlmock.Sqlmock)) CheckResult {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		setup(mock)
		return fn(context.Background(), db)
	}
	cases := []struct {
		name    string
		fn      func(context.Context, *sql.DB) CheckResult
		setup   func(sqlmock.Sqlmock)
		kind    string
		subject string
	}{
		{"log_bin off", checkLogBin, func(m sqlmock.Sqlmock) {
			m.ExpectQuery("@@log_bin").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("0"))
		}, KindBinlogSettings, "log_bin"},
		{"log_bin query failed", checkLogBin, func(m sqlmock.Sqlmock) {
			m.ExpectQuery("@@log_bin").WillReturnError(errors.New("Error 2013"))
		}, "", ""},
		{"binlog_format statement", checkBinlogFormat, func(m sqlmock.Sqlmock) {
			m.ExpectQuery("binlog_format").WillReturnRows(sqlmock.NewRows([]string{"n", "v"}).AddRow("binlog_format", "STATEMENT"))
		}, KindBinlogSettings, "binlog_format"},
		{"binlog_format query failed", checkBinlogFormat, func(m sqlmock.Sqlmock) {
			m.ExpectQuery("binlog_format").WillReturnError(errors.New("Error 2013"))
		}, "", ""},
		{"row image minimal", checkBinlogRowImage, func(m sqlmock.Sqlmock) {
			m.ExpectQuery("binlog_row_image").WillReturnRows(sqlmock.NewRows([]string{"n", "v"}).AddRow("binlog_row_image", "MINIMAL"))
		}, KindBinlogSettings, "binlog_row_image"},
		{"row image query failed", checkBinlogRowImage, func(m sqlmock.Sqlmock) {
			m.ExpectQuery("binlog_row_image").WillReturnError(errors.New("Error 2013"))
		}, "", ""},
		{"row image full", checkBinlogRowImage, func(m sqlmock.Sqlmock) {
			m.ExpectQuery("binlog_row_image").WillReturnRows(sqlmock.NewRows([]string{"n", "v"}).AddRow("binlog_row_image", "FULL"))
		}, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := run(t, c.fn, c.setup)
			if got.Kind != c.kind {
				t.Errorf("kind = %q, want %q (status %v, detail %q)", got.Kind, c.kind, got.Status, got.Detail)
			}
			var want []string
			if c.subject != "" {
				want = []string{c.subject}
			}
			if !slices.Equal(got.Subjects, want) {
				t.Errorf("subjects = %v, want %v", got.Subjects, want)
			}
		})
	}
}

// Every kind is a fixed word from this package, never built from input: the
// DSN scrubber runs over Detail, and a kind bypasses it.
func TestKindsAreAClosedSet(t *testing.T) {
	for _, k := range Kinds() {
		if k == "" || strings.ToLower(k) != k || strings.ContainsAny(k, " :/@") {
			t.Errorf("kind %q is not a plain lowercase word", k)
		}
	}
	if len(Kinds()) != 8 {
		t.Errorf("Kinds() = %v; a new kind needs a screen to draw it (#1804), add it on purpose", Kinds())
	}
}

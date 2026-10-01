package ext

import (
	"bytes"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
)

// recordOpen swaps the connector OpenSource calls and records what it got.
func recordOpen(t *testing.T, answer error) *[]config.SSL {
	t.Helper()
	orig := connectSourceSSL
	t.Cleanup(func() { connectSourceSSL = orig })
	var got []config.SSL
	connectSourceSSL = func(_ string, ssl config.SSL, _ func(error)) (*sql.DB, error) {
		got = append(got, ssl)
		if answer != nil {
			return nil, answer
		}
		return sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/x") // never dials
	}
	return &got
}

// The zero value is what every extension built before the field existed
// receives, and what the agent and the standalone MCP server hand over: it
// must mean the same "preferred" an entry with no ssl_mode means, not an
// error and not cleartext.
func TestOpenSource_ZeroValueIsPreferred(t *testing.T) {
	got := recordOpen(t, nil)
	db, err := OpenSource("u:p@tcp(h:3306)/", SourceTLS{})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	if len(*got) != 1 || (*got)[0] != (config.SSL{Mode: config.DefaultSourceSSLMode}) {
		t.Fatalf("connected with %+v, want mode %q", *got, config.DefaultSourceSSLMode)
	}
	if config.DefaultSourceSSLMode != "preferred" {
		t.Fatalf("default mode = %q, want preferred", config.DefaultSourceSSLMode)
	}
}

// An empty mode with files set keeps the files: only the mode is defaulted.
func TestOpenSource_PassesEverySettingThrough(t *testing.T) {
	got := recordOpen(t, nil)
	for _, in := range []SourceTLS{
		{Mode: "verify-ca", CA: "/ca.pem", Cert: "/c.pem", Key: "/k.pem"},
		{Mode: "disabled"},
		{CA: "/only-ca.pem"},
	} {
		db, err := OpenSource("u:p@tcp(h:3306)/", in)
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
	want := []config.SSL{
		{Mode: "verify-ca", CA: "/ca.pem", Cert: "/c.pem", Key: "/k.pem"},
		{Mode: "disabled"},
		{Mode: "preferred", CA: "/only-ca.pem"},
	}
	for i := range want {
		if (*got)[i] != want[i] {
			t.Errorf("call %d connected with %+v, want %+v", i, (*got)[i], want[i])
		}
	}
}

// A bad setting is the extension's to show (possibly to an MCP client), so
// it must not name a command-line flag the reader has no way to pass, and it
// must stay matchable as the typed settings error.
func TestOpenSource_SettingsErrorNamesNoFlag(t *testing.T) {
	for _, in := range []SourceTLS{
		{Mode: "require"},
		{Mode: "PREFERRED"},
		{Mode: "verify-ca", CA: "/nonexistent/ca.pem"},
		{Mode: "required", Cert: "/c.pem"},
	} {
		_, err := OpenSource("u:p@tcp(h:3306)/", in)
		if err == nil {
			t.Errorf("%+v: no error", in)
			continue
		}
		if strings.Contains(err.Error(), "--") {
			t.Errorf("%+v: error names a flag: %q", in, err)
		}
		var se *config.TLSSettingsError
		if !errors.As(err, &se) {
			t.Errorf("%+v: error %v does not unwrap to the settings error", in, err)
		}
		if !strings.Contains(err.Error(), "TLS") || !strings.Contains(err.Error(), "ssl_") {
			t.Errorf("%+v: error %q does not name the TLS setting as the registry spells it", in, err)
		}
	}
}

// A connect failure comes back as the server said it, so an extension can
// still match the MySQL error number (3159: the server demands TLS).
func TestOpenSource_ConnectErrorUnchanged(t *testing.T) {
	refused := &mysql.MySQLError{Number: 3159, Message: "insecure transport"}
	recordOpen(t, refused)
	_, err := OpenSource("u:p@tcp(h:3306)/", SourceTLS{Mode: "disabled"})
	var me *mysql.MySQLError
	if !errors.As(err, &me) || me.Number != 3159 {
		t.Fatalf("err = %v, want the server's 3159 unchanged", err)
	}
}

// SourceTLS must stay convertible from the core's own settings type: every
// construction site writes SourceTLS(entry.SourceSSL()). This fails to
// compile if the two drift, which is the point.
func TestSourceTLS_ConvertsFromCoreSettings(t *testing.T) {
	in := config.SSL{Mode: "verify-identity", CA: "a", Cert: "b", Key: "c"}
	if got := SourceTLS(in); got != (SourceTLS{Mode: "verify-identity", CA: "a", Cert: "b", Key: "c"}) {
		t.Fatalf("got %+v", got)
	}
}

// stubConnect makes every connect "succeed"; a DSN listed in plain falls back
// to cleartext (onCleartext fires, as config.ConnectSSL does once the
// cleartext retry has connected), any other connects over TLS.
func stubConnect(t *testing.T, plain map[string]bool) {
	t.Helper()
	orig := connectSourceSSL
	t.Cleanup(func() { connectSourceSSL = orig })
	connectSourceSSL = func(dsn string, _ config.SSL, onCleartext func(error)) (*sql.DB, error) {
		if plain[dsn] {
			onCleartext(mysql.ErrNoTLS)
		}
		return sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/x")
	}
}

// captureLog routes slog to a buffer and starts from an empty warned set, so
// the test also passes under -count=2.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	cleartextWarned.Clear()
	t.Cleanup(func() { slog.SetDefault(prev); cleartextWarned.Clear() })
	return &buf
}

func openAll(t *testing.T, dsns ...string) {
	t.Helper()
	for _, dsn := range dsns {
		db, err := OpenSource(dsn, SourceTLS{})
		if err != nil {
			t.Fatalf("%s: %v", dsn, err)
		}
		db.Close()
	}
}

// Falling back to cleartext must be visible: an extension may be the only
// thing in the process that opens this source (a console serving views and
// tools runs no capture), so it warns, once per source address.
func TestOpenSource_CleartextFallbackWarnsOncePerHost(t *testing.T) {
	const a, b = "u:p@tcp(warn-a.example:3306)/", "u:p@tcp(warn-b.example:3306)/"
	stubConnect(t, map[string]bool{a: true, b: true})
	buf := captureLog(t)
	openAll(t, a, a, b)
	out := buf.String()
	if n := strings.Count(out, "level=WARN"); n != 2 {
		t.Fatalf("%d warnings, want one per host (2):\n%s", n, out)
	}
	if strings.Count(out, "level=DEBUG") != 1 || !strings.Contains(out, "WITHOUT encryption") {
		t.Fatalf("want the repeat at debug level and the warning to say it is unencrypted:\n%s", out)
	}
}

// The warned set is keyed by the whole address: two servers on one host
// (different ports), two unix sockets, and two DSNs that do not parse are
// each their own source and each warn; none share an empty key.
func TestOpenSource_WarnKeyIsTheAddress(t *testing.T) {
	dsns := []string{
		"u:p@tcp(shared.example:3306)/", "u:p@tcp(shared.example:3307)/",
		"u:p@unix(/run/a.sock)/", "u:p@unix(/run/b.sock)/",
		"not a dsn one", "not a dsn two",
	}
	plain := map[string]bool{}
	for _, d := range dsns {
		plain[d] = true
	}
	stubConnect(t, plain)
	buf := captureLog(t)
	openAll(t, dsns...)
	if n := strings.Count(buf.String(), "level=WARN"); n != len(dsns) {
		t.Fatalf("%d warnings, want %d (one per distinct source):\n%s", n, len(dsns), buf)
	}
}

// A source that connected over TLS and later stops offering it is a
// regression worth a fresh warning: a TLS success clears the warned mark.
func TestOpenSource_TLSSuccessRearmsTheWarning(t *testing.T) {
	const dsn = "u:p@tcp(flaky.example:3306)/"
	plain := map[string]bool{dsn: true}
	stubConnect(t, plain)
	buf := captureLog(t)
	openAll(t, dsn) // cleartext: warn
	plain[dsn] = false
	openAll(t, dsn) // TLS: clears
	plain[dsn] = true
	openAll(t, dsn) // cleartext again: warn again
	if n := strings.Count(buf.String(), "level=WARN"); n != 2 {
		t.Fatalf("%d warnings, want 2 (before and after the TLS success):\n%s", n, buf)
	}
}

// OpenSource speaks MySQL only. A Postgres DSN must be refused by name, not
// fail inside the MySQL DSN parser with an unrelated message.
func TestOpenSource_RefusesPostgresDSN(t *testing.T) {
	stubConnect(t, nil)
	for _, dsn := range []string{
		"postgres://u:p@h:5432/db?sslmode=require",
		"POSTGRESQL://u:p@h/db",
		"host=h port=5432 user=u dbname=db sslmode=require",
	} {
		_, err := OpenSource(dsn, SourceTLS{})
		if err == nil || !strings.Contains(err.Error(), "MySQL/MariaDB sources only") || !strings.Contains(err.Error(), "sslmode=") {
			t.Errorf("%q: err = %v, want the MySQL-only refusal", dsn, err)
		}
	}
	// A MySQL DSN whose password contains "host=" is still MySQL.
	openAll(t, "u:host=x@tcp(h:3306)/")
}

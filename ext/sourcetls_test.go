package ext

import (
	"database/sql"
	"errors"
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
		if !strings.Contains(err.Error(), "TLS") {
			t.Errorf("%+v: error %q does not say it is about TLS", in, err)
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

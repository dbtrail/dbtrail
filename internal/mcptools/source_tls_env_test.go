package mcptools

import (
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// The standalone server takes its source from BINTRAIL_SOURCE_DSN, so its
// TLS comes from the same environment capture honors (BINTRAIL_SSL_*):
// otherwise verify-ca with a CA reaches extension tools as "preferred", an
// encrypted but unverified connection, and nothing says so.
func TestEnvSourceTLS(t *testing.T) {
	t.Setenv("BINTRAIL_SSL_MODE", "verify-ca")
	t.Setenv("BINTRAIL_SSL_CA", "/env/ca.pem")
	t.Setenv("BINTRAIL_SSL_CERT", "/env/cert.pem")
	t.Setenv("BINTRAIL_SSL_KEY", "/env/key.pem")
	want := ext.SourceTLS{Mode: "verify-ca", CA: "/env/ca.pem", Cert: "/env/cert.pem", Key: "/env/key.pem"}
	if got := envSourceTLS(); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	// Unset: the zero value ("preferred"), as for an entry with no ssl_mode.
	for _, k := range []string{"BINTRAIL_SSL_MODE", "BINTRAIL_SSL_CA", "BINTRAIL_SSL_CERT", "BINTRAIL_SSL_KEY"} {
		t.Setenv(k, "")
	}
	if got := envSourceTLS(); got != (ext.SourceTLS{}) {
		t.Fatalf("unset: got %+v, want the zero value", got)
	}

	// A misspelled mode is passed through as written, never turned into
	// "preferred": ext.OpenSource then refuses it, naming the setting.
	t.Setenv("BINTRAIL_SSL_MODE", "verify_ca")
	if got := envSourceTLS(); got.Mode != "verify_ca" {
		t.Fatalf("bad mode became %q, want it passed through", got.Mode)
	}
	if _, err := ext.OpenSource("u:p@tcp(127.0.0.1:1)/", envSourceTLS()); err == nil {
		t.Fatal("a bad BINTRAIL_SSL_MODE reached a connect attempt")
	}
}

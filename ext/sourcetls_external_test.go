package ext_test

import (
	"errors"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// An extension lives in another module and cannot name the core's internal
// error type, so a bad TLS setting must match an exported sentinel.
func TestOpenSource_SettingsErrorMatchesExportedSentinel(t *testing.T) {
	for _, tls := range []ext.SourceTLS{
		{Mode: "require"},
		{Mode: "verify-ca", CA: "/nonexistent/ca.pem"},
		{Mode: "required", Cert: "/c.pem"},
	} {
		_, err := ext.OpenSource("u:p@tcp(127.0.0.1:1)/", tls)
		if !errors.Is(err, ext.ErrSourceTLSSettings) {
			t.Errorf("%+v: err = %v, want errors.Is ErrSourceTLSSettings", tls, err)
		}
	}
	// A connect failure is not a settings error.
	_, err := ext.OpenSource("u:p@tcp(127.0.0.1:1)/?timeout=1s", ext.SourceTLS{Mode: "disabled"})
	if err == nil || errors.Is(err, ext.ErrSourceTLSSettings) {
		t.Fatalf("connect failure: err = %v, want an error that is not ErrSourceTLSSettings", err)
	}
}

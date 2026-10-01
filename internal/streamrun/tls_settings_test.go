package streamrun

import (
	"strings"
	"testing"
)

// A settings error (unreadable CA, unknown mode) is a local problem: it must
// come back without tlsHint's "enable TLS on the server" advice.
func TestConnectHelper_SettingsErrorCarriesNoServerHint(t *testing.T) {
	for _, tc := range []struct{ mode, ca string }{
		{"required", "/nonexistent/ca.pem"},
		{"verify-identity", "/nonexistent/ca.pem"},
		{"bogus", ""},
	} {
		_, err := connectHelper("u:p@tcp(127.0.0.1:1)/x?timeout=1s", "index database", tc.mode, tc.ca, "", "")
		if err == nil {
			t.Fatalf("mode %q ca %q: no error", tc.mode, tc.ca)
		}
		if strings.Contains(err.Error(), "requires TLS") || strings.Contains(err.Error(), "connect index database") {
			t.Errorf("mode %q ca %q: settings error wrapped as a server problem: %v", tc.mode, tc.ca, err)
		}
	}
}

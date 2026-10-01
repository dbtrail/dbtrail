package doctor

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
)

// The server's own 3159 text names a server flag and an error code; what a
// person reads must say what happened and what to change, in plain words.
func TestRefusesUnencrypted(t *testing.T) {
	e3159 := &mysql.MySQLError{Number: 3159, SQLState: [5]byte{'0', '8', '0', '0', '4'},
		Message: "Connections using insecure transport are prohibited while --require_secure_transport=ON."}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"3159", e3159, true},
		{"3159 wrapped by the ping", fmt.Errorf("failed to ping MySQL: %w", e3159), true},
		{"3159 after a cleartext retry", fmt.Errorf("cleartext retry after the server offered no TLS: %w", e3159), true},
		{"access denied (a REQUIRE SSL user) is not this", &mysql.MySQLError{Number: 1045, Message: "Access denied"}, false},
		{"the text alone, without the error, is not this", errors.New("Error 3159 (08004): Connections using insecure transport"), false},
	}
	for _, tt := range tests {
		if got := refusesUnencrypted(tt.err); got != tt.want {
			t.Errorf("%s: refusesUnencrypted = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestUnencryptedRefusal(t *testing.T) {
	const plainDSN = "u:p@tcp(db.example.com:3306)/"
	ssl := func(mode string) *config.SSL { return &config.SSL{Mode: mode} }
	tests := []struct {
		name        string
		dsn         string
		ssl         *config.SSL
		wantDetail  string
		wantFix     []string
		wantFixNone []string
	}{
		{"TLS mode disabled on purpose", plainDSN, ssl("disabled"),
			"set to connect to it without encryption",
			[]string{"preferred or required", "ssl_mode", "console-servers.yaml", "--ssl-mode"}, nil},
		{"preferred, but the server offered no TLS", plainDSN, ssl("preferred"),
			"did not offer TLS",
			[]string{"Turn TLS on in the server's configuration"}, []string{"ssl_mode", "tls="}},
		{"the DSN's own tls=false wins over the mode", plainDSN + "?tls=false", ssl("preferred"),
			"tls=false",
			[]string{"tls=preferred"}, []string{"ssl_mode"}},
		{"tls=preferred in the DSN fell back because the server offered none", plainDSN + "?tls=preferred", nil,
			"did not offer TLS",
			[]string{"Turn TLS on"}, []string{"Add tls=preferred"}},
		{"tls=false beats a disabled mode too", plainDSN + "?tls=false", ssl("disabled"),
			"tls=false",
			[]string{"Remove tls=false"}, []string{"ssl_mode"}},
		{"no TLS mode at all (bintrail doctor)", plainDSN, nil,
			"connected without encryption",
			[]string{"tls=preferred", "source DSN"}, []string{"ssl_mode", "--ssl-mode="}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detail, fix := unencryptedRefusal(tt.dsn, tt.ssl)
			t.Logf("detail: %s", detail)
			t.Logf("fix:    %s", fix)
			if !strings.HasPrefix(detail, "This server only accepts encrypted (TLS) connections") {
				t.Errorf("detail does not open with what the server demands: %q", detail)
			}
			if !strings.Contains(detail, tt.wantDetail) {
				t.Errorf("detail %q, want it to say %q", detail, tt.wantDetail)
			}
			for _, w := range tt.wantFix {
				if !strings.Contains(fix, w) {
					t.Errorf("fix %q, want it to name %q", fix, w)
				}
			}
			for _, w := range tt.wantFixNone {
				if strings.Contains(fix, w) {
					t.Errorf("fix %q must not name %q here", fix, w)
				}
			}
			for _, raw := range []string{"08004", "require_secure_transport", "insecure transport", "—"} {
				if strings.Contains(detail+fix, raw) {
					t.Errorf("text carries %q: %q / %q", raw, detail, fix)
				}
			}
		})
	}
}

// A TLS setting that cannot be used names the field, never the network.
func TestTLSSettingsText(t *testing.T) {
	_, err := config.BuildTLSConfig("require", "", "", "", "h")
	detail, fix, ok := TLSSettingsText(err)
	t.Logf("detail: %s", detail)
	t.Logf("fix:    %s", fix)
	if !ok || !strings.Contains(detail, `"require" is not a TLS mode`) || !strings.Contains(fix, "ssl_mode") ||
		!strings.Contains(fix, "console-servers.yaml") || strings.Contains(detail, "--") {
		t.Fatalf("detail %q fix %q ok %v", detail, fix, ok)
	}
	_, err = config.BuildTLSConfig("verify-ca", "/nonexistent/ca.pem", "", "", "h")
	if _, fix, _ := TLSSettingsText(err); !strings.Contains(fix, "ssl_ca") {
		t.Fatalf("CA fix %q", fix)
	}
	if _, _, ok := TLSSettingsText(errors.New("dial tcp: refused")); ok {
		t.Fatal("a network error read as a settings error")
	}
}

// Build reports a bad TLS setting as itself, before dialing: no security-group
// advice for a typo in ssl_mode.
func TestBuild_TLSSettingsErrorIsLocal(t *testing.T) {
	r := Build(t.Context(), "u:p@tcp(127.0.0.1:1)/", "", "", 0, ForUnsavedServer(),
		WithSourceSSL(config.SSL{Mode: "verify-ca", CA: "/nonexistent/ca.pem"}))
	if len(r.Checks) != 1 {
		t.Fatalf("checks = %+v", r.Checks)
	}
	c := r.Checks[0]
	t.Logf("detail: %s", c.Detail)
	t.Logf("fix:    %s", c.Remediation)
	if c.Status != StatusFail || !strings.Contains(c.Remediation, "ssl_ca") || strings.Contains(c.Remediation, "security group") {
		t.Fatalf("check = %+v", c)
	}
}

// The console's own source reads (snapshot pre-checks, verify, schema
// snapshot) word a TLS refusal the way the startup checks do: 3159 names the
// setting that decided it, and a mode that requires TLS against a server with
// none says so instead of the driver's bare "server does not support TLS".
func TestSourceTLSRefusalText(t *testing.T) {
	refused := &mysql.MySQLError{Number: 3159, Message: "Connections using insecure transport are prohibited"}
	noTLS := fmt.Errorf("failed to ping: %w", mysql.ErrNoTLS)
	const dsn = "u:p@tcp(db:3306)/"
	for _, tc := range []struct {
		name   string
		err    error
		dsn    string
		mode   string
		wantOK bool
		want   string
	}{
		{"3159 under disabled names the mode", refused, dsn, "disabled", true, "its TLS mode is disabled"},
		{"3159 under a DSN tls=false names the DSN", refused, dsn + "?tls=false", "preferred", true, "tls=false"},
		{"3159 under preferred: the server offered no TLS", refused, dsn, "preferred", true, "did not offer TLS"},
		{"required against a server without TLS", noTLS, dsn, "required", true, "its TLS mode is required"},
		{"verify-ca against a server without TLS", noTLS, dsn, "verify-ca", true, "its TLS mode is verify-ca"},
		{"verify-identity against a server without TLS", noTLS, dsn, "verify-identity", true, "its TLS mode is verify-identity"},
		{"no TLS under preferred is not a refusal (it retries)", noTLS, dsn, "preferred", false, ""},
		{"no TLS under a DSN tls= is the DSN's doing", noTLS, dsn + "?tls=skip-verify", "required", false, ""},
		{"access denied is not a TLS refusal", &mysql.MySQLError{Number: 1045, Message: "Access denied"}, dsn, "required", false, ""},
		{"nil", nil, dsn, "required", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			detail, fix, ok := SourceTLSRefusalText(tc.err, tc.dsn, config.SSL{Mode: tc.mode})
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (%q %q)", ok, tc.wantOK, detail, fix)
			}
			if !ok {
				return
			}
			if !strings.Contains(detail+" "+fix, tc.want) {
				t.Errorf("text %q / %q does not contain %q", detail, fix, tc.want)
			}
			if strings.Contains(detail+fix, "insecure transport") {
				t.Errorf("raw server text leaked: %q", detail)
			}
		})
	}
}

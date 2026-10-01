package consoleapp

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
)

// A request built without TLS settings reads as an entry with no ssl_mode
// (preferred), never as an invalid mode; a mode that was set is kept as is.
func TestSourceSSLOrDefault(t *testing.T) {
	if got := sourceSSLOrDefault(config.SSL{}); got.Mode != "preferred" {
		t.Errorf("zero SSL -> mode %q, want preferred", got.Mode)
	}
	in := config.SSL{Mode: "disabled", CA: "/ca.pem"}
	if got := sourceSSLOrDefault(in); got != in {
		t.Errorf("explicit SSL rewritten: %+v", got)
	}
}

// The capture status reads each source with the TLS its capture uses: the
// entry's for a saved server, the daemon's --ssl-* for its own (boot) one.
func TestCaptureStatusSourceOf_TLS(t *testing.T) {
	saved := console.ServerEntry{ID: "s1", SourceDSN: "u:p@tcp(a:3306)/"}
	disabled := console.ServerEntry{ID: "s2", SourceDSN: "u:p@tcp(b:3306)/", SSLMode: "disabled"}
	boot := console.ServerEntry{ID: bootCaptureServerID}

	c := newCaptureStatusReporter("u:p@tcp(boot:3306)/")
	if _, _, ssl := c.sourceOf(saved); ssl.Mode != "preferred" {
		t.Errorf("entry with no ssl_mode: %q, want preferred", ssl.Mode)
	}
	if _, _, ssl := c.sourceOf(disabled); ssl.Mode != "disabled" {
		t.Errorf("entry with ssl_mode disabled: %q", ssl.Mode)
	}
	if dsn, _, ssl := c.sourceOf(boot); dsn != "u:p@tcp(boot:3306)/" || ssl.Mode != "preferred" {
		t.Errorf("boot without withBootSSL: %q %q, want the boot DSN, preferred", dsn, ssl.Mode)
	}
	c.withBootSSL(config.SSL{Mode: "verify-ca", CA: "/ca.pem"})
	if _, _, ssl := c.sourceOf(boot); ssl.Mode != "verify-ca" || ssl.CA != "/ca.pem" {
		t.Errorf("boot with withBootSSL: %+v", ssl)
	}
	if _, _, ssl := c.sourceOf(saved); ssl.Mode != "preferred" {
		t.Errorf("the daemon's TLS leaked onto a saved entry: %+v", ssl)
	}
}

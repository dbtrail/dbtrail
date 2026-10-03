package console

import (
	"context"
	"errors"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"strings"
	"testing"
)

// newFlashbackServer builds the minimal Server the flashback resolution seam
// touches: a connManager over reg plus a token. It bypasses New() (no HTTP mux,
// no boot bundle) because flashbackTarget/ResolveFlashback only read s.cm + s.token.
func newFlashbackServer(t *testing.T, token string) (*Server, *Registry) {
	t.Helper()
	reg, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	return &Server{cm: newConnManager(reg, false), token: token}, reg
}

// TestFlashbackTarget: a username resolves to a canonical server id by registry
// ID, by display Name, or "default" for a visible boot entry — and to nothing
// for an empty/unknown selector or a hidden boot.
func TestFlashbackTarget(t *testing.T) {
	s, reg := newFlashbackServer(t, "tok")
	a, err := reg.Add(ServerEntry{Name: "alpha", DSN: "u:p@tcp(h:3306)/idxA"})
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := s.flashbackTarget(a.ID); !ok || id != a.ID {
		t.Fatalf("by id: got (%q,%v), want (%q,true)", id, ok, a.ID)
	}
	if id, ok := s.flashbackTarget("alpha"); !ok || id != a.ID {
		t.Fatalf("by name: got (%q,%v), want (%q,true)", id, ok, a.ID)
	}
	if _, ok := s.flashbackTarget("nope"); ok {
		t.Fatal("unknown selector must not resolve")
	}
	if _, ok := s.flashbackTarget(""); ok {
		t.Fatal("empty selector must not resolve")
	}

	// Boot: not a target until it exists AND is visible.
	if _, ok := s.flashbackTarget(bootServerID); ok {
		t.Fatal("absent boot must not resolve")
	}
	s.cm.boot = &bundle{}
	if id, ok := s.flashbackTarget(bootServerID); !ok || id != bootServerID {
		t.Fatalf("visible boot: got (%q,%v), want (%q,true)", id, ok, bootServerID)
	}
	s.cm.hideBoot = true
	if _, ok := s.flashbackTarget(bootServerID); ok {
		t.Fatal("hidden boot (source-less watch anchor) must not be a flashback target")
	}
}

// TestResolveFlashbackUnknown: an unknown selector is reported as
// ErrUnknownServer (the serving layer maps it to a MySQL "no such database"),
// without opening any connection.
func TestResolveFlashbackUnknown(t *testing.T) {
	s, _ := newFlashbackServer(t, "tok")
	if _, err := s.ResolveFlashback(context.Background(), "ghost"); !errors.Is(err, ErrUnknownServer) {
		t.Fatalf("ResolveFlashback(unknown) err = %v, want ErrUnknownServer", err)
	}
}

// TestFlashbackDefaultSchema derives the USE-less default schema from a
// registry entry's SourceDSN; empty for a source-less entry or the boot entry.
func TestFlashbackDefaultSchema(t *testing.T) {
	s, reg := newFlashbackServer(t, "tok")
	a, err := reg.Add(ServerEntry{Name: "alpha", DSN: "u:p@tcp(h:3306)/idxA", SourceDSN: "r:pw@tcp(src:3306)/shopdb"})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.flashbackDefaultSchema(a.ID); got != "shopdb" {
		t.Fatalf("default schema = %q, want shopdb", got)
	}
	b, err := reg.Add(ServerEntry{Name: "beta", DSN: "u:p@tcp(h:3306)/idxB"})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.flashbackDefaultSchema(b.ID); got != "" {
		t.Fatalf("source-less entry: default schema = %q, want empty", got)
	}
	if got := s.flashbackDefaultSchema(bootServerID); got != "" {
		t.Fatalf("boot: default schema = %q, want empty", got)
	}
}

// TestSplitBaselineSource: a resolved baseline source maps to dir/S3 by scheme.
func TestSplitBaselineSource(t *testing.T) {
	for _, tc := range []struct {
		src, dir, s3 string
	}{
		{"", "", ""},
		{"/var/lib/baselines", "/var/lib/baselines", ""},
		{"s3://bucket/prefix/", "", "s3://bucket/prefix/"},
	} {
		dir, s3 := splitBaselineSource(tc.src)
		if dir != tc.dir || s3 != tc.s3 {
			t.Errorf("splitBaselineSource(%q) = (%q,%q), want (%q,%q)", tc.src, dir, s3, tc.dir, tc.s3)
		}
	}
}

// The port's executor turns the route's own refusals into
// sqlsandbox.UnavailableError, worded for a MySQL client (never "the
// browser"). A bundle with no index behind it fails view discovery before
// any runner, which is the shape of a route refusal.
func TestSQLOnCopyRun_refusalsAreUnavailableOnTheWire(t *testing.T) {
	q := &SQLOnCopy{s: &Server{sqlRunner: &fakeSQLRunner{}}, b: &bundle{}, user: "server:x"}
	_, err := q.Run(context.Background(), "SELECT 1", "")
	var un *sqlsandbox.UnavailableError
	if !errors.As(err, &un) {
		t.Fatalf("err = %v (%T), want *sqlsandbox.UnavailableError", err, err)
	}
	if strings.Contains(un.Reason, "browser") {
		t.Errorf("reason names the browser on the MySQL wire: %q", un.Reason)
	}
	if strings.Contains(sqlCopyNotLocalPortMessage, "browser") {
		t.Errorf("port wording for a non-local copy names the browser: %q", sqlCopyNotLocalPortMessage)
	}
}

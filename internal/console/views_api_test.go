package console

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// newViewsServer builds a Server whose boot bundle points at a baseline source
// and no index DB — the archive half degrades to empty, which is exactly the
// baseline-only shape this endpoint must still serve.
func newViewsServer(t *testing.T, src string, noArchive bool) *Server {
	t.Helper()
	s := &Server{token: "t", version: "v0.50.0", cm: newConnManager(nil, false)}
	s.cm.boot = &bundle{baselineSrc: src, baselineConfigured: src != "", noArchive: noArchive}
	s.mux = s.buildHandler()
	return s
}

func TestViewsAPI_servesADownloadableSQLFile(t *testing.T) {
	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "users.parquet")
	// An OLDER snapshot: its tables must not appear. Two snapshots' rows are two
	// points in time, and a schema spanning them would describe a state that
	// never existed.
	writeBaselineFixture(t, dir, "2026-06-01T00-00-00Z", "shop", "retired.parquet")

	srv := newViewsServer(t, dir, false)
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain", ct)
	}
	if cd := rec.Header().Get("Content-Disposition"); !strings.Contains(cd, `filename="views.sql"`) {
		t.Errorf("Content-Disposition = %q, want a views.sql attachment", cd)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		// A cached copy would describe a layout that has since rotated.
		t.Errorf("Cache-Control = %q, want no-store", cc)
	}

	sql := string(body)
	for _, want := range []string{`CREATE OR REPLACE VIEW "shop"."orders"`, `CREATE OR REPLACE VIEW "shop"."users"`} {
		if !strings.Contains(sql, want) {
			t.Errorf("generated SQL is missing %s:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "shop.retired") {
		t.Error("a superseded snapshot's table leaked into the schema")
	}
}

// TestViewsAPI_neverLeaksCredentials is the property that makes the file safe to
// download and paste into a shared notebook.
func TestViewsAPI_neverLeaksCredentials(t *testing.T) {
	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	srv := newViewsServer(t, dir, false)
	srv.token = "super-secret-token"

	_, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	for _, forbidden := range []string{"super-secret-token", "Bearer ", "KEY_ID '", "password"} {
		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "--") {
				continue // the commented-out explicit-key alternative
			}
			if strings.Contains(line, forbidden) {
				t.Errorf("executable line leaks %q: %s", forbidden, line)
			}
		}
	}
}

// TestViewsAPI_disabledWithoutArchives: no-archive servers have no Parquet
// layout to describe, and the capability hides the button for the same reason.
func TestViewsAPI_disabledWithoutArchives(t *testing.T) {
	srv := newViewsServer(t, t.TempDir(), true)
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 404 {
		t.Fatalf("code = %d, body = %s; want 404 for a no-archive server", rec.Code, body)
	}
}

// TestViewsAPI_nothingToDescribe: a fresh install with neither archives nor a
// baseline gets a 404, not a file of comments explaining there is nothing in it.
func TestViewsAPI_nothingToDescribe(t *testing.T) {
	srv := newViewsServer(t, "", false)
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 404 {
		t.Fatalf("code = %d, body = %s; want 404 when there is no layout yet", rec.Code, body)
	}
	if !strings.Contains(string(body), "nothing to generate views over") {
		t.Errorf("404 body is not actionable: %s", body)
	}
}

// TestViewsAvailable mirrors the handler's gate, which is the point: the
// capability decides whether the UI shows a button, and a button that only 404s
// is a lie.
func TestViewsAvailable(t *testing.T) {
	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")

	for _, tc := range []struct {
		name string
		b    *bundle
		want bool
	}{
		{"baseline source configured", &bundle{baselineSrc: dir}, true},
		{"archives disabled", &bundle{baselineSrc: dir, noArchive: true}, false},
		{"nothing configured", &bundle{}, false},
		{"no bundle", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{cm: newConnManager(nil, false)}
			r := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/views.sql", nil)
			if got := s.viewsAvailable(r, tc.b); got != tc.want {
				t.Fatalf("viewsAvailable = %v, want %v", got, tc.want)
			}
		})
	}
}

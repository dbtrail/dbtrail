package console

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
)

// TestBaselinesAPI_locationOnly: GET /api/baselines?location_only=1 says where
// the selected server's snapshots live (the same resolution the listing uses)
// and stops there: no schedule probe, no walk of the storage, nothing but the
// location in the answer. Connect AI asks it on every open to print the
// Iceberg export command, and the full listing reads the whole store (paid
// listings on S3, #1679).
func TestBaselinesAPI_locationOnly(t *testing.T) {
	decode := func(t *testing.T, body []byte) (baselineLocationResponse, []string) {
		t.Helper()
		var got baselineLocationResponse
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		var raw map[string]any
		if err := json.Unmarshal(body, &raw); err != nil {
			t.Fatal(err)
		}
		keys := make([]string, 0, len(raw))
		for k := range raw {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		return got, keys
	}

	t.Run("a directory with snapshots: the location and nothing read from it", func(t *testing.T) {
		dir := t.TempDir()
		writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
		srv := newBaselineServer(t, dir, true)
		rec, body := doServersReq(t, srv, "GET", "/api/baselines?location_only=1", "")
		if rec.Code != 200 {
			t.Fatalf("code = %d, body = %s", rec.Code, body)
		}
		got, keys := decode(t, body)
		if !got.Configured || got.Source != dir || got.Kind != "dir" {
			t.Fatalf("got %+v, want the directory as a configured dir source", got)
		}
		if !slices.Equal(keys, []string{"configured", "kind", "source"}) {
			t.Fatalf("keys = %q, want exactly configured, kind, source: anything else was read from the storage", keys)
		}
		// The listing itself is unchanged without the parameter.
		rec, body = doServersReq(t, srv, "GET", "/api/baselines", "")
		var listing baselinesResponse
		if err := json.Unmarshal(body, &listing); err != nil {
			t.Fatal(err)
		}
		if rec.Code != 200 || len(listing.Snapshots) != 1 || listing.Source != dir || listing.Kind != "dir" {
			t.Fatalf("plain listing: code %d, body %s; want the one snapshot under the same location", rec.Code, body)
		}
	})

	t.Run("a directory that does not exist: still answered, nothing read", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "nope")
		srv := newBaselineServer(t, dir, true)
		rec, body := doServersReq(t, srv, "GET", "/api/baselines?location_only=1", "")
		if got, _ := decode(t, body); rec.Code != 200 || got.Source != dir {
			t.Fatalf("code = %d, body = %s; want 200 naming the directory (the listing answers 502 because it reads it)", rec.Code, body)
		}
	})

	t.Run("an S3 destination nobody can reach: answered at once", func(t *testing.T) {
		src := "s3://no-such-bucket-dbtrail-test/baselines"
		srv := newBaselineServer(t, src, true)
		start := time.Now()
		rec, body := doServersReq(t, srv, "GET", "/api/baselines?location_only=1", "")
		if rec.Code != 200 {
			t.Fatalf("code = %d, body = %s", rec.Code, body)
		}
		if got, _ := decode(t, body); got.Source != src || got.Kind != "s3" {
			t.Fatalf("got %+v, want the bucket as an s3 source", got)
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("took %s: the bucket was contacted", d)
		}
	})

	t.Run("nothing configured", func(t *testing.T) {
		srv := newBaselineServer(t, "", false)
		rec, body := doServersReq(t, srv, "GET", "/api/baselines?location_only=1", "")
		got, keys := decode(t, body)
		if rec.Code != 200 || got.Configured || !slices.Equal(keys, []string{"configured"}) {
			t.Fatalf("code = %d, body = %s; want 200 and only configured:false", rec.Code, body)
		}
	})

	t.Run("any other value is refused, not read as no", func(t *testing.T) {
		srv := newBaselineServer(t, t.TempDir(), true)
		for _, q := range []string{"location_only=true", "location_only=0", "location_only=", "location_only=1&location_only=1"} {
			if rec, body := doServersReq(t, srv, "GET", "/api/baselines?"+q, ""); rec.Code != 400 {
				t.Errorf("?%s: code = %d, body = %s; want 400 (a caller that meant to skip the walk would pay for it)", q, rec.Code, body)
			}
		}
	})

	t.Run("a named startup profile with no rules yet is refused too", func(t *testing.T) {
		// Wider than the listing (sessionRestricted): the command this location
		// goes into exports every row unredacted, and data_profile, which the
		// page asks first, is keyed on the same profileActiveFor.
		srv := newBaselineServer(t, t.TempDir(), true)
		srv.profileActive = true
		rec, body := doServersReq(t, srv, "GET", "/api/baselines?location_only=1", "")
		if rec.Code != 403 {
			t.Fatalf("code = %d, body = %s; want 403 under serve --profile", rec.Code, body)
		}
	})

	t.Run("a session with a data profile is refused, as the listing is", func(t *testing.T) {
		srv := newBaselineServer(t, t.TempDir(), true)
		req := httptest.NewRequest("GET", "/api/baselines?location_only=1", nil)
		req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{}, &ext.AccessPolicy{Profile: "sensitive"}))
		w := httptest.NewRecorder()
		srv.handleBaselines(w, req)
		if w.Code != 403 {
			t.Fatalf("code = %d, body = %s; want 403: the location and the index address it sits beside are not for a profiled session", w.Code, w.Body.String())
		}
	})
}

// TestBaselinesAPI_locationOnlyRegistry: for registry servers the location is
// configuration, answered without opening the server's index. Every entry
// here points at an index nobody listens on, so an answer that went through
// the index connection comes back 502. And the answer is the location the
// server's bundle carries (the listing's primary source): its own directory,
// else its own bucket, else the daemon's default, never a mix.
func TestBaselinesAPI_locationOnlyRegistry(t *testing.T) {
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, BaselineDir: "/daemon/baselines"})
	if err != nil {
		t.Fatal(err)
	}
	const deadIndex = "u:p@tcp(127.0.0.1:1)/idx"
	cases := []struct {
		entry     ServerEntry
		src, kind string
	}{
		{ServerEntry{Name: "inherits", DSN: deadIndex}, "/daemon/baselines", "dir"},
		{ServerEntry{Name: "own-dir", DSN: deadIndex, BaselineDir: "/own/dir"}, "/own/dir", "dir"},
		{ServerEntry{Name: "own-s3", DSN: deadIndex, BaselineS3: "s3://own-bucket/b"}, "s3://own-bucket/b", "s3"},
		{ServerEntry{Name: "both", DSN: deadIndex, BaselineDir: "/both/dir", BaselineS3: "s3://both-bucket/b"}, "/both/dir", "dir"},
	}
	ids := make([]string, len(cases))
	for i, tc := range cases {
		e, err := reg.Add(tc.entry)
		if err != nil {
			t.Fatalf("add %s: %v", tc.entry.Name, err)
		}
		ids[i] = e.ID
	}
	for i, tc := range cases {
		rec, body := doServersReqHeader(t, srv, "GET", "/api/baselines?location_only=1", "", ids[i])
		if rec.Code != 200 {
			t.Fatalf("%s: code = %d, body = %s; want 200 without touching the dead index", tc.entry.Name, rec.Code, body)
		}
		var got baselineLocationResponse
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if !got.Configured || got.Source != tc.src || got.Kind != tc.kind {
			t.Errorf("%s: got %+v, want %s source %q", tc.entry.Name, got, tc.kind, tc.src)
		}
		// The same location the bundle a lazy open would publish carries (the
		// listing reads the bundle), derived here without the dial it needs.
		entry, _ := reg.Get(ids[i])
		b := newBundleDerived(nil, "", srv.cm.withBaselineDefaults(entry), false)
		if b.baselineSrc != got.Source {
			t.Errorf("%s: location_only %q, the bundle carries %q", tc.entry.Name, got.Source, b.baselineSrc)
		}
	}
	if len(srv.cm.bundles) != 0 {
		t.Errorf("bundles opened: %d; location_only must not open a server's index", len(srv.cm.bundles))
	}

	// No header, with the daemon's --baseline-dir set: that seeds the
	// command-line entry, which is then the default, so the answer is the
	// daemon's own location.
	if got := headerless(t, srv); got != "/daemon/baselines" {
		t.Errorf("no header, command-line entry present: source %q, want /daemon/baselines", got)
	}

	if rec, body := doServersReqHeader(t, srv, "GET", "/api/baselines?location_only=1", "", "no-such-id"); rec.Code != 404 {
		t.Errorf("unknown server: code = %d, body = %s; want 404 as the listing answers", rec.Code, body)
	}
}

// TestBaselinesAPI_locationOnlyHeaderlessRegistry: with no command-line entry,
// a request with no server header resolves to the first registry server, and
// the location is that server's own.
func TestBaselinesAPI_locationOnlyHeaderlessRegistry(t *testing.T) {
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []ServerEntry{
		{Name: "first", DSN: "u:p@tcp(127.0.0.1:1)/idx", BaselineDir: "/first/dir"},
		{Name: "second", DSN: "u:p@tcp(127.0.0.1:1)/idx", BaselineDir: "/second/dir"},
	} {
		if _, err := reg.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := reg.Get(srv.cm.defaultID()); !ok {
		t.Fatalf("defaultID %q is not a registry entry: this test would not test a registry default", srv.cm.defaultID())
	}
	if got := headerless(t, srv); got != "/first/dir" {
		t.Errorf("no header: source %q, want /first/dir, the first registry server's own", got)
	}
}

// headerless asks location_only with no server header and returns the source.
func headerless(t *testing.T, srv *Server) string {
	t.Helper()
	rec, body := doServersReq(t, srv, "GET", "/api/baselines?location_only=1", "")
	if rec.Code != 200 {
		t.Fatalf("no header: code = %d, body = %s", rec.Code, body)
	}
	var got baselineLocationResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	return got.Source
}

// TestCapabilitiesDataProfile: data_profile tells the page a data profile
// governs this request (the session's, or a named startup one even with no
// rules yet), so Connect AI does not ask for the backup location the server
// would refuse, and audit, on every visit.
func TestCapabilitiesDataProfile(t *testing.T) {
	read := func(t *testing.T, srv *Server, tok string) bool {
		t.Helper()
		rec := getPath(t, srv, "127.0.0.1:8090", "/api/capabilities", tok)
		if rec.Code != 200 {
			t.Fatalf("GET /api/capabilities = %d", rec.Code)
		}
		var caps struct {
			DataProfile *bool `json:"data_profile"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &caps); err != nil {
			t.Fatal(err)
		}
		if caps.DataProfile == nil {
			t.Fatal("data_profile missing from /api/capabilities")
		}
		return *caps.DataProfile
	}
	profiled, tok := scopedServer(t, "sensitive")
	if !read(t, profiled, tok) {
		t.Error("a session with a data profile: data_profile = false, want true")
	}
	if read(t, profiled, "static-tok") {
		t.Error("the static token (no profile): data_profile = true, want false")
	}
	// A console started under --profile whose profile has no rules yet: no
	// table is denied and no column redacted (rbacActiveFor is false), and the
	// profile is still in force.
	profiled.profileActive = true
	if !read(t, profiled, "static-tok") {
		t.Error("a named startup profile with no rules: data_profile = false, want true")
	}
}

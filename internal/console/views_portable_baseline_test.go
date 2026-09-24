package console

import (
	"strings"
	"testing"
)

// newTwoLocationServer is a server whose backups exist in BOTH places: the
// directory it reads by default, and the copy they were uploaded to. That
// pairing is what bundle.baselineFallbackSrc means, and it is the only shape
// where the download has a choice to offer.
func newTwoLocationServer(t *testing.T, dir, uploaded string) *Server {
	t.Helper()
	s := &Server{token: "t", version: "v0.50.0", cm: newConnManager(nil, false)}
	s.cm.boot = &bundle{
		baselineSrc:         dir,
		baselineFallbackSrc: uploaded,
		baselineConfigured:  dir != "",
	}
	s.mux = s.buildHandler()
	return s
}

// TestViewsPortableBaseline_readsTheOtherLocation is #1551 itself: the console
// used to emit whichever location its server happened to be configured with, and
// the person most likely to want the portable file is the one least likely to
// have a shell on that host.
//
// The second location is a directory here, where production has an s3:// prefix.
// What is under test is the SELECTION, which does not read the scheme: it takes
// baselineFallbackSrc, the field that is set only when a server has both. Making
// it a real bucket would test DuckDB's S3 listing, which is neither new nor what
// this changes, and would need credentials to run at all.
//
// Each location holds a DIFFERENT table, so the assertion is which snapshot was
// read and not which string appears. Matching on the path alone would pass for a
// build that emitted the right root and listed the wrong one.
func TestViewsPortableBaseline_readsTheOtherLocation(t *testing.T) {
	onHost, uploaded := t.TempDir(), t.TempDir()
	writeBaselineFixture(t, onHost, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, uploaded, "2026-06-10T12-00-00Z", "shop", "invoices.parquet")
	srv := newTwoLocationServer(t, onHost, uploaded)

	rec, local := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("default download: code = %d, body = %s", rec.Code, local)
	}
	rec, portable := doServersReq(t, srv, "GET", "/api/views.sql?portable_baseline=1", "")
	if rec.Code != 200 {
		t.Fatalf("portable download: code = %d, body = %s", rec.Code, portable)
	}

	// Both directions. Asserting only that the portable file reads the second
	// location would pass for a build that read it in EVERY file, which breaks
	// the reader on the host instead of the one off it.
	if !strings.Contains(string(local), "state_shop_orders") {
		t.Error("the default download does not read the snapshot directory on this host")
	}
	if strings.Contains(string(local), "state_shop_invoices") {
		t.Error("the default download reads the uploaded copy, which needs credentials " +
			"this host may not have")
	}
	if !strings.Contains(string(portable), "state_shop_invoices") {
		t.Error("portable_baseline=1 did not move the state views to the uploaded copy")
	}
	if strings.Contains(string(portable), "state_shop_orders") {
		t.Error("the portable download still reads the snapshot directory on this host, " +
			"which is not on the machine it was downloaded to")
	}
	if cd := rec.Header().Get("Content-Disposition"); !strings.Contains(cd, `filename="views-portable.sql"`) {
		t.Errorf("Content-Disposition = %q, want views-portable.sql; saved under the same name "+
			"as the local file it overwrites it", cd)
	}
}

// TestViewsPortableBaseline_refusedWithoutASecondLocation: the parameter names a
// place, and a server with one backup location has no second place to name.
//
// Refused, never quietly ignored. Falling back to the local directory answers a
// request for a portable file with a 200 and a file whose every state view names
// a path the reader's machine does not have, which is the exact failure the
// parameter exists to avoid.
func TestViewsPortableBaseline_refusedWithoutASecondLocation(t *testing.T) {
	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	srv := newViewsServer(t, dir, false) // local only, no S3 prefix

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?portable_baseline=1", "")
	if rec.Code != 422 {
		t.Fatalf("code = %d, want 422; body = %s", rec.Code, body)
	}
	if !strings.Contains(string(body), "no S3 snapshot prefix") {
		t.Errorf("the refusal does not say what is missing: %s", body)
	}
}

// TestViewsPortableBaseline_capabilityFollowsTheSecondLocation keeps the control
// and the route agreeing. The UI renders the box on this flag, so a flag that
// said yes where the route says 422 would put a box on the card that only fails.
func TestViewsPortableBaseline_capabilityFollowsTheSecondLocation(t *testing.T) {
	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")

	for _, tc := range []struct {
		name string
		srv  *Server
		want bool
	}{
		{"two locations", newTwoLocationServer(t, dir, "s3://bucket/baselines/"), true},
		{"local only", newViewsServer(t, dir, false), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, body := doServersReq(t, tc.srv, "GET", "/api/capabilities", "")
			if rec.Code != 200 {
				t.Fatalf("code = %d, body = %s", rec.Code, body)
			}
			got := strings.Contains(string(body), `"views_portable_baseline":true`)
			if got != tc.want {
				t.Errorf("views_portable_baseline = %v, want %v; body = %s", got, tc.want, body)
			}
		})
	}
}

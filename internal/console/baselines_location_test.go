package console

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
)

// TestBaselinesAPI_locationOnly: GET /api/baselines?location_only=1 says where
// the selected server's snapshots live (the same resolution the listing uses)
// and stops there: no schedule probe, no walk of the storage. Connect AI asks
// it on every open to print the Iceberg export command, and the full listing
// reads the whole store (paid listings on S3, #1679).
func TestBaselinesAPI_locationOnly(t *testing.T) {
	decode := func(t *testing.T, body []byte) baselinesResponse {
		t.Helper()
		var got baselinesResponse
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	t.Run("a directory with snapshots: location, no listing", func(t *testing.T) {
		dir := t.TempDir()
		writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
		srv := newBaselineServer(t, dir, true)
		rec, body := doServersReq(t, srv, "GET", "/api/baselines?location_only=1", "")
		if rec.Code != 200 {
			t.Fatalf("code = %d, body = %s", rec.Code, body)
		}
		got := decode(t, body)
		if !got.Configured || got.Source != dir || got.Kind != "dir" {
			t.Fatalf("got %+v, want the directory as a configured dir source", got)
		}
		if got.Snapshots == nil || len(got.Snapshots) != 0 {
			t.Fatalf("snapshots = %+v, want an empty (non-null) list: the store was walked", got.Snapshots)
		}
		// The listing itself is unchanged without the parameter.
		rec, body = doServersReq(t, srv, "GET", "/api/baselines", "")
		if rec.Code != 200 || len(decode(t, body).Snapshots) != 1 {
			t.Fatalf("plain listing: code %d, body %s; want the one snapshot", rec.Code, body)
		}
	})

	t.Run("a directory that does not exist: still answered, nothing read", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "nope")
		srv := newBaselineServer(t, dir, true)
		rec, body := doServersReq(t, srv, "GET", "/api/baselines?location_only=1", "")
		if rec.Code != 200 || decode(t, body).Source != dir {
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
		got := decode(t, body)
		if got.Source != src || got.Kind != "s3" {
			t.Fatalf("got %+v, want the bucket as an s3 source", got)
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("took %s: the bucket was contacted", d)
		}
	})

	t.Run("nothing configured", func(t *testing.T) {
		srv := newBaselineServer(t, "", false)
		rec, body := doServersReq(t, srv, "GET", "/api/baselines?location_only=1", "")
		got := decode(t, body)
		if rec.Code != 200 || got.Configured || got.Source != "" {
			t.Fatalf("code = %d, got %+v; want 200, not configured, no source", rec.Code, got)
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

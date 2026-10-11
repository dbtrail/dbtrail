package console

import (
	"encoding/json"
	"testing"
)

// TestRotationGet_defaultThenOverride: GET reports the injected daemon defaults
// until an override is saved, then the override — the source field flips
// default→override so the UI can show "(daemon default)" vs an explicit value.
func TestRotationGet_defaultThenOverride(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	srv.rotationDefaults = RotationDefaults{Retain: "30d", Interval: "1h", AddFuture: 3, Enabled: true}

	rec, body := doServersReq(t, srv, "GET", "/api/rotation", "")
	if rec.Code != 200 {
		t.Fatalf("GET code=%d body=%s", rec.Code, body)
	}
	var got rotationDTO
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Retain != "30d" || got.Interval != "1h" || got.AddFuture != 3 || got.Source != "default" || !got.Enabled {
		t.Fatalf("default GET = %+v, want 30d/1h/3 source=default enabled", got)
	}

	rec, body = doServersReq(t, srv, "PUT", "/api/rotation", `{"retain":"7d","interval":"30m","add_future":5}`)
	if rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Retain != "7d" || got.Interval != "30m" || got.AddFuture != 5 || got.Source != "override" {
		t.Fatalf("override PUT response = %+v, want 7d/30m/5 source=override", got)
	}

	// A fresh GET reflects the persisted override.
	_, body = doServersReq(t, srv, "GET", "/api/rotation", "")
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Source != "override" || got.Retain != "7d" {
		t.Fatalf("re-GET after override = %+v, want the saved 7d override", got)
	}
}

// TestRotationGet_overrideWhenDaemonOff: when the daemon booted with rotation
// off, StartLoop runs no loop, so a saved override is dormant until a restart.
// The override persists, but the API must NOT claim it is live (enabled:false) —
// otherwise the panel would tell the operator rotation is bounded when the index
// is in fact growing unbounded. Regression guard for the review finding.
func TestRotationGet_overrideWhenDaemonOff(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	srv.rotationDefaults = RotationDefaults{Retain: "off", Interval: "", AddFuture: 0, Enabled: false}

	// Before any override, GET reports the off state.
	_, body := doServersReq(t, srv, "GET", "/api/rotation", "")
	var got rotationDTO
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Enabled {
		t.Fatalf("daemon booted off must report enabled:false, got %+v", got)
	}

	// The PUT is accepted (a supervisor is present) and persists...
	rec, body := doServersReq(t, srv, "PUT", "/api/rotation", `{"retain":"7d","interval":"30m","add_future":5}`)
	if rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}

	// ...but GET must still report enabled:false — the loop never started, so the
	// saved override does not apply until a restart.
	_, body = doServersReq(t, srv, "GET", "/api/rotation", "")
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Source != "override" || got.Retain != "7d" {
		t.Fatalf("override should be persisted: %+v", got)
	}
	if got.Enabled {
		t.Error("a saved override on a daemon booted with rotation off must report enabled:false (loop not running until restart)")
	}
}

// TestRotationUpdate_validation: a bad retain/interval/add_future is rejected at
// the API boundary (400) rather than silently breaking a rotation cycle. "off"
// is forbidden here — disabling rotation stays a daemon-level decision.
func TestRotationUpdate_validation(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	cases := []struct{ name, body string }{
		{"bad retain unit", `{"retain":"soon","interval":"1h","add_future":3}`},
		{"off forbidden", `{"retain":"off","interval":"1h","add_future":3}`},
		{"bad interval", `{"retain":"7d","interval":"later","add_future":3}`},
		{"nonpositive interval", `{"retain":"7d","interval":"0s","add_future":3}`},
		{"negative add_future", `{"retain":"7d","interval":"1h","add_future":-1}`},
	}
	for _, tc := range cases {
		rec, body := doServersReq(t, srv, "PUT", "/api/rotation", tc.body)
		if rec.Code != 400 {
			t.Errorf("%s: code=%d body=%s, want 400", tc.name, rec.Code, body)
		}
	}
}

// TestRotationUpdate_readOnlyConsoleForbidden: the standalone console (no
// supervisor) refuses the PUT — nothing there would consume the policy, so a
// silent "saved but ignored" must not happen. GET still works (reports
// defaults).
func TestRotationUpdate_readOnlyConsoleForbidden(t *testing.T) {
	srv := newRegistryServer(t) // no MonitorCtrl
	rec, body := doServersReq(t, srv, "PUT", "/api/rotation", `{"retain":"7d","interval":"1h","add_future":3}`)
	if rec.Code != 403 {
		t.Fatalf("read-only console PUT code=%d body=%s, want 403", rec.Code, body)
	}
	if rec, _ := doServersReq(t, srv, "GET", "/api/rotation", ""); rec.Code != 200 {
		t.Errorf("GET on read-only console code=%d, want 200", rec.Code)
	}
}

// TestRotationGet_explicitRetain_2315: with --rotate-retain /
// BINTRAIL_ROTATE_RETAIN set, the loop drops every index on that window and
// reads no record, so index_retain must be that window (basis "configured")
// and the index must not be asked — the panel used to say "this server keeps
// 48h" under a 6h loop. Without an explicit window the record still governs.
func TestRotationGet_explicitRetain_2315(t *testing.T) {
	get := func(t *testing.T, srv *Server) rotationDTO {
		t.Helper()
		rec, body := doServersReq(t, srv, "GET", "/api/rotation", "")
		if rec.Code != 200 {
			t.Fatalf("GET code=%d body=%s", rec.Code, body)
		}
		var got rotationDTO
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	t.Run("explicit: index_retain is the configured window and the index is not asked", func(t *testing.T) {
		srv, _ := newSupervisorServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "6h", Interval: "1h", AddFuture: 3, Enabled: true, Explicit: true}
		// No read expected: an unexpected query would make ResolveEffective
		// answer the legacy 30d, which the assertion below would catch.
		db, _ := recordedRetainDB(t, "48h", false)
		srv.cm.boot = &bundle{db: db, dbName: "binlog_index"}
		got := get(t, srv)
		if got.Source != "default" || got.Retain != "6h" || got.IndexRetain != "6h" || got.IndexBasis != "configured" {
			t.Fatalf("GET = %+v, want retain=index_retain=6h source=default index_basis=configured", got)
		}
	})

	t.Run("implicit: the record the index was created under governs (#1709)", func(t *testing.T) {
		srv, _ := newSupervisorServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "48h", Interval: "1h", AddFuture: 3, Enabled: true}
		db, mock := recordedRetainDB(t, "30d", true)
		srv.cm.boot = &bundle{db: db, dbName: "binlog_index"}
		got := get(t, srv)
		if got.Retain != "48h" || got.IndexRetain != "30d" || got.IndexBasis != "recorded" {
			t.Fatalf("GET = %+v, want retain=48h index_retain=30d index_basis=recorded", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("override the loop refuses (bad interval): reported as the configured default it runs", func(t *testing.T) {
		srv, _ := newSupervisorServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "6h", Interval: "1h", AddFuture: 3, Enabled: true, Explicit: true}
		if err := srv.cm.reg.SetRotation(RotationConfig{Retain: "7d", Interval: "1x", AddFuture: 3}); err != nil {
			t.Fatal(err)
		}
		got := get(t, srv)
		if got.Source != "default" || got.Retain != "6h" || got.IndexRetain != "6h" || got.IndexBasis != "configured" {
			t.Fatalf("GET = %+v, want the 6h default the loop falls back to, not the refused 7d", got)
		}
	})

	t.Run("override saved: explicit changes nothing, the override applies to every index", func(t *testing.T) {
		srv, _ := newSupervisorServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "6h", Interval: "1h", AddFuture: 3, Enabled: true, Explicit: true}
		if err := srv.cm.reg.SetRotation(RotationConfig{Retain: "7d", Interval: "1h", AddFuture: 3}); err != nil {
			t.Fatal(err)
		}
		got := get(t, srv)
		if got.Source != "override" || got.Retain != "7d" || got.IndexRetain != "" || got.IndexBasis != "" {
			t.Fatalf("GET = %+v, want the 7d override with no index_retain", got)
		}
	})
}

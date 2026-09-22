package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// GET reports what the daemon runs, which since #1681 is its own flag and
// nothing else: the console-saved override is gone, and so is the "source"
// field that named which of the two had won.
func TestBaselineRefreshGet_reportsTheDaemonFlag(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	srv.baselineRefreshDefaults = BaselineRefreshDefaults{CarryForwardUnchanged: true, Enabled: true}

	rec, body := doServersReq(t, srv, "GET", "/api/baseline-refresh", "")
	if rec.Code != 200 {
		t.Fatalf("GET code=%d body=%s", rec.Code, body)
	}
	var got baselineRefreshDTO
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if !got.CarryForwardUnchanged || !got.Enabled {
		t.Fatalf("got %+v, want the daemon flag reported as it is", got)
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["source"]; ok {
		t.Errorf("the wire still carries a source field: %s; there is one source now, so naming it invites a reader to look for the other", body)
	}

	// A daemon started with reuse off is the one case left where this is false.
	srv.baselineRefreshDefaults = BaselineRefreshDefaults{CarryForwardUnchanged: false, Enabled: true}
	_, body = doServersReq(t, srv, "GET", "/api/baseline-refresh", "")
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.CarryForwardUnchanged {
		t.Errorf("got %+v, want the flag's own value: the card draws what the daemon does", got)
	}
}

// TestBaselineRefreshUpdate_isGone (#1681): the console cannot write this
// setting any more. The route is not registered, so the write a stale client
// or an old bookmark sends is refused by the mux rather than half-handled.
func TestBaselineRefreshUpdate_isGone(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	rec, body := doServersReq(t, srv, "PUT", "/api/baseline-refresh", `{"carry_forward_unchanged":false}`)
	if rec.Code < 400 {
		t.Fatalf("PUT code=%d body=%s, want a refusal: the setting is not editable any more", rec.Code, body)
	}
	// And the read still works, since the card still reports what the daemon does.
	if rec, _ := doServersReq(t, srv, "GET", "/api/baseline-refresh", ""); rec.Code != 200 {
		t.Errorf("GET code=%d, want 200", rec.Code)
	}
}

// TestBaselineRefreshGet_targetsAreLiveAndOmittedOffWatch pins the #1579
// shape: enabled+scheduled both true was reachable while the loop covered
// ZERO servers, and the DTO had no way to say so. With the counter wired the
// counts are computed at REQUEST time (the loop recomputes per tick, so a
// boot snapshot goes stale the moment a server is added); without it — serve,
// or no loop — the keys are omitted entirely, so the page cannot alarm on a
// zero that means "nobody is counting".
func TestBaselineRefreshGet_targetsAreLiveAndOmittedOffWatch(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	srv.baselineRefreshDefaults = BaselineRefreshDefaults{Enabled: true, Scheduled: true}

	// No counter wired: both keys absent, not zero.
	_, body := doServersReq(t, srv, "GET", "/api/baseline-refresh", "")
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["targets"]; ok {
		t.Fatalf("targets serialised without a counter wired: %s (a real zero and \"nobody is counting\" must differ)", body)
	}

	// Wired: live values, re-read per request.
	n := 0
	srv.baselineRefreshTargets = func() (int, int) { n++; return n, 2 }
	var got baselineRefreshDTO
	_, body = doServersReq(t, srv, "GET", "/api/baseline-refresh", "")
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Targets == nil || *got.Targets != 1 || got.SkippedS3Only != 2 {
		t.Fatalf("first read: %+v, want targets=1 skipped=2", got)
	}
	_, body = doServersReq(t, srv, "GET", "/api/baseline-refresh", "")
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Targets == nil || *got.Targets != 2 {
		t.Fatalf("second read: %+v; the count is a boot snapshot, not live — a server added later would "+
			"keep the stale zero and the alarm would cry wolf forever", got)
	}

	// The counts are appended after everything else the DTO carries, and a
	// browser-side render matrix cannot see a server-side early return, so a
	// third read pins that they keep coming back.
	//
	// Decoded into a FRESH struct, never the one above: Unmarshal leaves a
	// field the payload omits at its previous value, so reusing `got` would
	// carry the pointer from the read before and the assertion would pass
	// against a response that dropped the key.
	_, body = doServersReq(t, srv, "GET", "/api/baseline-refresh", "")
	var after baselineRefreshDTO
	if err := json.Unmarshal(body, &after); err != nil {
		t.Fatal(err)
	}
	if after.Targets == nil || after.SkippedS3Only != 2 {
		t.Fatalf("third read: %+v (%s), want both counts", after, body)
	}
}

// TestBaselineRefreshGet_defaultsTravelThroughNew closes the last hop of the
// READ path: console.Config -> Server -> the DTO the panel renders.
//
// Every other test in this file sets srv.baselineRefreshDefaults by direct
// field write, which is convenient and skips exactly the assignment that could
// be deleted. Built through New() instead, so dropping that line reports the
// zero value: reuse off, no schedule, on a daemon running with both.
func TestBaselineRefreshGet_defaultsTravelThroughNew(t *testing.T) {
	// The registry carries the block an older console saved, saying the
	// opposite of the daemon flag below: this build ignores it (#1681), so
	// what the card reads is the flag. A reader is the only way to see that
	// the ignoring is real rather than a missing field.
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	if err := os.WriteFile(path, []byte("version: 1\nbaseline_refresh:\n  carry_forward_unchanged: false\nservers: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: &stubMonitorCtrl{},
		BaselineRefreshDefaults: BaselineRefreshDefaults{CarryForwardUnchanged: true, Enabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, body := doServersReq(t, srv, "GET", "/api/baseline-refresh", "")
	if rec.Code != 200 {
		t.Fatalf("GET code=%d body=%s", rec.Code, body)
	}
	var got baselineRefreshDTO
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if !got.CarryForwardUnchanged {
		t.Error("the daemon's reuse flag did not survive New() (or the old saved block beat it); the card " +
			"would draw every table being rewritten while the daemon reuses them")
	}
	if !got.Enabled {
		t.Error("the daemon's loop liveness did not survive New(); the panel would call a live setting dormant")
	}
}

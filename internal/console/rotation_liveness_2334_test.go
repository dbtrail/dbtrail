package console

import (
	"encoding/json"
	"testing"
)

// #2334: a daemon started with --rotate-retain off over an already saved
// policy runs the loop on that policy (rotation.StartLoop reads the saved
// policy at start). The web interface took "is rotation on" from the flag
// alone, so it said "rotation is turned off" and graded unbounded growth over
// a loop that was dropping partitions. Enabled now means "a loop runs".
func TestRotationLiveness_2334(t *testing.T) {
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
	save := func(t *testing.T, srv *Server) {
		t.Helper()
		if err := srv.cm.reg.SetRotation(RotationConfig{Retain: "7d", Interval: "1h", AddFuture: 3}); err != nil {
			t.Fatal(err)
		}
	}
	// The flags as --rotate-retain off leaves them, with the loop's own
	// verdict in Enabled.
	flagsOff := func(loopRuns bool) RotationDefaults {
		return RotationDefaults{Retain: "off", Interval: "1h", AddFuture: 3, Enabled: loopRuns}
	}

	t.Run("flag off, loop running on the saved 7d: the panel and the capacity check say 7d, live", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = flagsOff(true)
		save(t, srv)
		if got := get(t, srv); got.Source != "override" || got.Retain != "7d" || !got.Enabled {
			t.Fatalf("GET = %+v, want the saved 7d, enabled", got)
		}
		stubCapacityProbe(srv, capacityFixture(0, 2_000_000_000, true), nil)
		got := capacityGet(t, srv)
		if got.Reason != "ok" || got.ProjectedBytes != 168_000_000 || !got.Retention.Enabled || got.Retention.Source != "override" {
			t.Fatalf("capacity = %s projected=%v retention=%+v, want ok over the 7d the loop drops on", got.Reason, got.ProjectedBytes, got.Retention)
		}
	})

	t.Run("flag off, no loop, 7d saved afterwards: dormant until a restart", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = flagsOff(false)
		save(t, srv)
		if got := get(t, srv); got.Source != "override" || got.Enabled {
			t.Fatalf("GET = %+v, want the saved policy reported as NOT live", got)
		}
		stubCapacityProbe(srv, capacityFixture(0, 2_000_000_000, true), nil)
		if got := capacityGet(t, srv); got.Reason != "no_retention" || got.ProjectedBytes != 0 {
			t.Fatalf("capacity = %s projected=%v, want no_retention: nothing drops until a restart", got.Reason, got.ProjectedBytes)
		}
	})

	t.Run("a loop that started on a saved policy, the policy no longer usable: the flags' off, not 'off but on'", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = flagsOff(true)
		if err := srv.cm.reg.SetRotation(RotationConfig{Retain: "7d", Interval: "1x", AddFuture: 3}); err != nil {
			t.Fatal(err)
		}
		if got := get(t, srv); got.Source != "default" || got.Retain != "off" || got.Enabled {
			t.Fatalf("GET = %+v, want the flags' off, not enabled", got)
		}
		stubCapacityProbe(srv, capacityFixture(0, 2_000_000_000, true), nil)
		if got := capacityGet(t, srv); got.Reason != "no_retention" {
			t.Fatalf("capacity = %s, want no_retention", got.Reason)
		}
	})

	t.Run("flag on: unchanged", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		if got := get(t, srv); got.Source != "default" || got.Retain != "30d" || !got.Enabled {
			t.Fatalf("GET = %+v, want the 30d default, enabled", got)
		}
	})
}

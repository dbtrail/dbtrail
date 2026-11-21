package consoleapp

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/notify"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The figure the rotation form refuses by is the line the fold ends chains
// on: at or under it no chain a cycle begins is there at the next, one
// nanosecond over it one can be. Whatever the rule knows about the index's
// own window.
func TestMinRetainForTableDeltas_isTheLineTheFoldEndsChainsOn_2959(t *testing.T) {
	for _, interval := range []time.Duration{0, 5 * time.Minute, 20 * time.Minute, 30 * time.Minute, time.Hour, 6 * time.Hour, 23 * time.Hour, 24*time.Hour - time.Second} {
		min, comfortable, ok := minRetainForTableDeltas(interval)
		if want := 2*interval + time.Hour; !ok || min != want || comfortable != want+time.Hour {
			t.Errorf("interval %s: must exceed %s, comfortable %s, ok=%v; want %s and one hour more", interval, min, comfortable, ok, want)
		}
		for _, c := range []struct {
			retain time.Duration
			want   bool
		}{
			{time.Hour, time.Hour > min},
			{min - time.Hour, false},
			{min - time.Nanosecond, false},
			{min, false},
			{min + time.Nanosecond, true},
			{min + time.Hour, true},
			{30 * 24 * time.Hour, 30*24*time.Hour > min},
		} {
			if c.retain <= 0 {
				continue
			}
			for _, dropsAfter := range []time.Duration{0, c.retain, 2 * c.retain} {
				rule := coverageRule{retain: c.retain, interval: interval, dropsAfter: dropsAfter}
				if got := rule.chainOutlivesCycle(); got != c.want {
					t.Errorf("interval %s, retain %s, dropsAfter %s: chainOutlivesCycle = %v, want %v",
						interval, c.retain, dropsAfter, got, c.want)
				}
				if got := rule.retentionEndsEveryChain(); got != !c.want {
					t.Errorf("interval %s, retain %s, dropsAfter %s: retentionEndsEveryChain = %v, want %v",
						interval, c.retain, dropsAfter, got, !c.want)
				}
			}
		}
	}
}

// From the comfortable retention on, the chain a cycle began survives the
// next wherever the floor sits in its hour: rotation leaves the oldest live
// hour anywhere from on the retention's edge to just under an hour inside it.
// Just over the minimum it survives only on the kindest of those floors.
func TestComfortableRetention_survivesOnEveryFloor_2959(t *testing.T) {
	now := time.Date(2026, 11, 20, 15, 5, 0, 0, time.UTC)
	for _, interval := range []time.Duration{5 * time.Minute, 20 * time.Minute, time.Hour, 6 * time.Hour} {
		min, comfortable, _ := minRetainForTableDeltas(interval)
		start := now.Add(-interval)
		survives := func(retain, ahead time.Duration) bool {
			rule := coverageRule{retain: retain, interval: interval, dropsAfter: retain}
			line, ok := rule.reanchorBy(now.Add(-retain).Add(ahead), now)
			return ok && start.After(line)
		}
		for ahead := time.Duration(0); ahead < time.Hour; ahead += time.Minute {
			if !survives(comfortable, ahead) {
				t.Errorf("interval %s, retain %s, floor %s inside the retention: the chain is ended", interval, comfortable, ahead)
			}
		}
		if !survives(min+time.Minute, 0) || survives(min+time.Minute, 30*time.Minute) {
			t.Errorf("interval %s: just over the minimum the chain should survive on the retention's edge and not half an hour inside it", interval)
		}
	}
}

// Updates a day apart or more: the fold ends every chain by age at the next
// one (reconstruct.TableDeltaMaxAge), so no retention is asked for and the
// retention is never named as the cause.
func TestNoRetentionAskedForUpdatesADayApart_2959(t *testing.T) {
	if reconstruct.TableDeltaMaxAge != 24*time.Hour {
		t.Fatalf("the age a chain is ended at is %s; the intervals of this test assume a day", reconstruct.TableDeltaMaxAge)
	}
	for _, interval := range []time.Duration{reconstruct.TableDeltaMaxAge, 48 * time.Hour, 7 * 24 * time.Hour} {
		if must, comfortable, ok := minRetainForTableDeltas(interval); ok || must != 0 || comfortable != 0 {
			t.Errorf("interval %s: asks for a retention over %s (ok=%v)", interval, must, ok)
		}
		for _, retain := range []time.Duration{time.Hour, 48 * time.Hour, 10 * 24 * time.Hour, 60 * 24 * time.Hour} {
			if (coverageRule{retain: retain, interval: interval, dropsAfter: retain}).retentionEndsEveryChain() {
				t.Errorf("interval %s, retain %s: the retention is named as what ends every chain", interval, retain)
			}
		}
	}
	// One second under a day is still the retention's to decide.
	if _, _, ok := minRetainForTableDeltas(reconstruct.TableDeltaMaxAge - time.Second); !ok {
		t.Error("an interval just under a day asks for no retention")
	}
}

// What the form would refuse does not work today on any floor an index as old
// as its retention can have: between rotation runs the oldest live hour is
// anywhere from hours behind now-retain (a rotation that is late, or off) to
// just under an hour ahead of it. On every one of them the chain a cycle
// began is ended at the next. So the refusal takes away nothing that worked.
func TestRefusedRetention_endsEveryChainOnEveryFloor_2959(t *testing.T) {
	now := time.Date(2026, 11, 20, 15, 5, 0, 0, time.UTC)
	for _, interval := range []time.Duration{5 * time.Minute, 30 * time.Minute, time.Hour, 23 * time.Hour} {
		min, _, _ := minRetainForTableDeltas(interval)
		for _, retain := range []time.Duration{time.Hour, min - time.Minute, min} {
			if retain < time.Hour {
				continue
			}
			rule := coverageRule{retain: retain, interval: interval, dropsAfter: retain}
			start := now.Add(-interval)
			for behind := -59 * time.Minute; behind <= 6*time.Hour; behind += time.Minute {
				floor := now.Add(-retain).Add(-behind)
				line, ok := rule.reanchorBy(floor, now)
				if !ok || start.After(line) {
					t.Fatalf("interval %s, retain %s, floor %s behind now-retain: a chain begun at the last cycle survives (line %s, start %s)",
						interval, retain, behind, line, start)
				}
			}
		}
	}
}

// The values the product ships with, and the ones of the report.
func TestChainOutlivesCycle_factoryAndReported_2959(t *testing.T) {
	// Factory: retention 48h, a snapshot every 5m.
	if !(coverageRule{retain: 48 * time.Hour, interval: 5 * time.Minute, dropsAfter: 48 * time.Hour}).chainOutlivesCycle() {
		t.Error("the factory retention leaves no room for a chain")
	}
	if got, well, _ := minRetainForTableDeltas(5 * time.Minute); got != 70*time.Minute || well != 130*time.Minute {
		t.Errorf("a snapshot every 5m needs more than %s and works well from %s, want 1h10m and 2h10m", got, well)
	}
	// #2959: retention 1h, a snapshot every 5m.
	if (coverageRule{retain: time.Hour, interval: 5 * time.Minute, dropsAfter: time.Hour}).chainOutlivesCycle() {
		t.Error("retention 1h with a snapshot every 5m is said to leave room for a chain")
	}
	// No retention chosen and an index created under a shorter window than
	// the daemon's default: the index's own window is what drops its events.
	if (coverageRule{retain: 48 * time.Hour, interval: 5 * time.Minute, dropsAfter: time.Hour}).chainOutlivesCycle() {
		t.Error("an index that drops on 1h is said to leave room because the daemon default is 48h")
	}
	if !(coverageRule{interval: 5 * time.Minute, dropsAfter: 6 * time.Hour}).chainOutlivesCycle() {
		t.Error("an index that drops on 6h is said to leave no room for a chain at 5m")
	}
	// No retention configured: not this rule's to say.
	if !(coverageRule{interval: 24 * time.Hour}).chainOutlivesCycle() {
		t.Error("no retention configured is said to end every chain")
	}
}

// The cycle says it once and takes it back when the retention is fixed.
func TestReportNoRoomForChains_2959(t *testing.T) {
	s := &baselineSupervisor{gateEdge: notify.NewEdge(time.Hour)}
	req := refreshRequest{ServerID: "s1", ServerName: "orders"}
	key := noRoomForChainsKey("s1", 5*time.Minute)
	s.reportNoRoomForChains(req, coverageRule{retain: 48 * time.Hour, interval: 5 * time.Minute})
	if s.gateEdge.Active(key) {
		t.Fatal("a retention with room was reported")
	}
	s.reportNoRoomForChains(req, coverageRule{retain: time.Hour, interval: 5 * time.Minute})
	if !s.gateEdge.Active(key) {
		t.Fatal("a retention with no room was not reported")
	}
	s.reportNoRoomForChains(req, coverageRule{retain: 2 * time.Hour, interval: 5 * time.Minute})
	if s.gateEdge.Active(key) {
		t.Fatal("the report stands after the retention was fixed")
	}
	// The same server on a second cadence that has no room either: its own
	// condition, which the first one's verdict neither raises nor resolves.
	s.reportNoRoomForChains(req, coverageRule{retain: 2 * time.Hour, interval: 6 * time.Hour})
	s.reportNoRoomForChains(req, coverageRule{retain: 2 * time.Hour, interval: 5 * time.Minute})
	if !s.gateEdge.Active(noRoomForChainsKey("s1", 6*time.Hour)) || s.gateEdge.Active(key) {
		t.Fatal("two cadences of one server share a condition")
	}
	// Updates a day apart or more are not the retention's doing: not reported
	// on any retention, the factory one with a daily update included.
	for _, every := range []time.Duration{24 * time.Hour, 48 * time.Hour, 7 * 24 * time.Hour} {
		for _, retain := range []time.Duration{time.Hour, 48 * time.Hour, 10 * 24 * time.Hour} {
			s.reportNoRoomForChains(req, coverageRule{retain: retain, interval: every})
			if s.gateEdge.Active(noRoomForChainsKey("s1", every)) {
				t.Fatalf("an update every %s under a retention of %s was reported as the retention's fault", every, retain)
			}
		}
	}
	// A supervisor built without the edge (a test's) says nothing and does not panic.
	(&baselineSupervisor{}).reportNoRoomForChains(req, coverageRule{retain: time.Hour, interval: 5 * time.Minute})
}

func TestRefreshEveryOrZero_2959(t *testing.T) {
	for raw, want := range map[string]time.Duration{"": 0, "  ": 0, "6h": 6 * time.Hour, " 30m ": 30 * time.Minute, "1d": 24 * time.Hour, "soon": 0} {
		if got := refreshEveryOrZero(raw); got != want {
			t.Errorf("refreshEveryOrZero(%q) = %s, want %s", raw, got, want)
		}
	}
}

package console

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #1736: under a constant load every update applies about the same number
// of events, so the marginal rate (events beyond the shortest run per
// second beyond it) is a slope with no lever arm on the events axis: a few
// hundred events over tens of seconds of noise. Read off such a sample it
// made a five-minute window look like days and cut the schedule over to a
// full backup, with lock, every slot. The per-event cost is not
// distinguishable from such runs, exactly as it is not from runs whose
// TIMES are flat, and the model says so the same way: no rate.

// The measured updates the daemon actually fitted at 19:10:35 UTC on
// 2026-09-18 (its own history file; the 18:45 refresh, the first after a
// full backup, carried no count and was not in the sample).
var rigSeries = []BaselineRunRecord{
	{Kind: BaselineRunRefresh, Events: 938_813, UpdateSeconds: 34.136798111},
	{Kind: BaselineRunRefresh, Events: 924_606, UpdateSeconds: 34.695162604},
	{Kind: BaselineRunRefresh, Events: 939_009, UpdateSeconds: 78.475738033},
	{Kind: BaselineRunRefresh, Events: 935_668, UpdateSeconds: 49.751031609},
}

func historyWith(t *testing.T, recs []BaselineRunRecord) *BaselineRunHistory {
	t.Helper()
	h, err := OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		r.ServerID = "s"
		if err := h.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	return h
}

func TestUpdateModel_flatEventsHaveNoRate(t *testing.T) {
	h := historyWith(t, rigSeries)
	fixed, rate := h.UpdateModel("s")
	shortest := rigSeries[0].UpdateSeconds
	// Before: 196 events beyond the shortest run over 44.3 s beyond it,
	// 4.42 events/s, "943,207 events would take about 59h 17m".
	if rate != 0 || fixed != time.Duration(shortest*float64(time.Second)) {
		t.Fatalf("rig series: fixed=%s rate=%v, want the shortest run and NO rate (the per-event cost is not distinguishable)", fixed, rate)
	}
	// The whole rule on that slot: no rate, a five-minute-old anchor, so
	// an update; whatever the count.
	now := time.Date(2026, 9, 18, 19, 10, 35, 0, time.UTC)
	w := BackupWindow{Anchor: now.Add(-5 * time.Minute), Events: 943_207, FoldFixed: fixed, FoldRate: rate, Proven: 939_009, LastFull: 15 * time.Minute}
	if why := CutoverToFull(w, 5*time.Minute, now); why != "" {
		t.Fatalf("the 19:10 slot cut over: %q", why)
	}
	// The same slot with the artefact rate it had, held back by the
	// proven margin alone (#1736's second belt): 943,207 is within 1.5x
	// of the 939,009 an update applied in under the full backup's time.
	w.FoldRate = 196 / (rigSeries[2].UpdateSeconds - shortest)
	if why := CutoverToFull(w, 5*time.Minute, now); why != "" {
		t.Fatalf("within the proven margin but cut over: %q", why)
	}
	margin := BackupProvenMargin // a variable: the conversions below are not constant folding
	w.Events = int64(margin*float64(939_009)) + 1
	if why := CutoverToFull(w, 5*time.Minute, now); BackupWhyCode(why) != "window_measured" {
		t.Fatalf("past the proven margin with a dear rate, want the measured cut-over, got %q", why)
	}
}

// The events guard from both sides, mirroring the time guard: the events
// beyond the shortest run under a tenth of the sample's mean read as no
// rate; at or over it, the marginal rate as before.
func TestUpdateModel_eventsSpreadThreshold(t *testing.T) {
	// The slope is kept plausible on both sides (the shortest run's
	// 1,000,000 in 60 s is 16,667/s, a tenth of it 1,667/s, under both
	// slopes below), so only the spread guard is being read. A million
	// rather than the hundred thousand this test had before #1738: every
	// run here applies at least 20,000 events more than the shortest, so
	// the per-run floor (measuredFoldMinEvents) passes too.
	series := func(more int64, sec float64) []BaselineRunRecord {
		out := []BaselineRunRecord{{Kind: BaselineRunRefresh, Events: 1_000_000, UpdateSeconds: 60}}
		for range 4 {
			out = append(out, BaselineRunRecord{Kind: BaselineRunRefresh, Events: more, UpdateSeconds: sec})
		}
		return out
	}
	// Four runs of 1,020,000 in 70 s: 80,000 events beyond, mean
	// 1,016,000, a tenth is 101,600: not distinguishable. The other guards
	// let this one through on purpose (40 s beyond is over a tenth of the
	// total, 2,000/s is over a tenth of the 16,667/s floor, and each run
	// applied 20,000 more than the shortest), so this is the band only the
	// spread guard covers.
	if fixed, rate := historyWith(t, series(1_020_000, 70)).UpdateModel("s"); rate != 0 || fixed != time.Minute {
		t.Fatalf("under the tenth: fixed=%s rate=%v, want no rate", fixed, rate)
	}
	// Four runs of 1,200,000 in 72 s: 800,000 beyond, a tenth of the mean
	// is 116,000: a rate, 800,000 events over 48 s.
	if fixed, rate := historyWith(t, series(1_200_000, 72)).UpdateModel("s"); rate != 800_000/48.0 || fixed != time.Minute {
		t.Fatalf("over the tenth: fixed=%s rate=%v, want 800000/48", fixed, rate)
	}
	// Two runs of the same size on different days: nothing beyond, no rate
	// (a whole-run rate here, 200,000/100 s, would look real on a loaded
	// server and be an artefact on a quiet one; the model cannot tell).
	two := []BaselineRunRecord{{Kind: BaselineRunRefresh, Events: 200_000, UpdateSeconds: 60}, {Kind: BaselineRunRefresh, Events: 200_000, UpdateSeconds: 100}}
	if _, rate := historyWith(t, two).UpdateModel("s"); rate != 0 {
		t.Fatalf("two equal runs: rate=%v, want none", rate)
	}
}

// A quiet server whose updates take the fixed cost plus noise: the events
// axis is flat (a hundred events each), the time axis is not (one slow
// day), and a burst after that must still be an update on a fresh anchor.
// This passes on the rule as it was too (nothing is beyond the shortest
// run on both axes); it is here to pin the design decision against the
// whole-run fallback #1736 first proposed, which would have broken it:
// 100 events in 150 s is 0.67 events/s, and 60,000 events at that "rate"
// read as a day.
func TestUpdateModel_quietServerWithASlowDayKeepsNoRate(t *testing.T) {
	var recs []BaselineRunRecord
	for range 4 {
		recs = append(recs, BaselineRunRecord{Kind: BaselineRunRefresh, Events: 100, UpdateSeconds: 90})
	}
	recs = append(recs, BaselineRunRecord{Kind: BaselineRunRefresh, Events: 100, UpdateSeconds: 150})
	fixed, rate := historyWith(t, recs).UpdateModel("s")
	if rate != 0 || fixed != 90*time.Second {
		t.Fatalf("quiet server, slow day: fixed=%s rate=%v, want no rate", fixed, rate)
	}
	now := time.Date(2026, 9, 18, 10, 30, 0, 0, time.UTC)
	w := BackupWindow{Anchor: now.Add(-5 * time.Minute), Events: 60_000, FoldFixed: fixed, FoldRate: rate, Proven: 100, LastFull: 40 * time.Minute}
	if why := CutoverToFull(w, 5*time.Minute, now); why != "" {
		t.Fatalf("a burst on a quiet server cut over: %q", why)
	}
}

// The proven margin: nothing proven leaves the rule as it was.
func TestCutoverToFull_provenMargin(t *testing.T) {
	now := time.Date(2026, 9, 18, 10, 30, 0, 0, time.UTC)
	fresh := now.Add(-5 * time.Minute)
	dear := BackupWindow{Anchor: fresh, Events: 17_000_000, FoldRate: 4000, LastFull: 8 * time.Minute}
	cases := []struct {
		name   string
		proven int64
		want   string
	}{
		{"nothing proven", 0, "window_measured"},
		{"proven 11,000,000: 1.5x is short of 17,000,000", 11_000_000, "window_measured"},
		{"proven 11,400,000: 1.5x covers it", 11_400_000, ""},
		{"the largest proven that still cuts over (1.5x = 16,999,999.5)", 11_333_333, "window_measured"},
		{"one more proven and it is covered (1.5x = 17,000,001)", 11_333_334, ""},
	}
	for _, c := range cases {
		w := dear
		w.Proven = c.proven
		if got := BackupWhyCode(CutoverToFull(w, 5*time.Minute, now)); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

// The plausibility guard (the review's check): the shortest run's whole
// rate, events over all its seconds, is a floor under the true per-event
// rate; a marginal rate a tenth of it or slower is noise in the
// denominator, whatever the spread guard says.
func TestUpdateModel_marginalRateBelowTheFloorIsNoise(t *testing.T) {
	// One notch noisier than the rig: 2.5% more events on four runs whose
	// durations are noise. 98,000 beyond passes the spread guard (a tenth
	// of the mean is 95,700) and the slope reads 1,010 events/s against a
	// floor of 27,600.
	noisy := []BaselineRunRecord{
		{Kind: BaselineRunRefresh, Events: 939_000, UpdateSeconds: 34},
		{Kind: BaselineRunRefresh, Events: 963_000, UpdateSeconds: 78},
		{Kind: BaselineRunRefresh, Events: 964_000, UpdateSeconds: 50},
		{Kind: BaselineRunRefresh, Events: 962_000, UpdateSeconds: 60},
		{Kind: BaselineRunRefresh, Events: 965_000, UpdateSeconds: 45},
	}
	if _, rate := historyWith(t, noisy).UpdateModel("s"); rate != 0 {
		t.Fatalf("noisy series: rate=%v, want none (1,010/s against a floor of 27,600/s)", rate)
	}
	// The rig's four runs plus a real burst, 1,040,000 in 40 s: the burst
	// alone says about 17,000/s, but the 78 s run's 44 s of noise sit in
	// the same denominator and the slope reads 2,019/s. Not a rate.
	withBurst := append(append([]BaselineRunRecord(nil), rigSeries...), BaselineRunRecord{Kind: BaselineRunRefresh, Events: 1_040_000, UpdateSeconds: 40})
	if _, rate := historyWith(t, withBurst).UpdateModel("s"); rate != 0 {
		t.Fatalf("rig plus a burst: rate=%v, want none (2,019/s against a floor of 27,500/s)", rate)
	}
	// A real burst over a clean floor keeps its rate: four runs of 200,000
	// in 60 s and one of 400,000 in 100 s is 200,000 more events in 40 s,
	// 5,000/s, above the floor of 3,333/s (the fixed cost inside the floor
	// explains the difference).
	clean := []BaselineRunRecord{}
	for range 4 {
		clean = append(clean, BaselineRunRecord{Kind: BaselineRunRefresh, Events: 200_000, UpdateSeconds: 60})
	}
	clean = append(clean, BaselineRunRecord{Kind: BaselineRunRefresh, Events: 400_000, UpdateSeconds: 100})
	if _, rate := historyWith(t, clean).UpdateModel("s"); rate != 200_000/40.0 {
		t.Fatalf("a real burst: rate=%v, want 200000/40", rate)
	}
	// Exactly at the floor's tenth from both sides: shortest 100,000 in
	// 100 s (floor 1,000/s), four runs of 200,000 whose extra seconds put
	// the slope at 100/s (kept) or just under (dropped).
	at := func(extra float64) float64 {
		recs := []BaselineRunRecord{{Kind: BaselineRunRefresh, Events: 100_000, UpdateSeconds: 100}}
		for range 4 {
			recs = append(recs, BaselineRunRecord{Kind: BaselineRunRefresh, Events: 200_000, UpdateSeconds: 100 + extra})
		}
		_, rate := historyWith(t, recs).UpdateModel("s")
		return rate
	}
	if rate := at(1000); rate != 100 {
		t.Fatalf("slope exactly a tenth of the floor: rate=%v, want 100 (kept)", rate)
	}
	if rate := at(1001); rate != 0 {
		t.Fatalf("slope just under a tenth of the floor: rate=%v, want none", rate)
	}
}

// Proven is read off the same sample as the model: a large update proven
// cheap months ago, outside the newest five measured updates, no longer
// licenses updates today.
func TestProvenUpdate_sameSampleAsTheModel(t *testing.T) {
	recs := []BaselineRunRecord{{Kind: BaselineRunRefresh, Events: 20_000_000, UpdateSeconds: 400}}
	for range 5 {
		recs = append(recs, BaselineRunRecord{Kind: BaselineRunRefresh, Events: 100_000, UpdateSeconds: 60})
	}
	h := historyWith(t, recs)
	if got := h.ProvenUpdate("s", 8*time.Minute); got != 100_000 {
		t.Fatalf("ProvenUpdate = %d, want the sample's 100000, not the old 20000000", got)
	}
	// Within the sample it counts; the unmeasured records between do not
	// push it out.
	recs = append(recs[:4], BaselineRunRecord{Kind: BaselineRunDump, StartedAt: "2026-09-18T05:00:00Z", FinishedAt: "2026-09-18T05:07:00Z"})
	if got := historyWith(t, recs).ProvenUpdate("s", 8*time.Minute); got != 20_000_000 {
		t.Fatalf("ProvenUpdate = %d, want the 20000000 run still in the newest five measured", got)
	}
}

// Without a rate the fixed cost alone still decides one thing: when even
// the cheapest recent update took longer than the last full backup, the
// full backup is the cheaper producer, and no estimate is needed. The age
// rule could never say so on a server whose every update succeeds, since
// each one renews the anchor.
func TestCutoverToFull_fixedCostAboveTheFullBackupWithoutARate(t *testing.T) {
	now := time.Date(2026, 9, 18, 10, 30, 0, 0, time.UTC)
	fresh := now.Add(-5 * time.Minute)
	cases := []struct {
		name string
		w    BackupWindow
		want string
	}{
		{"six-minute updates, two-minute full backups: full", BackupWindow{Anchor: fresh, Events: 1_000_000, FoldFixed: 6 * time.Minute, LastFull: 2 * time.Minute}, "window_measured"},
		{"count unknown, same costs: full", BackupWindow{Anchor: fresh, Events: -1, FoldFixed: 6 * time.Minute, LastFull: 2 * time.Minute}, "window_measured"},
		{"nothing to fold: update", BackupWindow{Anchor: fresh, Events: 0, FoldFixed: 6 * time.Minute, LastFull: 2 * time.Minute}, ""},
		{"fixed cost under the full backup, fresh anchor: update", BackupWindow{Anchor: fresh, Events: 1_000_000, FoldFixed: 6 * time.Minute, LastFull: 8 * time.Minute}, ""},
		{"no full backup on record, fresh anchor: update", BackupWindow{Anchor: fresh, Events: 1_000_000, FoldFixed: 6 * time.Minute}, ""},
		{"with a rate the estimate decides, not this", BackupWindow{Anchor: fresh, Events: 1, FoldFixed: 6 * time.Minute, FoldRate: 1000, LastFull: 2 * time.Minute}, "window_measured"},
	}
	for _, c := range cases {
		why := CutoverToFull(c.w, 5*time.Minute, now)
		if got := BackupWhyCode(why); got != c.want {
			t.Errorf("%s: got %q (%s), want %q", c.name, got, why, c.want)
		}
	}
	// The review's series: five updates of about a million events taking
	// about six minutes (one 700 s outlier so the time guard passes), a
	// two-minute full backup. No rate, and cut over all the same.
	recs := []BaselineRunRecord{
		{Kind: BaselineRunRefresh, Events: 1_000_000, UpdateSeconds: 360},
		{Kind: BaselineRunRefresh, Events: 1_010_000, UpdateSeconds: 370},
		{Kind: BaselineRunRefresh, Events: 990_000, UpdateSeconds: 365},
		{Kind: BaselineRunRefresh, Events: 1_005_000, UpdateSeconds: 700},
		{Kind: BaselineRunRefresh, Events: 1_002_000, UpdateSeconds: 380},
	}
	fixed, rate := historyWith(t, recs).UpdateModel("s")
	if rate != 0 || fixed != 6*time.Minute {
		t.Fatalf("fixed=%s rate=%v, want six minutes and no rate", fixed, rate)
	}
	w := BackupWindow{Anchor: fresh, Events: 1_000_000, FoldFixed: fixed, FoldRate: rate, LastFull: 2 * time.Minute}
	if why := CutoverToFull(w, 5*time.Minute, now); BackupWhyCode(why) != "window_measured" || !strings.Contains(why, "cheapest recent update took 6m") {
		t.Fatalf("six-minute updates against a two-minute full backup were not cut over: %q", why)
	}
}

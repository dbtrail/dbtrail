package console

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

// #2201: the Overview says how old the copy's DATA is, and warns when capture
// keeps falling further behind. These pin the two server halves: the run
// history's answer for one snapshot, and the lag trend the coverage read
// carries.

func TestDataAsOfFor(t *testing.T) {
	h, err := OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	add := func(r BaselineRunRecord) {
		t.Helper()
		r.ServerID = "s"
		if r.StartedAt == "" {
			r.StartedAt = r.SnapshotTime
		}
		if err := h.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	add(BaselineRunRecord{Kind: BaselineRunDump, SnapshotTime: "2026-10-06T10:00:00Z"})
	add(BaselineRunRecord{Kind: BaselineRunRefresh, SnapshotTime: "2026-10-06T11:00:00Z", DataAsOf: "2026-10-06T09:58:00Z"})
	add(BaselineRunRecord{Kind: BaselineRunRefresh, SnapshotTime: "2026-10-06T11:05:00Z"}) // recorded before the field existed
	add(BaselineRunRecord{Kind: BaselineRunRestore, SnapshotTime: "2026-10-06T11:10:00Z", DataAsOf: "2026-10-06T08:00:00Z"})
	add(BaselineRunRecord{Kind: BaselineRunRefresh, SnapshotTime: "2026-10-06T11:15:00Z", DataAsOf: "not a time"})
	for _, c := range []struct {
		snap string
		want string
	}{
		// A full read holds the source as of its own instant.
		{"2026-10-06T10:00:00Z", "2026-10-06T10:00:00Z"},
		// An update holds what it recorded.
		{"2026-10-06T11:00:00Z", "2026-10-06T09:58:00Z"},
		// Unknown: an update with no record of it, a restore (a chosen past
		// instant, never "the newest copy"), a stamp that does not parse, and
		// a snapshot this daemon did not make.
		{"2026-10-06T11:05:00Z", ""},
		{"2026-10-06T11:10:00Z", ""},
		{"2026-10-06T11:15:00Z", ""},
		{"2026-10-06T12:00:00Z", ""},
	} {
		got, ok := h.DataAsOfFor("s", c.snap)
		gotS := ""
		if ok {
			gotS = got.UTC().Format(time.RFC3339)
		}
		if gotS != c.want {
			t.Errorf("DataAsOfFor(%s) = %q (%v), want %q", c.snap, gotS, ok, c.want)
		}
	}
	var nilH *BaselineRunHistory
	if _, ok := nilH.DataAsOfFor("s", "2026-10-06T10:00:00Z"); ok {
		t.Error("a missing history answered")
	}
}

// TestLagGrowth pins when the coverage read says capture is falling further
// behind. Growth is measured as the time that passed on this host minus how
// far the newest indexed change moved, so a constant offset between the
// source's clock and this host's cancels out.
func TestLagGrowth(t *testing.T) {
	t0 := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	type obs struct {
		after   time.Duration // since t0, on this host
		deltaTo time.Duration // newest indexed change, since t0, on the source clock
	}
	for _, c := range []struct {
		name         string
		obs          []obs
		wantGrown    int64 // 0 = no growth reported
		wantOver     int64
		wantIndexing bool
	}{
		// Capture still indexing, at a quarter of the source's pace, read
		// every minute (the Overview's pace while changes arrive).
		{name: "slow capture, still indexing", obs: []obs{{0, -time.Hour}, {time.Minute, -time.Hour + 15*time.Second},
			{2 * time.Minute, -time.Hour + 30*time.Second}, {3 * time.Minute, -time.Hour + 45*time.Second}, {4 * time.Minute, -time.Hour + time.Minute}},
			wantGrown: 180, wantOver: 240, wantIndexing: true},
		// One write right after the first read and nothing since: the gap
		// grows like a stuck capture's, and the one move is not "indexing".
		{name: "quiet after one write", obs: []obs{{0, 0}, {5 * time.Minute, 10 * time.Second}}, wantGrown: 290, wantOver: 300},
		// Capture stuck an hour behind: nothing new indexed for five minutes.
		{name: "an hour behind and stuck", obs: []obs{{0, -time.Hour}, {5 * time.Minute, -time.Hour}}, wantGrown: 300, wantOver: 300},
		// The issue's shape: capture indexing about a quarter of what the
		// source writes, so the newest change moves 75 s in 5 min.
		{name: "falling behind slowly", obs: []obs{{0, -65 * time.Minute}, {5 * time.Minute, -65*time.Minute + 75*time.Second}}, wantGrown: 225, wantOver: 300},
		// Steady small lag: the newest change keeps pace with the clock.
		{name: "steady small lag", obs: []obs{{0, -10 * time.Second}, {time.Minute, 50 * time.Second}, {5 * time.Minute, 290 * time.Second}}},
		// Behind but catching up: the gap shrinks.
		{name: "catching up", obs: []obs{{0, -time.Hour}, {5 * time.Minute, -time.Hour + 10*time.Minute}}},
		// Not several minutes yet.
		{name: "too short to say", obs: []obs{{0, -time.Hour}, {3 * time.Minute, -time.Hour}}},
		// A few seconds of drift is noise, not a trend.
		{name: "jitter", obs: []obs{{0, -2 * time.Minute}, {5 * time.Minute, 3*time.Minute - 30*time.Second}}},
		// Growth under two minutes is not yet worth a card.
		{name: "grew less than two minutes", obs: []obs{{0, -10 * time.Second}, {5 * time.Minute, 3*time.Minute + 10*time.Second}}},
		// The source clock runs ahead of this host by ten minutes: the lag
		// this host computes is clamped at zero, the slope is not.
		{name: "source clock ahead, capture stuck", obs: []obs{{0, 10*time.Minute - 3*time.Minute}, {5 * time.Minute, 10*time.Minute - 3*time.Minute}}, wantGrown: 300, wantOver: 300},
		// The newest change went BACKWARDS: another index, or one rebuilt.
		// The old samples describe something else and are dropped.
		{name: "index went backwards", obs: []obs{{0, -time.Minute}, {5 * time.Minute, -3 * time.Hour}}},
		// Was falling behind, and the newest stretch caught up: no card.
		// Over ten minutes the gap grew four, but the last five minutes
		// shrank it: no card.
		{name: "grew, then caught up in the last stretch", obs: []obs{{0, -time.Hour}, {5 * time.Minute, -time.Hour}, {10 * time.Minute, -time.Hour + 6*time.Minute}}},
		// Grew early, then kept pace: what is happening NOW is not growth.
		{name: "grew, then kept pace", obs: []obs{{0, -time.Hour}, {5 * time.Minute, -time.Hour}, {10 * time.Minute, -time.Hour + 5*time.Minute}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := newLagTrendBook()
			var got *lagGrowthDTO
			for _, o := range c.obs {
				now := t0.Add(o.after)
				got = b.observe("s", now, t0.Add(o.deltaTo))
			}
			if c.wantGrown == 0 {
				if got != nil {
					t.Fatalf("growth = %+v, want none", *got)
				}
				return
			}
			if got == nil || got.GrownSeconds != c.wantGrown || got.OverSeconds != c.wantOver || got.Indexing != c.wantIndexing {
				t.Fatalf("growth = %+v, want grown %d over %d", got, c.wantGrown, c.wantOver)
			}
		})
	}
	// A source writing every four minutes, captured at once, read every 20
	// seconds: three and a half minutes after its last write the gap since
	// it counts as growth, but at every read right after a write it was
	// next to nothing. Growth, yes; "indexing", no, so no card without the
	// source's own word.
	t.Run("low-traffic source captured at once is not indexing", func(t *testing.T) {
		b := newLagTrendBook()
		var got *lagGrowthDTO
		for sec := 0; sec <= 690; sec += 20 {
			lastWrite := (sec / 240) * 240
			got = b.observe("s", t0.Add(time.Duration(sec)*time.Second), t0.Add(time.Duration(lastWrite)*time.Second))
		}
		if got == nil || got.Indexing {
			t.Fatalf("growth = %+v, want growth reported and not indexing", got)
		}
	})
	t.Run("a burst of reads keeps the older sample", func(t *testing.T) {
		b := newLagTrendBook()
		b.observe("s", t0, t0.Add(-time.Hour))
		// Page reloads every few seconds must not push the five-minute-old
		// sample out of the window it needs.
		for i := 1; i <= 100; i++ {
			b.observe("s", t0.Add(time.Duration(i)*3*time.Second), t0.Add(-time.Hour))
		}
		if got := b.observe("s", t0.Add(5*time.Minute+time.Second), t0.Add(-time.Hour)); got == nil || got.OverSeconds < 300 {
			t.Fatalf("growth = %+v, want the first sample still counted", got)
		}
	})
	t.Run("servers are kept apart", func(t *testing.T) {
		b := newLagTrendBook()
		b.observe("a", t0, t0.Add(-time.Hour))
		if got := b.observe("b", t0.Add(5*time.Minute), t0.Add(-time.Hour)); got != nil {
			t.Fatalf("server b grew from server a's sample: %+v", *got)
		}
	})
	t.Run("samples older than the window are forgotten", func(t *testing.T) {
		b := newLagTrendBook()
		b.observe("s", t0, t0.Add(-time.Hour))
		if got := b.observe("s", t0.Add(lagTrendWindow+time.Minute), t0.Add(-time.Hour)); got != nil {
			t.Fatalf("growth = %+v from a sample older than the window", *got)
		}
	})
	t.Run("an empty index is not sampled", func(t *testing.T) {
		b := newLagTrendBook()
		if got := b.observe("s", t0, time.Time{}); got != nil {
			t.Fatal("an empty index produced a trend")
		}
		if n := len(b.samples["s"]); n != 0 {
			t.Fatalf("%d samples kept for an empty index", n)
		}
	})
}

// TestCoverageAPI_samplesTheLag: the handler feeds the trend book on every
// read, under the selected server, and reports what the book decides.
func TestCoverageAPI_samplesTheLag(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	latest := now.Add(-time.Hour)
	part := now.Add(-100 * time.Hour).Format("p_2006010215")
	srv := newBaselineServer(t, t.TempDir(), true)
	srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
	srv.cm.boot.dbName = "binlog_index"
	// Five minutes ago the same newest change: capture has not moved.
	srv.lagTrend.observe("default", now.Add(-5*time.Minute), latest)
	got := coverageGet(t, srv)
	if got.LagGrowth == nil || got.LagGrowth.GrownSeconds < 290 {
		raw, _ := json.Marshal(got)
		t.Fatalf("coverage = %s, want the lag reported as growing", raw)
	}
	if n := len(srv.lagTrend.samples["default"]); n != 2 {
		t.Fatalf("%d samples after one read, want the read sampled", n)
	}
}

// TestBaselinesAPI_dataAsOf: the listing carries each snapshot's data instant
// from the run history, and leaves it out where none is on record.
func TestBaselinesAPI_dataAsOf(t *testing.T) {
	root := t.TempDir()
	writeBaselineFixture(t, root, "2026-10-06T11-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, root, "2026-10-06T10-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, root, "2026-10-06T09-00-00Z", "shop", "orders.parquet")
	srv := newBaselineServer(t, root, true)
	h, err := OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	srv.baselineHistory = h
	for _, r := range []BaselineRunRecord{
		{Kind: BaselineRunRefresh, SnapshotTime: "2026-10-06T11:00:00Z", DataAsOf: "2026-10-06T09:55:00Z"},
		{Kind: BaselineRunDump, SnapshotTime: "2026-10-06T10:00:00Z"},
	} {
		r.ServerID, r.StartedAt = bootServerID, r.SnapshotTime
		if err := h.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got baselinesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"2026-10-06 11:00:00": "2026-10-06 09:55:00",
		"2026-10-06 10:00:00": "2026-10-06 10:00:00",
		"2026-10-06 09:00:00": "",
	}
	if len(got.Snapshots) != 3 {
		t.Fatalf("snapshots = %+v", got.Snapshots)
	}
	for _, s := range got.Snapshots {
		if s.DataAsOf != want[s.Time] {
			t.Errorf("snapshot %s: data_as_of = %q, want %q", s.Time, s.DataAsOf, want[s.Time])
		}
	}
}

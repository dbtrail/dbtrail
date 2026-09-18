package console

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #1721: the cut-over from an update to a full backup, decided on what the
// daemon measured. The rule is pure (CutoverToFull); the decision plumbs it
// through a probe on the gates so a process with no measurements (a test,
// the read-only console) never cuts over.

func TestCutoverToFull(t *testing.T) {
	now := time.Date(2026, 9, 18, 10, 30, 0, 0, time.UTC)
	fresh := now.Add(-5 * time.Minute)
	old := now.Add(-5*time.Hour - 30*time.Minute)
	cases := []struct {
		name     string
		w        BackupWindow
		interval time.Duration
		want     string // "" = update; otherwise the code of the reason
	}{
		{"nothing known: update", BackupWindow{Events: -1}, 5 * time.Minute, ""},
		{"measured cheaper than a full backup: update", BackupWindow{Anchor: fresh, Events: 100_000, FoldRate: 1000, LastFull: 8 * time.Minute}, 5 * time.Minute, ""},
		{"measured dearer than a full backup: full", BackupWindow{Anchor: fresh, Events: 17_000_000, FoldRate: 4000, LastFull: 8 * time.Minute}, 5 * time.Minute, "window_measured"},
		{"the fixed cost alone exceeds the full backup: full", BackupWindow{Anchor: fresh, Events: 1, FoldFixed: 9 * time.Minute, FoldRate: 4000, LastFull: 8 * time.Minute}, 5 * time.Minute, "window_measured"},
		// Evidence beats the model: an update this size was done cheaper.
		{"dearer by the model but proven cheaper: update", BackupWindow{Anchor: fresh, Events: 17_000_000, FoldRate: 4000, Proven: 20_000_000, LastFull: 8 * time.Minute}, 5 * time.Minute, ""},
		// A tiny rate and a long stop: the estimate overflows a Duration,
		// and must not wrap into "cheaper than a full backup".
		{"estimate past what a Duration holds: full", BackupWindow{Anchor: fresh, Events: 17_000_000, FoldRate: 0.0000001, LastFull: 8 * time.Minute}, 5 * time.Minute, "window_measured"},
		// A quiet server has a fixed cost and no rate: the age rule, and a
		// fresh anchor after a burst is an update.
		{"fixed cost, no rate, fresh anchor after a burst: update", BackupWindow{Anchor: fresh, Events: 60_000, FoldFixed: 90 * time.Second, LastFull: 40 * time.Minute}, 5 * time.Minute, ""},
		// The estimate is the better evidence: an old anchor with a cheap
		// update is updated, and a fresh one with a dear update is not.
		{"old anchor but measured cheaper: update", BackupWindow{Anchor: old, Events: 1000, FoldRate: 1000, LastFull: 8 * time.Minute}, 5 * time.Minute, ""},
		{"estimate exactly the full backup: update", BackupWindow{Anchor: fresh, Events: 480_000, FoldRate: 1000, LastFull: 8 * time.Minute}, 5 * time.Minute, ""},
		{"nothing to fold: update", BackupWindow{Anchor: old, Events: 0, FoldRate: 1000, LastFull: 8 * time.Minute}, 5 * time.Minute, ""},
		// Without one of the three the age rule decides.
		{"events unknown, old anchor: full on age", BackupWindow{Anchor: old, Events: -1, FoldRate: 1000, LastFull: 8 * time.Minute}, 5 * time.Minute, "window_age"},
		{"no rate, old anchor: full on age", BackupWindow{Anchor: old, Events: 100, LastFull: 8 * time.Minute}, 5 * time.Minute, "window_age"},
		{"no full backup on record, old anchor: full on age", BackupWindow{Anchor: old, Events: 100, FoldRate: 1000}, 5 * time.Minute, "window_age"},
		{"events unknown, fresh anchor: update", BackupWindow{Anchor: fresh, Events: -1}, 5 * time.Minute, ""},
		{"anchor exactly at the cut-over age: update", BackupWindow{Anchor: now.Add(-BackupCutoverMinAge), Events: -1}, 5 * time.Minute, ""},
		{"anchor a second past it: full on age", BackupWindow{Anchor: now.Add(-BackupCutoverMinAge - time.Second), Events: -1}, 5 * time.Minute, "window_age"},
		{"no interval known: the floor applies", BackupWindow{Anchor: old, Events: -1}, 0, "window_age"},
		// Six intervals: a daily schedule's normal window is never too old.
		{"daily schedule, one-day-old anchor: update", BackupWindow{Anchor: now.Add(-25 * time.Hour), Events: -1}, 24 * time.Hour, ""},
		{"daily schedule, week-old anchor: full on age", BackupWindow{Anchor: now.Add(-7 * 24 * time.Hour), Events: -1}, 24 * time.Hour, "window_age"},
		{"hourly schedule, five-hour-old anchor: update", BackupWindow{Anchor: now.Add(-5 * time.Hour), Events: -1}, time.Hour, ""},
		{"no anchor: update", BackupWindow{Events: -1}, 5 * time.Minute, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			why := CutoverToFull(c.w, c.interval, now)
			if got := BackupWhyCode(why); got != c.want {
				t.Fatalf("why = %q (code %q), want code %q", why, got, c.want)
			}
		})
	}
	// The reasons carry the numbers the decision was made on.
	why := CutoverToFull(BackupWindow{Anchor: fresh, Events: 17_000_000, FoldRate: 4000, LastFull: 7*time.Minute + 41*time.Second}, 5*time.Minute, now)
	for _, want := range []string{"17,000,000 events", "about 1h 11m", "took 8m"} {
		if !strings.Contains(why, want) {
			t.Errorf("measured reason %q lacks %q", why, want)
		}
	}
	// The age reason names what was missing, and only that: after a restart
	// the history still has the rate and the full backup.
	why = CutoverToFull(BackupWindow{Anchor: old, Events: -1, FoldRate: 4000, LastFull: 8 * time.Minute}, 5*time.Minute, now)
	for _, want := range []string{"5h 30m old", "cut-over is 2h", "no count of the changes since it"} {
		if !strings.Contains(why, want) {
			t.Errorf("age reason %q lacks %q", why, want)
		}
	}
	if strings.Contains(why, "no usable update rate") || strings.Contains(why, "no full backup") {
		t.Errorf("age reason %q claims something the history has", why)
	}
	why = CutoverToFull(BackupWindow{Anchor: old, Events: 100}, 5*time.Minute, now)
	if !strings.Contains(why, "no usable update rate (none measured, or the measured updates all cost about the same), no full backup on record") || strings.Contains(why, "no count") {
		t.Errorf("age reason %q, want the two missing measurements named", why)
	}
	if got := roundSeconds(1e30); got != roundDuration(time.Duration(1<<63-1)) {
		t.Errorf("roundSeconds past the Duration range = %q", got)
	}
}

// The schedule's interval scales the cut-over age: the same seven-hour-old
// anchor with nothing measured is an update on a daily schedule (cut-over
// six days) and a full backup on an hourly one (six hours).
func TestChooseBackupMethod_intervalScalesTheCutoverAge(t *testing.T) {
	dir := t.TempDir()
	fakeSnapshot(t, dir)
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	sevenHours := func(_ context.Context, _ ServerEntry, _ time.Time) BackupWindow {
		return BackupWindow{Anchor: now.Add(-7 * time.Hour), Events: -1}
	}
	gates := BackupScheduleGates{LoopRunning: true, FullBackups: true, Window: sevenHours}
	for every, want := range map[string]string{"1d": BackupMethodRefresh, "1h": BackupMethodFull} {
		e := ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: dir, BackupSchedule: &BackupSchedule{Every: every}}
		method, why, err := ChooseBackupMethodAt(context.Background(), e, gates, now)
		if err != nil || method != want {
			t.Fatalf("every %s: method=%q why=%q err=%v, want %s", every, method, why, err, want)
		}
	}
}

// A schedule that no longer parses is an error, not a silent fall to the
// two-hour floor (a daily schedule would then cut over after two hours).
func TestChooseBackupMethod_unparsableScheduleIsAnError(t *testing.T) {
	dir := t.TempDir()
	fakeSnapshot(t, dir)
	e := ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: dir, BackupSchedule: &BackupSchedule{Every: "sometimes"}}
	probe := func(_ context.Context, _ ServerEntry, anchor time.Time) BackupWindow {
		return BackupWindow{Anchor: anchor, Events: -1}
	}
	_, _, err := ChooseBackupMethod(context.Background(), e, BackupScheduleGates{LoopRunning: true, FullBackups: true, Window: probe})
	if err == nil || !strings.Contains(err.Error(), "schedule could not be read") {
		t.Fatalf("err = %v, want the parse failure", err)
	}
}

func TestBackupCutoverAge(t *testing.T) {
	for interval, want := range map[time.Duration]time.Duration{
		0: 2 * time.Hour, 5 * time.Minute: 2 * time.Hour, 20 * time.Minute: 2 * time.Hour,
		time.Hour: 6 * time.Hour, 24 * time.Hour: 6 * 24 * time.Hour,
	} {
		if got := BackupCutoverAge(interval); got != want {
			t.Errorf("BackupCutoverAge(%s) = %s, want %s", interval, got, want)
		}
	}
}

func TestRoundDurationAndFormatCount(t *testing.T) {
	for d, want := range map[time.Duration]string{
		0: "0s", 41 * time.Second: "41s", 59*time.Second + 600*time.Millisecond: "1m", 7*time.Minute + 41*time.Second: "8m",
		time.Hour: "1h", 70 * time.Minute: "1h 10m", 5*time.Hour + 30*time.Minute: "5h 30m", 2*time.Hour - time.Second: "2h",
		time.Hour - 20*time.Second: "1h",
		6 * 24 * time.Hour:         "144h",
	} {
		if got := roundDuration(d); got != want {
			t.Errorf("roundDuration(%s) = %q, want %q", d, got, want)
		}
	}
	for n, want := range map[int64]string{0: "0", 999: "999", 1000: "1,000", 17_000_000: "17,000,000", -5: "-5"} {
		if got := formatCount(n); got != want {
			t.Errorf("formatCount(%d) = %q, want %q", n, got, want)
		}
	}
}

// The history's measurements: the update model (fixed cost = the shortest
// measured run, rate from the time beyond it), what an update is proven to
// do cheaper than a full backup, the last full backup, and the mark an
// update read (for the count after a restart).
func TestBaselineHistory_updateModel(t *testing.T) {
	h, err := OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	if fixed, rate := h.UpdateModel("s"); fixed != 0 || rate != 0 {
		t.Fatal("an empty history measured something")
	}
	if h.LastFullBackup("s") != 0 || h.ProvenUpdate("s", time.Hour) != 0 {
		t.Fatal("an empty history proved something")
	}
	add := func(rec BaselineRunRecord) {
		rec.ServerID = "s"
		if err := h.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	add(BaselineRunRecord{Kind: BaselineRunDump, StartedAt: "2026-09-18T05:00:00Z", FinishedAt: "2026-09-18T05:07:41Z"})
	add(BaselineRunRecord{Kind: BaselineRunDump, StartedAt: "2026-09-18T06:00:00Z", FinishedAt: "2026-09-18T06:03:00Z", Error: "mydumper: exit 2"}) // failed: not the last full
	add(BaselineRunRecord{Kind: BaselineRunRefresh, Events: 0, UpdateSeconds: 10})                                                                  // unmeasured
	add(BaselineRunRecord{Kind: BaselineRunRefresh, Events: 1000, UpdateSeconds: 0})                                                                // unmeasured
	add(BaselineRunRecord{Kind: BaselineRunRefresh, Events: 6000, UpdateSeconds: 2, Error: "gap"})                                                  // failed
	add(BaselineRunRecord{Kind: BaselineRunRefresh, Events: 1000, UpdateSeconds: 90, SnapshotTime: "2026-09-18T07:00:00Z", IndexMark: 500})
	fixed, rate := h.UpdateModel("s")
	if fixed != 90*time.Second || rate != 0 {
		t.Fatalf("one sample: fixed=%s rate=%v, want 90s and no rate", fixed, rate)
	}
	add(BaselineRunRecord{Kind: BaselineRunRefresh, Events: 601_000, UpdateSeconds: 390})
	fixed, rate = h.UpdateModel("s")
	if fixed != 90*time.Second || rate != 600_000/300.0 { // the shortest run's 1000 events are inside the fixed cost
		t.Fatalf("two samples: fixed=%s rate=%v, want 90s and 600000/300", fixed, rate)
	}
	if got := h.LastFullBackup("s"); got != 7*time.Minute+41*time.Second {
		t.Fatalf("LastFullBackup = %s, want 7m41s", got)
	}
	if got := h.ProvenUpdate("s", 7*time.Minute+41*time.Second); got != 601_000 {
		t.Fatalf("ProvenUpdate within 7m41s = %d, want the 390 s run's 601000", got)
	}
	if got := h.ProvenUpdate("s", 2*time.Minute); got != 1000 {
		t.Fatalf("ProvenUpdate within 2m = %d, want the 90 s run's 1000", got)
	}
	if mark, ok := h.IndexMarkFor("s", "2026-09-18T07:00:00Z"); !ok || mark != 500 {
		t.Fatalf("IndexMarkFor = %d,%v, want the recorded mark", mark, ok)
	}
	if _, ok := h.IndexMarkFor("s", "2026-09-18T08:00:00Z"); ok {
		t.Fatal("a snapshot with no record has a mark")
	}
	// Only the newest five measured updates fit the model; a quiet server
	// whose updates all cost the same has no rate (the per-event cost is
	// not distinguishable from the fixed one).
	for range 5 {
		add(BaselineRunRecord{Kind: BaselineRunRefresh, Events: 100, UpdateSeconds: 90})
	}
	if fixed, rate = h.UpdateModel("s"); fixed != 90*time.Second || rate != 0 {
		t.Fatalf("quiet server: fixed=%s rate=%v, want no rate", fixed, rate)
	}
	// A newer successful full backup replaces the older one; unparsable
	// stamps read as none.
	// The rate is MARGINAL: four runs of 200,000 events in 60 s and one of
	// 400,000 in 120 s is 200,000 more events for 60 more seconds, 3,333/s;
	// counting the whole 400,000 would read 6,667/s and estimate a long
	// window at half its cost. A longer run that applied no more events
	// says nothing and is left out.
	h3, err := OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		_ = h3.Append(BaselineRunRecord{ServerID: "s", Kind: BaselineRunRefresh, Events: 200_000, UpdateSeconds: 60})
	}
	_ = h3.Append(BaselineRunRecord{ServerID: "s", Kind: BaselineRunRefresh, Events: 400_000, UpdateSeconds: 120})
	if fixed, rate := h3.UpdateModel("s"); fixed != time.Minute || rate != 200_000/60.0 {
		t.Fatalf("marginal rate: fixed=%s rate=%v, want 1m and 200000/60", fixed, rate)
	}
	_ = h3.Append(BaselineRunRecord{ServerID: "s", Kind: BaselineRunRefresh, Events: 200_000, UpdateSeconds: 100}) // slow day, same work
	if _, rate := h3.UpdateModel("s"); rate != 200_000/60.0 {
		t.Fatalf("a longer run with no more events changed the rate: %v", rate)
	}
	// The tenth threshold from both sides: five runs whose time beyond the
	// shortest is under a tenth of the total read as no rate; just over,
	// a rate.
	for _, c := range []struct {
		last float64
		rate bool
	}{{95, false}, {135, false}, {140, true}} {
		h2, err := OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
		if err != nil {
			t.Fatal(err)
		}
		for range 4 {
			_ = h2.Append(BaselineRunRecord{ServerID: "s", Kind: BaselineRunRefresh, Events: 100, UpdateSeconds: 90})
		}
		_ = h2.Append(BaselineRunRecord{ServerID: "s", Kind: BaselineRunRefresh, Events: 200, UpdateSeconds: c.last})
		if _, rate := h2.UpdateModel("s"); (rate > 0) != c.rate {
			t.Fatalf("four runs of 90 s and one of %v s: rate=%v, want a rate: %v", c.last, rate, c.rate)
		}
	}
	add(BaselineRunRecord{Kind: BaselineRunDump, StartedAt: "2026-09-18T09:00:00Z", FinishedAt: "2026-09-18T09:09:00Z"})
	if got := h.LastFullBackup("s"); got != 9*time.Minute {
		t.Fatalf("LastFullBackup = %s, want 9m", got)
	}
	add(BaselineRunRecord{Kind: BaselineRunDump, StartedAt: "not a time", FinishedAt: "2026-09-18T10:00:00Z"})
	if got := h.LastFullBackup("s"); got != 0 {
		t.Fatalf("LastFullBackup with a bad stamp = %s, want 0", got)
	}
}

// ChooseBackupMethod asks the probe with the newest snapshot's instant and
// cuts over on its answer; without a probe, or when a full backup cannot
// start, it updates as before.
func TestChooseBackupMethod_cutsOverOnTheMeasuredWindow(t *testing.T) {
	dir := t.TempDir()
	fakeSnapshot(t, dir) // 2026-08-27T03:00:00Z
	e := ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: dir, BackupSchedule: &BackupSchedule{Every: "5m"}}
	var asked time.Time
	dear := func(_ context.Context, _ ServerEntry, anchor time.Time) BackupWindow {
		asked = anchor
		return BackupWindow{Anchor: anchor, Events: 17_000_000, FoldRate: 4000, LastFull: 8 * time.Minute}
	}
	method, why, err := ChooseBackupMethod(context.Background(), e, BackupScheduleGates{LoopRunning: true, FullBackups: true, Window: dear})
	if err != nil || method != BackupMethodFull || BackupWhyCode(why) != "window_measured" {
		t.Fatalf("method=%q why=%q err=%v, want a full backup on the measured window", method, why, err)
	}
	if want := time.Date(2026, 8, 27, 3, 0, 0, 0, time.UTC); !asked.Equal(want) {
		t.Fatalf("the probe was asked with anchor %s, want the newest snapshot's instant %s", asked, want)
	}
	// Full backups off: the update is the producer that can, so it runs.
	method, why, err = ChooseBackupMethod(context.Background(), e, BackupScheduleGates{LoopRunning: true, Window: dear})
	if err != nil || method != BackupMethodRefresh {
		t.Fatalf("with full backups off: method=%q why=%q err=%v, want the update", method, why, err)
	}
	// No probe (a process that measures nothing): the update, as before.
	method, _, err = ChooseBackupMethod(context.Background(), e, BackupScheduleGates{LoopRunning: true, FullBackups: true})
	if err != nil || method != BackupMethodRefresh {
		t.Fatalf("without a probe: method=%q err=%v", method, err)
	}
	// A probe that measures nothing on a fresh anchor: the update.
	unknown := func(_ context.Context, _ ServerEntry, anchor time.Time) BackupWindow {
		return BackupWindow{Anchor: time.Now(), Events: -1}
	}
	method, _, err = ChooseBackupMethod(context.Background(), e, BackupScheduleGates{LoopRunning: true, FullBackups: true, Window: unknown})
	if err != nil || method != BackupMethodRefresh {
		t.Fatalf("with nothing measured: method=%q err=%v", method, err)
	}
	// The snapshot on disk IS old (2026-08-27): with the probe returning it
	// as the anchor and nothing measured, the age rule fires.
	byAge := func(_ context.Context, _ ServerEntry, anchor time.Time) BackupWindow {
		return BackupWindow{Anchor: anchor, Events: -1}
	}
	method, why, err = ChooseBackupMethod(context.Background(), e, BackupScheduleGates{LoopRunning: true, FullBackups: true, Window: byAge})
	if err != nil || method != BackupMethodFull || BackupWhyCode(why) != "window_age" {
		t.Fatalf("old anchor, nothing measured: method=%q why=%q err=%v", method, why, err)
	}
}

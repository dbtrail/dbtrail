package console

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #2659, the remaining case: nothing was indexed since an old snapshot and
// the source cannot be asked whether capture recorded everything it wrote.
// That slot is skipped: no full read (no evidence that anything is missing),
// and no update (it would publish the old rows under a new date). A source
// that answers "behind", and a capture whose own record says it lost
// changes, keep the full read.

// Every reason the capture probe can give for not confirming the capture,
// as the daemon words them (consoleapp captureClasses, where a test pins
// these texts and their classes against the code that returns them).

// cannotBeAsked: the source cannot be asked, and the slot is skipped.
var cannotBeAsked = []string{
	"PostgreSQL sources are not compared yet",
	"this server has no source to ask",
	"the capture runs in binlog-position mode, which is not compared",
	"the capture has no GTID checkpoint",
	"the source has no gtid_mode (a MariaDB source), which is not compared",
	"the index lives on the source server, whose own checkpoint writes keep the source ahead of the capture",
	"the index and the source could not be told apart",
	"the source did not answer",
	"the source did not report its GTID set",
	"the source did not answer in time",
	// An index with no capture on record: a streaming capture that never
	// ran, or an index loaded from binary log files. Worded for both.
	"the index has no live capture on record: no streaming capture has run against this index; if you expect one, check that it is running",
	"",
}

// evidenceFound: something on record says the index is missing changes. A
// full read, worded as what was found.
var evidenceFound = []string{
	"the capture lost events to a binlog gap after the previous snapshot",
	"the capture dropped events that no full read has read from the source since",
	"the capture holds transactions the source does not have: the source may have been reset, restored or replaced",
	// A source that does not serve a reader by GTID while capture follows
	// it by GTID: capture cannot be reading it.
	"GTIDs are off on the source (gtid_mode=OFF) while capture follows it by GTID, so capture cannot read from it",
	"the source's gtid_mode is OFF_PERMISSIVE, not ON, while capture follows it by GTID, so capture cannot read from it",
	"the source's gtid_mode is ON_PERMISSIVE, not ON, while capture follows it by GTID, so capture cannot read from it",
	"the source's gtid_mode is not ON while capture follows it by GTID, so capture cannot read from it",
}

// lossNotRuledOut: not confirmed, and not for a reason on the list above. A
// full read, as before the skip existed; a reason added tomorrow lands here.
var lossNotRuledOut = []string{
	"the capture's record of dropped events is not readable",
	"the index predates the capture's loss record",
	"the capture's GTID set does not parse",
	"the source's GTID set does not parse",
	"a refusal nobody classified yet",
	"",
}

// indexNotRead: the index did not give the capture's record. The update.
var indexNotRead = []string{
	"the index did not answer",
	"the index did not answer in time",
	"the capture's checkpoint could not be read",
	"the request that asked was cancelled",
	"the probe did not finish in time",
	"",
}

// dearer is the margin an update must pass to be read in full on cost
// (#2181), written again here so the oracle below does not call the rule.
func dearer(update, full time.Duration) bool {
	return update > 2*full && update > full+30*time.Second
}

// Every capture state, with and without a rate, with and without a full
// read on record, with each kind of fixed cost, with the count known zero,
// unknown and positive, with the snapshot below and above the cut-over age,
// and with a snapshot that can wait and one that cannot: the choice against
// an oracle written from the rule's statement, not from its code.
func TestWindowChoice_everyState_2659(t *testing.T) {
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	now := time.Date(2026, 11, 11, 10, 30, 0, 0, time.UTC)
	const interval = 5 * time.Minute // cut-over age: the two-hour floor
	n, skips, fulls, forced, measured, unread := 0, 0, 0, 0, 0, 0
	for _, capture := range []string{"", CaptureBehind, CaptureSuspect, CaptureUnaskable, CaptureUnread, CaptureCaughtUp} {
		for _, rate := range []float64{0, 1000} {
			for _, lastFull := range []time.Duration{0, 8 * time.Minute} {
				// No fixed cost on record, one under the last full read, one
				// a little over it (inside the margin) and one clearly over.
				for _, fixed := range []time.Duration{0, 90 * time.Second, 9 * time.Minute, 20 * time.Minute} {
					for _, events := range []int64{-1, 0, 40} {
						for _, age := range []time.Duration{5 * time.Minute, 2 * time.Hour, 2*time.Hour + time.Second, 30 * 24 * time.Hour} {
							for _, canWait := range []bool{false, true} {
								w := BackupWindow{Anchor: now.Add(-age), Events: events, FoldRate: rate, FoldFixed: fixed, LastFull: lastFull,
									Capture: capture, CaptureDetail: "the detail", CanWait: canWait, CannotWaitWhy: "the snapshot cannot wait"}
								method, why := WindowChoice(w, interval, now)
								n++
								desc := fmt.Sprintf("capture=%q rate=%v lastFull=%s fixed=%s events=%d age=%s canWait=%v", capture, rate, lastFull, fixed, events, age, canWait)
								// The measured rules first, whatever the capture
								// says: an estimate (a count, a rate, a full read)
								// clearly over the last full read, or with no
								// estimate (no rate, or a rate and no count,
								// #2674) a fixed cost clearly over it and
								// something, or an unknown amount, to apply.
								cost := false
								switch {
								case events > 0 && rate > 0 && lastFull > 0:
									cost = dearer(fixed+time.Duration(float64(events)/rate*float64(time.Second)), lastFull)
								case events != 0 && (rate == 0 || events < 0) && fixed > 0 && lastFull > 0:
									cost = dearer(fixed, lastFull)
								}
								// With nothing to apply, a rate and a full read on
								// record settle the slot as an update when the
								// estimate (the fixed cost alone) is under the full
								// read, or so far over it that the evidence of an
								// update that size done cheaper applies (zero
								// events are never more than what was proven).
								settled := rate > 0 && lastFull > 0 && (fixed <= lastFull || dearer(fixed, lastFull))
								doubt := events == 0 && age > 2*time.Hour && !settled
								wantMethod, wantWhy := BackupMethodRefresh, ""
								switch {
								case cost:
									wantMethod, wantWhy = BackupMethodFull, BackupWhyWindowPrefix
									measured++
								case !doubt || capture == CaptureCaughtUp:
								case capture == CaptureUnread:
									// The index did not give the capture's
									// record: the update, whether or not the
									// snapshot could wait.
									wantMethod, wantWhy = BackupMethodRefresh, BackupIndexUnreadPrefix
									unread++
								case capture != CaptureUnaskable:
									// Behind, suspect, and not confirmed for no
									// listed reason: the full read.
									wantMethod, wantWhy = BackupMethodFull, BackupWhyIndexDoubtPrefix
									fulls++
								case canWait:
									wantMethod, wantWhy = BackupMethodSkip, BackupSkipUnconfirmedPrefix
									skips++
								default:
									wantMethod, wantWhy = BackupMethodRefresh, BackupSkipUnconfirmedPrefix
									forced++
								}
								if method != wantMethod || !strings.HasPrefix(why, wantWhy) || (wantWhy == "" && why != "") {
									t.Fatalf("%s: method=%q why=%q, want %q with a why starting %q", desc, method, why, wantMethod, wantWhy)
								}
								// CutoverToFull is the same choice, saying only
								// the full read.
								full := CutoverToFull(w, interval, now)
								if (method == BackupMethodFull) != (full != "") || (full != "" && full != why) {
									t.Fatalf("%s: CutoverToFull=%q beside WindowChoice %q %q", desc, full, method, why)
								}
								switch method {
								case BackupMethodSkip:
									if !IsUnconfirmedSkip(why) || BackupWhyCode(why) != "" {
										t.Fatalf("%s: the skip reason %q is not recognized as one, or is coded as a full read", desc, why)
									}
								case BackupMethodRefresh:
									if IsUnconfirmedSkip(why) {
										t.Fatalf("%s: an update whose why %q reads as a skip", desc, why)
									}
									if why != "" && !IsUnconfirmedUpdate(why) {
										t.Fatalf("%s: the update's why %q is not recognized as the update taken in place of a skip", desc, why)
									}
									if why != "" && capture == CaptureUnaskable && !strings.Contains(why, "the snapshot cannot wait") {
										t.Fatalf("%s: the update's why %q does not say what stopped the skip", desc, why)
									}
									if capture == CaptureUnread && strings.Contains(why, "the source cannot confirm") {
										t.Fatalf("%s: the why %q blames the source for an index that was not read", desc, why)
									}
								case BackupMethodFull:
									if code := BackupWhyCode(why); (cost && code != "window_measured") || (!cost && code != "window_age") {
										t.Fatalf("%s: the full read's why %q is coded %q", desc, why, code)
									}
								}
							}
						}
					}
				}
			}
		}
	}
	t.Logf("%d windows: %d skipped, %d read in full on evidence or doubt, %d read in full on cost, %d updated because the snapshot could not wait", n, skips, fulls, measured, forced)
	if skips == 0 || fulls == 0 || forced == 0 || measured == 0 || unread == 0 {
		t.Fatal("one of the outcomes was never reached, so the matrix proves nothing about it")
	}
}

// The reasons as the operator reads them, generated with every real detail
// of every class. The skip's reason carries no time, so each slot of one
// quiet stretch records the same text and the history keeps one record for
// it.
func TestSkipUnconfirmedReason_asRendered_2659(t *testing.T) {
	now := time.Date(2026, 11, 11, 10, 30, 0, 0, time.UTC)
	old := now.Add(-5 * time.Hour)
	for _, detail := range cannotBeAsked {
		w := BackupWindow{Anchor: old, Events: 0, Capture: CaptureUnaskable, CaptureDetail: detail, CanWait: true}
		method, why := WindowChoice(w, 5*time.Minute, now)
		if method != BackupMethodSkip {
			t.Fatalf("detail %q: method=%q why=%q, want the skip", detail, method, why)
		}
		t.Logf("skip:   %s", why)
		later, laterWhy := WindowChoice(w, 5*time.Minute, now.Add(49*time.Hour))
		if later != BackupMethodSkip || laterWhy != why {
			t.Fatalf("detail %q: two days later the reason reads %q, want the same text %q", detail, laterWhy, why)
		}
		for _, want := range []string{"nothing was indexed since the previous snapshot", "the source cannot confirm that capture recorded everything it wrote",
			"so no snapshot is made at this slot", "the copy keeps the data it has until a change is indexed or the source can be asked"} {
			if !strings.Contains(why, want) {
				t.Errorf("the reason %q lacks %q", why, want)
			}
		}
		if detail != "" && !strings.Contains(why, "("+detail+")") {
			t.Errorf("the reason %q lacks the detail %q", why, detail)
		}
		// "at its last snapshot" would be false: the snapshot is replaced,
		// with the same data, each time it nears what the index keeps.
		if strings.Contains(why, "()") || strings.Contains(why, "full read") || strings.Contains(why, "\u2014") || strings.Contains(why, "stays at its last snapshot") {
			t.Errorf("the reason %q has an empty detail, names a full read, carries a dash, or says the snapshot stays", why)
		}
	}
	// The same source, a snapshot that cannot wait: the update, and why.
	for _, whyNot := range []string{
		"the previous snapshot is close to the oldest change the index still keeps, and could not be updated once rotation drops those hours",
		"a table was created since the previous snapshot",
		"capture lost changes since the previous snapshot",
		"how far back the index still keeps changes could not be read",
		"the index did not answer within 3s, so whether a table was created since the previous snapshot is not known",
		"",
	} {
		w := BackupWindow{Anchor: old, Events: 0, Capture: CaptureUnaskable, CaptureDetail: "PostgreSQL sources are not compared yet", CannotWaitWhy: whyNot}
		method, why := WindowChoice(w, 5*time.Minute, now)
		if method != BackupMethodRefresh || why == "" {
			t.Fatalf("whyNot %q: method=%q why=%q, want the update with its reason", whyNot, method, why)
		}
		t.Logf("update: %s", why)
		if !strings.HasSuffix(why, "so the snapshot is updated from the recorded changes instead of the slot being skipped") || strings.Contains(why, "but ,") {
			t.Errorf("the update's why reads %q", why)
		}
	}
	// The index did not give the capture's record: the update, and a why
	// that names the index. The source was never asked, so it is not said
	// to be unable to confirm anything.
	for _, detail := range indexNotRead {
		w := BackupWindow{Anchor: old, Events: 0, Capture: CaptureUnread, CaptureDetail: detail, CanWait: true}
		method, why := WindowChoice(w, 5*time.Minute, now)
		t.Logf("update: %s", why)
		want := "nothing was indexed since the previous snapshot, and the index did not give capture's record"
		if detail != "" {
			want += " (" + detail + ")"
		}
		want += ", so whether capture recorded everything the source wrote is not known; the snapshot is updated from the recorded changes instead of the slot being skipped"
		if method != BackupMethodRefresh || why != want || !IsUnconfirmedUpdate(why) || IsUnconfirmedSkip(why) {
			t.Fatalf("detail %q: method=%q why=%q, want the update reading %q", detail, method, why, want)
		}
		if strings.Contains(why, "the source cannot confirm") || strings.Contains(why, "()") {
			t.Errorf("the why %q blames the source, or has an empty detail", why)
		}
	}
	// Evidence: the full read, worded as what was found. Nothing here "could
	// not be checked": the capture's record, or the source, did answer.
	for _, detail := range evidenceFound {
		method, why := WindowChoice(BackupWindow{Anchor: old, Events: 0, Capture: CaptureSuspect, CaptureDetail: detail, CanWait: true}, 5*time.Minute, now)
		t.Logf("full:   %s", why)
		if want := "the index may be missing changes made since the previous snapshot: nothing new was indexed since it was taken 5h ago, and " + detail; method != BackupMethodFull || why != want {
			t.Errorf("evidence %q: method=%q why=%q, want %q", detail, method, why, want)
		}
		if strings.Contains(why, "could not be checked") {
			t.Errorf("evidence %q is worded as a question that was not answered: %q", detail, why)
		}
	}
	// A loss that cannot be ruled out, a reason nobody classified: the full
	// read, as before the skip existed.
	for _, detail := range lossNotRuledOut {
		method, why := WindowChoice(BackupWindow{Anchor: old, Events: 0, CaptureDetail: detail, CanWait: true}, 5*time.Minute, now)
		t.Logf("full:   %s", why)
		want := "the index may be missing changes made since the previous snapshot: nothing new was indexed since it was taken 5h ago, and whether the source changed could not be checked"
		if detail != "" {
			want += " (" + detail + ")"
		}
		if method != BackupMethodFull || why != want {
			t.Errorf("unsettled %q: method=%q why=%q, want %q", detail, method, why, want)
		}
	}
	_, behind := WindowChoice(BackupWindow{Anchor: old, Events: 0, Capture: CaptureBehind, CaptureDetail: "ignored"}, 5*time.Minute, now)
	t.Logf("full:   %s", behind)
	if behind != "the index may be missing changes made since the previous snapshot: nothing new was indexed since it was taken 5h ago, but the source reports transactions the capture's checkpoint does not include, so the capture may have stopped or fallen behind" {
		t.Errorf("the behind reason changed: %q", behind)
	}
}

// Through the whole decision, on a daemon that may take full reads and on
// one that may not: the skip does not depend on it, the evidence cases do
// (a full read that cannot start is the update, as before).
func TestChooseBackupMethod_skipsWhenTheSourceCannotBeAsked_2659(t *testing.T) {
	dir := t.TempDir()
	fakeSnapshot(t, dir)
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	probe := func(w BackupWindow) BackupWindowProbe {
		return func(_ context.Context, _ ServerEntry, anchor time.Time) BackupWindow {
			w.Anchor = anchor
			return w
		}
	}
	e := ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: dir, BackupSchedule: &BackupSchedule{Every: "5m"}}
	for _, c := range []struct {
		name      string
		w         BackupWindow
		withFull  string
		noFull    string
		whyPrefix string
	}{
		{"cannot be asked, the snapshot can wait", BackupWindow{Events: 0, Capture: CaptureUnaskable, CaptureDetail: "PostgreSQL sources are not compared yet", CanWait: true},
			BackupMethodSkip, BackupMethodSkip, BackupSkipUnconfirmedPrefix},
		{"cannot be asked, the snapshot cannot wait", BackupWindow{Events: 0, Capture: CaptureUnaskable, CaptureDetail: "PostgreSQL sources are not compared yet", CannotWaitWhy: "a table was created since the previous snapshot"},
			BackupMethodRefresh, BackupMethodRefresh, BackupSkipUnconfirmedPrefix},
		{"the source is ahead", BackupWindow{Events: 0, Capture: CaptureBehind, CanWait: true},
			BackupMethodFull, BackupMethodRefresh, ""},
		{"the capture's record says it lost changes", BackupWindow{Events: 0, Capture: CaptureSuspect, CaptureDetail: "the capture lost events to a binlog gap after the previous snapshot", CanWait: true},
			BackupMethodFull, BackupMethodRefresh, ""},
		{"the source confirms capture is caught up", BackupWindow{Events: 0, Capture: CaptureCaughtUp, CanWait: true},
			BackupMethodRefresh, BackupMethodRefresh, ""},
		{"the index did not give capture's record", BackupWindow{Events: 0, Capture: CaptureUnread, CaptureDetail: "the index did not answer", CanWait: true},
			BackupMethodRefresh, BackupMethodRefresh, BackupIndexUnreadPrefix},
		{"not confirmed, for a reason nobody classified", BackupWindow{Events: 0, CaptureDetail: "a refusal nobody classified yet", CanWait: true},
			BackupMethodFull, BackupMethodRefresh, ""},
		{"the count unknown", BackupWindow{Events: -1, Capture: CaptureUnaskable, CaptureDetail: "PostgreSQL sources are not compared yet", CanWait: true},
			BackupMethodRefresh, BackupMethodRefresh, ""},
		{"changes to apply", BackupWindow{Events: 12, Capture: CaptureUnaskable, CaptureDetail: "PostgreSQL sources are not compared yet", CanWait: true},
			BackupMethodRefresh, BackupMethodRefresh, ""},
	} {
		for _, fullReads := range []bool{true, false} {
			method, why, err := ChooseBackupMethodAt(context.Background(), e,
				BackupScheduleGates{LoopRunning: true, FullBackups: fullReads, Window: probe(c.w)}, now)
			want := c.noFull
			if fullReads {
				want = c.withFull
			}
			if err != nil || method != want {
				t.Fatalf("%s, full reads %v: method=%q why=%q err=%v, want %q", c.name, fullReads, method, why, err, want)
			}
			if method != BackupMethodFull && c.whyPrefix != "" && !strings.HasPrefix(why, c.whyPrefix) {
				t.Fatalf("%s, full reads %v: why=%q, want it to start %q", c.name, fullReads, why, c.whyPrefix)
			}
			if method == BackupMethodRefresh && c.whyPrefix == "" && why != "no load on your database" {
				t.Fatalf("%s, full reads %v: a plain update's why reads %q", c.name, fullReads, why)
			}
		}
	}
}

// The run history tells a mark recorded as zero from no mark (#2659): an
// index that held no event when the snapshot was made gives a base of zero,
// so the count of changes since it is known. A record written by a build
// before the field existed keeps meaning what it meant, since those builds
// wrote zero for both.
func TestIndexMarkFor_aRecordedZeroIsAMark_2659(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.json")
	// As an earlier build wrote it: no index_mark at all for a zero, and no
	// index_mark_known anywhere.
	old := `{"version":1,"servers":{"s":[
	  {"server_id":"s","kind":"dump","started_at":"2026-11-01T00:00:00Z","finished_at":"2026-11-01T00:05:00Z","snapshot_time":"2026-11-01T00:00:00Z"},
	  {"server_id":"s","kind":"dump","started_at":"2026-11-02T00:00:00Z","finished_at":"2026-11-02T00:05:00Z","snapshot_time":"2026-11-02T00:00:00Z","index_mark":0},
	  {"server_id":"s","kind":"refresh","started_at":"2026-11-03T00:00:00Z","finished_at":"2026-11-03T00:05:00Z","snapshot_time":"2026-11-03T00:00:00Z","index_mark":77}
	]}}`
	if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	h, err := OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(h.List("s")); got != 3 {
		t.Fatalf("the old file loaded %d records, want 3 (its shape is not the one the history reads)", got)
	}
	for _, c := range []struct {
		snapshot string
		mark     uint64
		ok       bool
	}{
		{"2026-11-01T00:00:00Z", 0, false}, // an old record with no mark
		{"2026-11-02T00:00:00Z", 0, false}, // an old record with an explicit zero: still no mark
		{"2026-11-03T00:00:00Z", 77, true}, // an old record with a mark: a mark, without the new field
	} {
		if mark, ok := h.IndexMarkFor("s", c.snapshot); mark != c.mark || ok != c.ok {
			t.Errorf("old record %s: mark=%d ok=%v, want %d %v", c.snapshot, mark, ok, c.mark, c.ok)
		}
	}
	// As this build writes it.
	for _, rec := range []BaselineRunRecord{
		{ServerID: "s", Kind: BaselineRunDump, StartedAt: "2026-11-04T00:00:00Z", FinishedAt: "2026-11-04T00:05:00Z", SnapshotTime: "2026-11-04T00:00:00Z", IndexMarkKnown: true},
		{ServerID: "s", Kind: BaselineRunRefresh, StartedAt: "2026-11-05T00:00:00Z", FinishedAt: "2026-11-05T00:05:00Z", SnapshotTime: "2026-11-05T00:00:00Z", IndexMarkKnown: true},
		{ServerID: "s", Kind: BaselineRunRefresh, StartedAt: "2026-11-06T00:00:00Z", FinishedAt: "2026-11-06T00:05:00Z", SnapshotTime: "2026-11-06T00:00:00Z", IndexMark: 5, IndexMarkKnown: true},
		// An update that failed names no usable base, known or not.
		{ServerID: "s", Kind: BaselineRunRefresh, StartedAt: "2026-11-07T00:00:00Z", FinishedAt: "2026-11-07T00:05:00Z", SnapshotTime: "2026-11-07T00:00:00Z", IndexMarkKnown: true, Error: "refused"},
		// An index that did not answer: no mark.
		{ServerID: "s", Kind: BaselineRunDump, StartedAt: "2026-11-08T00:00:00Z", FinishedAt: "2026-11-08T00:05:00Z", SnapshotTime: "2026-11-08T00:00:00Z"},
	} {
		if err := h.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	// Read back from disk, as after a daemon restart.
	h, err = OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		snapshot string
		mark     uint64
		ok       bool
	}{
		{"2026-11-02T00:00:00Z", 0, false},
		{"2026-11-03T00:00:00Z", 77, true},
		{"2026-11-04T00:00:00Z", 0, true},
		{"2026-11-05T00:00:00Z", 0, true},
		{"2026-11-06T00:00:00Z", 5, true},
		{"2026-11-07T00:00:00Z", 0, false},
		{"2026-11-08T00:00:00Z", 0, false},
	} {
		if mark, ok := h.IndexMarkFor("s", c.snapshot); mark != c.mark || ok != c.ok {
			t.Errorf("after a reload, %s: mark=%d ok=%v, want %d %v", c.snapshot, mark, ok, c.mark, c.ok)
		}
	}
	// On the wire: the new field only where a mark was read, so a file this
	// build writes still loads in an earlier one with the meaning it had.
	raw, err := json.Marshal(BaselineRunRecord{ServerID: "s", Kind: BaselineRunDump, IndexMarkKnown: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("a recorded zero on disk: %s", raw)
	if !strings.Contains(string(raw), `"index_mark_known":true`) || strings.Contains(string(raw), `"index_mark":`) {
		t.Errorf("a recorded zero is written as %s", raw)
	}
	raw, _ = json.Marshal(BaselineRunRecord{ServerID: "s", Kind: BaselineRunDump})
	if strings.Contains(string(raw), "index_mark") {
		t.Errorf("a record with no mark is written as %s", raw)
	}
}

// The page's own code, EXECUTED over the reasons the daemon writes: which
// skips the page recognizes as this one, the skipped slot's line on the
// schedule card and the next-run line. A skip of this kind is not worded as
// a run that failed to start and promises no retry at the next slot (none
// is coming while it lasts); every other skip keeps its line.
func TestUnconfirmedSkipOnThePage_executed_2659(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	type in struct{ Name, Reason string }
	var cases []in
	for _, d := range cannotBeAsked {
		cases = append(cases, in{"skip: " + d, SkipUnconfirmedReason(d)})
	}
	others := []in{
		{"the quiet slot the gate skips", "nothing had been indexed since the last snapshot, so this slot had nothing to add to it"},
		{"a collision", "another snapshot job was running for this server at the scheduled time"},
		{"the update taken where the skip would be", UnconfirmedUpdateWhy("PostgreSQL sources are not compared yet", "a table was created since the previous snapshot")},
	}
	cases = append(cases, others...)
	payload, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	script := strings.Join([]string{
		functionBody(t, js, "function plainWords("),
		functionBody(t, js, "function backupFoldError("),
		functionBody(t, js, "function touchedRowBudgetText("),
		functionBody(t, js, "function unconfirmedSkipText("),
		functionBody(t, js, "function unconfirmedNextLine("),
		functionBody(t, js, "function unconfirmedSkipLine("),
		functionBody(t, js, "function scheduleSkipTail("),
		functionBody(t, js, "function refreshRunningText("),
	}, "\n") + `
const utcLabel = (s) => s;
const cases = ` + string(payload) + `;
console.error(JSON.stringify({
  update: refreshRunningText({ state: "running", since: "2026-11-11 18:00:05 UTC" }),
  sending: refreshRunningText({ state: "running", since: "2026-11-11 18:00:05 UTC", sending_held: true }),
  undated: refreshRunningText({ state: "running", sending_held: true }),
}));
console.log(JSON.stringify(cases.map((c) => ({
  name: c.Name,
  is: unconfirmedSkipText(c.Reason),
  // The card's own branch: this skip has its line, any other the old one.
  card: unconfirmedSkipText(c.Reason) ? unconfirmedSkipLine("2026-11-11 10:30 UTC", c.Reason, true)
    : "Did not run at 2026-11-11 10:30 UTC: " + backupFoldError(c.Reason) + scheduleSkipTail(c.Reason),
  alone: unconfirmedSkipLine("2026-11-11 10:30 UTC", c.Reason, false),
  next: unconfirmedNextLine(c.Reason),
}))));
`
	path := filepath.Join(t.TempDir(), "skip.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(node, path)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("node: %v\n%s\n%s", err, out, stderr.String())
	}
	// What holds the refresh slot, as the page words it: an update, or only
	// the send to S3 of a snapshot kept on this machine.
	var running struct{ Update, Sending, Undated string }
	if err := json.Unmarshal([]byte(stderr.String()), &running); err != nil {
		t.Fatalf("decode %q: %v", stderr.String(), err)
	}
	t.Logf("slot: %s", running.Update)
	t.Logf("slot: %s", running.Sending)
	if running.Update != "Automatic refresh running since 2026-11-11 18:00:05 UTC\u2026" ||
		running.Sending != "Sending a snapshot kept on this machine to S3 since 2026-11-11 18:00:05 UTC\u2026" ||
		running.Undated != "Sending a snapshot kept on this machine to S3\u2026" {
		t.Errorf("the running slot reads %+v", running)
	}
	// Its declaration and its two callers; the words themselves once.
	if strings.Count(js, "refreshRunningText(rf)") != 3 || strings.Count(js, `"Automatic refresh running"`) != 1 {
		t.Error("a place that says the refresh slot is running does not go through refreshRunningText")
	}
	var got []struct {
		Name, Card, Next, Alone string
		Is                      bool
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if len(got) != len(cases) {
		t.Fatalf("%d answers for %d cases", len(got), len(cases))
	}
	for i, g := range got {
		own := i < len(cannotBeAsked)
		t.Logf("card: %s", g.Card)
		if g.Is != own {
			t.Errorf("%s: recognized as this skip = %v, want %v", g.Name, g.Is, own)
		}
		if retry := strings.Contains(g.Card, "It will try again at the next scheduled time."); retry == own {
			t.Errorf("%s: the card promises a retry = %v: %q", g.Name, retry, g.Card)
		}
		if own {
			t.Logf("next: %s", g.Next)
			if !strings.HasPrefix(g.Next, "Next run will make no snapshot: nothing was indexed since the previous snapshot") ||
				!strings.HasSuffix(g.Next, "until a change is indexed or the source can be asked.") ||
				!strings.HasPrefix(g.Card, "No snapshot was made at 2026-11-11 10:30 UTC: nothing was indexed since the previous snapshot") ||
				!strings.HasSuffix(g.Card, "until a change is indexed or the source can be asked. As things stand, the next run will make none either.") ||
				!strings.HasSuffix(g.Alone, "until a change is indexed or the source can be asked.") || strings.Contains(g.Card, "Did not run") {
				t.Errorf("%s: next=%q card=%q alone=%q", g.Name, g.Next, g.Card, g.Alone)
			}
		}
		for _, v := range []string{g.Card, g.Next} {
			if strings.Contains(v, "..") || strings.Contains(v, "—") || strings.Contains(v, "()") {
				t.Errorf("%s: a double period, an em dash or an empty detail: %q", g.Name, v)
			}
		}
	}
	// Every place that reads next_method treats "skip" as its own answer:
	// a page that fell through to "anything but refresh is a full read"
	// would announce a read of the whole database that is not coming.
	for _, want := range []string{
		`sch.next_method === "skip" ? (nextAt ? "no snapshot at the next slot" : "")`,
		`else if (sch.next_method === "skip") cp.push(row("next run", "no snapshot", sch.next_method_why || ""));`,
		`} else if (sch.runnable && sch.next_method === "skip") {`,
		// The skipped slot's own line, in grey and ahead of the red branch.
		`if (skip && unconfirmedSkipText(skip.reason) && (!run || skip.at >= (run.finished_at || ""))) {`,
		// The copy's headline: amber with the reason, not "did not start".
		`return { tone: "warn", why: "No snapshot was made at the last scheduled time: " + plainWords(skip.reason), link: "settings" };`,
	} {
		if !strings.Contains(js, want) {
			t.Errorf("app.js no longer handles a skipped next run at: %s", want)
		}
	}
	// The drawing of the chain reads anything but "refresh" as a full read,
	// so it must stay behind the branch that handles the skip first.
	skipBranch := strings.Index(js, `} else if (sch.runnable && sch.next_method === "skip") {`)
	chain := strings.Index(js, "body.append(scheduleChainDraw(sch, nextLine));")
	if skipBranch < 0 || chain < 0 || chain < skipBranch {
		t.Error("the chain drawing is reachable before the skipped next run is handled")
	}
}

// The page is told the truth about the next run in this state: the API
// answers next_method "skip" with the reason the skip will record, no
// full-read code and no error, on the save and on the listing alike.
func TestBackupScheduleAPI_aSkippedNextRunReachesTheWire_2659(t *testing.T) {
	quiet := func(_ context.Context, _ ServerEntry, anchor time.Time) BackupWindow {
		return BackupWindow{Anchor: anchor, Events: 0, Capture: CaptureUnaskable, CaptureDetail: "PostgreSQL sources are not compared yet", CanWait: true}
	}
	srv, id := newScheduleServer(t, &stubScheduleReporter{full: true, probe: quiet})
	e, _ := srv.cm.reg.Get(id)
	fakeSnapshot(t, e.BaselineDir) // dated 2026-08-27: far past any cut-over age
	rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", `{"every":"1h","at":"00:00"}`)
	if rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	want := SkipUnconfirmedReason("PostgreSQL sources are not compared yet")
	check := func(where string, got *backupScheduleDTO) {
		t.Helper()
		if got == nil || !got.Runnable || got.NextMethod != BackupMethodSkip || got.NextMethodWhy != want || got.NextMethodWhyCode != "" || got.NextMethodError != "" {
			t.Fatalf("%s: schedule = %+v, want a runnable schedule whose next run is a skip with its reason", where, got)
		}
	}
	check("PUT", scheduleOf(t, body))
	_, body = doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	check("GET /api/baselines", scheduleOf(t, body))
	if !strings.Contains(string(body), `"next_method":"skip"`) {
		t.Fatalf("the wire does not carry next_method skip: %s", body)
	}
}

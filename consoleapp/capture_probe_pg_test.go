package consoleapp

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/status"
)

// The snapshot schedule's capture probe for a PostgreSQL source (#2844).
// The one rule: nothing this probe answers takes a full read.

// stubCaptureProbePG replaces the PostgreSQL probe and records what it was
// asked with.
func stubCaptureProbePG(t *testing.T, r captureProbeResult) *[]string {
	t.Helper()
	var asked []string
	prev := probeCapturePG
	t.Cleanup(func() { probeCapturePG = prev })
	probeCapturePG = func(_ context.Context, indexDSN, sourceDSN string, anchor, _ time.Time) captureProbeResult {
		asked = append(asked, indexDSN+"|"+sourceDSN+"|"+anchor.UTC().Format(time.RFC3339))
		return r
	}
	return &asked
}

func TestComparePGPositions(t *testing.T) {
	for _, tc := range []struct {
		name         string
		sent, source uint64
		savedAgo     time.Duration
		verdict      string
		detail       string
	}{
		{name: "equal", sent: 500, source: 500, verdict: console.CaptureCaughtUp},
		{name: "equal, however long ago it was saved", sent: 500, source: 500, savedAgo: 6 * time.Hour, verdict: console.CaptureCaughtUp},
		{name: "the source ahead, capture saving", sent: 500, source: 501, savedAgo: time.Second, detail: captureDetailPGAhead},
		{name: "the source ahead, saved at the edge of recent", sent: 500, source: 900, savedAgo: pgSaveFresh, detail: captureDetailPGAhead},
		{name: "the source ahead, capture stopped saving", sent: 500, source: 900, savedAgo: pgSaveFresh + time.Second, detail: captureDetailPGNotSaving},
		{name: "the capture past the source", sent: 900, source: 500, detail: captureDetailPGPast},
		{name: "the capture past the source and not saving: past is what is said", sent: 900, source: 500, savedAgo: time.Hour, detail: captureDetailPGPast},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verdict, detail := comparePGPositions(tc.sent, tc.source, tc.savedAgo)
			if verdict != tc.verdict || detail != tc.detail {
				t.Fatalf("got %q, %q; want %q, %q", verdict, detail, tc.verdict, tc.detail)
			}
			if verdict == console.CaptureBehind {
				t.Fatal("a PostgreSQL source was called behind, which takes a full read")
			}
		})
	}
}

func TestPGCaptureComparable(t *testing.T) {
	anchor := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	state := func(edit func(*status.StreamStateInfo)) *status.StreamStateInfo {
		st := &status.StreamStateInfo{Mode: "gtid", GapColumnsPresent: true,
			GTIDSet: sql.NullString{String: "0/277C4C48", Valid: true}}
		if edit != nil {
			edit(st)
		}
		return st
	}
	lost := func(at time.Time) func(*status.StreamStateInfo) {
		return func(st *status.StreamStateInfo) { st.GapLostAt = sql.NullTime{Time: at, Valid: true} }
	}
	// The last full read of the source: before the snapshot's anchor, as an
	// update after a full read leaves it.
	sourceRead := anchor.Add(-2 * time.Hour)
	dropped := func(doc string) func(*status.StreamStateInfo) {
		return func(st *status.StreamStateInfo) { st.CaptureSkips = sql.NullString{String: doc, Valid: true} }
	}
	skipAt := func(at time.Time) string {
		return `{"replica_identity_not_full":{"count":2,"last_at":"` + at.UTC().Format(time.RFC3339Nano) + `","tables":["public.b"]}}`
	}
	for _, tc := range []struct {
		name   string
		st     *status.StreamStateInfo
		ok     bool
		detail string
	}{
		{name: "a capture with a saved position and no loss", st: state(nil), ok: true},
		{name: "a loss after the snapshot", st: state(lost(anchor.Add(time.Minute))), detail: captureDetailPGLost},
		{name: "a loss at the snapshot's own instant", st: state(lost(anchor)), detail: captureDetailPGLost},
		{name: "a loss before the snapshot, which the snapshot's full read covers", st: state(lost(anchor.Add(-time.Minute))), ok: true},
		{name: "no capture on record", st: nil, detail: captureDetailNoCapture},
		{name: "no ledger of dropped rows, as a PostgreSQL capture from before the ledger leaves it", st: state(func(st *status.StreamStateInfo) { st.CaptureSkips = sql.NullString{} }), ok: true},
		{name: "an empty ledger", st: state(dropped(`{}`)), ok: true},
		{name: "a ledger that does not parse cannot be read", st: state(func(st *status.StreamStateInfo) { st.CaptureSkips = sql.NullString{String: "{", Valid: true} }), detail: captureDetailPGNoLossRecord},
		{name: "a change dropped after the source was last read", st: state(dropped(skipAt(sourceRead.Add(time.Minute)))), detail: captureDetailPGLost},
		{name: "a change dropped at the source read's own instant", st: state(dropped(skipAt(sourceRead))), detail: captureDetailPGLost},
		{name: "a change dropped before the source was last read, which that read covers", st: state(dropped(skipAt(sourceRead.Add(-time.Minute)))), ok: true},
		{name: "a drop on record with no time", st: state(dropped(`{"replica_identity_not_full":{"count":1}}`)), detail: captureDetailPGLost},
		{name: "a reason with a zero count", st: state(dropped(`{"replica_identity_not_full":{"count":0,"last_at":"` + anchor.Add(time.Hour).Format(time.RFC3339) + `"}}`)), ok: true},
		{name: "an index with no loss record", st: state(func(st *status.StreamStateInfo) { st.GapColumnsPresent = false }), detail: captureDetailPGNoLossRecord},
		{name: "position mode, which a PostgreSQL capture never saves",
			st: state(func(st *status.StreamStateInfo) { st.Mode = "position" }), detail: captureDetailPositionMode},
		{name: "the row a health poll wrote, before the first checkpoint",
			st: state(func(st *status.StreamStateInfo) { st.GTIDSet = sql.NullString{} }), detail: captureDetailNoCheckpoint},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ok, detail := pgCaptureComparable(tc.st, anchor, sourceRead)
			if ok != tc.ok || detail != tc.detail {
				t.Fatalf("got %v, %q; want %v, %q", ok, detail, tc.ok, tc.detail)
			}
		})
	}
}

// Through the schedule: a quiet PostgreSQL server with an old snapshot is
// asked with its own probe, and no answer of it reads the source in full. A
// loss on record makes the snapshot one that cannot wait, so the update
// runs: that update is refused for the gap, and the full read taken in its
// place is the one with a brake.
func TestBackupScheduler_quietPostgresServer_2844(t *testing.T) {
	now := snapshotAnchor.Add(8 * 24 * time.Hour)
	for _, tc := range []struct {
		name   string
		probe  captureProbeResult
		lost   bool // a loss on record since the snapshot
		method string
		why    func(string) bool
	}{
		{name: "the two positions equal: confirmed, nothing read",
			probe: captureProbeResult{verdict: console.CaptureCaughtUp}, method: console.BackupMethodRefresh,
			why: func(w string) bool { return w == "no load on your database" }},
		{name: "the source ahead while capture saves: the slot is skipped",
			probe: captureProbeResult{detail: captureDetailPGAhead}, method: console.BackupMethodSkip,
			why: func(w string) bool { return w == console.SkipUnconfirmedReason(captureDetailPGAhead) }},
		{name: "capture stopped saving: skipped, not read in full",
			probe: captureProbeResult{detail: captureDetailPGNotSaving}, method: console.BackupMethodSkip,
			why: func(w string) bool { return w == console.SkipUnconfirmedReason(captureDetailPGNotSaving) }},
		{name: "the capture past its source: skipped, not read in full",
			probe: captureProbeResult{detail: captureDetailPGPast}, method: console.BackupMethodSkip,
			why: func(w string) bool { return w == console.SkipUnconfirmedReason(captureDetailPGPast) }},
		{name: "the source did not answer: skipped",
			probe: captureProbeResult{detail: captureDetailSourceSilent, cause: "dial"}, method: console.BackupMethodSkip,
			why: func(w string) bool { return w == console.SkipUnconfirmedReason(captureDetailSourceSilent) }},
		{name: "a loss on record since the snapshot: the update, which the gap refuses, and never a full read chosen here",
			probe: captureProbeResult{detail: captureDetailPGLost}, lost: true, method: console.BackupMethodRefresh,
			why: func(w string) bool {
				return w == console.UnconfirmedUpdateWhy(captureDetailPGLost, "capture lost changes since the previous snapshot")
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The MySQL probe answers "behind", which would be a full read:
			// it must not be what a PostgreSQL server is asked with.
			b, _, e, _, _ := quietUnconfirmed(t, captureProbeResult{verdict: console.CaptureBehind, detail: captureDetailBehind})
			stubCoverage(t, true, true)
			prevLoss := lossOnRecordSince
			t.Cleanup(func() { lossOnRecordSince = prevLoss })
			lossOnRecordSince = func(context.Context, string, time.Time) (bool, error) { return tc.lost, nil }
			asked := stubCaptureProbePG(t, tc.probe)
			e.Flavor = console.FlavorPostgres
			e.SourceDSN = "postgres://u:pw@src/db"
			// Without these a full read is not possible at all, and the
			// case that expects one would pass for the wrong reason.
			e.SourceSlot, e.SourcePublication = "slot", "pub"
			method, why := freshDecision(t, b, e, now)
			t.Logf("%s: %s", method, why)
			if method != tc.method || !tc.why(why) {
				t.Fatalf("method=%q why=%q, want %q", method, why, tc.method)
			}
			if method == console.BackupMethodFull {
				t.Fatal("the probe of a PostgreSQL server chose a full read")
			}
			if len(*asked) == 0 {
				t.Fatal("the PostgreSQL probe was not asked")
			}
			for _, a := range *asked {
				if a != e.DSN+"|"+e.SourceDSN+"|"+snapshotAnchor.UTC().Format(time.RFC3339) {
					t.Fatalf("the probe was asked with %q", a)
				}
			}
		})
	}
}

// What the log says about a PostgreSQL probe. "The source is ahead" is how a
// healthy capture reads on a server that is not idle, in turns with "caught
// up": neither it nor the turns are written. A capture that stopped saving
// and a capture past its source are warnings.
func TestReportCaptureProbe_postgres(t *testing.T) {
	b, _, _ := newScheduleFixture(t, true)
	e := console.ServerEntry{ID: "p", Name: "p", Flavor: console.FlavorPostgres}
	anchor := time.Date(2026, 11, 14, 6, 0, 0, 0, time.UTC)
	logs := captureSlogFor(t)
	caughtUp := captureProbeResult{verdict: console.CaptureCaughtUp}
	ahead := captureProbeResult{detail: captureDetailPGAhead}
	for range 3 {
		b.reportCaptureProbe(e, anchor, caughtUp)
		b.reportCaptureProbe(e, anchor, ahead)
	}
	if out := logs.String(); strings.Count(out, "the source confirms it wrote nothing the capture has not recorded") != 1 ||
		strings.Contains(out, "does not confirm") || strings.Contains(out, "level=WARN") {
		t.Fatalf("caught up and ahead in turns: %q, want the one line that says caught up", out)
	}
	for _, loud := range []string{captureDetailPGNotSaving, captureDetailPGPast} {
		before := strings.Count(logs.String(), "level=WARN")
		b.reportCaptureProbe(e, anchor, captureProbeResult{detail: loud})
		b.reportCaptureProbe(e, anchor, captureProbeResult{detail: loud})
		if out := logs.String(); strings.Count(out, "level=WARN") != before+1 || !strings.Contains(out, loud) {
			t.Fatalf("%q twice: %q, want one more Warn line that says it", loud, out)
		}
	}
	// A loss on record is the update's news (it is refused for the gap, and
	// says so): here it is a reason like the structural ones, at Info.
	before := strings.Count(logs.String(), "level=WARN")
	b.reportCaptureProbe(e, anchor, captureProbeResult{detail: captureDetailPGLost})
	if out := logs.String(); strings.Count(out, "level=WARN") != before || !strings.Contains(out, captureDetailPGLost) {
		t.Fatalf("a loss on record: %q, want an Info line that says it", out)
	}
}

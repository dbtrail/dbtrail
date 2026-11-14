package consoleapp

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/status"
)

// The snapshot schedule's capture probe for a PostgreSQL source (#2844).
// The one rule: only a loss on record takes a full read.

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
		{name: "no ledger of dropped rows, which a PostgreSQL capture never writes", st: state(func(st *status.StreamStateInfo) { st.CaptureSkips = sql.NullString{} }), ok: true},
		{name: "a ledger that does not parse is not read at all", st: state(func(st *status.StreamStateInfo) { st.CaptureSkips = sql.NullString{String: "{", Valid: true} }), ok: true},
		{name: "an index with no loss record", st: state(func(st *status.StreamStateInfo) { st.GapColumnsPresent = false }), detail: captureDetailPGNoLossRecord},
		{name: "position mode, which a PostgreSQL capture never saves",
			st: state(func(st *status.StreamStateInfo) { st.Mode = "position" }), detail: captureDetailPositionMode},
		{name: "the row a health poll wrote, before the first checkpoint",
			st: state(func(st *status.StreamStateInfo) { st.GTIDSet = sql.NullString{} }), detail: captureDetailNoCheckpoint},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ok, detail := pgCaptureComparable(tc.st, anchor)
			if ok != tc.ok || detail != tc.detail {
				t.Fatalf("got %v, %q; want %v, %q", ok, detail, tc.ok, tc.detail)
			}
		})
	}
}

// Through the schedule: a quiet PostgreSQL server with an old snapshot is
// asked with its own probe, and only a loss on record reads the source in
// full.
func TestBackupScheduler_quietPostgresServer_2844(t *testing.T) {
	now := snapshotAnchor.Add(8 * 24 * time.Hour)
	for _, tc := range []struct {
		name   string
		probe  captureProbeResult
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
		{name: "a loss on record since the snapshot: the full read",
			probe: captureProbeResult{detail: captureDetailPGLost}, method: console.BackupMethodFull,
			why: func(w string) bool { return console.BackupWhyCode(w) == "window_age" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The MySQL probe answers "behind", which would be a full read:
			// it must not be what a PostgreSQL server is asked with.
			b, _, e, _, _ := quietUnconfirmed(t, captureProbeResult{verdict: console.CaptureBehind, detail: captureDetailBehind})
			stubCoverage(t, true, true)
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

package consoleapp

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
)

// #2659: "the source reported no GTID set" was one answer for three
// different sources, and only one of them is a source that cannot be asked.
// The probe gets this far only for a capture that follows its source by
// GTID and has a GTID position saved, so:
//
//   - a MariaDB source (no gtid_mode variable at all) cannot be compared:
//     the slot is skipped;
//   - a MySQL source whose gtid_mode is not ON does not serve a reader by
//     GTID (the manual, SOURCE_AUTO_POSITION: "GTID_MODE=ON on the source"),
//     so capture cannot be reading it while it keeps writing: a full read;
//   - a MySQL source with GTIDs on and an EMPTY executed set, beside a
//     capture that holds transactions, was reset or replaced: the capture
//     holds what the source does not have. A full read.
//
// Each one from the source's own answers (the two statements the probe
// runs) through the comparison, the class and the slot's decision.
func TestSourceGTIDStates_throughTheSlotsDecision_2659(t *testing.T) {
	anchor := time.Date(2026, 9, 22, 6, 0, 0, 0, time.UTC)
	uuidQ := regexp.QuoteMeta("SELECT @@GLOBAL.server_uuid")
	gtidQ := regexp.QuoteMeta("SELECT @@GLOBAL.gtid_mode, @@GLOBAL.gtid_executed")
	noSuchVariable := &mysql.MySQLError{Number: 1193, Message: "Unknown system variable"}
	mysqlSource := func(mode, executed string) func(sqlmock.Sqlmock) {
		return func(m sqlmock.Sqlmock) {
			m.ExpectQuery(uuidQ).WillReturnRows(sqlmock.NewRows([]string{"u"}).AddRow(uuidB))
			m.ExpectQuery(gtidQ).WillReturnRows(sqlmock.NewRows([]string{"m", "e"}).AddRow(mode, executed))
		}
	}
	now := snapshotAnchor.Add(8 * 24 * time.Hour)
	for _, c := range []struct {
		name    string
		source  func(sqlmock.Sqlmock)
		verdict string
		detail  string
		method  string
	}{
		{"a MariaDB source: no gtid_mode", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(uuidQ).WillReturnError(noSuchVariable)
			m.ExpectQuery(gtidQ).WillReturnError(noSuchVariable)
		}, "", captureDetailNoGTIDMode, console.BackupMethodSkip},
		{"MySQL, gtid_mode=OFF", mysqlSource("OFF", ""), "", captureDetailGTIDModeOff, console.BackupMethodFull},
		{"MySQL, gtid_mode=OFF_PERMISSIVE", mysqlSource("OFF_PERMISSIVE", ""), "", captureDetailGTIDOffPermissive, console.BackupMethodFull},
		// ON_PERMISSIVE writes GTIDs, and its executed set may even hold
		// the capture's: it still serves no reader by GTID.
		{"MySQL, gtid_mode=ON_PERMISSIVE, with the capture's set", mysqlSource("ON_PERMISSIVE", uuidB+":1-10"), "", captureDetailGTIDOnPermissive, console.BackupMethodFull},
		{"MySQL, a gtid_mode this build does not know", mysqlSource("SOMETHING_NEW", uuidB+":1-10"), "", captureDetailGTIDModeOther, console.BackupMethodFull},
		{"MySQL, lower case off", mysqlSource("off", ""), "", captureDetailGTIDModeOff, console.BackupMethodFull},
		{"MySQL, GTIDs on, an empty set (after RESET MASTER)", mysqlSource("ON", ""), "", captureDetailDiverged, console.BackupMethodFull},
		{"MySQL, GTIDs on, the capture's own set: caught up", mysqlSource("ON", uuidB+":1-10"), console.CaptureCaughtUp, "", console.BackupMethodRefresh},
		{"MySQL, GTIDs on, the source ahead", mysqlSource("ON", uuidB+":1-12"), console.CaptureBehind, captureDetailBehind, console.BackupMethodFull},
	} {
		t.Run(c.name, func(t *testing.T) {
			idx, im, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer idx.Close()
			src, sm, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			im.ExpectQuery(uuidQ).WillReturnRows(sqlmock.NewRows([]string{"u"}).AddRow(uuidA))
			c.source(sm)
			sm.ExpectClose()
			// The capture follows by GTID and has a position saved.
			r, err := compareCapture(context.Background(), idx, streamStateFor(uuidB+":1-10"), anchor, time.Time{}, func() (*sql.DB, error) { return src, nil })
			if err != nil || r.verdict != c.verdict || r.detail != c.detail {
				t.Fatalf("the probe: %+v err=%v, want verdict %q and %q", r, err, c.verdict, c.detail)
			}
			if err := sm.ExpectationsWereMet(); err != nil {
				t.Fatalf("the source was not asked what this test says it answers: %v", err)
			}
			// The same answer, in the slot.
			b, _, e, _, _ := quietUnconfirmed(t, r)
			stubCoverage(t, true, true)
			method, why := freshDecision(t, b, e, now)
			t.Logf("%s: %s", method, why)
			if method != c.method {
				t.Fatalf("method=%q why=%q, want %q", method, why, c.method)
			}
			switch {
			case c.method == console.BackupMethodSkip:
				if why != console.SkipUnconfirmedReason(c.detail) {
					t.Fatalf("the skip's reason: %q", why)
				}
			case c.verdict == console.CaptureBehind:
			case c.method == console.BackupMethodFull:
				// Worded as what was found, never as a question nobody
				// could answer, and never as "no GTID set".
				if console.BackupWhyCode(why) != "window_age" || !strings.HasSuffix(why, "ago, and "+c.detail) || strings.Contains(why, "could not be checked") {
					t.Fatalf("the full read's reason: %q", why)
				}
			}
			if strings.Contains(why, "no GTID set") {
				t.Fatalf("the reason still reads as the old single answer: %q", why)
			}
		})
	}
}

// The bound on the whole probe covers the index too, and the index's record
// of the capture is where a loss shows. A probe that ran out of time before
// that record was read is an index that did not answer (the update); only
// one that had read it is a source that did not answer in time (the skip).
func TestTimedOutSide_2659(t *testing.T) {
	timedOut := func() captureProbeResult {
		release := make(chan struct{})
		defer close(release)
		return boundedCaptureProbe(context.Background(), 20*time.Millisecond, func(context.Context) captureProbeResult {
			<-release
			return captureProbeResult{verdict: console.CaptureCaughtUp}
		})
	}
	now := snapshotAnchor.Add(8 * 24 * time.Hour)
	for _, c := range []struct {
		name      string
		indexRead bool
		detail    string
		method    string
	}{
		{"the index's part had not finished", false, captureDetailIndexSlow, console.BackupMethodRefresh},
		{"the index's part was done, the source was slow", true, captureDetailSourceSlow, console.BackupMethodSkip},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := timedOutSide(timedOut(), c.indexRead)
			if r.verdict != "" || r.detail != c.detail || r.cause == "" {
				t.Fatalf("the probe: %+v, want %q with the timeout as its cause", r, c.detail)
			}
			b, _, e, _, _ := quietUnconfirmed(t, r)
			stubCoverage(t, true, true)
			logs := captureSlogFor(t)
			method, why := freshDecision(t, b, e, now)
			t.Logf("%s: %s", method, why)
			if method != c.method {
				t.Fatalf("method=%q why=%q, want %q", method, why, c.method)
			}
			if c.indexRead {
				if why != console.SkipUnconfirmedReason("the source did not answer in time") {
					t.Fatalf("the skip's reason: %q", why)
				}
			} else if why != console.IndexUnreadUpdateWhy("the index did not answer in time") || strings.Contains(why, "the source cannot confirm") {
				t.Fatalf("the update's why blames the source, or reads %q", why)
			}
			// A read that failed is worth a look whichever side it was on.
			if line := logLine(logs.String(), "so the index may be missing changes"); !strings.Contains(line, "level=WARN") || !strings.Contains(line, c.detail) {
				t.Fatalf("the probe's line: %q", logs.String())
			}
		})
	}
	// Anything but a timeout is passed on as it is.
	for _, r := range []captureProbeResult{{verdict: console.CaptureCaughtUp}, {detail: captureDetailSourceSilent}, {detail: captureDetailCancelled}} {
		if got := timedOutSide(r, false); got != r {
			t.Fatalf("%+v became %+v", r, got)
		}
	}
}

// The line that ends a skipped stretch with an update is true whatever the
// update then does. When the index did not give the capture's record, the
// update usually finds nothing to add and publishes no snapshot: the line
// does not promise one, and the skipping resumes at the next slot with a
// start of its own.
func TestBackupScheduler_theLogWhenTheIndexWasNotRead_2659(t *testing.T) {
	b, sup, e, _, folds := quietUnconfirmed(t, captureProbeResult{detail: positionMode})
	stubCoverage(t, true, true)
	memo := sup.foldedMarks[e.ID]
	logs := captureSlogFor(t)
	t0 := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)
	b.tick(context.Background(), t0)
	slot := func(i int, r captureProbeResult) {
		t.Helper()
		sup.mu.Lock()
		sup.foldedMarks[e.ID] = memo
		sup.mu.Unlock()
		stubCaptureProbe(t, r)
		forgetWindow(b)
		b.tick(context.Background(), t0.Add(time.Duration(i)*time.Hour))
	}
	slot(1, captureProbeResult{detail: positionMode}) // skipped: the start
	// The index does not give the capture's record at this slot: the
	// update runs. Nothing was indexed and the snapshot is covered, so its
	// own gate skips it: no fold, no new snapshot.
	slot(2, captureProbeResult{detail: captureDetailIndexSilent, cause: "dial tcp: i/o timeout"})
	sup.postRefresh.Wait()
	b.watchers.Wait()
	if folds.Load() != 0 {
		t.Fatalf("%d folds: the update was expected to find nothing to add", folds.Load())
	}
	slot(3, captureProbeResult{detail: positionMode}) // the index answers again: skipped, a new start
	var seq []string
	for _, l := range strings.Split(logs.String(), "\n") {
		switch {
		case strings.Contains(l, unconfirmedSkipLine):
			seq = append(seq, "start")
		case strings.Contains(l, "scheduled slots are no longer skipped: an update runs at this slot instead"):
			seq = append(seq, "end-by-update")
			t.Logf("log: %s", l)
			if !strings.Contains(l, "the index did not give capture's record (the index did not answer)") || !strings.Contains(l, "if it finds nothing to add, from the next slot") {
				t.Errorf("the end line: %q", l)
			}
			if strings.Contains(l, "the source cannot confirm") {
				t.Errorf("the end line blames the source for an index that did not answer: %q", l)
			}
		}
	}
	if got := strings.Join(seq, ", "); got != "start, end-by-update, start" {
		t.Fatalf("the log reads %q, want start, end-by-update, start", got)
	}
}

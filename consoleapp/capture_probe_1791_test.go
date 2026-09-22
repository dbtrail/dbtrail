package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/go-sql-driver/mysql"
)

// #1791: whether the capture has checkpointed everything the source wrote.
// Only "caught up" skips a full backup, so every case below that is NOT
// caught up is a case where the net must stay.

const (
	uuidA = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	uuidB = "5fa2c8b1-72ca-11e1-9e33-c80aa9429562"
)

// streamStateFor builds the stream_state a healthy, skip-aware GTID capture
// leaves on a current index; cases change one thing at a time.
func streamStateFor(gtid string) *status.StreamStateInfo {
	return &status.StreamStateInfo{
		Mode: "gtid", GTIDSet: sql.NullString{String: gtid, Valid: true},
		GapColumnsPresent: true, CaptureSkips: sql.NullString{String: "{}", Valid: true},
	}
}

// skipLedger is a capture_skips document with one reason.
func skipLedger(count int, at time.Time) sql.NullString {
	return sql.NullString{Valid: true,
		String: fmt.Sprintf(`{"column_count_mismatch":{"count":%d,"last_at":%q}}`, count, at.UTC().Format(time.RFC3339))}
}

func TestCaptureComparable(t *testing.T) {
	anchor := time.Date(2026, 9, 22, 6, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		st   func(*status.StreamStateInfo) *status.StreamStateInfo
		ok   bool
	}{
		{"a healthy GTID capture", func(s *status.StreamStateInfo) *status.StreamStateInfo { return s }, true},
		{"no row: a file-mode index, not a live capture", func(*status.StreamStateInfo) *status.StreamStateInfo { return nil }, false},
		{"an index without the loss record: unevaluable, not no gap", func(s *status.StreamStateInfo) *status.StreamStateInfo { s.GapColumnsPresent = false; return s }, false},
		{"a loss after the anchor", func(s *status.StreamStateInfo) *status.StreamStateInfo {
			s.GapLostAt = sql.NullTime{Time: anchor.Add(time.Minute), Valid: true}
			return s
		}, false},
		{"a loss exactly at the anchor", func(s *status.StreamStateInfo) *status.StreamStateInfo {
			s.GapLostAt = sql.NullTime{Time: anchor, Valid: true}
			return s
		}, false},
		{"a loss before the anchor: the backup was taken after it", func(s *status.StreamStateInfo) *status.StreamStateInfo {
			s.GapLostAt = sql.NullTime{Time: anchor.Add(-time.Minute), Valid: true}
			return s
		}, true},
		{"no skip ledger (an older daemon): unevaluable, not clean", func(s *status.StreamStateInfo) *status.StreamStateInfo { s.CaptureSkips = sql.NullString{}; return s }, false},
		{"an unparsable skip ledger", func(s *status.StreamStateInfo) *status.StreamStateInfo {
			s.CaptureSkips = sql.NullString{String: "{not json", Valid: true}
			return s
		}, false},
		{"rows dropped after the anchor", func(s *status.StreamStateInfo) *status.StreamStateInfo {
			s.CaptureSkips = skipLedger(12, anchor.Add(time.Hour))
			return s
		}, false},
		{"rows dropped and acknowledged after the anchor: the rows did not come back", func(s *status.StreamStateInfo) *status.StreamStateInfo {
			s.CaptureSkips = skipLedger(12, anchor.Add(time.Hour))
			s.CaptureSkipsAck = sql.NullString{String: fmt.Sprintf(`{"column_count_mismatch":{"count":100,"at":%q}}`, anchor.Add(2*time.Hour).Format(time.RFC3339)), Valid: true}
			return s
		}, false},
		{"rows dropped with no date", func(s *status.StreamStateInfo) *status.StreamStateInfo {
			s.CaptureSkips = skipLedger(12, time.Time{})
			return s
		}, false},
		{"rows dropped before the anchor", func(s *status.StreamStateInfo) *status.StreamStateInfo {
			s.CaptureSkips = skipLedger(12, anchor.Add(-time.Hour))
			return s
		}, true},
		{"a reason with a zero count is not a skip", func(s *status.StreamStateInfo) *status.StreamStateInfo {
			s.CaptureSkips = skipLedger(0, anchor.Add(time.Hour))
			return s
		}, true},
		{"position mode: a healthy capture stops short of each commit", func(s *status.StreamStateInfo) *status.StreamStateInfo { s.Mode = "position"; return s }, false},
		{"no GTID set", func(s *status.StreamStateInfo) *status.StreamStateInfo { s.GTIDSet = sql.NullString{}; return s }, false},
		{"a blank GTID set", func(s *status.StreamStateInfo) *status.StreamStateInfo {
			s.GTIDSet = sql.NullString{String: " ", Valid: true}
			return s
		}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, detail := captureComparable(c.st(streamStateFor(uuidA+":1-10")), anchor)
			if ok != c.ok {
				t.Fatalf("comparable=%v (%s), want %v", ok, detail, c.ok)
			}
			if !ok && detail == "" {
				t.Fatal("a refusal must say why, for the reason line")
			}
		})
	}
}

func TestCompareGTIDSets(t *testing.T) {
	cases := []struct {
		name               string
		captured, executed string
		want               string // console.CaptureCaughtUp, console.CaptureBehind, or "" (unknown)
	}{
		{"equal", uuidA + ":1-10", uuidA + ":1-10", console.CaptureCaughtUp},
		{"equal, written differently (case, order)", uuidB + ":1-3," + uuidA + ":1-10", strings.ToUpper(uuidA) + ":1-10," + uuidB + ":1-3", console.CaptureCaughtUp},
		{"the source one transaction ahead", uuidA + ":1-10", uuidA + ":1-11", console.CaptureBehind},
		{"the source has a server the capture never saw", uuidA + ":1-10", uuidA + ":1-10," + uuidB + ":1", console.CaptureBehind},
		// Ahead is not caught up: the source's history changed under the
		// capture (RESET BINARY LOGS AND GTIDS reuses the numbers, a restore
		// keeps the server_uuid), which is the stuck capture.
		{"the capture holds more than the source (a reset)", uuidA + ":1-1000", uuidA + ":1-5", ""},
		{"the capture holds another server's transactions", uuidA + ":1-10," + uuidB + ":1-3", uuidA + ":1-10", ""},
		{"disjoint sets (another server entirely)", uuidA + ":1-10", uuidB + ":1-10", ""},
		{"the source reported no set (gtid_mode not ON)", uuidA + ":1-10", "", ""},
		{"an unparsable capture set", "not a gtid", uuidA + ":1-10", ""},
		{"an unparsable source set", uuidA + ":1-10", "junk", ""},
		{"a MariaDB set", "0-1-100", "0-1-100", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, detail := compareGTIDSets(c.captured, c.executed)
			if got != c.want {
				t.Fatalf("verdict %q (%s), want %q", got, detail, c.want)
			}
			// An empty source set would also read as unknown through the
			// comparison below it; the reason is what pins the check.
			if c.executed == "" && detail != "the source reported no GTID set" {
				t.Fatalf("empty source set: detail %q", detail)
			}
			if got != console.CaptureCaughtUp && detail == "" {
				t.Fatal("a verdict other than caught up must say why")
			}
		})
	}
}

// stubCaptureProbe replaces the probe and records what it was asked.
func stubCaptureProbe(t *testing.T, r captureProbeResult) *[]string {
	t.Helper()
	var asked []string
	prev := probeCapture
	t.Cleanup(func() { probeCapture = prev })
	probeCapture = func(_ context.Context, indexDSN, sourceDSN string, anchor time.Time) captureProbeResult {
		asked = append(asked, indexDSN+"|"+sourceDSN+"|"+anchor.UTC().Format(time.RFC3339))
		return r
	}
	return &asked
}

// The window probe asks the source only in the one case it matters: nothing
// indexed since the anchor, on an anchor past the cut-over age. Everything
// else is decided without touching the source.
func TestMeasureWindow_asksTheSourceOnlyWhenNothingWasIndexedOnAnOldAnchor(t *testing.T) {
	e := console.ServerEntry{ID: "a", Name: "a", DSN: "idx", SourceDSN: "src", BackupSchedule: &console.BackupSchedule{Every: "5m"}}
	old := time.Now().Add(-3 * time.Hour).Truncate(time.Second) // past the 2h cut-over of a 5m schedule
	fresh := time.Now().Add(-10 * time.Minute)
	caughtUp := captureProbeResult{verdict: console.CaptureCaughtUp}
	setup := func(t *testing.T, e console.ServerEntry, anchor time.Time, base, current uint64, r captureProbeResult) (*backupScheduler, *[]string) {
		t.Helper()
		b, _, sup := newScheduleFixture(t, true)
		sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: base}, publishedAt: anchor, indexDSN: e.DSN}
		mark := indexMark{events: current}
		stubIndexMark(t, &mark, true)
		return b, stubCaptureProbe(t, r)
	}
	run := func(t *testing.T, e console.ServerEntry, anchor time.Time, base, current uint64, r captureProbeResult) (console.BackupWindow, []string) {
		t.Helper()
		b, asked := setup(t, e, anchor, base, current, r)
		return b.measureWindow(context.Background(), e, anchor), *asked
	}
	t.Run("nothing indexed on an old anchor: asked, with the anchor, and the verdict carried", func(t *testing.T) {
		w, asked := run(t, e, old, 1000, 1000, captureProbeResult{verdict: console.CaptureBehind, detail: "the source reports transactions the capture's checkpoint does not include"})
		if len(asked) != 1 || asked[0] != "idx|src|"+old.UTC().Format(time.RFC3339) || w.Capture != console.CaptureBehind || w.CaptureDetail == "" {
			t.Fatalf("window=%+v asked=%v, want one probe of this server's index and source, from the anchor", w, asked)
		}
	})
	t.Run("something indexed: not asked", func(t *testing.T) {
		if w, asked := run(t, e, old, 1000, 1500, caughtUp); len(asked) != 0 || w.Capture != "" {
			t.Fatalf("window=%+v asked=%v, want the source left alone", w, asked)
		}
	})
	t.Run("fresh anchor: not asked", func(t *testing.T) {
		if w, asked := run(t, e, fresh, 1000, 1000, caughtUp); len(asked) != 0 || w.Capture != "" {
			t.Fatalf("window=%+v asked=%v, want the source left alone", w, asked)
		}
	})
	t.Run("a daily schedule's day-old anchor is not past its cut-over: not asked", func(t *testing.T) {
		daily := e
		daily.BackupSchedule = &console.BackupSchedule{Every: "1d"}
		if _, asked := run(t, daily, time.Now().Add(-25*time.Hour), 1000, 1000, caughtUp); len(asked) != 0 {
			t.Fatalf("asked=%v, want the source left alone inside six days", asked)
		}
	})
	t.Run("a PostgreSQL source: not asked, and said why", func(t *testing.T) {
		pg := e
		pg.Flavor = console.FlavorPostgres
		w, asked := run(t, pg, old, 1000, 1000, caughtUp)
		if len(asked) != 0 || w.Capture != "" || w.CaptureDetail == "" {
			t.Fatalf("window=%+v asked=%v, want no probe and a reason", w, asked)
		}
	})
	t.Run("no source configured: not asked", func(t *testing.T) {
		none := e
		none.SourceDSN = ""
		if _, asked := run(t, none, old, 1000, 1000, caughtUp); len(asked) != 0 {
			t.Fatalf("asked=%v, want no probe", asked)
		}
	})
	t.Run("a window cached before the cut-over is measured again past it", func(t *testing.T) {
		b, asked := setup(t, e, old, 1000, 1000, caughtUp)
		b.mu.Lock()
		b.windows = map[string]windowSample{e.ID: {anchor: old, at: time.Now(), w: console.BackupWindow{Anchor: old, Events: 0}}}
		b.mu.Unlock()
		if w := b.measureWindow(context.Background(), e, old); w.Capture != console.CaptureCaughtUp || len(*asked) != 1 {
			t.Fatalf("window=%+v asked=%v, want the cached verdict-less window measured again", w, *asked)
		}
	})
	t.Run("a cancelled caller's window is not cached", func(t *testing.T) {
		b, _ := setup(t, e, old, 1000, 1000, captureProbeResult{detail: "the probe did not finish in time"})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		b.measureWindow(ctx, e, old)
		b.mu.Lock()
		_, cached := b.windows[e.ID]
		b.mu.Unlock()
		if cached {
			t.Fatal("a window measured for a cancelled request was cached")
		}
	})
}

// "Caught up" all the way to the decision: a quiet server past the cut-over
// with the source's confirmation is updated, not fully backed up.
func TestChooseBackupMethod_updatesAQuietServerTheSourceConfirms(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	e := addScheduled(t, reg, true) // the fake snapshot at snapshotAnchor (weeks old), an hourly schedule (cut-over 6h)
	sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: snapshotAnchor, indexDSN: e.DSN}
	mark := indexMark{events: 1000}
	stubIndexMark(t, &mark, true)
	now := snapshotAnchor.Add(8 * time.Hour)
	for _, c := range []struct {
		verdict, want string
	}{{console.CaptureCaughtUp, console.BackupMethodRefresh}, {console.CaptureBehind, console.BackupMethodFull}, {"", console.BackupMethodFull}} {
		stubCaptureProbe(t, captureProbeResult{verdict: c.verdict, detail: "x"})
		b.mu.Lock()
		b.windows = nil
		b.mu.Unlock()
		method, why, err := console.ChooseBackupMethodAt(context.Background(), e, b.gates(), now)
		if err != nil || method != c.want {
			t.Fatalf("verdict %q: method=%q why=%q err=%v, want %s", c.verdict, method, why, err, c.want)
		}
	}
}

// Once per condition and server, loud only when it is worth a look.
func TestReportCaptureProbe(t *testing.T) {
	b, _, _ := newScheduleFixture(t, true)
	e := console.ServerEntry{ID: "a", Name: "a"}
	anchor := time.Date(2026, 9, 22, 6, 0, 0, 0, time.UTC)
	logs := captureSlogFor(t)
	structural := captureProbeResult{detail: "the capture runs in binlog-position mode, which is not compared"}
	b.reportCaptureProbe(e, anchor, structural)
	b.reportCaptureProbe(e, anchor, structural)
	if out := logs.String(); strings.Count(out, "could not confirm it wrote nothing") != 1 || !strings.Contains(out, "level=INFO") || strings.Contains(out, "level=WARN") {
		t.Fatalf("a structural reason twice: %q, want one Info line", out)
	}
	failed := captureProbeResult{detail: "the source did not answer", cause: "dial tcp 10.0.0.1:3306: i/o timeout"}
	b.reportCaptureProbe(e, anchor, failed)
	b.reportCaptureProbe(e, anchor, failed)
	if out := logs.String(); strings.Count(out, "level=WARN") != 1 || !strings.Contains(out, "i/o timeout") {
		t.Fatalf("a failed read twice: %q, want one Warn line with the cause", out)
	}
	b.reportCaptureProbe(e, anchor, captureProbeResult{verdict: console.CaptureBehind, detail: "the source reports transactions the capture's checkpoint does not include"})
	if out := logs.String(); strings.Count(out, "level=WARN") != 2 {
		t.Fatalf("behind: %q, want a second Warn line", out)
	}
	b.reportCaptureProbe(e, anchor, captureProbeResult{verdict: console.CaptureCaughtUp})
	b.reportCaptureProbe(e, anchor, captureProbeResult{verdict: console.CaptureCaughtUp})
	if out := logs.String(); strings.Count(out, "updating instead of taking a full backup on age") != 1 {
		t.Fatalf("caught up twice for one anchor: %q, want one Info line", out)
	}
	// Behind again after caught up is said again, even with the reason it
	// had before: caught up resolved it.
	b.reportCaptureProbe(e, anchor, captureProbeResult{verdict: console.CaptureBehind, detail: "the source reports transactions the capture's checkpoint does not include"})
	if out := logs.String(); strings.Count(out, "level=WARN") != 3 {
		t.Fatalf("behind after caught up: %q, want it said again", out)
	}
}

// The whole probe answers within its bound, whatever one step does: a
// source that accepts the connection and then stalls must cost the page the
// bound and an "unknown", not the sum of every step's own timeout.
func TestBoundedCaptureProbe(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	start := time.Now()
	r := boundedCaptureProbe(context.Background(), 50*time.Millisecond, func(ctx context.Context) captureProbeResult {
		<-release // a step that ignores the context, as a driver ping does
		return captureProbeResult{verdict: console.CaptureCaughtUp}
	})
	if r.verdict != "" || r.detail != "the probe did not finish in time" {
		t.Fatalf("stalled probe: %+v, want unknown", r)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("stalled probe held the caller %s", took)
	}
	if r = boundedCaptureProbe(context.Background(), time.Second, func(context.Context) captureProbeResult {
		return captureProbeResult{verdict: console.CaptureBehind}
	}); r.verdict != console.CaptureBehind {
		t.Fatalf("prompt probe: %+v", r)
	}
}

// The source probe bounds its reads as well as its dial: config.Connect's
// ping runs without the probe's context.
func TestSourceProbeDSN(t *testing.T) {
	for dsn, want := range map[string][]string{
		"u:p@tcp(10.0.0.1:3306)/":                 {"timeout=3s", "readTimeout=3s", "writeTimeout=3s"},
		"u:p@tcp(10.0.0.1:3306)/?readTimeout=30s": {"readTimeout=3s"},
		"u:p@tcp(10.0.0.1:3306)/?readTimeout=1s":  {"readTimeout=1s"},
	} {
		got := sourceProbeDSN(dsn)
		for _, w := range want {
			if !strings.Contains(got, w) {
				t.Errorf("sourceProbeDSN(%q) = %q, want it to carry %q", dsn, got, w)
			}
		}
	}
	if got := sourceProbeDSN("not a dsn"); got != "not a dsn" {
		t.Errorf("an unparsable DSN is handed on as is, got %q", got)
	}
}

// The source's executed set as the probe reads it: MySQL puts a newline
// between UUID blocks, which the single-UUID test server never shows.
func TestReadExecutedGTIDs(t *testing.T) {
	q := regexp.QuoteMeta("SELECT @@GLOBAL.gtid_mode, @@GLOBAL.gtid_executed")
	cases := []struct {
		name    string
		setup   func(sqlmock.Sqlmock)
		want    string
		wantErr bool
	}{
		{"GTIDs on, several servers", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(q).WillReturnRows(sqlmock.NewRows([]string{"m", "e"}).AddRow("ON", uuidA+":1-10,\n"+uuidB+":1-3"))
		}, uuidA + ":1-10," + uuidB + ":1-3", false},
		{"GTIDs off", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(q).WillReturnRows(sqlmock.NewRows([]string{"m", "e"}).AddRow("OFF", uuidA+":1-10"))
		}, "", false},
		{"on the way up (ON_PERMISSIVE) is not on", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(q).WillReturnRows(sqlmock.NewRows([]string{"m", "e"}).AddRow("ON_PERMISSIVE", uuidA+":1-10"))
		}, "", false},
		{"MariaDB: no such variable", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(q).WillReturnError(&mysql.MySQLError{Number: 1193, Message: "Unknown system variable 'gtid_mode'"})
		}, "", false},
		{"a real failure", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(q).WillReturnError(&mysql.MySQLError{Number: 1227, Message: "Access denied"})
		}, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, m, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			c.setup(m)
			got, err := readExecutedGTIDs(context.Background(), db)
			if (err != nil) != c.wantErr || got != c.want {
				t.Fatalf("got %q err=%v, want %q err=%v", got, err, c.want, c.wantErr)
			}
		})
	}
}

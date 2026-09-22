package consoleapp

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/console"
)

// #1791: whether the capture has checkpointed everything the source wrote.

const (
	uuidA = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	uuidB = "5fa2c8b1-72ca-11e1-9e33-c80aa9429562"
)

func TestCompareCapture(t *testing.T) {
	cases := []struct {
		name string
		cp   captureCheckpoint
		src  sourceHead
		want string // console.CaptureCaughtUp, console.CaptureBehind, or "" (unknown)
	}{
		{"no capture on record", captureCheckpoint{}, sourceHead{gtid: uuidA + ":1-10"}, ""},
		{"GTID: the capture holds everything", captureCheckpoint{present: true, mode: "gtid", gtidSet: uuidA + ":1-10"}, sourceHead{gtid: uuidA + ":1-10"}, console.CaptureCaughtUp},
		{"GTID: the capture holds more (another server's transactions too)", captureCheckpoint{present: true, mode: "gtid", gtidSet: uuidA + ":1-10," + uuidB + ":1-3"}, sourceHead{gtid: uuidA + ":1-10"}, console.CaptureCaughtUp},
		{"GTID: the source is one transaction ahead", captureCheckpoint{present: true, mode: "gtid", gtidSet: uuidA + ":1-10"}, sourceHead{gtid: uuidA + ":1-11"}, console.CaptureBehind},
		{"GTID: the source has a server the capture never saw", captureCheckpoint{present: true, mode: "gtid", gtidSet: uuidA + ":1-10"}, sourceHead{gtid: uuidA + ":1-10," + uuidB + ":1"}, console.CaptureBehind},
		{"GTID: upper-case UUIDs on the source still compare", captureCheckpoint{present: true, mode: "gtid", gtidSet: uuidA + ":1-10"}, sourceHead{gtid: strings.ToUpper(uuidA) + ":1-10"}, console.CaptureCaughtUp},
		{"GTID: the capture has no set", captureCheckpoint{present: true, mode: "gtid", gtidSet: "  "}, sourceHead{gtid: uuidA + ":1-10"}, ""},
		{"GTID: the source has no set (gtid_mode not ON)", captureCheckpoint{present: true, mode: "gtid", gtidSet: uuidA + ":1-10"}, sourceHead{}, ""},
		{"GTID: unparsable capture set", captureCheckpoint{present: true, mode: "gtid", gtidSet: "not a gtid"}, sourceHead{gtid: uuidA + ":1-10"}, ""},
		{"GTID: unparsable source set", captureCheckpoint{present: true, mode: "gtid", gtidSet: uuidA + ":1-10"}, sourceHead{gtid: "junk"}, ""},
		{"GTID on MariaDB: not compared in this slice", captureCheckpoint{present: true, mode: "gtid", flavor: "mariadb", gtidSet: "0-1-100"}, sourceHead{gtid: "0-1-100"}, ""},
		{"position: exactly where the source is", captureCheckpoint{present: true, mode: "position", file: "binlog.000012", pos: 4711}, sourceHead{file: "binlog.000012", pos: 4711}, console.CaptureCaughtUp},
		{"position: the source is further in the same file", captureCheckpoint{present: true, mode: "position", file: "binlog.000012", pos: 4711}, sourceHead{file: "binlog.000012", pos: 5000}, console.CaptureBehind},
		{"position: the source rotated to the next file", captureCheckpoint{present: true, mode: "position", file: "binlog.000012", pos: 4711}, sourceHead{file: "binlog.000013", pos: 157}, console.CaptureBehind},
		{"position: the capture is on a later file", captureCheckpoint{present: true, mode: "position", file: "binlog.000013", pos: 157}, sourceHead{file: "binlog.000012", pos: 4711}, console.CaptureCaughtUp},
		{"position: the suffix grows a digit", captureCheckpoint{present: true, mode: "position", file: "binlog.999999", pos: 4711}, sourceHead{file: "binlog.1000000", pos: 157}, console.CaptureBehind},
		{"position on MariaDB compares like MySQL", captureCheckpoint{present: true, mode: "position", flavor: "mariadb", file: "mysql-bin.000003", pos: 900}, sourceHead{file: "mysql-bin.000003", pos: 900}, console.CaptureCaughtUp},
		{"position: another base name (the capture is on another server's log)", captureCheckpoint{present: true, mode: "position", file: "mysql-bin.000012", pos: 4711}, sourceHead{file: "binlog.000012", pos: 4711}, ""},
		{"position: no numeric suffix", captureCheckpoint{present: true, mode: "position", file: "binlog", pos: 4711}, sourceHead{file: "binlog", pos: 4711}, ""},
		{"position: the capture has no file yet", captureCheckpoint{present: true, mode: "position"}, sourceHead{file: "binlog.000012", pos: 4711}, ""},
		{"position: the source reported no file (binary logging off)", captureCheckpoint{present: true, mode: "position", file: "binlog.000012", pos: 4711}, sourceHead{}, ""},
		{"a mode this build does not know", captureCheckpoint{present: true, mode: "lsn", file: "binlog.000012"}, sourceHead{file: "binlog.000012"}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, detail := compareCapture(c.cp, c.src)
			if got != c.want {
				t.Fatalf("verdict %q (%s), want %q", got, detail, c.want)
			}
			if got != console.CaptureCaughtUp && detail == "" {
				t.Fatal("a verdict other than caught up must say why, for the reason line")
			}
		})
	}
}

func TestReadCaptureCheckpoint(t *testing.T) {
	q := regexp.QuoteMeta(captureCheckpointQuery)
	cols := []string{"mode", "binlog_file", "binlog_position", "gtid_set", "flavor"}
	cases := []struct {
		name      string
		setup     func(sqlmock.Sqlmock)
		want      captureCheckpoint
		wantKnown bool
	}{
		{"a GTID capture", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(q).WillReturnRows(sqlmock.NewRows(cols).AddRow("gtid", "binlog.000012", 4711, uuidA+":1-10", "mysql"))
		}, captureCheckpoint{present: true, mode: "gtid", file: "binlog.000012", pos: 4711, gtidSet: uuidA + ":1-10", flavor: "mysql"}, true},
		{"a position capture, NULL GTID set", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(q).WillReturnRows(sqlmock.NewRows(cols).AddRow("position", "binlog.000012", 4711, nil, "mysql"))
		}, captureCheckpoint{present: true, mode: "position", file: "binlog.000012", pos: 4711, flavor: "mysql"}, true},
		// A file-mode index (`bintrail index`) never had a live capture: a
		// definite answer, not a failure, and not a dead capture either.
		{"no row: no live capture on record", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(q).WillReturnRows(sqlmock.NewRows(cols))
		}, captureCheckpoint{}, true},
		{"the read fails", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(q).WillReturnError(errors.New("Error 1146: Table 'idx.stream_state' doesn't exist"))
		}, captureCheckpoint{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, m, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			c.setup(m)
			got, known := readCaptureCheckpoint(context.Background(), db)
			if known != c.wantKnown || got != c.want {
				t.Fatalf("got %+v known=%v, want %+v known=%v", got, known, c.want, c.wantKnown)
			}
			if err := m.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The window probe asks the source only in the one case it matters: nothing
// indexed since the anchor, on an anchor past the cut-over age. Everything
// else is decided without touching the source.
func TestMeasureWindow_asksTheSourceOnlyWhenNothingWasIndexedOnAnOldAnchor(t *testing.T) {
	e := console.ServerEntry{ID: "a", Name: "a", DSN: "idx", SourceDSN: "src", BackupSchedule: &console.BackupSchedule{Every: "5m"}}
	old := time.Now().Add(-3 * time.Hour) // past the 2h cut-over of a 5m schedule
	fresh := time.Now().Add(-10 * time.Minute)
	run := func(t *testing.T, e console.ServerEntry, anchor time.Time, base, current uint64, verdict, detail string) (console.BackupWindow, []string) {
		t.Helper()
		b, _, sup := newScheduleFixture(t, true)
		sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: base}, publishedAt: anchor, indexDSN: e.DSN}
		mark := indexMark{events: current}
		stubIndexMark(t, &mark, true)
		var asked []string
		prev := probeCapture
		t.Cleanup(func() { probeCapture = prev })
		probeCapture = func(_ context.Context, indexDSN, sourceDSN string) (string, string) {
			asked = append(asked, indexDSN+"|"+sourceDSN)
			return verdict, detail
		}
		return b.measureWindow(context.Background(), e, anchor), asked
	}
	t.Run("nothing indexed on an old anchor: asked, and the verdict carried", func(t *testing.T) {
		w, asked := run(t, e, old, 1000, 1000, console.CaptureBehind, "the source is further in binlog.000012")
		if len(asked) != 1 || asked[0] != "idx|src" || w.Capture != console.CaptureBehind || w.CaptureDetail != "the source is further in binlog.000012" {
			t.Fatalf("window=%+v asked=%v, want one probe of this server's index and source", w, asked)
		}
	})
	t.Run("something indexed: not asked", func(t *testing.T) {
		if w, asked := run(t, e, old, 1000, 1500, console.CaptureCaughtUp, ""); len(asked) != 0 || w.Capture != "" {
			t.Fatalf("window=%+v asked=%v, want the source left alone", w, asked)
		}
	})
	t.Run("fresh anchor: not asked", func(t *testing.T) {
		if w, asked := run(t, e, fresh, 1000, 1000, console.CaptureCaughtUp, ""); len(asked) != 0 || w.Capture != "" {
			t.Fatalf("window=%+v asked=%v, want the source left alone", w, asked)
		}
	})
	t.Run("a daily schedule's day-old anchor is not past its cut-over: not asked", func(t *testing.T) {
		daily := e
		daily.BackupSchedule = &console.BackupSchedule{Every: "1d"}
		if _, asked := run(t, daily, time.Now().Add(-25*time.Hour), 1000, 1000, console.CaptureCaughtUp, ""); len(asked) != 0 {
			t.Fatalf("asked=%v, want the source left alone inside six days", asked)
		}
	})
	t.Run("a PostgreSQL source: not asked, and said why", func(t *testing.T) {
		pg := e
		pg.Flavor = console.FlavorPostgres
		w, asked := run(t, pg, old, 1000, 1000, console.CaptureCaughtUp, "")
		if len(asked) != 0 || w.Capture != "" || w.CaptureDetail == "" {
			t.Fatalf("window=%+v asked=%v, want no probe and a reason", w, asked)
		}
	})
	t.Run("no source configured: not asked", func(t *testing.T) {
		none := e
		none.SourceDSN = ""
		if _, asked := run(t, none, old, 1000, 1000, console.CaptureCaughtUp, ""); len(asked) != 0 {
			t.Fatalf("asked=%v, want no probe", asked)
		}
	})
}

// The source probe bounds its reads as well as its dial: config's helpers
// take no context, so the connection's own read timeout is the bound.
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

// The whole probe answers within its bound, whatever one step does: a
// source that accepts the connection and then stalls must cost the page the
// bound and an "unknown", not the sum of every step's own timeout.
func TestBoundedCaptureProbe(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	start := time.Now()
	verdict, detail := boundedCaptureProbe(context.Background(), 50*time.Millisecond, func(ctx context.Context) (string, string) {
		<-release // a step that ignores the context, as config's helpers do
		return console.CaptureCaughtUp, ""
	})
	if verdict != "" || detail != "the probe did not finish in time" {
		t.Fatalf("stalled probe: verdict=%q detail=%q, want unknown", verdict, detail)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("stalled probe held the caller %s", took)
	}
	// An answer inside the bound is the answer.
	verdict, _ = boundedCaptureProbe(context.Background(), time.Second, func(context.Context) (string, string) {
		return console.CaptureBehind, "ahead"
	})
	if verdict != console.CaptureBehind {
		t.Fatalf("prompt probe: verdict=%q", verdict)
	}
}

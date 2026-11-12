package consoleapp

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/notify"
	"github.com/dbtrail/dbtrail/internal/rotation"
)

// #2669 point 3. The gate's Warn when it cannot answer one of its questions
// said what it could not tell and never why: the error itself (a refused
// connection, a missing grant, a timeout) stayed at Debug, which the default
// level does not print. The operator was told to fix something and not what.

// Every blind question of the quiet-server gate carries its cause in the line.
func TestRefreshCanSkip_blindLinesCarryTheCause_2669(t *testing.T) {
	mark := indexMark{events: 100, schemaChanges: 7}
	req := refreshRequest{ServerID: "s", ServerName: "shop", IndexDSN: "d"}
	for _, c := range []struct {
		question string
		why      string
		blind    func(t *testing.T, cause error)
	}{
		{"coverage", "cannot tell how far the index still reaches, so every cycle folds", func(t *testing.T, cause error) {
			prev := snapshotStillCovered
			t.Cleanup(func() { snapshotStillCovered = prev })
			snapshotStillCovered = func(context.Context, string, time.Time, time.Time, coverageRule) (bool, error) {
				return false, cause
			}
		}},
		{"tables-created", "cannot tell whether a table was created since the last snapshot, so every cycle folds", func(t *testing.T, cause error) {
			stubCoverage(t, true, true)
			prev := tablesCreatedSince
			t.Cleanup(func() { tablesCreatedSince = prev })
			tablesCreatedSince = func(context.Context, string, time.Time) (bool, error) { return false, cause }
		}},
		{"loss", "cannot tell whether capture lost events since the last snapshot, so every cycle folds", func(t *testing.T, cause error) {
			stubCoverage(t, true, true)
			prev := lossOnRecordSince
			t.Cleanup(func() { lossOnRecordSince = prev })
			lossOnRecordSince = func(context.Context, string, time.Time) (bool, error) { return false, cause }
		}},
	} {
		t.Run(c.question, func(t *testing.T) {
			log := captureLog(t, slog.LevelInfo) // the default level: Debug is not printed
			sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
			sup.gateEdge = notify.NewEdge(time.Hour)
			seedQuietServer(sup, req, mark)
			cause := errors.New("Error 1142 (42000): SELECT command denied to user 'bintrail'@'10.0.0.4' for table 'stream_state'")
			c.blind(t, cause)
			if sup.refreshCanSkip(context.Background(), req, mark, refreshAt, 0) {
				t.Fatal("skipped although the gate could not answer")
			}
			out := log.String()
			t.Logf("the line: %s", strings.TrimSpace(out))
			if got := strings.Count(out, "level=WARN"); got != 1 {
				t.Fatalf("%d Warn line(s), want 1: %q", got, out)
			}
			for _, want := range []string{c.why, "question=" + c.question, `error="Error 1142 (42000): SELECT command denied`} {
				if !strings.Contains(out, want) {
					t.Errorf("the line does not carry %q: %q", want, out)
				}
			}
			// Once per condition, although a different text comes back.
			c.blind(t, errors.New("dial tcp 10.0.0.9:3306: i/o timeout"))
			for range 3 {
				sup.refreshCanSkip(context.Background(), req, mark, refreshAt, 0)
			}
			if got := strings.Count(log.String(), "level=WARN"); got != 1 {
				t.Errorf("%d Warn line(s) after more blind cycles with another cause, want still 1", got)
			}
			if !sup.gateEdge.Active("gate-blind:" + c.question + ":s") {
				t.Error("the blind condition is not standing")
			}
		})
	}
}

// The mark question is asked by the cycle itself; its line goes through the
// same function, which puts the cause beside the question.
func TestReportGateBlind_carriesTheCause_2669(t *testing.T) {
	log := captureLog(t, slog.LevelInfo)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.gateEdge = notify.NewEdge(time.Hour)
	req := refreshRequest{ServerID: "s", ServerName: "shop"}
	sup.reportGateBlind("mark", errors.New("SELECT MAX(event_id) FROM binlog_events: driver: bad connection"), req,
		"cannot tell whether anything has been indexed, so every cycle folds; this server is not "+
			"being skipped and will not be until the index answers")
	out := log.String()
	t.Logf("the line: %s", strings.TrimSpace(out))
	if !strings.Contains(out, `error="SELECT MAX(event_id) FROM binlog_events: driver: bad connection"`) {
		t.Errorf("the line does not carry the cause: %q", out)
	}
	sup.reportGateBlind("mark", nil, req, "")
	if sup.gateEdge.Active("gate-blind:mark:s") {
		t.Error("an answered question did not end the condition")
	}
}

// The reads behind the mark and coverage questions return the cause instead of
// logging it at Debug and answering a bare "not known".
func TestGateReads_returnTheCause_2669(t *testing.T) {
	t.Run("mark: the failing query", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT MAX\\(event_id\\) FROM binlog_events").WillReturnError(errors.New("Error 1142: SELECT command denied"))
		_, err = readIndexMarkFrom(context.Background(), db)
		if err == nil || !strings.Contains(err.Error(), "SELECT MAX(event_id) FROM binlog_events: Error 1142: SELECT command denied") {
			t.Errorf("err = %v, want the query and its error", err)
		}
	})
	t.Run("mark: a negative high-water mark", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT MAX\\(event_id\\) FROM binlog_events").WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(-3))
		_, err = readIndexMarkFrom(context.Background(), db)
		if err == nil || !strings.Contains(err.Error(), "returned -3, a negative high-water mark") {
			t.Errorf("err = %v, want the negative value named", err)
		}
	})
	t.Run("coverage: no partition to measure from", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("information_schema.PARTITIONS").WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME",
			"PARTITION_DESCRIPTION", "PARTITION_ORDINAL_POSITION", "TABLE_ROWS"}).AddRow("p_future", "MAXVALUE", 1, 0))
		_, err = snapshotCoveredIn(context.Background(), db, "idx", time.Now(), time.Now(), coverageRule{})
		if !errors.Is(err, errNoLiveFloor) {
			t.Errorf("err = %v, want errNoLiveFloor", err)
		}
	})
	t.Run("coverage: the partition list fails", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("information_schema.PARTITIONS").WillReturnError(errors.New("gone away"))
		_, err = snapshotCoveredIn(context.Background(), db, "idx", time.Now(), time.Now(), coverageRule{})
		if err == nil || !strings.Contains(err.Error(), "read the index partitions: gone away") {
			t.Errorf("err = %v, want the cause", err)
		}
	})
}

// The other blind questions of the refresh cycle say their cause the same way:
// the chain floor, where a published snapshot's readers start, the retention
// the index was created under, and the schedule's update-size probe.
func TestOtherBlindLinesCarryTheCause_2669(t *testing.T) {
	req := refreshRequest{ServerID: "s", ServerName: "shop", IndexDSN: "d"}
	newSup := func(t *testing.T) *baselineSupervisor {
		sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
		sup.gateEdge = notify.NewEdge(time.Hour)
		return sup
	}
	check := func(t *testing.T, log *lockedBuf, wants ...string) {
		t.Helper()
		out := log.String()
		t.Logf("the line: %s", strings.TrimSpace(out))
		if got := strings.Count(out, "level=WARN"); got != 1 {
			t.Fatalf("%d Warn line(s), want 1: %q", got, out)
		}
		for _, want := range wants {
			if !strings.Contains(out, want) {
				t.Errorf("the line does not carry %q: %q", want, out)
			}
		}
	}

	t.Run("chain floor, the read fails", func(t *testing.T) {
		log := captureLog(t, slog.LevelInfo)
		stubLiveFloor(t, time.Time{}, false)
		newSup(t).chainStartFloor(req, refreshAt, time.Hour)
		check(t, log, "cannot tell how far back the index keeps events", "question=chain-floor", `error="stub: the index did not answer"`)
	})
	t.Run("chain floor, no partition to measure from", func(t *testing.T) {
		log := captureLog(t, slog.LevelInfo)
		stubLiveFloor(t, time.Time{}, true)
		newSup(t).chainStartFloor(req, refreshAt, time.Hour)
		check(t, log, "question=chain-floor", `error="the index has no partition to measure coverage from"`)
	})
	t.Run("where the readers start", func(t *testing.T) {
		log := captureLog(t, slog.LevelInfo)
		stubReadsFrom(t, time.Time{}, errors.New("read orders.parquet footer: unexpected EOF"))
		sup := newSup(t)
		sup.tableDeltas = true
		if _, ok := sup.publishedReadsFrom(req, refreshAt); ok {
			t.Fatal("the start is known although its read failed")
		}
		check(t, log, "cannot tell where the readers of the snapshot just published start, so the next cycle folds instead of skipping",
			"question=reads-from", `error="read orders.parquet footer: unexpected EOF"`)
	})
	for _, c := range []struct {
		name string
		e    rotation.Effective
		want string
	}{
		{"retention, the read fails", rotation.Effective{Source: rotation.RetainUnreadable, Err: errors.New("Error 1142: SELECT command denied")},
			`error="Error 1142: SELECT command denied"`},
		{"retention, unreadable without an error", rotation.Effective{Source: rotation.RetainUnreadable},
			`error="the index did not say"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			log := captureLog(t, slog.LevelInfo)
			stubIndexRetention(t, c.e)
			sup := newSup(t)
			sup.followRotation(func() rotation.Settings { return rotation.Settings{Enabled: true, Retain: 12 * time.Hour} })
			sup.coverageRuleFor(context.Background(), req, time.Hour)
			check(t, log, "cannot read the retention this index was created under", "question=retention", c.want)
		})
	}
	t.Run("update-size probe", func(t *testing.T) {
		log := captureLog(t, slog.LevelInfo)
		b, _, _ := newScheduleFixture(t, false)
		b.sup.gateEdge = notify.NewEdge(time.Hour)
		b.reportWindowBlind(console.ServerEntry{ID: "s", Name: "shop"}, context.DeadlineExceeded)
		check(t, log, "snapshot schedule: the index did not answer the update-size probe in time", "server=shop",
			`error="context deadline exceeded"`)
	})
}

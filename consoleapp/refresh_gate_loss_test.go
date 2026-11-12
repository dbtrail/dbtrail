package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/notify"
	"github.com/dbtrail/dbtrail/internal/status"
)

// realLossOnRecordSince is the production index read, taken before init below
// replaces it: unit tests run the quiet-server gate against a DSN that reaches
// nothing, where the real read would answer "could not ask" and fold every
// cycle (the same arrangement as realTablesCreatedSince).
var realLossOnRecordSince = lossOnRecordSince

func init() {
	lossOnRecordSince = func(context.Context, string, time.Time) (bool, error) { return false, nil }
}

// A capture loss moves neither index mark. On a server where nothing has been
// indexed since the last snapshot, the gate used to skip the cycle with
// "nothing had been indexed since the last snapshot": the update was never
// refused, and the full read that cures the loss was never taken in its place.
func TestRefreshCanSkip_notWhenCaptureLostEventsSinceTheSnapshot(t *testing.T) {
	mark := indexMark{events: 100, schemaChanges: 7}
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := refreshRequest{ServerID: "s", ServerName: "shop", IndexDSN: "d"}
	seedQuietServer(sup, req, mark)
	stubCoverage(t, true, true)
	if !sup.refreshCanSkip(context.Background(), req, mark, refreshAt, 0) {
		t.Fatal("fixture: a quiet server with no loss on record was not skipped")
	}

	prev := lossOnRecordSince
	t.Cleanup(func() { lossOnRecordSince = prev })
	var askedSince time.Time
	lossOnRecordSince = func(_ context.Context, dsn string, since time.Time) (bool, error) {
		if dsn != req.IndexDSN {
			t.Errorf("asked index %q, want %q", dsn, req.IndexDSN)
		}
		askedSince = since
		return true, nil
	}
	if sup.refreshCanSkip(context.Background(), req, mark, refreshAt, 0) {
		t.Fatal("skipped although capture lost events since the last snapshot: the update must run and be refused")
	}
	// Dated against the snapshot the update would start from.
	if !askedSince.Equal(refreshAt) {
		t.Errorf("asked for a loss since %v, want the published snapshot's instant %v", askedSince, refreshAt)
	}
	lossOnRecordSince = func(context.Context, string, time.Time) (bool, error) { return false, errors.New("index down") }
	if sup.refreshCanSkip(context.Background(), req, mark, refreshAt, 0) {
		t.Fatal("skipped although the index could not say whether capture lost events")
	}
	lossOnRecordSince = func(context.Context, string, time.Time) (bool, error) { return false, nil }
	if !sup.refreshCanSkip(context.Background(), req, mark, refreshAt, 0) {
		t.Fatal("not skipped once the index says no loss is on record since the snapshot")
	}
}

// The rule: a loss at or after the snapshot, acknowledged or not. An index
// that cannot say (no capture on record, or older than the loss record) adds
// no reason to run, which is how the gate behaved before it asked.
func TestLossSince(t *testing.T) {
	since := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	at := func(d time.Duration) sql.NullTime { return sql.NullTime{Time: since.Add(d), Valid: true} }
	for _, c := range []struct {
		name string
		st   *status.StreamStateInfo
		want bool
	}{
		{"no loss on record", &status.StreamStateInfo{GapColumnsPresent: true}, false},
		{"a loss after the snapshot", &status.StreamStateInfo{GapColumnsPresent: true, GapLostAt: at(time.Minute)}, true},
		{"a loss after the snapshot, acknowledged", &status.StreamStateInfo{GapColumnsPresent: true, GapLostAt: at(time.Minute), GapAckedAt: at(time.Hour)}, true},
		{"a loss exactly at the snapshot", &status.StreamStateInfo{GapColumnsPresent: true, GapLostAt: at(0)}, true},
		{"a loss before the snapshot: behind it", &status.StreamStateInfo{GapColumnsPresent: true, GapLostAt: at(-time.Second)}, false},
		{"a loss before the snapshot, acknowledged", &status.StreamStateInfo{GapColumnsPresent: true, GapLostAt: at(-time.Second), GapAckedAt: at(time.Hour)}, false},
		{"no capture on record (a file-mode index)", nil, false},
		{"an index older than the loss record: unchanged, still skippable", &status.StreamStateInfo{GapColumnsPresent: false}, false},
		{"an index older than the loss record wins over an unread stamp", &status.StreamStateInfo{GapColumnsPresent: false, GapLostAt: at(time.Minute)}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.st != nil && c.st.GapAckedAt.Valid && !c.st.GapAcknowledged() {
				t.Fatal("fixture: the loss must read as acknowledged")
			}
			if got := lossSince(c.st, since); got != c.want {
				t.Errorf("lossSince = %v, want %v", got, c.want)
			}
		})
	}
}

// #2664. When stream_state cannot be read the gate folds every cycle, which is
// safe, and said so only at Debug: on a quiet server the operator saw updates
// run every cycle with nothing at the default log level to explain it. The
// mark and coverage questions in the same gate say it at Warn, once per
// condition; this one now does the same.
func TestRefreshCanSkip_saysWhenItCannotAskAboutALoss(t *testing.T) {
	mark := indexMark{events: 100, schemaChanges: 7}
	req := refreshRequest{ServerID: "s", ServerName: "shop", IndexDSN: "d"}
	log := captureLog(t, slog.LevelInfo) // the default level: Debug is not printed
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.gateEdge = notify.NewEdge(time.Hour)
	seedQuietServer(sup, req, mark)
	stubCoverage(t, true, true)
	prev := lossOnRecordSince
	t.Cleanup(func() { lossOnRecordSince = prev })
	warns := func() int { return strings.Count(log.String(), "level=WARN") }

	lossOnRecordSince = func(context.Context, string, time.Time) (bool, error) { return false, nil }
	if !sup.refreshCanSkip(context.Background(), req, mark, refreshAt, 0) {
		t.Fatal("fixture: a quiet server with no loss on record was not skipped")
	}
	if got := warns(); got != 0 {
		t.Fatalf("%d Warn line(s) while the index answers, want 0: %q", got, log.String())
	}

	lossOnRecordSince = func(context.Context, string, time.Time) (bool, error) { return false, errors.New("index down") }
	if sup.refreshCanSkip(context.Background(), req, mark, refreshAt, 0) {
		t.Fatal("skipped although the index could not say whether capture lost events")
	}
	if got := warns(); got != 1 {
		t.Fatalf("%d Warn line(s) on the first blind cycle, want 1: %q", got, log.String())
	}
	t.Logf("the line: %s", strings.TrimSpace(log.String()))
	for _, want := range []string{
		"baseline refresh: cannot tell whether capture lost events since the last snapshot, so every cycle folds; " +
			"this server is not being skipped and will not be until the index answers",
		"server=shop", "id=s", "question=loss",
	} {
		if !strings.Contains(log.String(), want) {
			t.Errorf("the line does not carry %q: %q", want, log.String())
		}
	}
	for range 5 {
		sup.refreshCanSkip(context.Background(), req, mark, refreshAt, 0)
	}
	if got := warns(); got != 1 {
		t.Errorf("%d Warn line(s) after six blind cycles, want still 1", got)
	}
	if !sup.gateEdge.Active("gate-blind:loss:s") {
		t.Error("the blind condition is not standing while the read keeps failing")
	}

	// The index answers again: the condition ends without a line, and a
	// later failure is a new one.
	lossOnRecordSince = func(context.Context, string, time.Time) (bool, error) { return false, nil }
	if !sup.refreshCanSkip(context.Background(), req, mark, refreshAt, 0) {
		t.Fatal("not skipped once the index answers again")
	}
	if got := warns(); got != 1 || sup.gateEdge.Active("gate-blind:loss:s") {
		t.Errorf("after the read worked again: %d Warn line(s), condition standing = %v; want 1 and false",
			got, sup.gateEdge.Active("gate-blind:loss:s"))
	}
	lossOnRecordSince = func(context.Context, string, time.Time) (bool, error) { return false, errors.New("index down") }
	sup.refreshCanSkip(context.Background(), req, mark, refreshAt, 0)
	if got := warns(); got != 2 {
		t.Errorf("%d Warn line(s) after the read failed again, want 2", got)
	}

	// A loss the index DID report is an answer, not blindness: no Warn.
	lossOnRecordSince = func(context.Context, string, time.Time) (bool, error) { return true, nil }
	if sup.refreshCanSkip(context.Background(), req, mark, refreshAt, 0) {
		t.Fatal("skipped although capture lost events since the last snapshot")
	}
	if got := warns(); got != 2 || sup.gateEdge.Active("gate-blind:loss:s") {
		t.Errorf("a reported loss: %d Warn line(s), blind condition standing = %v; want 2 and false",
			got, sup.gateEdge.Active("gate-blind:loss:s"))
	}
}

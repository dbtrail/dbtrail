package status

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// #2669 point 4. LoadGapWindow runs on every status read (timers, page loads,
// the MCP tool), and warned on each of them while the floor could not be read,
// including reads whose request the browser had already cancelled.

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func captureStatusLog(t *testing.T) *syncBuf {
	t.Helper()
	buf := &syncBuf{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

func TestLoadGapWindow_warnsOncePerCondition_2669(t *testing.T) {
	log := captureStatusLog(t)
	const dbName = "idx_2669_once"
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	warns := func() int { return strings.Count(log.String(), "level=WARN") }
	read := func() {
		t.Helper()
		LoadGapWindow(context.Background(), db, dbName, ackedLostStream())
	}
	goodRead := func(below func()) {
		t.Helper()
		mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026062300", "p_future"))
		mock.ExpectQuery(covArchQ).WillReturnRows(gwArchRows(nil, nil, 0))
		below()
		read()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("the read took another branch: %v", err)
		}
	}
	nothingBelow := func() {
		mock.ExpectQuery(gwArchHeldQ).WillReturnRows(gwArchHeldRows(nil, nil))
		mock.ExpectQuery(gwEvHeldQ).WillReturnRows(gwEvHeldRows())
	}

	for range 3 {
		mock.ExpectQuery(covPartsQ).WillReturnError(errors.New("Error 1045: Access denied for user 'bintrail'"))
		read()
	}
	t.Logf("the lines:\n%s", strings.TrimSpace(log.String()))
	if got := warns(); got != 1 {
		t.Fatalf("%d Warn line(s) after three failed reads, want 1: %q", got, log.String())
	}
	for _, want := range []string{
		"could not determine the delta-coverage floor; the acknowledged capture loss stays on record",
		"db=" + dbName, `error="list live partitions: Error 1045: Access denied for user 'bintrail'"`,
	} {
		if !strings.Contains(log.String(), want) {
			t.Errorf("the line lacks %q", want)
		}
	}

	// The read works: one Info line, and the next failure is a new condition.
	goodRead(nothingBelow)
	goodRead(nothingBelow)
	if got := strings.Count(log.String(), "the delta-coverage floor can be read again"); got != 1 {
		t.Errorf("%d recovery line(s) after two good reads, want 1: %q", got, log.String())
	}
	mock.ExpectQuery(covPartsQ).WillReturnError(errors.New("i/o timeout"))
	read()
	if got := warns(); got != 2 {
		t.Errorf("%d Warn line(s) after the read failed again, want 2", got)
	}

	// The look below the floor is its own condition, said the same way.
	for range 3 {
		goodRead(func() {
			mock.ExpectQuery(gwArchHeldQ).WillReturnRows(gwArchHeldRows(nil, nil))
			mock.ExpectQuery(gwEvHeldQ).WillReturnError(errors.New("Error 1205: Lock wait timeout exceeded"))
		})
	}
	if got := strings.Count(log.String(), "could not check whether changes from before the acknowledged capture loss are still held"); got != 1 {
		t.Errorf("%d line(s) for three failed looks below the floor, want 1: %q", got, log.String())
	}
	goodRead(nothingBelow)
	t.Logf("the lines:\n%s", strings.TrimSpace(log.String()))
	if !strings.Contains(log.String(), "changes from before the acknowledged capture loss can be checked again") {
		t.Errorf("no line when the look below the floor worked again: %q", log.String())
	}
}

func TestLoadGapWindow_cancelledReadIsNotACondition_2669(t *testing.T) {
	log := captureStatusLog(t)
	const dbName = "idx_2669_cancel"
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(covPartsQ).WillDelayFor(time.Second).WillReturnRows(covPartRows("p_2026062300"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := ackedLostStream()
	LoadGapWindow(ctx, db, dbName, s)
	if got := ContinuityStatus(s, nil); got != ContinuityGapAcknowledged {
		t.Errorf("verdict = %q after a cancelled read, want gap_acknowledged", got)
	}
	if strings.Contains(log.String(), "level=WARN") {
		t.Errorf("a cancelled request warned: %q", log.String())
	}
	// Not a condition either: the first real failure still warns.
	mock.ExpectQuery(covPartsQ).WillReturnError(errors.New("i/o timeout"))
	LoadGapWindow(context.Background(), db, dbName, ackedLostStream())
	if got := strings.Count(log.String(), "level=WARN"); got != 1 {
		t.Errorf("%d Warn line(s) on the first real failure after a cancelled one, want 1: %q", got, log.String())
	}
}

// The coverage summary reads the same floor on every server switch and
// warned the same way.
func TestCollectCoverageSummary_warnsOncePerCondition_2669(t *testing.T) {
	log := captureStatusLog(t)
	const dbName = "idx_2669_cov"
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)
	summary := func(floorErr error) {
		t.Helper()
		if floorErr != nil {
			mock.ExpectQuery(covPartsQ).WillReturnError(floorErr)
		} else {
			mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_future"))
			mock.ExpectQuery(covArchQ).WillReturnRows(gwArchRows(nil, nil, 0))
		}
		mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_future"))
		mock.ExpectQuery(covProbeQ).WillReturnRows(covProbeRow(nil))
		mock.ExpectQuery(covStreamQ).WillReturnError(errors.New("skip"))
		_, _ = CollectCoverageSummary(context.Background(), db, dbName, time.Now())
	}
	for range 3 {
		summary(errors.New("i/o timeout"))
	}
	if got := strings.Count(log.String(), "coverage window start is unknown"); got != 1 {
		t.Errorf("%d floor line(s) after three summaries, want 1: %q", got, log.String())
	}
	summary(nil)
	t.Logf("the lines:\n%s", strings.TrimSpace(log.String()))
	if !strings.Contains(log.String(), "the delta-coverage floor can be read again; the coverage window has a start") {
		t.Errorf("no line when the floor could be read again: %q", log.String())
	}
}

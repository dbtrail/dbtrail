package streamrun

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// Edge cases written down before the recognizer (#1708). Each is a row below.
//
//	statement text : ours (3 spellings), lower case, extra spaces, tabs and
//	                 newlines, literals instead of "?", cut short after and
//	                 before the part that tells it apart, empty, NULL
//	not ours       : a DELETE on another table, a DELETE on a table whose name
//	                 only STARTS like ours, another DELETE on binlog_events,
//	                 a SELECT, an INSERT, a comment that quotes our statement
//	who runs it    : another user, another database, a database that differs
//	                 only in case, no database, this same connection
//	how many       : none, one, two at once
//	the look       : this connection missing from the list, no database on it

func nstr(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }

const (
	stmtPosition = "DELETE FROM binlog_events WHERE (CHAR_LENGTH(binlog_file) > CHAR_LENGTH(?)\n\t\t    OR (CHAR_LENGTH(binlog_file) = CHAR_LENGTH(?) AND binlog_file > ?))\n\t\t    OR (binlog_file = ? AND start_pos >= ?)"
	stmtFloor    = "DELETE FROM binlog_events WHERE event_id >= ? AND ((CHAR_LENGTH(binlog_file) > CHAR_LENGTH(?)\n\t\t    OR (CHAR_LENGTH(binlog_file) = CHAR_LENGTH(?) AND binlog_file > ?))\n\t\t    OR (binlog_file = ? AND start_pos >= ?))"
	stmtGTID     = "DELETE FROM binlog_events WHERE gtid IN (?,?,?)"
)

func TestIsResumeCleanupStatement(t *testing.T) {
	for _, c := range []struct {
		name, info string
		want       bool
	}{
		{"position delete", stmtPosition, true},
		{"position delete with floor", stmtFloor, true},
		{"gtid straggler delete", stmtGTID, true},
		{"lower case", strings.ToLower(stmtFloor), true},
		{"upper case", strings.ToUpper(stmtPosition), true},
		{"extra spaces and newlines", "  \n\tDELETE   FROM\n binlog_events \t WHERE\n\n gtid   IN   (?)  ", true},
		{"literals instead of placeholders", "DELETE FROM binlog_events WHERE event_id >= 48211907 AND ((CHAR_LENGTH(binlog_file) > CHAR_LENGTH('binlog.000042') OR", true},
		{"cut short after the telling part", "DELETE FROM binlog_events WHERE (CHAR_LENGTH(binlog_file) > CHAR_LENGTH(", true},
		{"cut short inside the telling part", "DELETE FROM binlog_events WHERE (CHAR_LENGTH(binlog_fi", false},
		{"cut short inside the floor", "DELETE FROM binlog_events WHERE event_id >= 482", false},
		{"cut short at WHERE", "DELETE FROM binlog_events WHERE", false},
		{"empty", "", false},
		{"only spaces", " \n\t ", false},
		{"delete on another table", "DELETE FROM stream_state WHERE id = 1", false},
		{"delete on a table that starts like ours", "DELETE FROM binlog_events_old WHERE gtid IN (?)", false},
		{"another delete on binlog_events", "DELETE FROM binlog_events WHERE event_timestamp < ?", false},
		{"delete on binlog_events with no WHERE", "DELETE FROM binlog_events", false},
		{"floor followed by something else", "DELETE FROM binlog_events WHERE event_id >= ? AND (schema_name = ?)", false},
		{"floor with two tokens", "DELETE FROM binlog_events WHERE event_id >= 1 OR 1 AND ((CHAR_LENGTH(binlog_file) > CHAR_LENGTH(", false},
		{"select", "SELECT DISTINCT gtid FROM binlog_events WHERE binlog_file = ? AND start_pos < ?", false},
		{"insert", "INSERT INTO binlog_events (gtid) VALUES (?)", false},
		{"our statement inside a comment", "/* DELETE FROM binlog_events WHERE gtid IN (?) */ SELECT 1", false},
		{"our statement inside a select", "SELECT 'DELETE FROM binlog_events WHERE gtid IN (?)'", false},
		{"schema qualified", "DELETE FROM other.binlog_events WHERE gtid IN (?)", false},
	} {
		if got := isResumeCleanupStatement(c.info); got != c.want {
			t.Errorf("%s: isResumeCleanupStatement = %v, want %v\n%q", c.name, got, c.want, c.info)
		}
	}
}

// capturingMatcher accepts every statement and keeps its text.
type capturingMatcher struct {
	mu   sync.Mutex
	seen []string
}

func (m *capturingMatcher) Match(_, actual string) error {
	m.mu.Lock()
	m.seen = append(m.seen, actual)
	m.mu.Unlock()
	return nil
}

// TestCleanupRecognizerMatchesTheRealStatements is the seam: the recognizer
// holds its own copy of how the cleanup's statements begin, and the cleanup is
// code nobody edits lightly. This runs the real functions and reads what they
// send, so the two cannot drift apart without a red test.
func TestCleanupRecognizerMatchesTheRealStatements(t *testing.T) {
	m := &capturingMatcher{}
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(m))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	saved, err := parseGTIDSetForFlavor("mysql", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec("").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"gtid"}).AddRow("3e11fa47-71ca-11e1-9e33-c80aa9429562:9"))
	mock.ExpectExec("").WillReturnResult(sqlmock.NewResult(0, 1))

	if _, err := deleteEventsSinceCheckpoint(db, "binlog.000042", 5000, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := deleteEventsSinceCheckpointGTID(db, "binlog.000042", 5000, saved, "mysql", 48211907); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	var deletes []string
	for _, s := range m.seen {
		isDelete := strings.HasPrefix(strings.ToUpper(strings.TrimSpace(s)), "DELETE")
		if got := isResumeCleanupStatement(s); got != isDelete {
			t.Errorf("isResumeCleanupStatement = %v for a statement the cleanup sent, want %v\n%q", got, isDelete, s)
		}
		if isDelete {
			deletes = append(deletes, s)
		}
	}
	// No floor, floor, and the straggler delete: all three spellings, or this
	// proved less than it says.
	if len(deletes) != 3 {
		t.Fatalf("the cleanup sent %d DELETE statements, want 3: %q", len(deletes), deletes)
	}
	if !strings.Contains(deletes[1], "event_id >=") || !strings.Contains(deletes[2], "gtid IN") || strings.Contains(deletes[0], "event_id") {
		t.Errorf("the three statements are not the three spellings: %q", deletes)
	}
}

func TestEarlierCleanups(t *testing.T) {
	self := processRow{ID: 10, User: nstr("bintrail"), DB: nstr("bintrail_idx_a"), Info: nstr("SELECT ID, USER ... FROM information_schema.PROCESSLIST"), Self: true}
	row := func(id uint64, user, db string, secs int64, info sql.NullString) processRow {
		r := processRow{ID: id, User: nstr(user), Seconds: secs, Info: info}
		if db != "" {
			r.DB = nstr(db)
		}
		return r
	}
	for _, c := range []struct {
		name string
		rows []processRow
		want []uint64
	}{
		{"nothing else running", []processRow{self}, nil},
		{"one earlier cleanup", []processRow{self, row(7, "bintrail", "bintrail_idx_a", 812, nstr(stmtFloor))}, []uint64{7}},
		{"two at once, oldest first", []processRow{
			row(9, "bintrail", "bintrail_idx_a", 40, nstr(stmtGTID)), self,
			row(7, "bintrail", "bintrail_idx_a", 812, nstr(stmtPosition))}, []uint64{7, 9}},
		{"two equally old, lower id first", []processRow{
			row(9, "bintrail", "bintrail_idx_a", 40, nstr(stmtGTID)), self,
			row(8, "bintrail", "bintrail_idx_a", 40, nstr(stmtGTID))}, []uint64{8, 9}},
		{"another user", []processRow{self, row(7, "reporting", "bintrail_idx_a", 812, nstr(stmtFloor))}, nil},
		{"user differs only in case", []processRow{self, row(7, "Bintrail", "bintrail_idx_a", 812, nstr(stmtFloor))}, nil},
		{"another database (another source of this daemon)", []processRow{self, row(7, "bintrail", "bintrail_idx_b", 812, nstr(stmtFloor))}, nil},
		{"database differs only in case", []processRow{self, row(7, "bintrail", "BINTRAIL_IDX_A", 812, nstr(stmtFloor))}, nil},
		{"database that starts like ours", []processRow{self, row(7, "bintrail", "bintrail_idx_a2", 812, nstr(stmtFloor))}, nil},
		{"no database", []processRow{self, row(7, "bintrail", "", 812, nstr(stmtFloor))}, nil},
		{"statement text is NULL", []processRow{self, row(7, "bintrail", "bintrail_idx_a", 812, sql.NullString{})}, nil},
		{"a select", []processRow{self, row(7, "bintrail", "bintrail_idx_a", 812, nstr("SELECT DISTINCT gtid FROM binlog_events WHERE binlog_file = ?"))}, nil},
		{"a delete on another table", []processRow{self, row(7, "bintrail", "bintrail_idx_a", 812, nstr("DELETE FROM index_state WHERE id = ?"))}, nil},
		{"another delete on binlog_events", []processRow{self, row(7, "bintrail", "bintrail_idx_a", 812, nstr("DELETE FROM binlog_events WHERE event_timestamp < ?"))}, nil},
		{"this connection running the statement itself", []processRow{{ID: 10, User: nstr("bintrail"), DB: nstr("bintrail_idx_a"), Info: nstr(stmtFloor), Self: true}}, nil},
		{"ours among strangers", []processRow{
			row(3, "reporting", "bintrail_idx_a", 9000, nstr(stmtFloor)),
			row(4, "bintrail", "bintrail_idx_b", 9000, nstr(stmtFloor)),
			row(5, "bintrail", "bintrail_idx_a", 9000, nstr("DELETE FROM stream_state")),
			self,
			row(7, "bintrail", "bintrail_idx_a", 12, nstr(stmtFloor))}, []uint64{7}},
	} {
		got, err := earlierCleanups(c.rows)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		var ids []uint64
		for _, r := range got {
			ids = append(ids, r.ID)
		}
		if len(ids) != len(c.want) {
			t.Errorf("%s: found %v, want %v", c.name, ids, c.want)
			continue
		}
		for i := range ids {
			if ids[i] != c.want[i] {
				t.Errorf("%s: found %v, want %v", c.name, ids, c.want)
				break
			}
		}
	}

	// A look that cannot tell who it is cannot tell whose statement it sees.
	for name, rows := range map[string][]processRow{
		"empty list":             nil,
		"this connection unseen": {row(7, "bintrail", "bintrail_idx_a", 812, nstr(stmtFloor))},
		"no database on this connection": {{ID: 10, User: nstr("bintrail"), Self: true},
			row(7, "bintrail", "bintrail_idx_a", 812, nstr(stmtFloor))},
		"no user on this connection": {{ID: 10, DB: nstr("bintrail_idx_a"), Self: true},
			row(7, "bintrail", "bintrail_idx_a", 812, nstr(stmtFloor))},
	} {
		if got, err := earlierCleanups(rows); err == nil {
			t.Errorf("%s: no error, found %v; an unusable look must say so", name, got)
		}
	}
}

// scriptedProbe answers each look from a list; the last answer repeats.
type scriptedProbe struct {
	mu      sync.Mutex
	answers []func() ([]processRow, error)
	looks   int
}

func (p *scriptedProbe) probe(ctx context.Context) ([]processRow, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	i := min(p.looks, len(p.answers)-1)
	p.looks++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return p.answers[i]()
}

func (p *scriptedProbe) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.looks
}

type phaseLog struct {
	mu      sync.Mutex
	phases  []string
	details []string
}

func (l *phaseLog) hooks() *Hooks {
	return &Hooks{
		OnPhase:       func(p string) { l.mu.Lock(); l.phases = append(l.phases, p); l.mu.Unlock() },
		OnPhaseDetail: func(d string) { l.mu.Lock(); l.details = append(l.details, d); l.mu.Unlock() },
	}
}

func (l *phaseLog) last() (string, string, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.phases) == 0 {
		return "", "", 0
	}
	return l.phases[len(l.phases)-1], l.details[len(l.details)-1], len(l.phases)
}

func waitFixture() (self processRow, busy, idle func() ([]processRow, error)) {
	self = processRow{ID: 10, User: nstr("bintrail"), DB: nstr("idx"), Self: true}
	orphan := processRow{ID: 7, User: nstr("bintrail"), DB: nstr("idx"), Seconds: 812, Info: nstr(stmtFloor)}
	busy = func() ([]processRow, error) { return []processRow{self, orphan}, nil }
	idle = func() ([]processRow, error) { return []processRow{self}, nil }
	return
}

func TestWaitForEarlierCleanup_nothingRunningStartsAtOnce(t *testing.T) {
	_, _, idle := waitFixture()
	p := &scriptedProbe{answers: []func() ([]processRow, error){idle}}
	var log phaseLog
	started := time.Now()
	if err := waitForEarlierCleanup(context.Background(), p.probe, log.hooks(), time.Hour, time.Hour); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(started); took > time.Second {
		t.Errorf("an index with nothing running waited %s", took)
	}
	if _, _, n := log.last(); n != 0 || p.count() != 1 {
		t.Errorf("%d phase reports and %d looks; a clean start reports no phase and looks once", n, p.count())
	}
}

func TestWaitForEarlierCleanup_endsWhenTheCleanupIsGone(t *testing.T) {
	_, busy, idle := waitFixture()
	p := &scriptedProbe{answers: []func() ([]processRow, error){busy, busy, busy, idle}}
	var log phaseLog
	if err := waitForEarlierCleanup(context.Background(), p.probe, log.hooks(), time.Hour, 5*time.Millisecond); err != nil {
		t.Fatalf("the cleanup went away and the wait still failed: %v", err)
	}
	if p.count() != 4 {
		t.Errorf("%d looks, want 4 (three busy, one idle)", p.count())
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if len(log.phases) != 4 || log.phases[0] != PhaseResumeCleanupWaiting || log.phases[3] != "" {
		t.Errorf("phases = %q, want waiting three times and then cleared", log.phases)
	}
	if log.details[0] != "connection 7, running for 13m32s" || log.details[3] != "" {
		t.Errorf("details = %q", log.details)
	}
}

func TestWaitForEarlierCleanup_ceiling(t *testing.T) {
	self, _, _ := waitFixture()
	two := func() ([]processRow, error) {
		return []processRow{self,
			{ID: 9, User: nstr("bintrail"), DB: nstr("idx"), Seconds: 30, Info: nstr(stmtGTID)},
			{ID: 7, User: nstr("bintrail"), DB: nstr("idx"), Seconds: 812, Info: nstr(stmtFloor)}}, nil
	}
	p := &scriptedProbe{answers: []func() ([]processRow, error){two}}
	var log phaseLog
	started := time.Now()
	err := waitForEarlierCleanup(context.Background(), p.probe, log.hooks(), 60*time.Millisecond, 10*time.Millisecond)
	took := time.Since(started)
	if !errors.Is(err, ErrEarlierCleanupRunning) {
		t.Fatalf("err = %v, want ErrEarlierCleanupRunning", err)
	}
	if took < 60*time.Millisecond || took > 2*time.Second {
		t.Errorf("gave up after %s with a 60ms ceiling", took)
	}
	var detail *EarlierCleanupError
	if !errors.As(err, &detail) {
		t.Fatalf("err is %T, want *EarlierCleanupError", err)
	}
	if detail.ConnectionID != 7 || detail.Running != 812*time.Second || detail.Count != 2 || detail.Waited < 60*time.Millisecond {
		t.Errorf("detail = %+v", detail)
	}
	msg := err.Error()
	for _, want := range []string{"connection id 7", "and 1 more", "running for 13m32s", "KILL 7", "--cleanup-wait-timeout"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the error does not say %q: %s", want, msg)
		}
	}
	if phase, d, _ := log.last(); phase != "" || d != "" {
		t.Errorf("the phase outlived the wait: %q %q", phase, d)
	}
}

func TestWaitForEarlierCleanup_cancel(t *testing.T) {
	_, busy, _ := waitFixture()
	p := &scriptedProbe{answers: []func() ([]processRow, error){busy}}
	var log phaseLog
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- waitForEarlierCleanup(ctx, p.probe, log.hooks(), time.Hour, time.Hour) }()
	// Wait until it is inside the wait, then stop it.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if phase, _, _ := log.last(); phase == PhaseResumeCleanupWaiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the wait never reported its phase")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
		if errors.Is(err, ErrEarlierCleanupRunning) {
			t.Errorf("a stop reads as the ceiling failure: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a stop did not end the wait; poll and ceiling are both one hour")
	}
	if phase, _, _ := log.last(); phase != "" {
		t.Errorf("the phase outlived the stop: %q", phase)
	}
}

func TestWaitForEarlierCleanup_aFailedLookIsNotAFailure(t *testing.T) {
	_, busy, _ := waitFixture()
	denied := func() ([]processRow, error) {
		return nil, errors.New("Error 1142 (42000): SELECT command denied to user")
	}
	unseen := func() ([]processRow, error) { return nil, nil }

	for name, answer := range map[string]func() ([]processRow, error){"query refused": denied, "this connection not listed": unseen} {
		p := &scriptedProbe{answers: []func() ([]processRow, error){answer}}
		var log phaseLog
		if err := waitForEarlierCleanup(context.Background(), p.probe, log.hooks(), time.Hour, time.Hour); err != nil {
			t.Errorf("%s: the start failed: %v", name, err)
		}
		if _, _, n := log.last(); n != 0 || p.count() != 1 {
			t.Errorf("%s: %d phase reports, %d looks", name, n, p.count())
		}
	}

	// One failed look in the middle keeps waiting...
	p := &scriptedProbe{answers: []func() ([]processRow, error){busy, denied, busy, denied, denied, busy}}
	err := waitForEarlierCleanup(context.Background(), p.probe, nil, 200*time.Millisecond, 5*time.Millisecond)
	if !errors.Is(err, ErrEarlierCleanupRunning) {
		t.Errorf("failed looks in between ended the wait early: err = %v after %d looks", err, p.count())
	}
	// ...and cleanupProbeMaxFailures in a row end it, without an error.
	p = &scriptedProbe{answers: []func() ([]processRow, error){busy, denied}}
	var log phaseLog
	if err := waitForEarlierCleanup(context.Background(), p.probe, log.hooks(), time.Hour, 5*time.Millisecond); err != nil {
		t.Errorf("a look that keeps failing failed the start: %v", err)
	}
	if want := 1 + cleanupProbeMaxFailures; p.count() != want {
		t.Errorf("%d looks, want %d", p.count(), want)
	}
	if phase, _, _ := log.last(); phase != "" {
		t.Errorf("the phase outlived the wait: %q", phase)
	}
}

func TestWaitForEarlierCleanup_zeroCeilingDoesNotLook(t *testing.T) {
	_, busy, _ := waitFixture()
	p := &scriptedProbe{answers: []func() ([]processRow, error){busy}}
	if err := waitForEarlierCleanup(context.Background(), p.probe, nil, 0, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if p.count() != 0 {
		t.Errorf("%d looks with the look turned off", p.count())
	}
}

func TestProcessListProbe(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM information_schema.PROCESSLIST").WillReturnRows(
		sqlmock.NewRows([]string{"ID", "USER", "DB", "TIME", "INFO", "self"}).
			AddRow(10, "bintrail", "idx", 0, "SELECT ...", true).
			AddRow(7, "bintrail", "idx", 812, stmtFloor, false).
			AddRow(8, "bintrail", nil, nil, nil, false))
	rows, err := processListProbe(db)(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || !rows[0].Self || rows[1].Seconds != 812 || rows[2].DB.Valid || rows[2].Info.Valid || rows[2].Seconds != 0 {
		t.Fatalf("rows = %+v", rows)
	}
	found, err := earlierCleanups(rows)
	if err != nil || len(found) != 1 || found[0].ID != 7 {
		t.Errorf("found %+v, err %v", found, err)
	}

	mock.ExpectQuery("FROM information_schema.PROCESSLIST").WillReturnError(errors.New("Error 1109 (42S02): Unknown table 'PROCESSLIST' in information_schema"))
	if err := waitForEarlierCleanup(context.Background(), processListProbe(db), nil, time.Hour, time.Hour); err != nil {
		t.Errorf("a server without the table failed the start: %v", err)
	}
}

func TestOneRejectsANegativeCleanupWait(t *testing.T) {
	old := ResumeCleanupWait
	defer func() { ResumeCleanupWait = old }()
	ResumeCleanupWait = -time.Second
	err := One(context.Background(), Config{Format: "text", GapTimeout: 30})
	if err == nil || !strings.Contains(err.Error(), "--cleanup-wait-timeout") {
		t.Errorf("err = %v", err)
	}
}

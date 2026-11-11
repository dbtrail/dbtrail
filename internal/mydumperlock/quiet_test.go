package mydumperlock

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

var plCols = []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}

type plRow struct {
	id      int64
	user    string
	command string
	seconds int64
	info    any
}

func processlist(rows ...plRow) *sqlmock.Rows {
	r := sqlmock.NewRows(plCols)
	for _, x := range rows {
		r = r.AddRow(x.id, x.user, "h:1", nil, x.command, x.seconds, "", x.info)
	}
	return r
}

// expectPoll stages one poll: the pinned connection's id and version are
// read once (expectStart), then each poll is one SHOW FULL PROCESSLIST.
func expectStart(mock sqlmock.Sqlmock, ownID int64, version string) {
	mock.ExpectQuery("SELECT CONNECTION_ID\\(\\), VERSION\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"id", "v"}).AddRow(ownID, version))
	mock.ExpectExec("SET SESSION wait_timeout = [0-9]+").WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectPoll(mock sqlmock.Sqlmock, rows ...plRow) {
	mock.ExpectQuery("SHOW FULL PROCESSLIST").WillReturnRows(processlist(rows...))
}

// fakeWait counts the sleeps a wait takes instead of sleeping.
func fakeWait(t *testing.T, interval, total time.Duration) *quietWait {
	t.Helper()
	w := &quietWait{interval: interval, total: total}
	w.sleep = func(ctx context.Context, d time.Duration) error {
		w.slept += d
		return ctx.Err()
	}
	return w
}

func TestWaitForQuietSource(t *testing.T) {
	ignored := []plRow{
		{1, "event_scheduler", "Daemon", 9000, nil},
		{2, "system user", "Query", 500, "UPDATE t SET a=1"}, // a replica's applier
		{3, "app", "Sleep", 3000, nil},
		{4, "dbtrail", "Binlog Dump GTID", 86400, nil},       // DBTrail's own capture
		{5, "app", "Query", 0, "SELECT 1"},                   // under a second
		{42, "dbtrail", "Query", 7, "SHOW FULL PROCESSLIST"}, // this check's own connection
	}
	cases := []struct {
		name    string
		version string
		polls   [][]plRow
		wantErr bool
		slept   time.Duration
	}{
		{name: "quiet at once", version: "8.0.46", polls: [][]plRow{ignored}},
		{name: "a query of 1 s waits, then quiet", version: "8.0.46",
			polls: [][]plRow{append(ignored, plRow{7, "app", "Query", 1, "SELECT SLEEP(30) FROM t"}), ignored}, slept: 10 * time.Second},
		// The Go driver, mysqli and Connector/J with server-side prepare run
		// statements as COM_STMT_EXECUTE: Command "Execute".
		{name: "a prepared statement waits", version: "8.0.46",
			polls: [][]plRow{append(ignored, plRow{8, "app", "Execute", 40, "SELECT * FROM t WHERE d > ?"}), ignored}, slept: 10 * time.Second},
		{name: "busy for the whole wait", version: "8.0.46",
			polls: [][]plRow{{{9, "app", "Query", 5, "x"}}, {{9, "app", "Query", 15, "x"}}, {{9, "app", "Query", 25, "x"}}}, wantErr: true, slept: 20 * time.Second},
		// MariaDB's FTWRL does not wait for a running SELECT, only for DDL
		// (docs/snapshot-lock-measurements.md): only DDL holds it up.
		{name: "MariaDB ignores a long SELECT", version: "10.11.6-MariaDB-log",
			polls: [][]plRow{{{10, "app", "Query", 90, "SELECT SLEEP(90)"}}}},
		{name: "MariaDB waits for a long UPDATE", version: "10.11.6-MariaDB",
			polls: [][]plRow{{{13, "app", "Query", 30, "UPDATE t SET a = a + 1"}}, nil}, slept: 10 * time.Second},
		// A statement killed but still rolling back keeps its tables open.
		{name: "a killed statement still rolling back waits", version: "8.0.46",
			polls: [][]plRow{{{14, "app", "Killed", 120, "DELETE FROM t"}}, nil}, slept: 10 * time.Second},
		{name: "MariaDB waits for an ALTER", version: "10.11.6-MariaDB",
			polls: [][]plRow{{{11, "app", "Query", 30, "  /* online */ alter table t add index (a)"}}, nil}, slept: 10 * time.Second},
		// Without PROCESS the text of other accounts' statements is not
		// shown: a statement with no text cannot be told apart from DDL, so it
		// is waited for.
		{name: "MariaDB waits for a statement it cannot read", version: "10.11.6-MariaDB",
			polls: [][]plRow{{{12, "app", "Query", 30, nil}}, nil}, slept: 10 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			expectStart(mock, 42, tc.version)
			for _, p := range tc.polls {
				expectPoll(mock, p...)
			}
			w := fakeWait(t, 10*time.Second, 20*time.Second)
			err = w.run(context.Background(), db, baseline.LockModeFTWRL, RemedyConsole)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v", err, tc.wantErr)
			}
			if w.slept != tc.slept {
				t.Errorf("slept %v, want %v", w.slept, tc.slept)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestWaitForQuietSource_refusal(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectStart(mock, 42, "8.0.46")
	busy := plRow{77, "reporting", "Execute", 300, "SELECT secret_value FROM payroll WHERE ssn = ?"}
	for range 3 {
		expectPoll(mock, busy)
	}
	w := fakeWait(t, 10*time.Second, 20*time.Second)
	err := w.run(context.Background(), db, baseline.LockModeFTWRL, RemedyConsole)
	var qe *QuietWaitError
	if !errors.As(err, &qe) {
		t.Fatalf("err = %v (%T), want a *QuietWaitError", err, err)
	}
	for _, msg := range []string{err.Error(), qe.Summary()} {
		if strings.Contains(msg, "payroll") || strings.Contains(msg, "secret") {
			t.Errorf("the statement's text reached the message: %s", msg)
		}
	}
	for _, want := range []string{"77", "reporting", "300 s", "lock-all"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q: %s", want, err)
		}
	}
	// The console's card shows 300 characters: the remedy must be inside them.
	if s := qe.Summary(); len([]rune(s)) > 300 || !strings.Contains(s, "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=lock-all") {
		t.Errorf("summary (%d chars) does not fit with its remedy: %s", len([]rune(s)), s)
	}
	t.Logf("error: %v\nsummary: %s", err, qe.Summary())
}

func TestWaitForQuietSource_onlyFTWRL(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	for _, m := range []baseline.LockMode{baseline.LockModeLockAll, baseline.LockModeSafeNoLock, baseline.LockModeNoLock} {
		if err := fakeWait(t, time.Second, time.Second).run(context.Background(), db, m, RemedyCLI); err != nil {
			t.Errorf("%s: %v", m, err)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a mode that takes no global lock asked the source: %v", err)
	}
}

// A daemon shutting down mid-wait returns at once, saying so.
func TestWaitForQuietSource_cancelled(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectStart(mock, 42, "8.0.46")
	expectPoll(mock, plRow{9, "app", "Query", 5, "x"})
	ctx, cancel := context.WithCancel(context.Background())
	w := fakeWait(t, 10*time.Second, time.Hour)
	w.sleep = func(context.Context, time.Duration) error { cancel(); return context.Canceled }
	if err := w.run(ctx, db, baseline.LockModeFTWRL, RemedyCLI); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// A process list that cannot be read lets the read go ahead, loudly: the
// account opening mydumper's connections is the same, so a failure here is a
// connection failure mydumper reports itself, and refusing every full read on
// it would leave the copy with no base at all.
func TestWaitForQuietSource_unreadable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stage func(sqlmock.Sqlmock)
	}{
		{"start query fails", func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SELECT CONNECTION_ID").WillReturnError(errors.New("boom"))
		}},
		{"process list fails", func(m sqlmock.Sqlmock) {
			expectStart(m, 42, "8.0.46")
			m.ExpectQuery("SHOW FULL PROCESSLIST").WillReturnError(errors.New("boom"))
		}},
		{"process list lacks a column", func(m sqlmock.Sqlmock) {
			expectStart(m, 42, "8.0.46")
			m.ExpectQuery("SHOW FULL PROCESSLIST").WillReturnRows(sqlmock.NewRows([]string{"Id", "User"}).AddRow(1, "x"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, _ := sqlmock.New()
			defer db.Close()
			tc.stage(mock)
			prev := slog.Default()
			t.Cleanup(func() { slog.SetDefault(prev) })
			var buf bytes.Buffer
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
			if err := fakeWait(t, time.Second, time.Second).run(context.Background(), db, baseline.LockModeFTWRL, RemedyCLI); err != nil {
				t.Fatalf("err = %v, want the read to go ahead", err)
			}
			if !strings.Contains(buf.String(), "could not check") {
				t.Errorf("went ahead silently; log: %s", buf.String())
			}
		})
	}
}

func TestFirstWordAndReadOnly(t *testing.T) {
	for text, want := range map[string]string{
		"ALTER TABLE t ADD c INT":                    "ALTER",
		"alter\ttable t engine=innodb":               "ALTER",
		"ALTER\r\nTABLE t ADD INDEX (a)":             "ALTER",
		"ALTER/*x*/TABLE t":                          "ALTER",
		"  /* online */ OPTIMIZE TABLE t":            "OPTIMIZE",
		"-- note\nALTER TABLE t FORCE":               "ALTER",
		"# note\nalter table t force":                "ALTER",
		"/* a */ /* b */\n\tCREATE INDEX i ON t (a)": "CREATE",
		"select(1)":                     "SELECT",
		"/* never closed ALTER TABLE t": "",
		"-- only a comment":             "",
		"":                              "",
	} {
		if got := firstWord(text); got != want {
			t.Errorf("firstWord(%q) = %q, want %q", text, got, want)
		}
	}
	for text, want := range map[string]bool{
		"SELECT SLEEP(90)":                     true,
		"/* report */ select * from t":         true,
		"SHOW FULL PROCESSLIST":                true,
		"UPDATE t SET a = 1":                   false,
		"INSERT INTO t SELECT * FROM u":        false,
		"alter\ntable t force":                 false,
		"WITH x AS (SELECT 1) SELECT * FROM x": false, // not assumed read-only
		"":                                     false,
	} {
		if got := isReadOnly(text); got != want {
			t.Errorf("isReadOnly(%q) = %v, want %v", text, got, want)
		}
	}
}

// A row the wait cannot read is no evidence of a quiet source: it counts.
func TestWaitForQuietSource_anUnreadableRowCounts(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectStart(mock, 42, "8.0.46")
	mock.ExpectQuery("SHOW FULL PROCESSLIST").WillReturnRows(sqlmock.NewRows(plCols).
		AddRow("18446744073709551615", "app", "h", nil, "Query", "18446744073709551615", "", "x"))
	expectPoll(mock)
	w := fakeWait(t, 10*time.Second, time.Minute)
	if err := w.run(context.Background(), db, baseline.LockModeFTWRL, RemedyCLI); err != nil {
		t.Fatal(err)
	}
	if w.slept != 10*time.Second {
		t.Fatalf("slept %v, want one wait for the unreadable row", w.slept)
	}
}

// A shutdown that lands during a look, not during the sleep, is a shutdown:
// not "could not check, going ahead".
func TestWaitForQuietSource_cancelledDuringALook(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	mock.ExpectQuery("SELECT CONNECTION_ID").WillReturnError(context.Canceled)
	cancel()
	err := fakeWait(t, time.Second, time.Minute).run(ctx, db, baseline.LockModeFTWRL, RemedyCLI)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// The compose `baseline` profile's refusal names that profile's variable: its
// operator never typed a --lock-mode flag, and the console's variable does
// nothing there.
func TestQuietWaitError_composeNamesTheProfileVariable(t *testing.T) {
	e := &QuietWaitError{Waited: QuietWaitLimit, Busy: []Busy{{ID: 42, User: "app", Command: "Execute", Seconds: 611}}, remedy: RemedyCompose}
	for _, msg := range []string{e.Error(), e.Summary()} {
		t.Logf("as the operator reads it: %s", msg)
		if !strings.Contains(msg, "set BASELINE_LOCK_MODE=lock-all") {
			t.Errorf("does not name the profile's variable: %s", msg)
		}
		for _, wrong := range []string{"--lock-mode", "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE"} {
			if strings.Contains(msg, wrong) {
				t.Errorf("names %s, which the compose profile does not have: %s", wrong, msg)
			}
		}
	}
}

// The standalone PROCESS warning (the compose profile's step, which does not
// run CheckPrivileges): said without PROCESS, silent with it or with ALL
// PRIVILEGES, and grants that cannot be read are said rather than taken as
// "sees everything".
func TestWarnQuietWaitBlind(t *testing.T) {
	for _, tc := range []struct {
		name   string
		grants []string
		fail   bool
		want   string // "" = no warning
	}{
		{"without PROCESS", []string{"GRANT RELOAD, BACKUP_ADMIN, SELECT ON *.* TO `dbtrail`@`%`"}, false, "lacks the PROCESS privilege"},
		{"with PROCESS", []string{"GRANT RELOAD, PROCESS, SELECT ON *.* TO `dbtrail`@`%`"}, false, ""},
		{"ALL PRIVILEGES", []string{"GRANT ALL PRIVILEGES ON *.* TO `root`@`%`"}, false, ""},
		{"PROCESS on one schema is not PROCESS", []string{"GRANT SELECT ON *.* TO `dbtrail`@`%`", "GRANT PROCESS ON `app`.* TO `dbtrail`@`%`"}, false, "lacks the PROCESS privilege"},
		{"grants unreadable", nil, true, "could not read the source account's privileges"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if tc.fail {
				mock.ExpectQuery("SHOW GRANTS").WillReturnError(errors.New("connection reset"))
			} else {
				mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(tc.grants...))
			}
			prev := slog.Default()
			t.Cleanup(func() { slog.SetDefault(prev) })
			var buf bytes.Buffer
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
			WarnQuietWaitBlind(context.Background(), db)
			got := buf.String()
			if tc.want == "" && got != "" {
				t.Errorf("warned with nothing to warn about: %s", got)
			}
			if tc.want != "" && !strings.Contains(got, tc.want) {
				t.Errorf("log = %q, want it to contain %q", got, tc.want)
			}
		})
	}
}

// The compose step end to end on one connection: an account without PROCESS
// and a source that never goes quiet. Both halves must happen: the warning
// (nothing else in that profile says the wait is blind) and a refusal that
// names the profile's variable.
func TestWaitForQuietSourceCompose_warnsThenRefusesWithTheProfileVariable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows("GRANT RELOAD, BACKUP_ADMIN, SELECT ON *.* TO `dbtrail`@`%`"))
	expectStart(mock, 7, "8.0.46")
	for range 3 {
		expectPoll(mock, plRow{9, "app", "Query", 300, "SELECT 1"})
	}
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	err = waitCompose(context.Background(), db, baseline.LockModeFTWRL, fakeWait(t, 10*time.Second, 20*time.Second))
	var qe *QuietWaitError
	if !errors.As(err, &qe) {
		t.Fatalf("err = %v, want the refusal", err)
	}
	if !strings.Contains(err.Error(), "set BASELINE_LOCK_MODE=lock-all") || strings.Contains(err.Error(), "--lock-mode") {
		t.Errorf("the refusal does not name the profile's variable: %v", err)
	}
	if !strings.Contains(buf.String(), "lacks the PROCESS privilege") {
		t.Errorf("an account without PROCESS waited with no warning: %q", buf.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A mode that does not wait asks the source nothing, grants included.
func TestWaitForQuietSourceCompose_otherModesAskNothing(t *testing.T) {
	for _, m := range []baseline.LockMode{baseline.LockModeLockAll, baseline.LockModeSafeNoLock, baseline.LockModeNoLock} {
		db, _, _ := sqlmock.New() // any query is an unexpected one and fails
		if err := waitCompose(context.Background(), db, m, fakeWait(t, time.Second, time.Second)); err != nil {
			t.Errorf("%s: %v", m, err)
		}
		db.Close()
	}
}

// A shutdown while the grants are being read is not "could not read the
// privileges, going ahead": nothing goes ahead.
func TestWarnQuietWaitBlind_silentOnShutdown(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery("SHOW GRANTS").WillReturnError(context.Canceled)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	WarnQuietWaitBlind(ctx, db)
	if buf.Len() != 0 {
		t.Errorf("warned during a shutdown: %s", buf.String())
	}
}

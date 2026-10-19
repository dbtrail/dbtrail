package indexer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	mysql "github.com/go-sql-driver/mysql"
)

// The event-type index (typeindex.go) without a server: how an existing
// index's keys are read, which failures are tried again, and that a process
// starts one background build per index. Builds on a real server are in
// typeindex_integration_test.go.

func statsRows(pairs ...string) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME"})
	for i := 0; i+1 < len(pairs); i += 2 {
		r.AddRow(pairs[i], pairs[i+1])
	}
	return r
}

// keyRows lists one index's key, column by column.
func keyRows(name string, cols ...string) []string {
	var out []string
	for _, c := range cols {
		out = append(out, name, c)
	}
	return out
}

func TestTypeIndexStatus(t *testing.T) {
	base := append(keyRows("PRIMARY", "event_id", "event_timestamp"),
		keyRows("idx_row_lookup", "schema_name", "table_name", "event_timestamp")...)
	with := func(extra ...string) []string { return append(append([]string{}, base...), extra...) }
	for name, tc := range map[string]struct {
		rows  []string
		state TypeIndexState
		named string
	}{
		"no table":                    {nil, TypeIndexNoTable, ""},
		"missing":                     {base, TypeIndexMissing, ""},
		"present":                     {with(keyRows("idx_type_lookup", "event_type", "schema_name", "table_name", "event_timestamp")...), TypeIndexPresent, ""},
		"present, name in upper case": {with(keyRows("IDX_TYPE_LOOKUP", "EVENT_TYPE", "SCHEMA_NAME", "TABLE_NAME", "EVENT_TIMESTAMP")...), TypeIndexPresent, ""},
		"present with a longer key":   {with(keyRows("idx_type_lookup", "event_type", "schema_name", "table_name", "event_timestamp", "event_id")...), TypeIndexPresent, ""},
		"same key under another name": {with(keyRows("my_type_idx", "event_type", "schema_name", "table_name", "event_timestamp")...), TypeIndexOtherName, "my_type_idx"},
		"our name, another key":       {with(keyRows("idx_type_lookup", "event_type")...), TypeIndexWrongShape, "event_type"},
		"our name, columns reordered": {with(keyRows("idx_type_lookup", "schema_name", "table_name", "event_type", "event_timestamp")...), TypeIndexWrongShape, "schema_name, table_name, event_type, event_timestamp"},
		"a prefix is not the key":     {with(keyRows("idx_t", "event_type", "schema_name", "table_name")...), TypeIndexMissing, ""},
	} {
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("information_schema.STATISTICS").WillReturnRows(statsRows(tc.rows...))
			state, named, err := TypeIndexStatus(context.Background(), db)
			if err != nil {
				t.Fatal(err)
			}
			if state != tc.state || named != tc.named {
				t.Fatalf("got (%v, %q), want (%v, %q)", state, named, tc.state, tc.named)
			}
		})
	}
}

// An index that already serves the filter, or one the build must not touch,
// never reaches the ALTER: sqlmock fails any statement not expected.
func TestEnsureTypeIndexBuildsNothingWhenNotDue(t *testing.T) {
	for name, rows := range map[string][]string{
		"present":    keyRows("idx_type_lookup", "event_type", "schema_name", "table_name", "event_timestamp"),
		"other name": keyRows("mine", "event_type", "schema_name", "table_name", "event_timestamp"),
		"wrong key":  keyRows("idx_type_lookup", "event_type"),
		"no table":   nil,
	} {
		t.Run(name, func(t *testing.T) {
			db, mock, _ := sqlmock.New()
			defer db.Close()
			mock.ExpectQuery("information_schema.STATISTICS").WillReturnRows(statsRows(rows...))
			if err := EnsureTypeIndex(context.Background(), db, nil); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The server already running the build (a previous daemon's, which the
// server finishes after the client is gone): no second one.
func TestEnsureTypeIndexDoesNotStartASecondBuild(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery("information_schema.STATISTICS").WillReturnRows(statsRows(keyRows("PRIMARY", "event_id")...))
	mock.ExpectQuery("information_schema.PROCESSLIST").WithArgs("%ADD INDEX idx_type_lookup%").
		WillReturnRows(sqlmock.NewRows([]string{"ID", "TIME"}).AddRow(42, 600))
	if err := EnsureTypeIndex(context.Background(), db, nil); !errors.Is(err, ErrTypeIndexBuildRunning) {
		t.Fatalf("got %v, want ErrTypeIndexBuildRunning", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// The build itself: the lock-wait bound is set on the connection the ALTER
// runs on, the ALTER is the online one, and a duplicate-name answer (built
// meanwhile) is success.
func expectBuild(mock sqlmock.Sqlmock) *sqlmock.ExpectedExec {
	mock.ExpectQuery("information_schema.STATISTICS").WillReturnRows(statsRows(keyRows("PRIMARY", "event_id")...))
	mock.ExpectQuery("information_schema.PROCESSLIST").WillReturnRows(sqlmock.NewRows([]string{"ID", "TIME"}))
	mock.ExpectExec("SET SESSION lock_wait_timeout = 5").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("TABLE_ROWS").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1000))
	alter := mock.ExpectExec(strings.ReplaceAll(strings.ReplaceAll(typeIndexDDL, "(", `\(`), ")", `\)`))
	// The connection goes back to the caller's pool without the short bound.
	mock.ExpectExec("SET SESSION lock_wait_timeout = DEFAULT").WillReturnResult(sqlmock.NewResult(0, 0))
	return alter
}

func TestEnsureTypeIndexRunsTheOnlineBuild(t *testing.T) {
	if !strings.HasSuffix(typeIndexDDL, "ALGORITHM=INPLACE, LOCK=NONE") {
		t.Fatalf("the build must refuse rather than lock: %s", typeIndexDDL)
	}
	for name, res := range map[string]error{"built": nil, "built meanwhile": &mysql.MySQLError{Number: 1061}} {
		t.Run(name, func(t *testing.T) {
			db, mock, _ := sqlmock.New()
			defer db.Close()
			e := expectBuild(mock)
			if res == nil {
				e.WillReturnResult(sqlmock.NewResult(0, 0))
			} else {
				e.WillReturnError(res)
			}
			if err := EnsureTypeIndex(context.Background(), db, nil); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestClassifyTypeIndexBuildErr(t *testing.T) {
	for num, want := range map[uint16]struct {
		retry bool
		hint  string
	}{
		1205: {true, "a long read held binlog_events"},
		2013: {true, "connection to the index server was lost"},
		1799: {false, "innodb_online_alter_log_max_size"},
		1114: {false, "ran out of space"},
		1142: {false, "privileges"},
		1846: {false, "cannot build the index online"},
		1317: {false, ""},
	} {
		err := classifyTypeIndexBuildErr(&mysql.MySQLError{Number: num, Message: "x"})
		var be *TypeIndexBuildError
		if !errors.As(err, &be) {
			t.Fatalf("%d: %T", num, err)
		}
		if be.Retry != want.retry || !strings.Contains(err.Error(), want.hint) {
			t.Errorf("%d: retry=%v %q, want retry=%v and %q", num, be.Retry, err, want.retry, want.hint)
		}
		var me *mysql.MySQLError
		if !errors.As(err, &me) || me.Number != num {
			t.Errorf("%d: the server error must stay reachable: %v", num, err)
		}
	}
	for _, lost := range []error{mysql.ErrInvalidConn, driver.ErrBadConn} {
		var be *TypeIndexBuildError
		if err := classifyTypeIndexBuildErr(lost); !errors.As(err, &be) || !be.Retry || !errors.Is(err, lost) {
			t.Errorf("%v: %v, want a retriable error wrapping it", lost, err)
		}
	}
}

// A build another thread runs (a previous daemon's, left running on the
// server) is waited for, and the index is checked again when it ends: if that
// build failed, nobody else is left to say so or to build it.
func TestRunTypeIndexBuildWaitsForARunningBuildThenLooksAgain(t *testing.T) {
	saved := typeIndexRunningPoll
	typeIndexRunningPoll = 0
	t.Cleanup(func() { typeIndexRunningPoll = saved })
	db, mock, _ := sqlmock.New()
	mock.ExpectQuery("information_schema.STATISTICS").WillReturnRows(statsRows(keyRows("PRIMARY", "event_id")...))
	mock.ExpectQuery("information_schema.PROCESSLIST").WillReturnRows(sqlmock.NewRows([]string{"ID", "TIME"}).AddRow(7, 30))
	mock.ExpectQuery("information_schema.PROCESSLIST").WillReturnRows(sqlmock.NewRows([]string{"ID", "TIME"}).AddRow(7, 90))
	mock.ExpectQuery("information_schema.PROCESSLIST").WillReturnRows(sqlmock.NewRows([]string{"ID", "TIME"}))
	expectBuild(mock).WillReturnResult(sqlmock.NewResult(0, 0)) // the other build failed: this one builds
	mock.ExpectClose()
	if err := runTypeIndexBuild(context.Background(), func() (*sql.DB, error) { return db, nil }, discardLogger()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A lock-wait failure is tried again, up to the cap; anything else is not.
func TestRunTypeIndexBuildRetriesOnlyLockWaits(t *testing.T) {
	saved := typeIndexRetryWaits
	typeIndexRetryWaits = []time.Duration{0, 0}
	t.Cleanup(func() { typeIndexRetryWaits = saved })
	for name, tc := range map[string]struct {
		errs     []uint16 // one ALTER answer per attempt
		attempts int
	}{
		"lock wait, then built":       {[]uint16{1205, 0}, 2},
		"lock wait every time":        {[]uint16{1205, 1205, 1205}, 3},
		"connection lost, then built": {[]uint16{2013, 0}, 2},
		"online log full, once":       {[]uint16{1799}, 1},
		"out of space, once":          {[]uint16{1114}, 1},
	} {
		t.Run(name, func(t *testing.T) {
			db, mock, _ := sqlmock.New()
			for _, n := range tc.errs {
				e := expectBuild(mock)
				if n == 0 {
					e.WillReturnResult(sqlmock.NewResult(0, 0))
				} else {
					e.WillReturnError(&mysql.MySQLError{Number: n})
				}
			}
			mock.ExpectClose()
			opened := 0
			err := runTypeIndexBuild(context.Background(), func() (*sql.DB, error) { opened++; return db, nil }, discardLogger())
			if tc.errs[len(tc.errs)-1] == 0 && err != nil {
				t.Fatal(err)
			}
			if last := tc.errs[len(tc.errs)-1]; last != 0 {
				// The build's own failure, not a later probe the mock did
				// not expect: a retried 1799 would end on that instead.
				var me *mysql.MySQLError
				if !errors.As(err, &me) || me.Number != last {
					t.Fatalf("got %v, want the build's %d returned", err, last)
				}
			}
			if opened != 1 {
				t.Fatalf("opened %d pools, want 1", opened)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("want %d attempts: %v", tc.attempts, err)
			}
		})
	}
}

// One background build per index database per process: a supervised stream
// that restarts calls StartTypeIndexBuild every time.
func TestStartTypeIndexBuildOncePerIndex(t *testing.T) {
	var opens atomic.Int32
	done := make(chan error, 4)
	release := make(chan struct{})
	// Held open until every call is made: the key is what refuses the
	// repeats, not a build that already ended.
	open := func() (*sql.DB, error) { opens.Add(1); <-release; return nil, errors.New("no server here") }
	key := "user@tcp(h:3306)/once_" + t.Name()
	for range 3 {
		StartTypeIndexBuild(key, open, discardLogger(), func(err error) { done <- err })
	}
	StartTypeIndexBuild(key+"_other", open, discardLogger(), func(err error) { done <- err })
	close(release)
	for range 2 {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the build goroutine never ended")
		}
	}
	select {
	case <-done:
		t.Fatal("a second build started for the same index")
	case <-time.After(100 * time.Millisecond):
	}
	if n := opens.Load(); n != 2 {
		t.Fatalf("opened %d times, want 2 (one per index)", n)
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// A failure before any build (no connection) frees the key, so the stream's
// next start tries again; a build that failed keeps it, so a restarting
// stream never repeats a build that fails at its end.
func TestStartTypeIndexBuildFreesTheKeyOnlyWhenNothingWasBuilt(t *testing.T) {
	key := "user@tcp(h:3306)/free_" + t.Name()
	done := make(chan error, 1)
	StartTypeIndexBuild(key, func() (*sql.DB, error) { return nil, errors.New("no server here") }, discardLogger(), func(err error) { done <- err })
	<-done
	if _, held := typeIndexBuilds.Load(key); held {
		t.Fatal("a failed connection kept the key: the next stream start would never try")
	}

	db, mock, _ := sqlmock.New()
	expectBuild(mock).WillReturnError(&mysql.MySQLError{Number: 1799})
	mock.ExpectClose()
	key2 := key + "_built"
	StartTypeIndexBuild(key2, func() (*sql.DB, error) { return db, nil }, discardLogger(), func(err error) { done <- err })
	<-done
	if _, held := typeIndexBuilds.Load(key2); !held {
		t.Fatal("a build that failed freed the key: a restarting stream would build again")
	}
}

// A failure before the ALTER (here the index check) is logged with what to
// run, not only returned: in the background nobody reads the return value.
func TestRunTypeIndexBuildLogsAFailureBeforeTheBuild(t *testing.T) {
	db, mock, _ := sqlmock.New()
	mock.ExpectQuery("information_schema.STATISTICS").WillReturnError(errors.New("connection reset"))
	mock.ExpectClose()
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	if err := runTypeIndexBuild(context.Background(), func() (*sql.DB, error) { return db, nil }, logger); err == nil {
		t.Fatal("want the check's error")
	}
	if out := buf.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "connection reset") || !strings.Contains(out, "bintrail init") {
		t.Fatalf("want a WARN with the error and `bintrail init`, got: %s", out)
	}
}

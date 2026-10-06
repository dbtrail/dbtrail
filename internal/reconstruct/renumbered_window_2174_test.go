package reconstruct

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// #2174: the shim runs the renumbering check on every statement. A lasting
// condition is logged once per process and line, not once per statement.
func TestNoticeOnce_logsEachLineOnce_2174(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	var once NoticeOnce
	// Two handlers (two connections) share the process-wide set.
	a, b := once.To(logger), once.To(logger)
	for range 3 {
		a(slog.LevelWarn, "backfilled", "mark", "m1")
		b(slog.LevelWarn, "backfilled", "mark", "m1")
	}
	a(slog.LevelWarn, "backfilled", "mark", "m2")
	if got := strings.Count(buf.String(), "msg=backfilled"); got != 2 {
		t.Fatalf("logged %d lines, want 2 (one per mark):\n%s", got, buf.String())
	}
}

func TestReadWindow_boundedOnlyWithTableAndUntil_2174(t *testing.T) {
	at := time.Date(2026, 10, 5, 12, 1, 0, 0, time.UTC)
	for _, tc := range []struct {
		w    *ReadWindow
		want bool
	}{
		{nil, false},
		{&ReadWindow{}, false},
		{&ReadWindow{Until: at}, false},
		{&ReadWindow{Schema: "s", Table: "t"}, false},
		{&ReadWindow{Schema: "s", Table: "t", Until: at}, true},
	} {
		if got := tc.w.bounded(); got != tc.want {
			t.Errorf("%+v: bounded = %v, want %v", tc.w, got, tc.want)
		}
	}
}

// A bounded check asks once about each change: a later statement asks only
// about changes indexed since, and, when it reads further in time, about the
// stretch past the last one's end. A change found there refuses.
func TestReadWindowCheck_asksOnlyWhatItHasNotSeen_2174(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m := &EventMark{ID: 10, File: "binlog.000007", End: 200}
	u1 := time.Date(2026, 10, 5, 12, 1, 0, 0, time.UTC)
	u2 := u1.Add(time.Hour)
	maxID := func(id int) {
		mock.ExpectQuery(`SELECT MAX\(event_id\)`).WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(id))
	}
	below := func(from, to int, at time.Time, rows *sqlmock.Rows, extra ...driver.Value) {
		args := append([]driver.Value{"s", "t", uint64(from), uint64(to), sqlmock.AnyArg()}, extra...)
		args = append(args, "binlog.000007", "binlog.000007", "binlog.000007", "binlog.000007", uint64(200), "binlog", "binlog")
		mock.ExpectQuery(`FORCE INDEX \(idx_row_lookup\)`).WithArgs(args...).WillReturnRows(rows)
	}
	none := func() *sqlmock.Rows { return sqlmock.NewRows([]string{"id", "f", "e"}) }

	w := &ReadWindow{Schema: "s", Table: "t", Until: u1}
	maxID(100)
	below(10, 100, u1, none())
	if err := w.check(context.Background(), db, m, time.Time{}); err != nil {
		t.Fatalf("first: %v", err)
	}
	maxID(120)
	below(100, 120, u1, none()) // only what was indexed since
	if err := w.check(context.Background(), db, m, time.Time{}); err != nil {
		t.Fatalf("second: %v", err)
	}
	w.Until = u2
	maxID(120)
	below(120, 120, u2, none())
	below(10, 120, u2, sqlmock.NewRows([]string{"id", "f", "e"}).AddRow(50, "binlog.000001", 300), sqlmock.AnyArg()) // the hour past u1
	if err := w.check(context.Background(), db, m, time.Time{}); !errors.Is(err, ErrBinlogRenumbered) {
		t.Fatalf("third: %v; want ErrBinlogRenumbered", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestParseUnixSeconds_2174(t *testing.T) {
	for _, s := range []string{"1759658400", "1759658400.000000"} {
		got, err := parseUnixSeconds(s)
		if err != nil || !got.Equal(time.Unix(1759658400, 0)) {
			t.Errorf("%q = %v, %v", s, got, err)
		}
	}
	if _, err := parseUnixSeconds("x"); err == nil {
		t.Error(`"x" parsed`)
	}
}

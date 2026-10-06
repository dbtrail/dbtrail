package reconstruct

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2186: the window's archived hours. archive_state is read for the hours
// the window reaches; a file whose newest event_id is at or below the mark's
// is skipped without a read; every other file (and one with no record) is
// asked the question; a file found clean as a whole is not read again; a
// file that cannot be opened is a "cannot check" note, after every other
// file was asked.

type probeCall struct {
	file     string
	windowed bool
}

// stubProbe replaces archiveProbe for one test: found maps a file to what a
// whole-file read finds; windowFound to what the windowed re-read finds.
func stubProbe(t *testing.T, found map[string]parquetquery.BelowMarkSpan, windowFound map[string]bool, fail map[string]error) *[]probeCall {
	t.Helper()
	var calls []probeCall
	prev := archiveProbe
	archiveProbe = func(_ context.Context, file string, q parquetquery.BelowMark) (parquetquery.BelowMarkSpan, error) {
		windowed := !q.Until.IsZero()
		calls = append(calls, probeCall{file, windowed})
		if err := fail[file]; err != nil {
			return parquetquery.BelowMarkSpan{}, err
		}
		if windowed {
			if windowFound[file] {
				return spanAt(q.From.Add(time.Minute), q.From.Add(time.Minute)), nil
			}
			return parquetquery.BelowMarkSpan{}, nil
		}
		return found[file], nil
	}
	t.Cleanup(func() { archiveProbe = prev })
	archiveSpans.reset()
	archiveMarks.reset()
	return &calls
}

// spanAt is a whole-file finding whose earliest change below the mark is at
// first and latest at last.
func spanAt(first, last time.Time) parquetquery.BelowMarkSpan {
	return parquetquery.BelowMarkSpan{Found: true,
		Earliest: parquetquery.BelowMarkRow{EventID: 99, File: "binlog.000001", End: 300, At: first},
		Latest:   parquetquery.BelowMarkRow{EventID: 98, File: "binlog.000001", End: 250, At: last}}
}

// archiveUntil is the end of every window these tests read; the hours after
// it are left out on the server (p_2026100111 is its hour).
var archiveUntil = time.Date(2026, 10, 1, 11, 30, 0, 0, time.UTC)

var archiveCols = []string{"partition_name", "local_path", "s3_bucket", "s3_key", "archived_at", "max_event_id", "min_event_ts"}

func expectArchives(mock sqlmock.Sqlmock, lower string, rows *sqlmock.Rows) {
	mock.ExpectQuery(`SELECT partition_name, local_path, s3_bucket, s3_key, UNIX_TIMESTAMP\(archived_at\), max_event_id, min_event_ts FROM archive_state\s+WHERE partition_name >= \? AND \(partition_name <= \? OR min_event_ts <= \?\)`).
		WithArgs(lower, "p_2026100111", archiveUntil).WillReturnRows(rows)
}

func localFile(t *testing.T, name string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCheckArchives_2186(t *testing.T) {
	m := &EventMark{ID: 100, File: "binlog.000007", End: 500}
	floor := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	until := archiveUntil
	w := &ReadWindow{Schema: "s", Table: "t", Since: floor, Until: until}
	ctx := context.Background()
	const lower = "p_2026100109"

	t.Run("a file with nothing indexed after the mark is not read; one with no record is", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		before, after, unrecorded := localFile(t, "a.parquet"), localFile(t, "b.parquet"), localFile(t, "c.parquet")
		calls := stubProbe(t, nil, nil, nil)
		expectArchives(mock, lower, sqlmock.NewRows(archiveCols).
			AddRow("p_2026100109", before, nil, nil, "1759309200", 100, nil). // at the mark: skipped
			AddRow("p_2026100110", after, nil, nil, "1759312800", 101, nil).
			AddRow("p_2026100111", unrecorded, nil, nil, "1759316400", nil, nil).
			AddRow("p_2026100112", localFile(t, "d.parquet"), nil, nil, "1759320000", 500, nil). // after until
			AddRow("not_an_hour", localFile(t, "e.parquet"), nil, nil, "1759320000", 500, nil))
		note, err := w.checkArchives(ctx, db, m, floor)
		if err != nil || note != "" {
			t.Fatalf("= %q, %v; want clean", note, err)
		}
		if len(*calls) != 2 || (*calls)[0].file != after || (*calls)[1].file != unrecorded {
			t.Fatalf("read %v; want only %s and %s", *calls, after, unrecorded)
		}
	})

	t.Run("a file whose oldest change is before its label hour is read when that is inside the window", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		misfiled := localFile(t, "m.parquet")
		calls := stubProbe(t, nil, nil, nil)
		expectArchives(mock, lower, sqlmock.NewRows(archiveCols).
			AddRow("p_2026100113", misfiled, nil, nil, "1759320000", 500, until.Add(-time.Hour)))
		if _, err := w.checkArchives(ctx, db, m, floor); err != nil {
			t.Fatal(err)
		}
		if len(*calls) != 1 {
			t.Fatalf("read %v; want the misfiled file", *calls)
		}
	})

	for _, tc := range []struct {
		name         string
		span         parquetquery.BelowMarkSpan
		windowFound  bool
		refuse       bool
		names        string
		reads        int // file reads over two statements
		windowedRead bool
	}{
		{name: "the earliest change below the mark is inside the window: refused, one read",
			span: spanAt(until.Add(-time.Hour), until.Add(time.Hour)), refuse: true, names: "binlog.000001:300", reads: 1},
		{name: "the latest is inside the window: refused",
			span: spanAt(floor.Add(-time.Hour), floor.Add(30*time.Minute)), refuse: true, names: "binlog.000001:250", reads: 1},
		{name: "every one after the window's end: clean, the file read once",
			span: spanAt(until.Add(time.Minute), until.Add(time.Hour)), reads: 1},
		{name: "every one before the window's start: clean, the file read once",
			span: spanAt(floor.Add(-2*time.Hour), floor.Add(-time.Minute)), reads: 1},
		{name: "around the window, none inside it: clean, the window read each time",
			span: spanAt(floor.Add(-time.Hour), until.Add(time.Hour)), reads: 3, windowedRead: true},
		{name: "around the window, one inside it: refused",
			span: spanAt(floor.Add(-time.Hour), until.Add(time.Hour)), windowFound: true, refuse: true, reads: 3, windowedRead: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, _ := sqlmock.New()
			defer db.Close()
			f := localFile(t, "f.parquet")
			calls := stubProbe(t, map[string]parquetquery.BelowMarkSpan{f: tc.span}, map[string]bool{f: tc.windowFound}, nil)
			for range 2 {
				expectArchives(mock, lower, sqlmock.NewRows(archiveCols).AddRow("p_2026100110", f, nil, nil, "1759312800", 900, nil))
				note, err := w.checkArchives(ctx, db, m, floor)
				if tc.refuse {
					if !errors.Is(err, ErrBinlogRenumbered) || !strings.Contains(err.Error(), tc.names) {
						t.Fatalf("err = %v; want ErrBinlogRenumbered naming %s", err, tc.names)
					}
					continue
				}
				if err != nil || note != "" {
					t.Fatalf("= %q, %v; want clean", note, err)
				}
			}
			windowed := false
			for _, c := range *calls {
				windowed = windowed || c.windowed
			}
			if len(*calls) != tc.reads || windowed != tc.windowedRead {
				t.Fatalf("reads %v; want %d (windowed: %v)", *calls, tc.reads, tc.windowedRead)
			}
		})
	}

	t.Run("what a file holds is remembered, until it is archived again", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		f := localFile(t, "h.parquet")
		calls := stubProbe(t, map[string]parquetquery.BelowMarkSpan{f: spanAt(until.Add(time.Minute), until.Add(time.Hour))}, nil, nil)
		for _, at := range []string{"1759312800", "1759312800", "1759316400"} {
			expectArchives(mock, lower, sqlmock.NewRows(archiveCols).AddRow("p_2026100110", f, nil, nil, at, 900, nil))
			if _, err := w.checkArchives(ctx, db, m, floor); err != nil {
				t.Fatal(err)
			}
		}
		if len(*calls) != 2 {
			t.Fatalf("reads %v; want the first and the re-archived one", *calls)
		}
	})

	t.Run("no file to open: a note naming the hour, after the others were asked", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		f := localFile(t, "i.parquet")
		calls := stubProbe(t, nil, nil, nil)
		expectArchives(mock, lower, sqlmock.NewRows(archiveCols).
			AddRow("p_2026100109", filepath.Join(t.TempDir(), "gone.parquet"), nil, nil, "1759309200", 900, nil).
			AddRow("p_2026100110", nil, "", "", "1759312800", nil, nil).
			AddRow("p_2026100111", f, nil, nil, "1759316400", 900, nil))
		note, err := w.checkArchives(ctx, db, m, floor)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"binlog numbering not checked", "p_2026100109 and 1 more", "no file this process can open"} {
			if !strings.Contains(note, want) {
				t.Errorf("note does not say %q: %s", want, note)
			}
		}
		if strings.Contains(note, "--") {
			t.Errorf("note names a flag: %s", note)
		}
		if len(*calls) != 1 {
			t.Fatalf("reads %v; want the one readable file", *calls)
		}
	})

	t.Run("a refusal elsewhere wins over a file that cannot be opened", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		f := localFile(t, "j.parquet")
		stubProbe(t, map[string]parquetquery.BelowMarkSpan{f: spanAt(floor.Add(time.Minute), floor.Add(time.Minute))}, nil, nil)
		expectArchives(mock, lower, sqlmock.NewRows(archiveCols).
			AddRow("p_2026100109", nil, nil, nil, "1759309200", nil, nil).
			AddRow("p_2026100110", f, nil, nil, "1759312800", 900, nil))
		if _, err := w.checkArchives(ctx, db, m, floor); !errors.Is(err, ErrBinlogRenumbered) {
			t.Fatalf("err = %v; want ErrBinlogRenumbered", err)
		}
	})

	t.Run("an S3 copy is read when the local one is gone", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		calls := stubProbe(t, nil, nil, nil)
		expectArchives(mock, lower, sqlmock.NewRows(archiveCols).
			AddRow("p_2026100110", "/nonexistent/x.parquet", "bkt", "/arch/x.parquet", "1759312800", 900, nil))
		if _, err := w.checkArchives(ctx, db, m, floor); err != nil {
			t.Fatal(err)
		}
		if len(*calls) != 1 || (*calls)[0].file != "s3://bkt/arch/x.parquet" {
			t.Fatalf("reads %v; want the S3 copy", *calls)
		}
	})

	t.Run("an S3 object that is not there is a file that cannot be opened", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		stubProbe(t, nil, nil, map[string]error{"s3://bkt/arch/x.parquet": fmt.Errorf("download: %w", parquetquery.ErrArchiveObjectMissing)})
		expectArchives(mock, lower, sqlmock.NewRows(archiveCols).
			AddRow("p_2026100110", nil, "bkt", "arch/x.parquet", "1759312800", 900, nil))
		note, err := w.checkArchives(ctx, db, m, floor)
		if err != nil || !strings.Contains(note, "p_2026100110") || !strings.Contains(note, "binlog numbering not checked") {
			t.Fatalf("= %q, %v; want the note naming the hour", note, err)
		}
	})

	t.Run("a read that fails is an error", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		f := localFile(t, "k.parquet")
		stubProbe(t, nil, nil, map[string]error{f: errors.New("boom")})
		expectArchives(mock, lower, sqlmock.NewRows(archiveCols).AddRow("p_2026100110", f, nil, nil, "1759312800", 900, nil))
		_, err := w.checkArchives(ctx, db, m, floor)
		if err == nil || errors.Is(err, ErrBinlogRenumbered) || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("err = %v; want the read's failure", err)
		}
	})

	t.Run("an archive_state an older build created is read without the newer columns, every file read", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		f := localFile(t, "l.parquet")
		calls := stubProbe(t, nil, nil, nil)
		mock.ExpectQuery(`max_event_id, min_event_ts FROM archive_state`).WillReturnError(&mysqldriver.MySQLError{Number: 1054, Message: "Unknown column"})
		mock.ExpectQuery(`UNIX_TIMESTAMP\(archived_at\) FROM archive_state WHERE partition_name >= \?`).WithArgs(lower).
			WillReturnRows(sqlmock.NewRows(archiveCols[:5]).AddRow("p_2026100110", f, nil, nil, "1759312800"))
		if _, err := w.checkArchives(ctx, db, m, floor); err != nil {
			t.Fatal(err)
		}
		if len(*calls) != 1 {
			t.Fatalf("reads %v; want the file", *calls)
		}
	})

	t.Run("no archive_state table: nothing to read", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		stubProbe(t, nil, nil, nil)
		mock.ExpectQuery(`FROM archive_state`).WillReturnError(&mysqldriver.MySQLError{Number: 1146, Message: "doesn't exist"})
		if note, err := w.checkArchives(ctx, db, m, floor); err != nil || note != "" {
			t.Fatalf("= %q, %v", note, err)
		}
	})

	t.Run("another failure reading archive_state is an error", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		stubProbe(t, nil, nil, nil)
		mock.ExpectQuery(`FROM archive_state`).WillReturnError(errors.New("gone away"))
		if _, err := w.checkArchives(ctx, db, m, floor); err == nil {
			t.Fatal("no error")
		}
	})

	t.Run("no floor reads every hour", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		stubProbe(t, nil, nil, nil)
		expectArchives(mock, "p_", sqlmock.NewRows(archiveCols))
		if _, err := w.checkArchives(ctx, db, m, time.Time{}); err != nil {
			t.Fatal(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

// The question asked of a file carries the mark and, on the re-read, this
// read's own window.
func TestCheckArchives_question_2186(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	f := localFile(t, "q.parquet")
	var got []parquetquery.BelowMark
	prev := archiveProbe
	archiveProbe = func(_ context.Context, _ string, q parquetquery.BelowMark) (parquetquery.BelowMarkSpan, error) {
		got = append(got, q)
		if len(got) == 1 {
			// Around the window: the window is asked again.
			return spanAt(time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC), time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)), nil
		}
		return parquetquery.BelowMarkSpan{}, nil
	}
	archiveSpans.reset()
	t.Cleanup(func() { archiveProbe = prev })
	floor := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	until := archiveUntil
	w := &ReadWindow{Schema: "s", Table: "t", Since: floor, Until: until}
	expectArchives(mock, "p_2026100109", sqlmock.NewRows(archiveCols).AddRow("p_2026100110", f, nil, nil, "1759312800", 900, nil))
	if _, err := w.checkArchives(context.Background(), db, &EventMark{ID: 100, File: "binlog.000007", End: 500}, floor); err != nil {
		t.Fatal(err)
	}
	want := parquetquery.BelowMark{Schema: "s", Table: "t", AfterID: 100, MarkFile: "binlog.000007", MarkEnd: 500, Base: "binlog"}
	if len(got) != 2 || got[0] != want {
		t.Fatalf("questions = %+v; want %+v first", got, want)
	}
	want.From, want.Until = floor, until
	if got[1] != want {
		t.Fatalf("re-read = %+v; want %+v", got[1], want)
	}
	// A mark file with no numeric extension has no base-name clause.
	got = nil
	expectArchives(mock, "p_2026100109", sqlmock.NewRows(archiveCols).AddRow("p_2026100110", f, nil, nil, "1759316400", 900, nil))
	if _, err := w.checkArchives(context.Background(), db, &EventMark{ID: 100, File: "binlog", End: 500}, floor); err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 || got[0].Base != "" {
		t.Fatalf("questions = %+v; want no base name", got)
	}
}

// CheckNumberingFromRead turns each "cannot check" cause into a note, and a
// check that ran into none (#2186).
func TestCheckNumberingFromRead_notes_2186(t *testing.T) {
	mark := EventMark{ID: 10, File: "binlog.000007", End: 200}
	pos := &query.BinlogPos{File: "binlog.000007", Pos: 200}
	idx := func(mock sqlmock.Sqlmock, backfilled bool) {
		rows := sqlmock.NewRows([]string{"one"})
		if backfilled {
			rows.AddRow(1)
		}
		mock.ExpectQuery(`FROM index_state`).WillReturnRows(rows)
	}
	for _, tc := range []struct {
		name  string
		raw   string
		setup func(sqlmock.Sqlmock)
		want  string
	}{
		{name: "unreadable mark", raw: "{not json", want: "event mark cannot be read",
			setup: func(m sqlmock.Sqlmock) {}},
		{name: "backfilled", raw: mark.Encode(), want: "`bintrail index` also wrote into this index",
			setup: func(m sqlmock.Sqlmock) { idx(m, true) }},
		{name: "rebuilt", raw: mark.Encode(), want: "is now another event",
			setup: func(m sqlmock.Sqlmock) {
				idx(m, false)
				m.ExpectQuery(`WHERE event_id = \?`).WillReturnRows(sqlmock.NewRows([]string{"f", "e"}).AddRow("binlog.000007", 900))
			}},
		{name: "deleted while older rows remain", raw: mark.Encode(), want: "was deleted from the index while older ones remain",
			setup: func(m sqlmock.Sqlmock) {
				idx(m, false)
				m.ExpectQuery(`WHERE event_id = \?`).WillReturnRows(sqlmock.NewRows([]string{"f", "e"}))
				m.ExpectQuery(`MIN\(event_id\)`).WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(5))
			}},
		{name: "checked", raw: mark.Encode(), want: "",
			setup: func(m sqlmock.Sqlmock) {
				idx(m, false)
				m.ExpectQuery(`WHERE event_id = \?`).WillReturnRows(sqlmock.NewRows([]string{"f", "e"}).AddRow("binlog.000007", 200))
				m.ExpectQuery(`ORDER BY event_id ASC`).WillReturnRows(sqlmock.NewRows([]string{"i", "f", "e"}))
			}},
		{name: "no mark", raw: "", want: "", setup: func(m sqlmock.Sqlmock) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, _ := sqlmock.New()
			defer db.Close()
			tc.setup(mock)
			note, err := CheckNumberingFromRead(context.Background(), db, pos, tc.raw, ReadWindow{Notice: func(_ slog.Level, _ string, _ ...any) {}})
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == "" {
				if note != "" {
					t.Fatalf("note = %q; want none", note)
				}
				return
			}
			for _, want := range []string{"binlog numbering not checked", tc.want, "cannot be told"} {
				if !strings.Contains(note, want) {
					t.Errorf("note does not say %q: %s", want, note)
				}
			}
			if strings.Contains(note, "--") {
				t.Errorf("note names a flag: %s", note)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
	// No position to read from: nothing read by position, no note.
	db, _, _ := sqlmock.New()
	defer db.Close()
	if note, err := CheckNumberingFromRead(context.Background(), db, nil, "{not json", ReadWindow{Notice: func(slog.Level, string, ...any) {}}); note != "" || err != nil {
		t.Fatalf("= %q, %v", note, err)
	}
}

// The unbounded form (a snapshot refresh, live-mode verify, reconstruct --at
// once its read was rotated out) reads no archive: when rotation took the
// mark's hour, it says it cannot tell whenever an archive may hold changes
// indexed after the mark (#2186).
func TestCheckNumberingFromRead_markRotatedUnbounded_2186(t *testing.T) {
	mark := EventMark{ID: 10, File: "binlog.000007", End: 200}
	pos := &query.BinlogPos{File: "binlog.000007", Pos: 200}
	for _, tc := range []struct {
		name    string
		live    bool // later rows are still live
		archive func(sqlmock.Sqlmock)
		want    bool
	}{
		{"an archive above the mark", true, func(m sqlmock.Sqlmock) {
			m.ExpectQuery(`FROM archive_state WHERE max_event_id > \? OR max_event_id IS NULL`).WithArgs(uint64(10)).
				WillReturnRows(sqlmock.NewRows([]string{"one"}).AddRow(1))
		}, true},
		{"nothing live, an archive above the mark", false, func(m sqlmock.Sqlmock) {
			m.ExpectQuery(`FROM archive_state WHERE max_event_id > \?`).WillReturnRows(sqlmock.NewRows([]string{"one"}).AddRow(1))
		}, true},
		{"every archive at or below the mark", true, func(m sqlmock.Sqlmock) {
			m.ExpectQuery(`FROM archive_state WHERE max_event_id > \?`).WillReturnRows(sqlmock.NewRows([]string{"one"}))
		}, false},
		{"no archive_state", true, func(m sqlmock.Sqlmock) {
			m.ExpectQuery(`FROM archive_state`).WillReturnError(&mysqldriver.MySQLError{Number: 1146})
		}, false},
		{"an archive_state without the record", true, func(m sqlmock.Sqlmock) {
			m.ExpectQuery(`FROM archive_state WHERE max_event_id`).WillReturnError(&mysqldriver.MySQLError{Number: 1054})
			m.ExpectQuery(`SELECT 1 FROM archive_state LIMIT 1`).WillReturnRows(sqlmock.NewRows([]string{"one"}).AddRow(1))
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, _ := sqlmock.New()
			defer db.Close()
			mock.ExpectQuery(`FROM index_state`).WillReturnRows(sqlmock.NewRows([]string{"one"}))
			mock.ExpectQuery(`WHERE event_id = \?`).WillReturnRows(sqlmock.NewRows([]string{"f", "e"}))
			if tc.live {
				mock.ExpectQuery(`MIN\(event_id\)`).WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(50))
				for range 2 {
					mock.ExpectQuery(`WHERE event_id > \?`).WillReturnRows(sqlmock.NewRows([]string{"i", "f", "e"}).AddRow(60, "binlog.000009", 100))
				}
			} else {
				mock.ExpectQuery(`MIN\(event_id\)`).WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(nil))
			}
			tc.archive(mock)
			note, err := CheckNumberingFromRead(context.Background(), db, pos, mark.Encode(), ReadWindow{Notice: func(slog.Level, string, ...any) {}})
			if err != nil {
				t.Fatal(err)
			}
			if (note != "") != tc.want || (tc.want && !strings.Contains(note, "moved to the Parquet archives")) {
				t.Fatalf("note = %q; want one: %v", note, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
	// A refusal from the live probes wins.
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery(`FROM index_state`).WillReturnRows(sqlmock.NewRows([]string{"one"}))
	mock.ExpectQuery(`WHERE event_id = \?`).WillReturnRows(sqlmock.NewRows([]string{"f", "e"}))
	mock.ExpectQuery(`MIN\(event_id\)`).WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(50))
	mock.ExpectQuery(`WHERE event_id > \?`).WillReturnRows(sqlmock.NewRows([]string{"i", "f", "e"}).AddRow(60, "binlog.000001", 100))
	if _, err := CheckNumberingFromRead(context.Background(), db, pos, mark.Encode(), ReadWindow{Notice: func(slog.Level, string, ...any) {}}); !errors.Is(err, ErrBinlogRenumbered) {
		t.Fatalf("err = %v; want ErrBinlogRenumbered", err)
	}
}

// A snapshot refresh (CheckNumberingFrom) reads nothing new: with the mark's
// hour rotated it asks archive_state nothing (#2186 review).
func TestCheckNumberingFrom_refreshReadsNoArchive_2186(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery(`FROM index_state`).WillReturnRows(sqlmock.NewRows([]string{"one"}))
	mock.ExpectQuery(`WHERE event_id = \?`).WillReturnRows(sqlmock.NewRows([]string{"f", "e"}))
	mock.ExpectQuery(`MIN\(event_id\)`).WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(50))
	for range 2 {
		mock.ExpectQuery(`WHERE event_id > \?`).WillReturnRows(sqlmock.NewRows([]string{"i", "f", "e"}).AddRow(60, "binlog.000009", 100))
	}
	mark := EventMark{ID: 10, File: "binlog.000007", End: 200}
	if err := CheckNumberingFrom(context.Background(), db, &query.BinlogPos{File: "binlog.000007", Pos: 200}, mark.Encode(), ReadWindow{Notice: func(slog.Level, string, ...any) {}}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A mark from an older numbering than the snapshot's own position says
// nothing about a later restart: a note, no refusal.
func TestCheckNumberingFromRead_markFromAnOlderNumbering_2186(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	mark := EventMark{ID: 10, File: "binlog.000007", End: 200}
	for _, anchor := range []query.BinlogPos{{File: "binlog.000001", Pos: 100}, {File: "mysql-bin.000009", Pos: 100}} {
		note, err := CheckNumberingFromRead(context.Background(), db, &anchor, mark.Encode(), ReadWindow{Notice: func(slog.Level, string, ...any) {}})
		if err != nil || !strings.Contains(note, "older binlog numbering than the snapshot's own position") {
			t.Fatalf("anchor %v: = %q, %v", anchor, note, err)
		}
	}
}

// markInArchives: whether the archives still hold the mark's own event, so
// the archived window is checked only from a mark they vouch for (#2186
// review: a capture's resume cleanup deletes the mark and captures its
// events again under new ids at their old positions).
func TestMarkInArchives_2186(t *testing.T) {
	m := &EventMark{ID: 10, File: "binlog.000007", End: 200}
	cols := []string{"partition_name", "local_path", "s3_bucket", "s3_key"}
	stub := func(t *testing.T, holds map[string]parquetquery.BelowMarkRow, fail map[string]error) *[]string {
		var calls []string
		prev := archiveEventByID
		archiveEventByID = func(_ context.Context, file string, id uint64) (parquetquery.BelowMarkRow, bool, error) {
			calls = append(calls, file)
			if err := fail[file]; err != nil {
				return parquetquery.BelowMarkRow{}, false, err
			}
			r, ok := holds[file]
			return r, ok, nil
		}
		t.Cleanup(func() { archiveEventByID = prev })
		archiveMarks.reset()
		return &calls
	}
	expect := func(mock sqlmock.Sqlmock, rows *sqlmock.Rows) {
		mock.ExpectQuery(`WHERE max_event_id >= \? OR max_event_id IS NULL\s+ORDER BY max_event_id IS NULL, max_event_id`).
			WithArgs(uint64(10), markProbeLimit+1).WillReturnRows(rows)
	}
	ctx := context.Background()
	t.Run("held, the same event; remembered", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		a, b := localFile(t, "a.parquet"), localFile(t, "b.parquet")
		calls := stub(t, map[string]parquetquery.BelowMarkRow{b: {EventID: 10, File: "binlog.000007", End: 200}}, nil)
		expect(mock, sqlmock.NewRows(cols).AddRow("p1", a, nil, nil).AddRow("p2", b, nil, nil))
		for range 2 {
			if st, _, err := markInArchives(ctx, db, m); err != nil || st != markArchived {
				t.Fatalf("= %v, %v; want archived", st, err)
			}
		}
		if len(*calls) != 2 {
			t.Fatalf("reads %v; want a then b, once", *calls)
		}
	})
	t.Run("held as another event", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		a := localFile(t, "a.parquet")
		stub(t, map[string]parquetquery.BelowMarkRow{a: {EventID: 10, File: "binlog.000007", End: 900}}, nil)
		expect(mock, sqlmock.NewRows(cols).AddRow("p1", a, nil, nil))
		if st, _, err := markInArchives(ctx, db, m); err != nil || st != markOtherEvent {
			t.Fatalf("= %v, %v; want another event", st, err)
		}
	})
	t.Run("in none of the archives that could hold it", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		a := localFile(t, "a.parquet")
		stub(t, nil, nil)
		expect(mock, sqlmock.NewRows(cols).AddRow("p1", a, nil, nil))
		if st, _, err := markInArchives(ctx, db, m); err != nil || st != markAbsent {
			t.Fatalf("= %v, %v; want absent", st, err)
		}
	})
	t.Run("no archive could hold it", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		stub(t, nil, nil)
		expect(mock, sqlmock.NewRows(cols))
		if st, _, err := markInArchives(ctx, db, m); err != nil || st != markNoArchive {
			t.Fatalf("= %v, %v; want no archive", st, err)
		}
	})
	t.Run("no archive_state", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		stub(t, nil, nil)
		mock.ExpectQuery(`FROM archive_state`).WillReturnError(&mysqldriver.MySQLError{Number: 1146})
		if st, _, err := markInArchives(ctx, db, m); err != nil || st != markNoArchive {
			t.Fatalf("= %v, %v; want no archive", st, err)
		}
	})
	t.Run("an archive_state without the record: every archive could", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		a := localFile(t, "a.parquet")
		stub(t, map[string]parquetquery.BelowMarkRow{a: {EventID: 10, File: "binlog.000007", End: 200}}, nil)
		mock.ExpectQuery(`WHERE max_event_id`).WillReturnError(&mysqldriver.MySQLError{Number: 1054})
		mock.ExpectQuery(`FROM archive_state\s+ORDER BY partition_name`).WillReturnRows(sqlmock.NewRows(cols).AddRow("p1", a, nil, nil))
		if st, _, err := markInArchives(ctx, db, m); err != nil || st != markArchived {
			t.Fatalf("= %v, %v; want archived", st, err)
		}
	})
	t.Run("an archive that could hold it cannot be opened: a note, not remembered", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		calls := stub(t, nil, map[string]error{"s3://b/k.parquet": fmt.Errorf("x: %w", parquetquery.ErrArchiveObjectMissing)})
		for range 2 {
			expect(mock, sqlmock.NewRows(cols).AddRow("p1", nil, "b", "k.parquet").AddRow("p2", "/gone/x.parquet", nil, nil))
			st, note, err := markInArchives(ctx, db, m)
			if err != nil || st != markUnreadable || !strings.Contains(note, "p1 and 1 more") {
				t.Fatalf("= %v, %q, %v; want unreadable naming p1", st, note, err)
			}
		}
		if len(*calls) != 2 {
			t.Fatalf("reads %v; want one per call", *calls)
		}
	})
	t.Run("another failure is an error", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		a := localFile(t, "a.parquet")
		stub(t, nil, map[string]error{a: errors.New("boom")})
		expect(mock, sqlmock.NewRows(cols).AddRow("p1", a, nil, nil))
		if _, _, err := markInArchives(ctx, db, m); err == nil || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("past the probe limit: a note", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		a := localFile(t, "a.parquet")
		stub(t, nil, nil)
		rows := sqlmock.NewRows(cols)
		for i := range markProbeLimit + 1 {
			rows.AddRow(fmt.Sprintf("p%d", i), a, nil, nil)
		}
		expect(mock, rows)
		if st, note, err := markInArchives(ctx, db, m); err != nil || st != markUnreadable || note == "" {
			t.Fatalf("= %v, %q, %v", st, note, err)
		}
	})
}

// The bounded check after a capture's resume cleanup took the mark, once
// rotation archived its hour: the replayed events (ids above the mark at
// positions before its end) are not read as a restart, the check says it
// cannot tell (#2186 review).
func TestCheckNumberingFromRead_markDeletedThenArchived_2186(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	a := localFile(t, "a.parquet")
	prev := archiveEventByID
	archiveEventByID = func(context.Context, string, uint64) (parquetquery.BelowMarkRow, bool, error) {
		return parquetquery.BelowMarkRow{}, false, nil
	}
	t.Cleanup(func() { archiveEventByID = prev })
	archiveMarks.reset()
	mock.ExpectQuery(`FROM index_state`).WillReturnRows(sqlmock.NewRows([]string{"one"}))
	mock.ExpectQuery(`WHERE event_id = \?`).WillReturnRows(sqlmock.NewRows([]string{"f", "e"}))
	mock.ExpectQuery(`WHERE max_event_id >= \?`).WillReturnRows(sqlmock.NewRows([]string{"p", "l", "b", "k"}).AddRow("p1", a, nil, nil))
	mark := EventMark{ID: 10, File: "binlog.000007", End: 200}
	note, err := CheckNumberingFromRead(context.Background(), db, &query.BinlogPos{File: "binlog.000007", Pos: 200}, mark.Encode(),
		ReadWindow{Schema: "s", Table: "t", Until: archiveUntil, Notice: func(slog.Level, string, ...any) {}})
	if err != nil || !strings.Contains(note, "was deleted from the index") {
		t.Fatalf("= %q, %v; want the deleted-mark note", note, err)
	}
}

func TestLRU_2186(t *testing.T) {
	c := newLRU[int, string](2)
	c.put(1, "a")
	c.put(2, "b")
	if _, ok := c.get(1); !ok { // 1 is now the most recent
		t.Fatal("1 missing")
	}
	c.put(3, "c") // evicts 2, the least recent
	if _, ok := c.get(2); ok {
		t.Fatal("2 kept")
	}
	for _, k := range []int{1, 3} {
		if _, ok := c.get(k); !ok {
			t.Fatalf("%d evicted", k)
		}
	}
	c.put(3, "d")
	if v, _ := c.get(3); v != "d" {
		t.Fatalf("3 = %q", v)
	}
}

package reconstruct

import (
	"context"
	"errors"
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
func stubProbe(t *testing.T, found, windowFound map[string]bool, fail map[string]error) *[]probeCall {
	t.Helper()
	var calls []probeCall
	prev := archiveProbe
	archiveProbe = func(_ context.Context, file string, q parquetquery.BelowMark) (parquetquery.BelowMarkRow, bool, error) {
		windowed := !q.Until.IsZero()
		calls = append(calls, probeCall{file, windowed})
		if err := fail[file]; err != nil {
			return parquetquery.BelowMarkRow{}, false, err
		}
		hit := found[file]
		if windowed {
			hit = windowFound[file]
		}
		if hit {
			return parquetquery.BelowMarkRow{EventID: 99, File: "binlog.000001", End: 300}, true, nil
		}
		return parquetquery.BelowMarkRow{}, false, nil
	}
	t.Cleanup(func() { archiveProbe = prev })
	archiveClean.Lock()
	archiveClean.m = map[archiveCleanKey]bool{}
	archiveClean.Unlock()
	return &calls
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

	t.Run("found in the file and in the window: refused", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		f := localFile(t, "f.parquet")
		calls := stubProbe(t, map[string]bool{f: true}, map[string]bool{f: true}, nil)
		expectArchives(mock, lower, sqlmock.NewRows(archiveCols).AddRow("p_2026100110", f, nil, nil, "1759312800", 900, nil))
		_, err := w.checkArchives(ctx, db, m, floor)
		if !errors.Is(err, ErrBinlogRenumbered) || !strings.Contains(err.Error(), "binlog.000001:300") {
			t.Fatalf("err = %v; want ErrBinlogRenumbered naming the change", err)
		}
		if len(*calls) != 2 || !(*calls)[1].windowed {
			t.Fatalf("reads %v; want whole file, then the window", *calls)
		}
	})

	t.Run("found in the file but outside the window: clean, and asked again next time", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		f := localFile(t, "g.parquet")
		calls := stubProbe(t, map[string]bool{f: true}, nil, nil)
		for range 2 {
			expectArchives(mock, lower, sqlmock.NewRows(archiveCols).AddRow("p_2026100110", f, nil, nil, "1759312800", 900, nil))
			if note, err := w.checkArchives(ctx, db, m, floor); err != nil || note != "" {
				t.Fatalf("= %q, %v; want clean", note, err)
			}
		}
		if len(*calls) != 4 {
			t.Fatalf("reads %v; want both reads twice (not remembered)", *calls)
		}
	})

	t.Run("a file clean as a whole is remembered, until it is archived again", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		f := localFile(t, "h.parquet")
		calls := stubProbe(t, nil, nil, nil)
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
		stubProbe(t, map[string]bool{f: true}, map[string]bool{f: true}, nil)
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
	archiveProbe = func(_ context.Context, _ string, q parquetquery.BelowMark) (parquetquery.BelowMarkRow, bool, error) {
		got = append(got, q)
		return parquetquery.BelowMarkRow{}, len(got) == 1, nil
	}
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

package console

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// dirFooter is a footer of a file in one snapshot directory: who wrote it,
// where it stops, and the event mark beside that position.
func dirFooter(producer, file string, pos int64, mark *reconstruct.EventMark) baseline.DumpMetadata {
	md := baseline.DumpMetadata{Producer: producer, BinlogFile: file, BinlogPos: pos}
	if mark != nil {
		md.EventMark = mark.Encode()
	}
	return md
}

func mk(id uint64, file string, end uint64) *reconstruct.EventMark {
	return &reconstruct.EventMark{ID: id, File: file, End: end}
}

// #2369: how far a snapshot directory's data reaches is the cut of the run
// that PUBLISHED it, never a file's own last write: an unchanged table is
// carried forward with its old footer. And the bound may only err OLD.
func TestCopyDirMark(t *testing.T) {
	const rec, dump = baseline.ProducerReconstruct, baseline.ProducerDump
	cases := []struct {
		name    string
		footers []baseline.DumpMetadata
		want    uint64 // the mark's id; 0 = unknown
		why     string // substring of the reason when unknown
	}{
		{name: "nothing read", why: "no file"},
		{name: "one refresh file", want: 10,
			footers: []baseline.DumpMetadata{dirFooter(rec, "binlog.000003", 900, mk(10, "binlog.000003", 900))}},
		{name: "a carried unchanged table does not pull the directory back to its own last write", want: 50,
			footers: []baseline.DumpMetadata{
				dirFooter(rec, "binlog.000002", 100, mk(5, "binlog.000002", 100)),
				dirFooter(rec, "binlog.000003", 900, mk(50, "binlog.000003", 900)),
			}},
		{name: "a later binlog file outranks a bigger position", want: 60,
			footers: []baseline.DumpMetadata{
				dirFooter(rec, "binlog.000003", 99999, mk(50, "binlog.000003", 99999)),
				dirFooter(rec, "binlog.000004", 4, mk(60, "binlog.000004", 4)),
			}},
		{name: "a carried dump past the run's cut does not lend its newer mark", want: 50,
			footers: []baseline.DumpMetadata{
				dirFooter(dump, "binlog.000009", 100, mk(90, "binlog.000009", 50)),
				dirFooter(rec, "binlog.000003", 900, mk(50, "binlog.000003", 900)),
			}},
		{name: "a refresh file with no position is skipped, the others decide", want: 50,
			footers: []baseline.DumpMetadata{
				dirFooter(rec, "", 0, mk(99, "binlog.000009", 1)),
				dirFooter(rec, "binlog.000003", 900, mk(50, "binlog.000003", 900)),
			}},
		{name: "a file written before producers were stamped is skipped beside refresh files", want: 50,
			footers: []baseline.DumpMetadata{
				dirFooter("", "binlog.000009", 100, mk(90, "binlog.000009", 100)),
				dirFooter(rec, "binlog.000003", 900, mk(50, "binlog.000003", 900)),
			}},
		{name: "refresh files on two binlog numberings", why: "numbering",
			footers: []baseline.DumpMetadata{
				dirFooter(rec, "binlog.000003", 900, mk(50, "binlog.000003", 900)),
				dirFooter(rec, "mysql-bin.000001", 4, mk(51, "mysql-bin.000001", 4)),
			}},
		{name: "the furthest refresh file has no mark", why: "no event mark",
			footers: []baseline.DumpMetadata{
				dirFooter(rec, "binlog.000002", 100, mk(5, "binlog.000002", 100)),
				dirFooter(rec, "binlog.000003", 900, nil),
			}},
		{name: "a mark past its own file's position", why: "past",
			footers: []baseline.DumpMetadata{dirFooter(rec, "binlog.000003", 900, mk(50, "binlog.000003", 901))}},
		{name: "a mark on another numbering than its file", why: "past",
			footers: []baseline.DumpMetadata{dirFooter(rec, "binlog.000003", 900, mk(50, "other.000001", 4))}},
		{name: "a dump's directory: the mark it wrote on every file", want: 7,
			footers: []baseline.DumpMetadata{
				dirFooter(dump, "binlog.000005", 300, mk(7, "binlog.000004", 5)),
				dirFooter(dump, "binlog.000005", 300, mk(7, "binlog.000004", 5)),
			}},
		{name: "a dump's directory with two marks is not one dump", why: "different event marks",
			footers: []baseline.DumpMetadata{
				dirFooter(dump, "binlog.000005", 300, mk(8, "binlog.000004", 10)),
				dirFooter(dump, "binlog.000005", 300, mk(7, "binlog.000004", 5)),
			}},
		{name: "a dump's directory with a file that has no mark", why: "no event mark",
			footers: []baseline.DumpMetadata{
				dirFooter(dump, "binlog.000005", 300, mk(7, "binlog.000004", 5)),
				dirFooter(dump, "binlog.000005", 300, nil),
			}},
		{name: "no refresh file and one of unknown origin", why: "who wrote",
			footers: []baseline.DumpMetadata{
				dirFooter(dump, "binlog.000005", 300, mk(7, "binlog.000004", 5)),
				dirFooter("", "binlog.000005", 300, mk(7, "binlog.000004", 5)),
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, why := copyDirMark(c.footers)
			if got.ID != c.want {
				t.Fatalf("mark %d (%q), want %d", got.ID, why, c.want)
			}
			if c.want == 0 && !strings.Contains(why, c.why) {
				t.Errorf("reason %q, want it to say %q", why, c.why)
			}
			if c.want != 0 && why != "" {
				t.Errorf("a mark AND a reason: %q", why)
			}
		})
	}
}

// The files whose footers say where a statement's data stops: every table of
// each snapshot directory the statement read, the table file and the last
// pair of its chain.
func TestCopyReadFrom(t *testing.T) {
	orders := views.BaselineTable{Schema: "shop", Table: "orders", Path: "/snap/2026-10-14T10-00-00Z/shop/orders.parquet", Rel: "shop/orders.parquet",
		DeltaFiles: []baseline.TableDeltaFile{
			{Upserts: "/snap/2026-10-14T10-00-00Z/shop/orders.000000.upserts"},
			{Upserts: "/snap/2026-10-14T10-00-00Z/shop/orders.000001.upserts"},
		}}
	countries := views.BaselineTable{Schema: "shop", Table: "countries", Path: "/snap/2026-10-14T10-00-00Z/shop/countries.parquet", Rel: "shop/countries.parquet"}
	in := views.Input{Baselines: []views.BaselineTable{orders, countries}}

	got, ok := copyReadFrom(in, []views.BaselineTable{countries})
	if !ok {
		t.Fatal("not ok")
	}
	want := []string{
		"/snap/2026-10-14T10-00-00Z/shop/orders.parquet",
		"/snap/2026-10-14T10-00-00Z/shop/orders.000001.upserts",
		"/snap/2026-10-14T10-00-00Z/shop/countries.parquet",
	}
	files := got["/snap/2026-10-14T10-00-00Z/"]
	if len(got) != 1 || strings.Join(files, ",") != strings.Join(want, ",") {
		t.Fatalf("got %v, want one directory with %v", got, want)
	}

	if _, ok := copyReadFrom(in, nil); ok {
		t.Error("a statement that read no table of the copy has no answer to give")
	}
	noRel := countries
	noRel.Rel = ""
	if got, ok := copyReadFrom(views.Input{Baselines: []views.BaselineTable{noRel}}, []views.BaselineTable{noRel}); !ok || len(got["/snap/2026-10-14T10-00-00Z/"]) != 1 {
		t.Errorf("a pinned view has no Rel, and its directory is the layout's: %v", got)
	}
	for _, path := range []string{
		"/snap/2026-10-14T10-00-00Z/other/countries.parquet", // another schema's directory
		"/snap/2026-10-14T10-00-00Z/shop/country.parquet",    // another table's file
		"/shop/countries.parquet",                            // no snapshot directory above
		"shop/countries.parquet",
	} {
		odd := countries
		odd.Path = path
		if _, ok := copyReadFrom(views.Input{Baselines: []views.BaselineTable{odd}}, []views.BaselineTable{odd}); ok {
			t.Errorf("%s: a path of another shape is not guessed at", path)
		}
	}
	s3 := countries
	s3.Path = "s3://bucket/2026-10-14T10-00-00Z/shop/countries.parquet"
	if _, ok := copyReadFrom(views.Input{Baselines: []views.BaselineTable{s3}}, []views.BaselineTable{s3}); ok {
		t.Error("a copy only in S3 is unknown: its footers are not read here")
	}
}

// The instant is the mark's own event, read from the index by id, and only
// when that row is still the event the mark names.
func TestCopyMarkTime(t *testing.T) {
	at := time.Date(2026, 10, 14, 14, 3, 12, 0, time.UTC)
	q := `SELECT event_timestamp, binlog_file, end_pos FROM binlog_events WHERE event_id = ?`
	cases := []struct {
		name string
		rows func(m sqlmock.Sqlmock)
		why  string
	}{
		{name: "found", rows: func(m sqlmock.Sqlmock) {
			m.ExpectQuery(q).WithArgs(50).WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c"}).AddRow(at, "binlog.000003", 900))
		}},
		{name: "rotated out", why: "no longer holds", rows: func(m sqlmock.Sqlmock) {
			m.ExpectQuery(q).WithArgs(50).WillReturnError(sql.ErrNoRows)
		}},
		{name: "the id now names another event", why: "another event", rows: func(m sqlmock.Sqlmock) {
			m.ExpectQuery(q).WithArgs(50).WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c"}).AddRow(at, "binlog.000003", 901))
		}},
		{name: "unreadable", why: "could not be read", rows: func(m sqlmock.Sqlmock) {
			m.ExpectQuery(q).WithArgs(50).WillReturnError(errors.New("boom"))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			c.rows(mock)
			got, why := copyMarkTime(context.Background(), db, *mk(50, "binlog.000003", 900))
			if c.why == "" {
				if why != "" || !got.Equal(at) {
					t.Fatalf("got %v %q, want %v", got, why, at)
				}
			} else if !got.IsZero() || !strings.Contains(why, c.why) {
				t.Fatalf("got %v %q, want unknown saying %q", got, why, c.why)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// What SHOW ROUTER STATUS says about the connection's last statement. Never
// a number for MySQL's answers, and "unknown" rather than a guess.
func TestLastAnswerRows(t *testing.T) {
	ran := time.Date(2026, 10, 14, 14, 5, 22, 0, time.UTC)
	through := time.Date(2026, 10, 14, 14, 3, 12, 0, time.UTC)
	cases := []struct {
		name string
		a    copyAnswer
		thr  time.Time
		want string
	}{
		{name: "nothing yet", want: "last_answer=none"},
		{name: "mysql", a: copyAnswer{side: "mysql"}, want: "last_answer=mysql"},
		{name: "refused", a: copyAnswer{side: "refused"}, want: "last_answer=refused"},
		{name: "copy, known", a: copyAnswer{side: "copy", ranAt: ran}, thr: through,
			want: "last_answer=copy;last_answer_indexed_through=2026-10-14T14:03:12Z;last_answer_age_seconds=130"},
		{name: "copy, unknown", a: copyAnswer{side: "copy", ranAt: ran},
			want: "last_answer=copy;last_answer_indexed_through=unknown;last_answer_age_seconds=unknown"},
		{name: "copy, an event dated after the statement ran (another clock): zero, never negative", a: copyAnswer{side: "copy", ranAt: ran}, thr: ran.Add(time.Minute),
			want: "last_answer=copy;last_answer_indexed_through=2026-10-14T14:06:22Z;last_answer_age_seconds=0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var parts []string
			for _, p := range lastAnswerRows(c.a, c.thr) {
				parts = append(parts, p[0]+"="+p[1])
			}
			if got := strings.Join(parts, ";"); got != c.want {
				t.Errorf("got  %s\nwant %s", got, c.want)
			}
		})
	}
}

// A connection's last answer: a run on the copy is pending until the port
// says what answered. A result it refuses goes elsewhere or nowhere, and a
// probe of the router's own (the zone check) is no client's statement.
func TestSQLOnCopy_lastAnswer(t *testing.T) {
	files := map[string][]string{"/d/": {"/d/a.parquet"}}
	ran := time.Now()
	q := &SQLOnCopy{}
	if a := q.lastAnswer(); a.side != "" {
		t.Fatalf("fresh connection: %+v", a)
	}
	q.NoteRoute("mysql")
	q.noteCopyAnswer(files, true, ran, time.Time{}, false) // the zone probe: no decision follows
	if a := q.lastAnswer(); a.side != "mysql" {
		t.Fatalf("a run the port did not answer with replaced the answer: %+v", a)
	}
	q.noteCopyAnswer(files, true, ran, time.Time{}, false)
	q.NoteRoute("copy")
	if a := q.lastAnswer(); a.side != "copy" || len(a.files) != 1 || !a.ok {
		t.Fatalf("the copy answered: %+v", a)
	}
	q.noteCopyAnswer(files, true, ran, time.Time{}, false) // ran, then refused by the port
	q.NoteRoute("refused")
	if a := q.lastAnswer(); a.side != "refused" || a.files != nil {
		t.Fatalf("refused after the copy ran: %+v", a)
	}
	q.noteCopyAnswer(files, true, ran, time.Time{}, false)
	q.noteCopyRefused()
	q.NoteRoute("copy") // nothing pending: never another statement's files
	if a := q.lastAnswer(); a.side != "copy" || a.files != nil {
		t.Fatalf("copy with nothing pending: %+v", a)
	}
	var nilQ *SQLOnCopy
	nilQ.NoteRoute("mysql") // a connection with no copy: nothing to note, no panic
}

// The wiring: a statement the copy really ran leaves, on its connection, the
// snapshot directory it read, and the one an EARLIER copy answered (#2210)
// leaves that copy's, never the newest: a newer bound there would err NEW.
func TestSQLOnCopy_noteTheSnapshotThatRan(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	const stmt = "SELECT status FROM shop.orders WHERE id = 1"
	dirs := func(q *SQLOnCopy) []string {
		a := q.lastAnswer()
		if a.side != "copy" || !a.ok {
			t.Fatalf("last answer: %+v", a)
		}
		var out []string
		for d := range a.files {
			out = append(out, d)
		}
		return out
	}

	t.Run("the newest copy", func(t *testing.T) {
		f := newSQLFixture(t, runner, false)
		q := &SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "u", id: bootServerID}
		if _, err := q.Run(context.Background(), stmt, "", sqlsandbox.Session{}); err != nil {
			t.Fatal(err)
		}
		q.NoteRoute("copy") // what the port says once it served the result
		want := filepath.Dir(f.schemaDir) + string(filepath.Separator)
		if got := dirs(q); len(got) != 1 || got[0] != want {
			t.Fatalf("read from %v, want %s", got, want)
		}
		// A statement the copy cannot run leaves nothing pending for the
		// port's next "copy" to pick up.
		if _, err := q.Run(context.Background(), "SELECT nope FROM", "", sqlsandbox.Session{}); err == nil {
			t.Fatal("a broken statement ran")
		}
		q.NoteRoute("copy")
		if a := q.lastAnswer(); a.files != nil {
			t.Fatalf("a refused run left its files: %+v", a)
		}
		if _, err := q.Run(context.Background(), stmt, "", sqlsandbox.Session{}); err != nil {
			t.Fatal(err)
		}
		q.NoteRoute("copy")
		// This fixture's footers record no producer and no mark: the status
		// says so, and guesses nothing.
		var rows []string
		for _, p := range q.lastAnswerStatus(context.Background()) {
			rows = append(rows, p[0]+"="+p[1])
		}
		if got := strings.Join(rows, ";"); got != "last_answer=copy;last_answer_indexed_through=unknown;last_answer_age_seconds=unknown" {
			t.Errorf("status: %s", got)
		}
	})

	t.Run("a statement whose tables the parse cannot tell reads every view: all of them bound it", func(t *testing.T) {
		f := newSQLFixture(t, runner, false)
		q := &SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "u", id: bootServerID}
		if _, err := q.Run(context.Background(), "SELECT count(*) FROM duckdb_tables()", "", sqlsandbox.Session{}); err != nil {
			t.Fatal(err)
		}
		q.NoteRoute("copy")
		want := filepath.Dir(f.schemaDir) + string(filepath.Separator)
		if got := dirs(q); len(got) != 1 || got[0] != want {
			t.Fatalf("read from %v, want %s", got, want)
		}
	})

	t.Run("an earlier copy", func(t *testing.T) {
		prev := sqlMaxChainBytes
		t.Cleanup(func() { sqlMaxChainBytes = prev })
		sqlMaxChainBytes = 10
		f := newSQLFixture(t, runner, false)
		addChain(t, f.schemaDir, "orders")
		older := writeOrdersSnapshot(t, f.root, "2026-04-30T02-00-00Z", "at two")
		q := &SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "u", id: bootServerID}
		res, err := q.Run(context.Background(), stmt, "", sqlsandbox.Session{})
		if err != nil || res.Note == "" {
			t.Fatalf("not answered from the earlier copy: note %q, err %v", res.Note, err)
		}
		q.NoteRoute("copy")
		want := filepath.Dir(older) + string(filepath.Separator)
		if got := dirs(q); len(got) != 1 || got[0] != want {
			t.Fatalf("read from %v, want the earlier copy %s", got, want)
		}
	})
}

// writeFooterFile writes a one-column Parquet file whose footer carries md.
func writeFooterFile(t *testing.T, path string, md map[string]string) {
	t.Helper()
	schemaFile := filepath.Join(t.TempDir(), "x.sql")
	if err := os.WriteFile(schemaFile, []byte("CREATE TABLE `x` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n);\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := baseline.ParseSchema(schemaFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// From real footers to the instant: the furthest refresh file's mark, read
// in the index; and a file of the snapshot that cannot be read makes the
// answer unknown, since leaving it out could raise a dump directory's bound.
func TestCopyIndexedThrough(t *testing.T) {
	dir := t.TempDir() + "/2026-10-14T10-00-00Z/"
	footer := func(file string, pos int, mark *reconstruct.EventMark) map[string]string {
		return map[string]string{
			baseline.MetaKeySnapshotProducer: baseline.ProducerReconstruct,
			baseline.MetaKeyBinlogFile:       file,
			baseline.MetaKeyBinlogPos:        strconv.Itoa(pos),
			baseline.MetaKeyEventMark:        mark.Encode(),
		}
	}
	orders, countries := dir+"shop/orders.parquet", dir+"shop/countries.parquet"
	writeFooterFile(t, orders, footer("binlog.000003", 900, mk(50, "binlog.000003", 900)))
	writeFooterFile(t, countries, footer("binlog.000002", 100, mk(5, "binlog.000002", 100))) // carried, unchanged
	at := time.Date(2026, 10, 14, 14, 3, 12, 0, time.UTC)

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT event_timestamp, binlog_file, end_pos FROM binlog_events WHERE event_id = ?`).
		WithArgs(50).WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c"}).AddRow(at, "binlog.000003", 900))

	s := &Server{}
	got, why := s.copyIndexedThrough(context.Background(), db, map[string][]string{dir: {orders, countries}})
	if why != "" || !got.Equal(at) {
		t.Fatalf("got %v %q, want %v", got, why, at)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}

	// Two snapshot directories: the older one's instant, whichever order
	// they are met in.
	olderDir := filepath.Dir(filepath.Dir(dir)) + "/2026-10-13T10-00-00Z/"
	olderOrders := olderDir + "shop/orders.parquet"
	writeFooterFile(t, olderOrders, footer("binlog.000001", 40, mk(3, "binlog.000001", 40)))
	before := at.Add(-24 * time.Hour)
	for range 4 { // map order varies: each run may meet either directory first
		mock.ExpectQuery(`SELECT event_timestamp, binlog_file, end_pos FROM binlog_events WHERE event_id = ?`).
			WithArgs(50).WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c"}).AddRow(at, "binlog.000003", 900))
		mock.ExpectQuery(`SELECT event_timestamp, binlog_file, end_pos FROM binlog_events WHERE event_id = ?`).
			WithArgs(3).WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c"}).AddRow(before, "binlog.000001", 40))
	}
	mock.MatchExpectationsInOrder(false)
	for range 4 {
		got, why := s.copyIndexedThrough(context.Background(), db, map[string][]string{dir: {orders}, olderDir: {olderOrders}})
		if why != "" || !got.Equal(before) {
			t.Fatalf("two directories: got %v %q, want the older %v", got, why, before)
		}
	}

	got, why = s.copyIndexedThrough(context.Background(), db, map[string][]string{dir: {orders, dir + "shop/gone.parquet"}})
	if !got.IsZero() || !strings.Contains(why, "a file of the snapshot") {
		t.Fatalf("a missing file: got %v %q", got, why)
	}
}

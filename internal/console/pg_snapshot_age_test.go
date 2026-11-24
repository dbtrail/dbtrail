package console

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/views"
)

var pgAgeDir = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

// pgAgeFooter is the footer of a PostgreSQL table file as a full read or an
// update writes it: a floor, the bound that goes with it, and the stamp that
// says it was read at one moment.
func pgAgeFooter(lsn uint64, since string) baseline.DumpMetadata {
	return baseline.DumpMetadata{LSN: lsn, PGDeltaSince: since, LockMode: baseline.LockStampPGRepeatableRead}
}

// How far one table's snapshot reaches (#3125): the bound its file records,
// and nothing when the footer does not say. Never the folder's time, and never newer than it.
func TestPGTableThroughOf(t *testing.T) {
	at := func(s string) time.Time {
		v, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	with := func(md baseline.DumpMetadata, change func(*baseline.DumpMetadata)) baseline.DumpMetadata {
		change(&md)
		return md
	}
	file := pgAgeFooter(100, "2026-11-20T11:59:00Z")
	cases := []struct {
		name string
		file baseline.DumpMetadata
		want string // RFC 3339, or a part of the reason for none
	}{
		{"a full read: a minute before it began", file, "2026-11-20T11:59:00Z"},
		{"an update's file", pgAgeFooter(250, "2026-11-20T11:58:30Z"), "2026-11-20T11:58:30Z"},
		// A table with no change is published again as the file it was: it
		// is as old as the update that wrote it, not as the folder.
		{"a file carried from a snapshot three days older", pgAgeFooter(40, "2026-11-17T09:00:00Z"), "2026-11-17T09:00:00Z"},
		// PGDeltaWindow never lets a bound pass the folder's time.
		{"a bound after its folder's time is the folder's time", pgAgeFooter(100, "2026-11-20T12:30:00Z"), "2026-11-20T12:00:00Z"},
		{"a snapshot from before the bound was recorded", pgAgeFooter(100, ""), "records no bound"},
		{"a bound that cannot be read", pgAgeFooter(100, "yesterday"), "cannot be read"},
		{"no floor", pgAgeFooter(0, "2026-11-20T11:59:00Z"), "no LSN"},
		{"a MySQL file", with(file, func(md *baseline.DumpMetadata) { md.BinlogFile, md.BinlogPos = "binlog.000003", 4 }), "no LSN"},
		{"published over a loss of captured changes", with(file, func(md *baseline.DumpMetadata) { md.CaptureGap = "lost slot" }), "known loss"},
		{"no stamp of how it was read", with(file, func(md *baseline.DumpMetadata) { md.LockMode = "" }), "one moment"},
	}
	for _, c := range cases {
		got, why := pgTableThroughOf(c.file, pgAgeDir)
		if why != "" {
			if !got.IsZero() || !strings.Contains(why, c.want) {
				t.Errorf("%s: no instant (%v) because %q, want %q", c.name, got, why, c.want)
			}
			continue
		}
		if !got.Equal(at(c.want)) {
			t.Errorf("%s: %s, want %s", c.name, got.Format(time.RFC3339), c.want)
		}
	}
}

// pgAgeTable writes an empty table file of public.<table> in the snapshot
// folder taken at dirAt under root, with a PostgreSQL footer whose bound is
// since ("" for none).
func pgAgeTable(t *testing.T, root string, dirAt time.Time, table string, lsn uint64, since string) views.BaselineTable {
	t.Helper()
	return pgAgeTableRead(t, root, dirAt, table, lsn, since, "2026-11-17T08:00:00Z")
}

// pgAgeTableRead is pgAgeTable for a file whose rows were read at readAt.
func pgAgeTableRead(t *testing.T, root string, dirAt time.Time, table string, lsn uint64, since, readAt string) views.BaselineTable {
	t.Helper()
	md := map[string]string{
		baseline.MetaKeyLSN:              strconv.FormatUint(lsn, 10),
		baseline.MetaKeyLockMode:         baseline.LockStampPGRepeatableRead,
		baseline.MetaKeySnapshotProducer: baseline.ProducerDump,
		baseline.MetaKeyLastDumpAt:       readAt,
		baseline.MetaKeyFoldGeneration:   "0",
	}
	if since != "" {
		md[baseline.MetaKeyPGDeltaSince] = since
	}
	path := filepath.Join(root, snapshotdir.Name(dirAt), "public", table+".parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, []baseline.Column{{Name: "id", RawText: true}}, baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return views.BaselineTable{Schema: "public", Table: table, Path: path}
}

// A statement is as old as the oldest table it read, from real files: a
// table carried unchanged into the newest snapshot makes the statement that
// reads it old, and leaves the one that does not as it was. One table with no
// bound leaves the whole statement with none.
func TestPGReadThrough(t *testing.T) {
	root := t.TempDir()
	written := pgAgeTable(t, root, pgAgeDir, "orders", 250, "2026-11-20T11:58:30Z")
	carried := pgAgeTable(t, root, pgAgeDir, "countries", 40, "2026-11-17T09:00:00Z")
	old := pgAgeTable(t, root, pgAgeDir, "legacy", 40, "")
	s := &Server{}

	through := func(read ...views.BaselineTable) string {
		t.Helper()
		got, why := s.pgReadThroughWith(read, func(views.BaselineTable, baseline.DumpMetadata) string { return "" })
		if why != "" {
			if !got.IsZero() {
				t.Fatalf("an instant (%v) with a reason (%s)", got, why)
			}
			return "unknown: " + why
		}
		return got.Format(time.RFC3339)
	}
	if got := through(written); got != "2026-11-20T11:58:30Z" {
		t.Errorf("the table the update wrote: %s", got)
	}
	if got := through(written, carried); got != "2026-11-17T09:00:00Z" {
		t.Errorf("with a table carried unchanged: %s, want the carried file's bound", got)
	}
	if got := through(carried, written); got != "2026-11-17T09:00:00Z" {
		t.Errorf("the same, the other way round: %s", got)
	}
	if got := through(written, old); !strings.HasPrefix(got, "unknown: public.legacy: ") {
		t.Errorf("with a table whose file records no bound: %s", got)
	}
	if got := through(); !strings.HasPrefix(got, "unknown: ") {
		t.Errorf("no table read: %s", got)
	}
	// Capture dropped changes of a table since its rows were read: an update
	// can have stamped its file with a new bound over them, so the file's
	// word is not taken, for that table and for no other.
	dropped := func(t views.BaselineTable, md baseline.DumpMetadata) string {
		if t.Table == "orders" && md.LSN == 250 {
			return "capture dropped its changes"
		}
		return ""
	}
	if got, why := s.pgReadThroughWith([]views.BaselineTable{carried, written}, dropped); why != "public.orders: capture dropped its changes" || !got.IsZero() {
		t.Errorf("a table capture dropped changes of: %v %q, want no instant", got, why)
	}
	if got, why := s.pgReadThroughWith([]views.BaselineTable{carried}, dropped); why != "" || got.IsZero() {
		t.Errorf("another table of the same server: %v %q, want its instant", got, why)
	}
	// With no index to ask what capture lost, nothing is said.
	if got, why := s.pgReadThrough(context.Background(), &bundle{pgSource: true}, "pg", []views.BaselineTable{written}); why == "" || !got.IsZero() {
		t.Errorf("no index: %v %q, want no instant", got, why)
	}
	// Not a table file in a local snapshot folder: nothing is guessed.
	for name, tbl := range map[string]views.BaselineTable{
		"a copy in S3":             {Schema: "public", Table: "orders", Path: "s3://bucket/2026-11-20T12-00-00Z/public/orders.parquet"},
		"a MySQL table delta":      {Schema: written.Schema, Table: written.Table, Path: written.Path, DeltaFiles: []baseline.TableDeltaFile{{}}},
		"a folder with no time":    {Schema: "public", Table: "orders", Path: filepath.Join(root, "current", "public", "orders.parquet")},
		"a file that is not there": {Schema: "public", Table: "gone", Path: filepath.Join(root, snapshotdir.Name(pgAgeDir), "public", "gone.parquet")},
	} {
		if got := through(tbl); !strings.HasPrefix(got, "unknown: ") {
			t.Errorf("%s: %s, want no instant", name, got)
		}
	}
}

// The lag a statement held to a server's limit is measured by (#3125). A
// PostgreSQL statement that reads its snapshot alone is as far behind as its
// oldest file's bound is old, by this machine's clock; one that cannot be
// placed says so; a MySQL server goes the way it always went.
func TestStatementBehindForPostgresSnapshot(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	dirAt := now.Add(-10 * time.Minute).Truncate(time.Second)
	rfc := func(d time.Duration) string { return now.Add(d).Truncate(time.Second).Format(time.RFC3339) }
	recent := pgAgeTable(t, root, dirAt, "orders", 250, rfc(-11*time.Minute))
	carried := pgAgeTable(t, root, dirAt, "countries", 40, rfc(-72*time.Hour))
	old := pgAgeTable(t, root, dirAt, "legacy", 40, "")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(true)
	srv := newRegistryServer(t)
	srv.tailCapturesHook = pgAgeCapturesAll
	pg := &bundle{pgSource: true, db: db}
	srv.cm.mu.Lock()
	srv.cm.boot = pg
	srv.cm.mu.Unlock()
	ctx := context.Background()
	behind := func(tail time.Time, allTailed bool, read ...views.BaselineTable) (time.Duration, string) {
		if !allTailed {
			// What capture lost is asked of the index for each statement.
			expectPGAgeLoss(mock)
		}
		return srv.statementBehindFor(ctx, pg, bootServerID, views.Input{Baselines: read}, read, tail, 0, allTailed)
	}
	near := func(got, want time.Duration) bool { return got >= want && got < want+time.Minute }

	if got, why := behind(time.Time{}, false, recent); why != "" || !near(got, 11*time.Minute) {
		t.Errorf("a snapshot updated ten minutes ago: %v %q, want eleven minutes (the bound sits a minute before)", got, why)
	}
	// The conservative side of a table with no change: its file's own bound.
	if got, why := behind(time.Time{}, false, recent, carried); why != "" || !near(got, 72*time.Hour) {
		t.Errorf("with a table carried unchanged for three days: %v %q, want three days", got, why)
	}
	if got, why := behind(time.Time{}, false, recent, old); why != copyAgeWhyNotPlaced || got != 0 {
		t.Errorf("with a file that records no bound: %v %q, want unknown", got, why)
	}
	// Some tables with their tails and some without: the older of the two.
	if got, why := behind(now.Add(-time.Hour), false, recent); why != "" || !near(got, time.Hour) {
		t.Errorf("a tail an hour old beside the snapshot: %v %q, want an hour", got, why)
	}
	if got, why := behind(now.Add(-time.Second), false, recent); why != "" || !near(got, 11*time.Minute) {
		t.Errorf("a tail a second old beside the snapshot: %v %q, want the snapshot's eleven minutes", got, why)
	}
	// The ledger of dropped changes cannot be read: unknown, never an age.
	mock.ExpectQuery("SELECT gap_lost_at, capture_skips FROM stream_state").WillReturnError(errors.New("connection refused"))
	if got, why := srv.statementBehindFor(ctx, pg, bootServerID, views.Input{}, []views.BaselineTable{recent}, time.Time{}, 0, false); why != copyAgeWhyNotPlaced || got != 0 {
		t.Errorf("the record of dropped changes unreadable: %v %q, want unknown", got, why)
	}
	// Every table with its tail: the path fresh mode already took, which
	// reads capture's state from the index (there is none here).
	if _, why := srv.statementBehindFor(ctx, &bundle{pgSource: true}, bootServerID, views.Input{}, []views.BaselineTable{recent}, now.Add(-time.Second), 0, true); why != copyAgeWhyNoIndex {
		t.Errorf("every table tailed: %q, want the tails' own path", why)
	}
	// The measure is kept for a caller that asked for the instant, so the
	// index is asked once for the statement.
	var kept PGSnapshotAge
	expectPGAgeLoss(mock)
	kctx := withTailsFor(WithPGSnapshotAge(ctx, &kept), bootServerID)
	if _, why := srv.statementBehindFor(kctx, pg, bootServerID, views.Input{}, []views.BaselineTable{recent}, time.Time{}, 0, false); why != "" {
		t.Fatalf("with a holder: %q", why)
	}
	srv.notePGSnapshotThrough(kctx, pg, []views.BaselineTable{recent}) // no query expected
	if kept.get().IsZero() {
		t.Error("the instant the limit measured by was not kept")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
	// The port's own listing of the copy's columns is not held to the limit,
	// whatever the files say.
	if got, why := srv.statementBehindFor(WithPGCatalogRead(ctx), pg, bootServerID, views.Input{}, []views.BaselineTable{recent, carried, old}, time.Time{}, 0, false); why != "" || got != 0 {
		t.Errorf("the port's listing of the copy's columns: %v %q, want not held", got, why)
	}
	// A MySQL server is not measured by PostgreSQL footers.
	srv.cm.mu.Lock()
	srv.cm.boot = &bundle{}
	srv.cm.mu.Unlock()
	if _, why := srv.statementBehindFor(ctx, &bundle{}, bootServerID, views.Input{Baselines: []views.BaselineTable{recent}}, []views.BaselineTable{recent}, time.Time{}, 0, false); why != copyAgeWhyNoIndex {
		t.Errorf("a MySQL server: %q, want its own path", why)
	}
}

// What the PostgreSQL router is handed for its status: the instant of the
// snapshot files a run read, only for a run that read a table without its
// recent changes, and only when every file says how far it reaches.
func TestPGSnapshotAgePlace(t *testing.T) {
	root := t.TempDir()
	written := pgAgeTable(t, root, pgAgeDir, "orders", 250, "2026-11-20T11:58:30Z")
	old := pgAgeTable(t, root, pgAgeDir, "legacy", 40, "")
	want := time.Date(2026, 11, 20, 11, 58, 30, 0, time.UTC)
	ran := pgAgeDir.Add(time.Hour)
	s := &Server{tailCapturesHook: pgAgeCapturesAll}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	pg := &bundle{pgSource: true, db: db}
	asked := func() { expectPGAgeLoss(mock) }

	var got PGSnapshotAge
	ctx := withTailsFor(WithPGSnapshotAge(context.Background(), &got), "pg")
	asked()
	s.notePGSnapshotThrough(ctx, pg, []views.BaselineTable{written})
	if a := got.Place(CopyRunAge{Ran: ran, Snapshot: true}); !a.Through.Equal(want) || !a.Snapshot || a.Tailed {
		t.Errorf("a snapshot answer: %+v, want through %s", a, want)
	}
	tailed := CopyRunAge{Ran: ran, Through: ran.Add(-time.Second), Tailed: true}
	if a := got.Place(tailed); a != tailed {
		t.Errorf("an answer with its tails was changed: %+v", a)
	}
	for name, a := range map[string]CopyRunAge{"files not told": {Ran: ran, Unknown: true}, "no table read": {}} {
		if p := got.Place(a); p != a {
			t.Errorf("%s: changed to %+v", name, p)
		}
	}

	var none PGSnapshotAge
	asked()
	s.notePGSnapshotThrough(withTailsFor(WithPGSnapshotAge(context.Background(), &none), "pg"), pg, []views.BaselineTable{written, old})
	if a := none.Place(CopyRunAge{Ran: ran, Snapshot: true}); !a.Through.IsZero() {
		t.Errorf("a file with no bound, and the answer has an instant: %+v", a)
	}
	s.notePGSnapshotThrough(WithPGSnapshotAge(context.Background(), &none), pg, nil)
	if a := none.Place(CopyRunAge{Ran: ran, Snapshot: true}); !a.Through.IsZero() {
		t.Errorf("no table read, and the answer has an instant: %+v", a)
	}
	// No holder in the context (the MySQL port, the web interface): nothing
	// is read and nothing breaks.
	s.notePGSnapshotThrough(context.Background(), pg, []views.BaselineTable{written})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
	if a := (*PGSnapshotAge)(nil).Place(CopyRunAge{Ran: ran, Snapshot: true}); !a.Through.IsZero() {
		t.Errorf("a nil holder placed an answer: %+v", a)
	}
	if WithPGSnapshotAge(context.Background(), nil) != context.Background() {
		t.Error("a nil holder changed the context")
	}
}

func pgAgeCapturesAll(string) (func(string, string) bool, bool) { return nil, true }

// expectPGAgeLoss is the one read of the index an age costs, with no loss
// and no drop on record.
func expectPGAgeLoss(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT gap_lost_at, capture_skips FROM stream_state").
		WillReturnRows(sqlmock.NewRows([]string{"gap_lost_at", "capture_skips"}).AddRow(nil, nil))
}

// What can make a file's own word about its age too young (#3125): each of
// these leaves the statement with no age, or an older one.
func TestPGReadThroughDoesNotReadYoung(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	rfc := func(d time.Duration) string { return now.Add(d).Format(time.RFC3339) }
	dirAt := now.Add(-2 * time.Minute)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	pg := &bundle{pgSource: true, db: db}
	ctx := context.Background()

	t.Run("a table outside what capture records", func(t *testing.T) {
		root := t.TempDir()
		tbl := pgAgeTable(t, root, dirAt, "orders", 250, rfc(-3*time.Minute))
		s := &Server{tailCapturesHook: func(string) (func(string, string) bool, bool) {
			return func(schema, _ string) bool { return schema == "shop" }, true
		}}
		expectPGAgeLoss(mock)
		if got, why := s.pgReadThrough(ctx, pg, "pg", []views.BaselineTable{tbl}); !got.IsZero() || !strings.Contains(why, "outside what capture records") {
			t.Errorf("%v %q, want no instant", got, why)
		}
	})
	t.Run("a table inside what capture records", func(t *testing.T) {
		root := t.TempDir()
		tbl := pgAgeTable(t, root, dirAt, "orders", 250, rfc(-3*time.Minute))
		s := &Server{tailCapturesHook: func(string) (func(string, string) bool, bool) {
			return func(schema, _ string) bool { return schema == "public" }, true
		}}
		expectPGAgeLoss(mock)
		if got, why := s.pgReadThrough(ctx, pg, "pg", []views.BaselineTable{tbl}); why != "" || got.IsZero() {
			t.Errorf("%v %q, want its instant", got, why)
		}
	})
	t.Run("what capture records is not known", func(t *testing.T) {
		root := t.TempDir()
		tbl := pgAgeTable(t, root, dirAt, "orders", 250, rfc(-3*time.Minute))
		s := &Server{tailCapturesHook: func(string) (func(string, string) bool, bool) { return nil, false }}
		// No expectation: the index is not asked.
		if got, why := s.pgReadThrough(ctx, pg, "pg", []views.BaselineTable{tbl}); !got.IsZero() || why == "" {
			t.Errorf("%v %q, want no instant", got, why)
		}
	})
	t.Run("a source clock ahead of this machine", func(t *testing.T) {
		root := t.TempDir()
		// Written 12 minutes ago here; the source's clock, 9 minutes ahead,
		// put the bound 3 minutes ago.
		tbl := pgAgeTable(t, root, dirAt, "orders", 250, rfc(-3*time.Minute))
		wrote := now.Add(-12 * time.Minute)
		if err := os.Chtimes(tbl.Path, wrote, wrote); err != nil {
			t.Fatal(err)
		}
		s := &Server{tailCapturesHook: pgAgeCapturesAll}
		expectPGAgeLoss(mock)
		got, why := s.pgReadThrough(ctx, pg, "pg", []views.BaselineTable{tbl})
		if why != "" || !got.Equal(wrote) {
			t.Errorf("%v %q, want the file's write time %v", got, why, wrote)
		}
	})
	t.Run("a file written after its bound keeps the bound", func(t *testing.T) {
		root := t.TempDir()
		tbl := pgAgeTable(t, root, dirAt, "orders", 250, rfc(-30*time.Minute))
		s := &Server{tailCapturesHook: pgAgeCapturesAll}
		expectPGAgeLoss(mock)
		if got, why := s.pgReadThrough(ctx, pg, "pg", []views.BaselineTable{tbl}); why != "" || !got.Equal(now.Add(-30*time.Minute)) {
			t.Errorf("%v %q, want the bound", got, why)
		}
	})
	noState := func() {
		mock.ExpectQuery("SELECT gap_lost_at, capture_skips FROM stream_state").
			WillReturnRows(sqlmock.NewRows([]string{"gap_lost_at", "capture_skips"}))
	}
	t.Run("no capture state, a snapshot just read", func(t *testing.T) {
		root := t.TempDir()
		tbl := pgAgeTableRead(t, root, dirAt, "orders", 250, rfc(-time.Minute), rfc(-5*time.Second))
		s := &Server{tailCapturesHook: pgAgeCapturesAll}
		noState()
		if got, why := s.pgReadThrough(ctx, pg, "pg", []views.BaselineTable{tbl}); why != "" || got.IsZero() {
			t.Errorf("%v %q, want its instant", got, why)
		}
	})
	t.Run("no capture state, read past the bound", func(t *testing.T) {
		root := t.TempDir()
		tbl := pgAgeTableRead(t, root, dirAt, "orders", 250, rfc(-time.Minute), rfc(-pgNoCaptureStateBound-time.Second))
		s := &Server{tailCapturesHook: pgAgeCapturesAll}
		noState()
		if got, why := s.pgReadThrough(ctx, pg, "pg", []views.BaselineTable{tbl}); !got.IsZero() || !strings.Contains(why, "saved no state") {
			t.Errorf("%v %q, want no instant", got, why)
		}
	})
	t.Run("no capture state, the file written past the bound", func(t *testing.T) {
		root := t.TempDir()
		tbl := pgAgeTableRead(t, root, dirAt, "orders", 250, rfc(-time.Minute), rfc(-5*time.Second))
		wrote := now.Add(-pgNoCaptureStateBound - time.Second)
		if err := os.Chtimes(tbl.Path, wrote, wrote); err != nil {
			t.Fatal(err)
		}
		s := &Server{tailCapturesHook: pgAgeCapturesAll}
		noState()
		if got, why := s.pgReadThrough(ctx, pg, "pg", []views.BaselineTable{tbl}); !got.IsZero() || !strings.Contains(why, "saved no state") {
			t.Errorf("%v %q, want no instant", got, why)
		}
	})
	t.Run("a lost slot since the read", func(t *testing.T) {
		root := t.TempDir()
		tbl := pgAgeTableRead(t, root, dirAt, "orders", 250, rfc(-time.Minute), rfc(-30*time.Second))
		s := &Server{tailCapturesHook: pgAgeCapturesAll}
		mock.ExpectQuery("SELECT gap_lost_at, capture_skips FROM stream_state").
			WillReturnRows(sqlmock.NewRows([]string{"gap_lost_at", "capture_skips"}).AddRow(now, nil))
		if got, why := s.pgReadThrough(ctx, pg, "pg", []views.BaselineTable{tbl}); !got.IsZero() || !strings.Contains(why, "lost events") {
			t.Errorf("%v %q, want no instant", got, why)
		}
	})
	t.Run("an index without the columns", func(t *testing.T) {
		root := t.TempDir()
		tbl := pgAgeTable(t, root, dirAt, "orders", 250, rfc(-3*time.Minute))
		s := &Server{tailCapturesHook: pgAgeCapturesAll}
		mock.ExpectQuery("SELECT gap_lost_at, capture_skips FROM stream_state").
			WillReturnError(&mysql.MySQLError{Number: 1054, Message: "Unknown column"})
		if got, why := s.pgReadThrough(ctx, pg, "pg", []views.BaselineTable{tbl}); !got.IsZero() || why == "" {
			t.Errorf("%v %q, want no instant", got, why)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

//go:build integration

package reconstruct_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A full-table PostgreSQL reconstruct to Parquet (#1596), against a real
// index: rows a PostgreSQL stream writes (start_pos == end_pos == the commit
// LSN, binlog_file the LSN's text), the table's shape as the stream records
// it, and a baseline the way `bintrail-pg baseline` writes it.

const pgOID = 16500

var pgOrdersRel = baseline.PGRelation{OID: pgOID, Columns: []baseline.PGColumn{
	{Name: "id", TypeOID: 23, TypeMod: -1, Type: "integer", NotNull: true, Ordinal: 1},
	{Name: "status", TypeOID: 25, TypeMod: -1, Type: "text", Ordinal: 2},
}, PrimaryKey: []string{"id"}}

type pgFixture struct {
	t    *testing.T
	db   *sql.DB
	dsn  string
	base time.Time
}

// newPGFixture is an index a PostgreSQL stream wrote: its flavor, the
// table's shape (seen before everything below), every event inside one hour
// so the planner sees no gap.
func newPGFixture(t *testing.T) *pgFixture {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	f := &pgFixture{t: t, db: db, dsn: testutil.BaseDSN() + "/" + dbName, base: time.Now().UTC().Truncate(time.Hour)}
	f.shape(0x300, pgOrdersRel.Columns...)
	// Another table's shape, recorded after: a PostgreSQL stream writes one
	// schema snapshot per table, so the newest one alone does not describe
	// orders.
	other := &metadata.PGRelationSchema{OID: pgOID + 1, Schema: "public", Table: "items", SeenLSN: 0x310, SeenAt: f.base,
		Columns: []metadata.PGRelationColumn{{Name: "id", Ordinal: 1, IsPK: true, TypeOID: 23, TypeMod: -1}}}
	if _, err := metadata.WritePGSnapshot(ctx, db, other); err != nil {
		t.Fatalf("WritePGSnapshot: %v", err)
	}
	return f
}

func (f *pgFixture) at(d time.Duration) time.Time { return f.base.Add(d) }

// shape records the table's shape as the stream saw it at lsn.
func (f *pgFixture) shape(lsn uint64, cols ...baseline.PGColumn) {
	f.t.Helper()
	rel := &metadata.PGRelationSchema{OID: pgOID, Schema: "public", Table: "orders", SeenLSN: lsn, SeenAt: f.base}
	for i, c := range cols {
		rel.Columns = append(rel.Columns, metadata.PGRelationColumn{Name: c.Name, Ordinal: i + 1, IsPK: c.Name == "id", TypeOID: c.TypeOID, TypeMod: c.TypeMod})
	}
	if _, err := metadata.WritePGSnapshot(context.Background(), f.db, rel); err != nil {
		f.t.Fatalf("WritePGSnapshot: %v", err)
	}
}

// event writes one row of the transaction committed at lsn, at d into the
// hour. after nil = a DELETE.
func (f *pgFixture) event(lsn uint64, d time.Duration, typ uint8, id string, after *string) {
	f.t.Helper()
	var img []byte
	if after != nil {
		img, _ = json.Marshal(map[string]string{"id": id, "status": *after})
	}
	before := []byte(fmt.Sprintf(`{"id":"%s","status":"x"}`, id))
	if typ == 1 {
		before = nil
	}
	testutil.InsertEvent(f.t, f.db, lsnString(lsn), lsn, lsn, f.at(d).Format("2006-01-02 15:04:05"), nil,
		"public", "orders", typ, id, nil, before, img)
}

// checkpoint is the stream's checkpoint: the commit LSN of the last complete
// transaction.
func (f *pgFixture) checkpoint(lsn uint64) {
	f.t.Helper()
	testutil.MustExec(f.t, f.db, fmt.Sprintf(`INSERT INTO stream_state (id, mode, binlog_file, binlog_position, flavor, events_indexed, last_checkpoint, server_id)
		VALUES (1, 'gtid', '%s', %d, 'postgres', 1, UTC_TIMESTAMP(), 1)
		ON DUPLICATE KEY UPDATE binlog_file = VALUES(binlog_file), binlog_position = VALUES(binlog_position)`, lsnString(lsn), lsn))
}

func lsnString(lsn uint64) string { return fmt.Sprintf("%X/%X", lsn>>32, uint32(lsn)) }

func s(v string) *string { return &v }

// baseline writes a source snapshot taken at at, with floor as its LSN and
// deltaSince ("" = none) as its bound, holding rows (id, status).
func (f *pgFixture) baseline(root string, at time.Time, floor uint64, deltaSince string, rows ...[2]string) {
	f.t.Helper()
	rel, err := pgOrdersRel.Encode()
	if err != nil {
		f.t.Fatal(err)
	}
	md := map[string]string{
		baseline.MetaKeySnapshotTimestamp: at.Format(time.RFC3339),
		baseline.MetaKeyLSN:               fmt.Sprint(floor),
		baseline.MetaKeyPGRelation:        rel,
		baseline.MetaKeyRenderGUCs:        baseline.RenderGUCsPinned,
		baseline.MetaKeyLockMode:          baseline.LockStampPGRepeatableRead,
		baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
	}
	if deltaSince != "" {
		md[baseline.MetaKeyPGDeltaSince] = deltaSince
	}
	path := filepath.Join(root, reconstruct.SnapshotDirName(at), "public", "orders.parquet")
	w, err := baseline.NewWriter(path, []baseline.Column{{Name: "id", RawText: true}, {Name: "status", RawText: true}},
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: md})
	if err != nil {
		f.t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.WriteRow([]string{r[0], r[1]}, []bool{false, false}); err != nil {
			f.t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		f.t.Fatal(err)
	}
}

type pgRun struct {
	at        time.Time
	format    string
	carry     bool
	deltas    bool
	tables    []string
	allowGaps bool
}

// run folds root's snapshots to r.at into root, and returns the table's
// rows and footer.
func (f *pgFixture) run(root string, r pgRun) (map[string]string, baseline.DumpMetadata, []*reconstruct.TableReport, error) {
	f.t.Helper()
	format := r.format
	if format == "" {
		format = reconstruct.OutputFormatParquet
	}
	tables := r.tables
	if tables == nil {
		tables = []string{"public.orders"}
	}
	out := root
	if format == reconstruct.OutputFormatMydumper {
		out = f.t.TempDir()
	}
	reps, err := reconstruct.ReconstructTables(context.Background(), reconstruct.FullTableConfig{
		IndexDSN: f.dsn, BaselineSrc: root, Tables: tables, At: r.at, OutputDir: out,
		OutputFormat: format, CarryForwardUnchanged: r.carry, TableDeltas: r.deltas, Parallelism: 1, AllowGaps: r.allowGaps,
	})
	if err != nil {
		return nil, baseline.DumpMetadata{}, reps, err
	}
	path := filepath.Join(root, reconstruct.SnapshotDirName(r.at), "public", "orders.parquet")
	md, err := baseline.ReadParquetMetadata(path)
	if err != nil {
		f.t.Fatalf("read the folded footer: %v", err)
	}
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		f.t.Fatal(err)
	}
	defer duck.Close()
	rs, err := duck.Query(`SELECT id, status FROM parquet_scan('` + path + `')`)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rs.Close()
	got := map[string]string{}
	for rs.Next() {
		var id, st string
		if err := rs.Scan(&id, &st); err != nil {
			f.t.Fatal(err)
		}
		got[id] = st
	}
	return got, md, reps, rs.Err()
}

func sameRows(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// The fold reads from the snapshot's floor, not its time: a commit that
// raced the snapshot's start is applied, one below the floor is not, a
// transaction is applied whole, and one past the point is left to the next
// fold, which starts where this one stopped.
func TestReconstructTables_PGParquetChain(t *testing.T) {
	f := newPGFixture(t)
	snap := f.at(10 * time.Minute)
	f.event(0x400, 8*time.Minute, 1, "9", s("ghost")) // below the floor
	// Below the floor with a time after the snapshot's: only the LSN keeps
	// it out.
	f.event(0x480, 9*time.Minute+45*time.Second, 1, "8", s("ghost"))
	f.event(0x600, 9*time.Minute+30*time.Second, 2, "1", s("racer")) // raced the snapshot
	f.event(0x700, 15*time.Minute, 1, "3", s("newbie"))
	f.event(0x700, 15*time.Minute, 3, "2", nil) // the same transaction
	f.event(0x900, 40*time.Minute, 2, "1", s("late"))
	f.checkpoint(0x900)

	root := t.TempDir()
	f.baseline(root, snap, 0x500, "", [2]string{"1", "paid"}, [2]string{"2", "open"}, [2]string{"5", "keep"})

	at1 := f.at(30 * time.Minute)
	// The read starts at the snapshot's bound and stays there: it does not
	// search older partitions for changes after the floor (#2138's
	// settling), which on PostgreSQL would read every archive.
	loads := query.PictureLoads()
	// The values are PostgreSQL's text: nothing to decode, nothing to warn
	// about.
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	got, md, _, err := f.run(root, pgRun{at: at1})
	slog.SetDefault(prev)
	if err != nil {
		t.Fatalf("fold to %s: %v", at1, err)
	}
	if !strings.Contains(logs.String(), "table reconstructed") || strings.Contains(logs.String(), "BASE64") {
		t.Errorf("the fold's log:\n%s", logs.String())
	}
	if n := query.PictureLoads() - loads; n != 0 {
		t.Errorf("the fold loaded %d partition pictures, want 0", n)
	}
	if tables, err := reconstruct.NewestSnapshotTables(context.Background(), root); err != nil || len(tables) != 1 || tables[0] != "public.orders" {
		t.Errorf("what a refresh finds to fold next: %v %v", tables, err)
	}
	if want := map[string]string{"1": "racer", "3": "newbie", "5": "keep"}; !sameRows(got, want) {
		t.Fatalf("rows at %s = %v, want %v", at1, got, want)
	}
	// Stopped right before the first change past the point; the bound a
	// minute before the point, since a change past it was found.
	if md.LSN != 0x900 || md.BinlogFile != "" || md.PGRelation == "" || md.RenderGUCs != baseline.RenderGUCsPinned {
		t.Fatalf("footer: LSN %X binlog %q rel %q gucs %q", md.LSN, md.BinlogFile, md.PGRelation, md.RenderGUCs)
	}
	if want := at1.Add(-reconstruct.PGDeltaMargin).Format(time.RFC3339); md.PGDeltaSince != want {
		t.Fatalf("delta since = %q, want %q", md.PGDeltaSince, want)
	}

	// The next fold, from the folded snapshot: the change it left.
	at2 := f.at(50 * time.Minute)
	got, md, _, err = f.run(root, pgRun{at: at2})
	if err != nil {
		t.Fatalf("fold to %s: %v", at2, err)
	}
	if want := map[string]string{"1": "late", "3": "newbie", "5": "keep"}; !sameRows(got, want) {
		t.Fatalf("rows at %s = %v, want %v", at2, got, want)
	}
	if md.LSN != 0x901 {
		t.Fatalf("second footer LSN %X, want 901", md.LSN)
	}
}

// Capture was behind when the snapshot was folded: a change committed
// before the point reaches the index only afterwards, with an LSN past the
// folded snapshot's. The folded snapshot's bound reaches it, so the next fold
// applies it. Hours apart, because a read by position prunes by time only to
// the hour (query.CoarseSinceFloor); the empty hours between are gaps the
// runs accept.
func TestReconstructTables_PGCaptureBehind(t *testing.T) {
	f := newPGFixture(t)
	snap := f.at(10 * time.Minute)
	f.event(0x700, 15*time.Minute, 2, "1", s("early"))
	f.checkpoint(0x700)
	root := t.TempDir()
	f.baseline(root, snap, 0x500, "", [2]string{"1", "paid"})

	at1 := f.at(3*time.Hour + 30*time.Minute)
	if _, md, _, err := f.run(root, pgRun{at: at1, allowGaps: true}); err != nil {
		t.Fatal(err)
	} else if want := f.at(14 * time.Minute).Format(time.RFC3339); md.LSN != 0x701 || md.PGDeltaSince != want {
		t.Fatalf("footer LSN %X since %q, want 701 and %q", md.LSN, md.PGDeltaSince, want)
	}
	// Delivered late: committed at 20 minutes, before the first fold's
	// point.
	f.event(0x800, 20*time.Minute, 2, "1", s("delivered-late"))
	f.checkpoint(0x800)
	got, _, _, err := f.run(root, pgRun{at: f.at(4 * time.Hour), allowGaps: true})
	if err != nil {
		t.Fatal(err)
	}
	if got["1"] != "delivered-late" {
		t.Fatalf("rows = %v: the late change was dropped", got)
	}
}

// A window holding one transaction at exactly the floor folds it: on
// PostgreSQL every row of a transaction carries its LSN in both positions,
// so a window whose end equals its start is not empty. A table with no
// change is rewritten, never carried forward or written as a delta: the
// previous file's footer holds the previous bound.
func TestReconstructTables_PGWindowAtTheFloor(t *testing.T) {
	f := newPGFixture(t)
	snap := f.at(10 * time.Minute)
	f.event(0x700, 12*time.Minute, 2, "1", s("at-the-floor"))
	f.checkpoint(0x700)
	root := t.TempDir()
	f.baseline(root, snap, 0x700, "", [2]string{"1", "paid"})
	at1 := f.at(20 * time.Minute)
	got, md, _, err := f.run(root, pgRun{at: at1, carry: true, deltas: true})
	if err != nil {
		t.Fatal(err)
	}
	if got["1"] != "at-the-floor" || md.LSN != 0x701 {
		t.Fatalf("rows %v LSN %X", got, md.LSN)
	}
	// Nothing new: rewritten, with the bound it was read with.
	firstSince := md.PGDeltaSince
	at2 := f.at(25 * time.Minute)
	got, md, reps, err := f.run(root, pgRun{at: at2, carry: true, deltas: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(reps) != 1 || reps[0].CarriedForward || got["1"] != "at-the-floor" || md.LSN != 0x701 || md.PGDeltaSince != firstSince {
		t.Fatalf("unchanged table: reps %+v rows %v LSN %X since %q, want since %q", reps, got, md.LSN, md.PGDeltaSince, firstSince)
	}
}

// A transaction the stream has not checkpointed may be in the index in
// part: the fold leaves it whole to the next one, even when its time is
// before the point.
func TestReconstructTables_PGUncheckpointedTransaction(t *testing.T) {
	f := newPGFixture(t)
	f.event(0x700, 15*time.Minute, 2, "1", s("done"))
	f.event(0x800, 20*time.Minute, 2, "2", s("in-flight"))
	f.checkpoint(0x700)
	root := t.TempDir()
	f.baseline(root, f.at(10*time.Minute), 0x500, "", [2]string{"1", "paid"}, [2]string{"2", "open"})
	got, md, _, err := f.run(root, pgRun{at: f.at(30 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]string{"1": "done", "2": "open"}; !sameRows(got, want) || md.LSN != 0x800 {
		t.Fatalf("rows %v LSN %X, want %v at 800", got, md.LSN, want)
	}
	f.checkpoint(0x800)
	if got, _, _, err = f.run(root, pgRun{at: f.at(40 * time.Minute)}); err != nil || got["2"] != "in-flight" {
		t.Fatalf("once checkpointed: rows %v, %v", got, err)
	}
}

// What the fold cannot see refuses: a TRUNCATE that raced the snapshot
// (no row events, so the fold would bring its rows back), and a column
// added in the window. A TRUNCATE before the bound is the snapshot's own
// history.
func TestReconstructTables_PGRefusals(t *testing.T) {
	ddl := func(f *pgFixture, d time.Duration) {
		testutil.MustExec(t, f.db, fmt.Sprintf(`INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
			VALUES ('%s', '0/650', 1616, 'public', 'orders', 'TRUNCATE TABLE', 'TRUNCATE public.orders')`, f.at(d).Format("2006-01-02 15:04:05")))
	}
	setup := func(t *testing.T) (*pgFixture, string) {
		f := newPGFixture(t)
		f.event(0x700, 15*time.Minute, 2, "1", s("x"))
		f.checkpoint(0x700)
		root := t.TempDir()
		f.baseline(root, f.at(10*time.Minute), 0x500, "", [2]string{"1", "paid"})
		return f, root
	}
	t.Run("a TRUNCATE that raced the snapshot", func(t *testing.T) {
		f, root := setup(t)
		ddl(f, 9*time.Minute+30*time.Second)
		if _, _, _, err := f.run(root, pgRun{at: f.at(30 * time.Minute)}); !errors.Is(err, reconstruct.ErrDestructiveDDL) {
			t.Fatalf("err = %v, want the destructive-DDL refusal", err)
		}
	})
	t.Run("a TRUNCATE before the bound", func(t *testing.T) {
		f, root := setup(t)
		ddl(f, 5*time.Minute)
		if _, _, _, err := f.run(root, pgRun{at: f.at(30 * time.Minute)}); err != nil {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("a column added in the window", func(t *testing.T) {
		f, root := setup(t)
		f.shape(0x680, append(pgOrdersRel.Columns, baseline.PGColumn{Name: "note", TypeOID: 25, TypeMod: -1})...)
		if _, _, _, err := f.run(root, pgRun{at: f.at(30 * time.Minute)}); err == nil || !strings.Contains(err.Error(), "added note") {
			t.Fatalf("err = %v, want the shape refusal", err)
		}
	})
	// Same columns and types, another key, recorded after the point: the
	// rows would be joined by a key the changes do not name.
	t.Run("a primary key changed after the point", func(t *testing.T) {
		f, root := setup(t)
		rel := &metadata.PGRelationSchema{OID: pgOID, Schema: "public", Table: "orders", SeenLSN: 0x9000, SeenAt: f.at(50 * time.Minute),
			Columns: []metadata.PGRelationColumn{{Name: "id", Ordinal: 1, TypeOID: 23, TypeMod: -1}, {Name: "status", Ordinal: 2, IsPK: true, TypeOID: 25, TypeMod: -1}}}
		if _, err := metadata.WritePGSnapshot(context.Background(), f.db, rel); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := f.run(root, pgRun{at: f.at(30 * time.Minute)}); err == nil || !strings.Contains(err.Error(), "primary key is now (status)") {
			t.Fatalf("err = %v, want the key refusal", err)
		}
	})
	t.Run("a mydumper dump", func(t *testing.T) {
		f, root := setup(t)
		if _, _, _, err := f.run(root, pgRun{at: f.at(30 * time.Minute), format: reconstruct.OutputFormatMydumper}); err == nil || !strings.Contains(err.Error(), "--output-format parquet") {
			t.Fatalf("err = %v, want the mydumper refusal", err)
		}
	})
	t.Run("a table with no snapshot, to a mydumper dump", func(t *testing.T) {
		f, root := setup(t)
		if _, _, _, err := f.run(root, pgRun{at: f.at(30 * time.Minute), format: reconstruct.OutputFormatMydumper, tables: []string{"public.items"}}); err == nil || !strings.Contains(err.Error(), "--output-format parquet") {
			t.Fatalf("err = %v, want the mydumper refusal", err)
		}
	})
	t.Run("a table with no snapshot", func(t *testing.T) {
		f, root := setup(t)
		if _, _, _, err := f.run(root, pgRun{at: f.at(30 * time.Minute), tables: []string{"public.items"}}); err == nil || !strings.Contains(err.Error(), "bintrail-pg baseline") {
			t.Fatalf("err = %v, want the no-snapshot refusal", err)
		}
	})
}

// The same fold written as SQL psql loads (#1596 slice 2): the CREATE TABLE
// from the snapshot's recorded shape, the folded rows as COPY text, one
// transaction. Nothing is published as a snapshot.
func TestReconstructTables_PGPsqlOutput(t *testing.T) {
	f := newPGFixture(t)
	snap := f.at(10 * time.Minute)
	f.event(0x600, 9*time.Minute+30*time.Second, 2, "1", s("tab\there\\x"))
	f.event(0x700, 15*time.Minute, 1, "3", s("newbie"))
	f.event(0x700, 15*time.Minute, 3, "2", nil)
	f.checkpoint(0x700)
	root := t.TempDir()
	f.baseline(root, snap, 0x500, "", [2]string{"1", "paid"}, [2]string{"2", "open"}, [2]string{"5", "keep"})
	out := t.TempDir()
	// The values are PostgreSQL's text: nothing to decode, nothing to warn
	// about.
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	reps, err := reconstruct.ReconstructTables(context.Background(), reconstruct.FullTableConfig{
		IndexDSN: f.dsn, BaselineSrc: root, Tables: []string{"public.orders"}, At: f.at(30 * time.Minute),
		OutputDir: out, OutputFormat: reconstruct.OutputFormatPsql, Parallelism: 1,
	})
	slog.SetDefault(prev)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "table reconstructed") {
		t.Errorf("the run's log was not captured:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), "BASE64") {
		t.Errorf("a PostgreSQL fold warned about base64:\n%s", logs.String())
	}
	if len(reps) != 1 || len(reps[0].Files) != 1 || reps[0].Files[0] != "public.orders.sql" || reps[0].RowsWritten != 3 {
		t.Fatalf("reports = %+v", reps[0])
	}
	b, err := os.ReadFile(filepath.Join(out, "public.orders.sql"))
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	t.Logf("\n%s", got)
	for _, want := range []string{
		"BEGIN;\nSET client_encoding = 'UTF8';\nSET TimeZone = 'UTC';",
		"CREATE TABLE \"public\".\"orders\" (\n  \"id\" integer NOT NULL,\n  \"status\" text,\n  PRIMARY KEY (\"id\")\n);\n",
		"COPY \"public\".\"orders\" (\"id\", \"status\") FROM stdin;\n",
		"1\ttab\\there\\\\x\n",
		"3\tnewbie\n",
		"5\tkeep\n",
		"\\.\nCOMMIT;\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q", want)
		}
	}
	if strings.Contains(got, "2\topen") {
		t.Error("the deleted row is in the output")
	}
	// The report locates the rows by position, for a loader that cannot
	// hand the file to psql.
	copyLine := "COPY \"public\".\"orders\" (\"id\", \"status\") FROM stdin;\n"
	pc := reps[0].PsqlCopy
	if pc == nil || pc.Statement+";\n" != copyLine {
		t.Fatalf("PsqlCopy = %+v", pc)
	}
	start := int64(strings.Index(got, copyLine) + len(copyLine))
	end := int64(strings.LastIndex(got, "\\.\n"))
	if pc.DataStart != start || pc.DataEnd != end || strings.Count(got[pc.DataStart:pc.DataEnd], "\n") != 3 {
		t.Errorf("PsqlCopy = %+v, want data [%d, %d): %q", pc, start, end, got[pc.DataStart:pc.DataEnd])
	}
	if entries, _ := os.ReadDir(root); len(entries) != 1 {
		t.Errorf("a psql run published a snapshot: %v", entries)
	}
}

// psql output is PostgreSQL's SQL: refused on an index that records no
// PostgreSQL source.
func TestReconstructTables_PsqlNeedsPostgres(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, 2, false, nil); err != nil {
		t.Fatal(err)
	}
	_, err := reconstruct.ReconstructTables(context.Background(), reconstruct.FullTableConfig{
		IndexDSN: testutil.BaseDSN() + "/" + dbName, BaselineSrc: t.TempDir(), Tables: []string{"shop.orders"},
		At: time.Now().UTC(), OutputDir: t.TempDir(), OutputFormat: reconstruct.OutputFormatPsql,
	})
	if !errors.Is(err, reconstruct.ErrPsqlNeedsPostgres) {
		t.Fatalf("err = %v", err)
	}
}

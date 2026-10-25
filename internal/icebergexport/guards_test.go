package icebergexport

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

func TestBinlogBefore(t *testing.T) {
	cases := []struct {
		aFile string
		aPos  uint64
		bFile string
		bPos  uint64
		want  bool
	}{
		{"binlog.000001", 100, "binlog.000001", 200, true},
		{"binlog.000001", 200, "binlog.000001", 200, false},
		{"binlog.000001", 300, "binlog.000001", 200, false},
		{"binlog.000099", 999999, "binlog.000100", 4, true},   // lexical within one length
		{"binlog.999999", 4, "binlog.1000000", 4, true},       // shorter name sorts first
		{"binlog.1000000", 4, "binlog.999999", 999999, false}, // and the reverse
	}
	for _, tc := range cases {
		if got := binlogBefore(tc.aFile, tc.aPos, tc.bFile, tc.bPos); got != tc.want {
			t.Errorf("binlogBefore(%s:%d, %s:%d) = %v, want %v", tc.aFile, tc.aPos, tc.bFile, tc.bPos, got, tc.want)
		}
	}
}

func TestClassify_onSentinels(t *testing.T) {
	cases := []struct {
		err  error
		want Verdict
	}{
		{&query.GapError{}, VerdictRefusedGap},
		{errors.Join(errors.New("wrapped"), reconstruct.ErrCaptureGap), VerdictRefusedGap},
		{errors.Join(errors.New("wrapped"), reconstruct.ErrSchemaChanged), VerdictRefusedDDL},
		{errors.Join(errors.New("wrapped"), reconstruct.ErrDestructiveDDL), VerdictRefusedDDL},
		{errors.New("capture gap in the message text only"), VerdictRefused},
	}
	for _, tc := range cases {
		if got := classify(tc.err); got != tc.want {
			t.Errorf("classify(%v) = %s, want %s", tc.err, got, tc.want)
		}
	}
	o := refusal("shop", "orders", errors.New("x"))
	if o.Err == nil || o.Detail != "x" {
		t.Fatalf("refusal must carry Err and Detail: %+v", o)
	}
}

func TestColumnFromMeta(t *testing.T) {
	c := columnFromMeta(metadata.ColumnMeta{Name: "amount", DataType: "decimal", ColumnType: "decimal(12,4)"})
	if c.MySQLType != "decimal" || c.DecimalPrecision != 12 || c.DecimalScale != 4 {
		t.Fatalf("decimal(12,4) parsed as %+v", c)
	}
	c = columnFromMeta(metadata.ColumnMeta{Name: "n", DataType: "int", ColumnType: "int(10) unsigned"})
	if !c.Unsigned {
		t.Fatal("int unsigned not detected")
	}
	c = columnFromMeta(metadata.ColumnMeta{Name: "d", DataType: "decimal", ColumnType: "decimal"})
	if c.DecimalPrecision != 10 || c.DecimalScale != 0 {
		t.Fatalf("bare decimal should be (10,0), got (%d,%d)", c.DecimalPrecision, c.DecimalScale)
	}
}

func TestSameTableTypes_refusesTypeOnlyAlter(t *testing.T) {
	cols, err := buildColumns([]baseline.Column{
		{Name: "id", MySQLType: "int"},
		{Name: "amount", MySQLType: "decimal", DecimalPrecision: 10, DecimalScale: 2},
	}, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	same := &metadata.TableMeta{Columns: []metadata.ColumnMeta{
		{Name: "id", DataType: "int", ColumnType: "int"},
		{Name: "amount", DataType: "decimal", ColumnType: "decimal(10,2)"},
	}}
	if err := sameTableTypes(cols, same, "shop", "orders"); err != nil {
		t.Fatalf("unchanged types refused: %v", err)
	}
	// Scale grew: Arrow would rescale 12.3456 to 12.35 without a word.
	widened := &metadata.TableMeta{Columns: []metadata.ColumnMeta{
		{Name: "id", DataType: "int", ColumnType: "int"},
		{Name: "amount", DataType: "decimal", ColumnType: "decimal(12,4)"},
	}}
	err = sameTableTypes(cols, widened, "shop", "orders")
	if !errors.Is(err, reconstruct.ErrSchemaChanged) || !strings.Contains(err.Error(), "decimal(12,4)") {
		t.Fatalf("err = %v, want ErrSchemaChanged naming decimal(12,4)", err)
	}
	// A kind change is the same refusal.
	retyped := &metadata.TableMeta{Columns: []metadata.ColumnMeta{
		{Name: "id", DataType: "varchar", ColumnType: "varchar(20)"},
		{Name: "amount", DataType: "decimal", ColumnType: "decimal(10,2)"},
	}}
	if err := sameTableTypes(cols, retyped, "shop", "orders"); !errors.Is(err, reconstruct.ErrSchemaChanged) {
		t.Fatalf("err = %v, want ErrSchemaChanged", err)
	}
	// A pre-#212 snapshot row has no COLUMN_TYPE to compare: not a refusal.
	untyped := &metadata.TableMeta{Columns: []metadata.ColumnMeta{{Name: "id", DataType: "int"}, {Name: "amount", DataType: "decimal"}}}
	if err := sameTableTypes(cols, untyped, "shop", "orders"); err != nil {
		t.Fatalf("untyped snapshot refused: %v", err)
	}
}

// TestSameShape: an Iceberg table that exists without a cursor is reused
// only if it is, column for column, what this export would create.
func TestSameShape(t *testing.T) {
	_, tbl, cols := newTestTable(t, ordersCols, []string{"id"})
	if err := sameShape(tbl.Schema(), cols); err != nil {
		t.Fatalf("the table this export created is refused: %v", err)
	}
	rescaled, err := buildColumns([]baseline.Column{
		{Name: "id", MySQLType: "bigint"},
		{Name: "status", MySQLType: "varchar"},
		{Name: "amount", MySQLType: "decimal", DecimalPrecision: 12, DecimalScale: 4},
		{Name: "updated_at", MySQLType: "datetime"},
	}, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	// Same names, same Arrow type ID: only the scale differs, and Arrow would
	// rescale 12.3456 to 12.35 on append without a word.
	if err := sameShape(tbl.Schema(), rescaled); err == nil || !strings.Contains(err.Error(), "decimal(12,4)") {
		t.Fatalf("err = %v, want the scale refusal naming decimal(12,4)", err)
	}
	swapped, err := buildColumns([]baseline.Column{
		{Name: "id", MySQLType: "bigint"},
		{Name: "amount", MySQLType: "decimal", DecimalPrecision: 10, DecimalScale: 2},
		{Name: "status", MySQLType: "varchar"},
		{Name: "updated_at", MySQLType: "datetime"},
	}, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	if err := sameShape(tbl.Schema(), swapped); err == nil || !strings.Contains(err.Error(), `named "status" where the export has "amount"`) {
		t.Fatalf("err = %v, want the order refusal", err)
	}
	otherKey, err := buildColumns(ordersCols, []string{"status"})
	if err != nil {
		t.Fatal(err)
	}
	if err := sameShape(tbl.Schema(), otherKey); err == nil || !strings.Contains(err.Error(), "identifier fields") {
		t.Fatalf("err = %v, want the key refusal", err)
	}
}

func TestCursor_newerVersionRefused(t *testing.T) {
	c := cursor{File: "binlog.000001", Pos: 4, At: time.Now().UTC()}
	props := c.properties()
	props[propVersion] = "2"
	_, err := readCursor(props)
	if err == nil || !strings.Contains(err.Error(), "newer export") {
		t.Fatalf("err = %v, want a newer-version refusal", err)
	}
	props[propVersion] = exportVersion
	if _, err := readCursor(props); err != nil {
		t.Fatalf("current version refused: %v", err)
	}
}

func TestCursor_originRoundTrip(t *testing.T) {
	c := cursor{File: "binlog.000001", Pos: 100, At: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC), FromBaseline: true}
	got, err := readCursor(c.loadProperties("/some/baseline.parquet"))
	if err != nil || got == nil || !got.FromBaseline {
		t.Fatalf("a load cursor must read back as FromBaseline: %+v (%v)", got, err)
	}
	got, err = readCursor(cursor{File: "binlog.000001", Pos: 200, At: c.At}.properties())
	if err != nil || got == nil || got.FromBaseline {
		t.Fatalf("a delta cursor must not read back as FromBaseline: %+v (%v)", got, err)
	}
}

func TestFirstLoad_refusesTableWithDataAndNoCursor(t *testing.T) {
	_, tbl, cols := newTestTable(t, ordersCols, []string{"id"})
	// Data without a cursor: a table this export did not write.
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	ops := foldOps(t, ordersPK, ev(1, event.EventInsert, "1", nil, orderRow(1, "x", "1.00", "2026-08-28 12:00:00")))
	var files []iceberg.DataFile
	for df, err := range table.WriteRecords(context.Background(), tbl, arrowSchema, upsertBatches(memory.DefaultAllocator, arrowSchema, cols, ops)) {
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, df)
	}
	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddRows(files...)
	if err := rd.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	tbl, err = tx.Commit(withCommitBase(context.Background(), tbl, nil))
	if err != nil {
		t.Fatal(err)
	}
	if cur, err := readCursor(tbl.Properties()); err != nil || cur != nil {
		t.Fatalf("precondition: table must have no cursor, got %v (%v)", cur, err)
	}

	d := &deps{cfg: Config{BaselineSrc: t.TempDir()}, mem: memory.DefaultAllocator}
	_, _, _, err = d.firstLoad(context.Background(), "shop", "orders", &metadata.TableMeta{}, ordersPK,
		catalog.ToIdentifier("shop", "orders"), tbl)
	if err == nil || !strings.Contains(err.Error(), "holds data but no export cursor") {
		t.Fatalf("err = %v, want the data-without-cursor refusal (a reload would duplicate every row)", err)
	}
}

// TestWriteBaselineRows_fixedBinaryIsTrimmedAndKeyed: a fixed BINARY(n)
// column is stored padded in the baseline and unpadded in the row events.
// The first load must trim BOTH the key (or the equality delete never
// matches) and any other BINARY(n) column (or the table carries two
// spellings of one value depending on which run wrote the row).
func TestWriteBaselineRows_fixedBinaryIsTrimmedAndKeyed(t *testing.T) {
	createSQL := "CREATE TABLE `keys` (\n  `k` binary(8) NOT NULL,\n  `tag` binary(4) DEFAULT NULL,\n  `v` varchar(10) DEFAULT NULL,\n  PRIMARY KEY (`k`)\n) ENGINE=InnoDB;\n"
	bcols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.parquet")
	w, err := baseline.NewWriter(path, bcols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatal(err)
	}
	// "abc" padded to 8 bytes, "ab" padded to 4: what a dump of BINARY holds.
	if err := w.WriteRow([]string{"0x6162630000000000", "0x61620000", "one"}, []bool{false, false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"0x6465660000000000", "0x64650000", "two"}, []bool{false, false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	pk := []metadata.ColumnMeta{{Name: "k", IsPK: true, DataType: "binary", ColumnType: "binary(8)"}}
	ctx := context.Background()
	cat, release, err := openWarehouse(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	if err := ensureNamespace(ctx, cat, "shop"); err != nil {
		t.Fatal(err)
	}
	cols, err := buildColumns(bcols, []string{"k"})
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := cat.CreateTable(ctx, catalog.ToIdentifier("shop", "keys"), icebergSchema(cols), catalog.WithProperties(tableProperties()))
	if err != nil {
		t.Fatal(err)
	}
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	d := &deps{cfg: Config{}, mem: memory.DefaultAllocator}
	files, rows, err := d.writeBaselineRows(ctx, tbl, arrowSchema, cols, pk, path)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 2 || len(files) == 0 {
		t.Fatalf("rows = %d, files = %d", rows, len(files))
	}
	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddRows(files...)
	if err := rd.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	tbl, err = tx.Commit(withCommitBase(ctx, tbl, nil))
	if err != nil {
		t.Fatal(err)
	}

	got := binaryCells(t, tbl, "k", "tag")
	if got["abc"] != "ab" || got["def"] != "de" {
		t.Fatalf("loaded cells = %v, want k and tag both trimmed of their padding", got)
	}

	// The row event names the key WITHOUT padding: the equality delete must
	// match the loaded row.
	ops := foldOps(t, pk, ev(1, event.EventDelete, "abc", map[string]any{"k": []byte("abc"), "tag": []byte("ab"), "v": "one"}, nil))
	tbl = commit(t, tbl, cols, ops, cursor{File: "binlog.000001", Pos: 100, At: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)})
	got = binaryCells(t, tbl, "k", "tag")
	if _, still := got["abc"]; still || len(got) != 1 {
		t.Fatalf("after deleting key abc: %v, want only def", got)
	}
}

// binaryCells reads two binary columns as a map keyed by the first.
func binaryCells(t *testing.T, tbl *table.Table, keyCol, valCol string) map[string]string {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer at.Release()
	out := map[string]string{}
	tr := array.NewTableReader(at, 1024)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		ki, vi := -1, -1
		for i, f := range rec.Schema().Fields() {
			switch f.Name {
			case keyCol:
				ki = i
			case valCol:
				vi = i
			}
		}
		for i := 0; i < int(rec.NumRows()); i++ {
			out[string(rec.Column(ki).(*array.Binary).Value(i))] = string(rec.Column(vi).(*array.Binary).Value(i))
		}
	}
	return out
}

func TestAppendValue_timeTypesFromNonUTCLocation(t *testing.T) {
	loc, err := time.LoadLocation("America/Bogota")
	if err != nil {
		t.Skip("zone database unavailable")
	}
	// A time.Time is an instant; the column stores its UTC wall clock.
	local := time.Date(2026, 8, 28, 7, 34, 56, 0, loc) // 12:34:56 UTC
	got, err := buildOne(t, baseline.Column{Name: "c", MySQLType: "datetime"}, local)
	if err != nil {
		t.Fatal(err)
	}
	if got != "2026-08-28 12:34:56" {
		t.Fatalf("datetime from a Bogota time.Time = %q, want the UTC wall clock", got)
	}
	got, err = buildOne(t, baseline.Column{Name: "c", MySQLType: "date"}, time.Date(2026, 8, 28, 23, 30, 0, 0, loc))
	if err != nil {
		t.Fatal(err)
	}
	if got != "2026-08-29" {
		t.Fatalf("date from a Bogota 23:30 = %q, want the UTC date 2026-08-29", got)
	}
}

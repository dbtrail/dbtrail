package reconstruct

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2207: the merge reads the base a page of rows at a time, because duckdb-go
// holds a query's whole result in memory. Paging must change nothing a reader
// can see: the same rows, in the same order, into a file with the same bytes.

func withScanPage(t *testing.T, n int64) {
	t.Helper()
	prev := scanPageRows
	scanPageRows = n
	t.Cleanup(func() { scanPageRows = prev })
}

// writeZooRows writes n zoo rows (ids 1..n) with row groups of rowGroup rows,
// under dir.
func writeZooRows(t *testing.T, dir string, n, rowGroup int, createSQL string) string {
	t.Helper()
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "orders.parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: rowGroup,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL, baseline.MetaKeyBinlogFile: "binlog.000007", baseline.MetaKeyBinlogPos: "4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	vals := make([]string, len(cols))
	nulls := make([]bool, len(cols))
	for id := 1; id <= n; id++ {
		for i, c := range cols {
			switch c.Name {
			case "id":
				vals[i] = strconv.Itoa(id)
			case "name":
				vals[i] = "row-" + strconv.Itoa(id)
			default:
				vals[i], nulls[i] = "", true
			}
		}
		if err := w.WriteRow(vals, nulls); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// mergeWindow is a window that updates, deletes and inserts around page and
// row group boundaries.
func mergeWindow() map[string]*query.ResultRow {
	return changeMap(upd(1, "one"), del(4), upd(5, "five"), del(12), upd(23, "last"), ins(40, "forty"), ins(30, "thirty"))
}

// mergedRows runs the merge over path with the given page size.
func mergedRows(t *testing.T, path string, page int64, owns *[spillBuckets]bool) ([]map[string]any, mergeStats) {
	t.Helper()
	withScanPage(t, page)
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	in := mergeCore{LocalBaselinePath: path, Schema: "mydb", Table: "orders", PKCols: pkColsIntID()}
	var out []map[string]any
	var stats mergeStats
	if err := scanBaselinePass(context.Background(), ddb, in, mergeWindow(), owns, func(r map[string]any) error {
		out = append(out, r)
		return nil
	}, &stats); err != nil {
		t.Fatalf("page %d: %v", page, err)
	}
	return out, stats
}

func TestScanBaselinePass_pagedReadsTheSameRowsInTheSameOrder(t *testing.T) {
	// A quote in the directory: the path goes into SQL as a literal.
	dir := filepath.Join(t.TempDir(), "it's")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := writeZooRows(t, dir, 23, 4, zooCreateTableSQL)
	want, wantStats := mergedRows(t, path, 0, nil) // 0: one query, the read before paging
	if len(want) != 23-2 {
		t.Fatalf("reference read %d rows, want 21 (23 less two deletes)", len(want))
	}
	// 1: a page per row. 3: pages cross the 4-row groups. 4: pages are the
	// groups. 23: one exact page. 24, 1000: one partial page. 5: 23 is not a
	// multiple, the last page is short.
	for _, page := range []int64{1, 3, 4, 5, 23, 24, 1000} {
		got, gotStats := mergedRows(t, path, page, nil)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("page %d: rows differ from the single read\n got %v\nwant %v", page, got, want)
		}
		if gotStats != wantStats {
			t.Errorf("page %d: stats %+v, want %+v", page, gotStats, wantStats)
		}
	}
}

func TestScanBaselinePass_pagedHonoursTheSpillPassFilter(t *testing.T) {
	path := writeZooRows(t, t.TempDir(), 23, 4, zooCreateTableSQL)
	var owns [spillBuckets]bool
	for i := range owns {
		owns[i] = i%2 == 0
	}
	want, _ := mergedRows(t, path, 0, &owns)
	if len(want) == 0 || len(want) >= 21 {
		t.Fatalf("the filter kept %d rows; the fixture should keep some and drop some", len(want))
	}
	// A page whose rows the filter drops entirely must not end the read.
	for _, page := range []int64{1, 2, 5} {
		got, _ := mergedRows(t, path, page, &owns)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("page %d: rows differ from the single read under the pass filter", page)
		}
	}
}

func TestScanBaselinePass_pagedOverAnEmptyTable(t *testing.T) {
	path := writeZooRows(t, t.TempDir(), 0, 4, zooCreateTableSQL)
	for _, page := range []int64{0, 1, 4} {
		got, stats := mergedRows(t, path, page, nil)
		if len(got) != 0 || stats.BaselineRows != 0 {
			t.Errorf("page %d: an empty base gave %d rows, stats %+v", page, len(got), stats)
		}
	}
}

// A column named file_row_number, in any case, collides with the option the
// page filter needs: that table is read in one query, as before.
func TestScanBaselinePass_columnNamedFileRowNumberIsReadWhole(t *testing.T) {
	create := strings.Replace(zooCreateTableSQL, "`name` varchar(64)", "`File_Row_Number` varchar(64)", 1)
	path := writeZooRows(t, t.TempDir(), 9, 4, create)
	want, _ := mergedRows(t, path, 0, nil)
	got, _ := mergedRows(t, path, 2, nil)
	if !reflect.DeepEqual(got, want) || len(got) == 0 {
		t.Fatalf("rows differ (got %d, want %d)", len(got), len(want))
	}
	// Row 2 is not in the window: it is the file's own row.
	if _, ok := got[1]["File_Row_Number"]; !ok {
		t.Fatalf("the table's own column is missing from the rows: %v", got[1])
	}
}

func TestScanBaselinePass_cancelBetweenPagesStops(t *testing.T) {
	path := writeZooRows(t, t.TempDir(), 23, 4, zooCreateTableSQL)
	withScanPage(t, 2)
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := 0
	var stats mergeStats
	err = scanBaselinePass(ctx, ddb, mergeCore{LocalBaselinePath: path, Schema: "mydb", Table: "orders", PKCols: pkColsIntID()},
		map[string]*query.ResultRow{}, nil, func(map[string]any) error {
			n++
			if n == 2 {
				cancel()
			}
			return nil
		}, &stats)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v after %d rows, want the cancellation", err, n)
	}
	if n >= 23 {
		t.Fatalf("read all %d rows after the cancellation", n)
	}
}

// The rewritten file is the same file, byte for byte.
func TestMergeBaselineIntoParquet_pagedWritesTheSameBytes(t *testing.T) {
	src := writeZooRows(t, t.TempDir(), 23, 4, zooCreateTableSQL)
	at := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	write := func(page int64) []byte {
		withScanPage(t, page)
		_, out, _ := emitSnapshot(t, src, mergeWindow(), &query.BinlogPos{File: "binlog.000009", Pos: 100}, at)
		b, err := os.ReadFile(out)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	want := write(0)
	for _, page := range []int64{1, 3, 5} {
		if got := write(page); !bytes.Equal(got, want) {
			t.Errorf("page %d: the rewritten file differs from the single read's (%d vs %d bytes)", page, len(got), len(want))
		}
	}
}

// The default page is the writer's row group, so a page of a file this
// package wrote is whole row groups.
func TestScanPageRowsDefault(t *testing.T) {
	if scanPageRows != ParquetWriterRowGroupSize {
		t.Fatalf("scanPageRows = %d, want the writer's row group (%d)", scanPageRows, ParquetWriterRowGroupSize)
	}
}

// A footer that declares fewer rows than its row groups hold: the paged read
// must fail, not stop at the footer's count and drop the rest.
func TestScanBaselinePass_footerShortOfItsRowsFails(t *testing.T) {
	path := writeZooRows(t, t.TempDir(), 23, 4, zooCreateTableSQL)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The footer is thrift compact: FileMetaData field 3 (num_rows, i64) has
	// the header byte 0x16 and a zigzag varint; 23 rows is the one byte 46.
	n := len(b)
	flen := int(b[n-8]) | int(b[n-7])<<8 | int(b[n-6])<<16 | int(b[n-5])<<24
	footer := b[n-8-flen : n-8]
	at := bytes.Index(footer, []byte{0x16, 46})
	if at < 0 {
		t.Fatal("num_rows not found in the footer")
	}
	footer[at+1] = 32 // zigzag 16
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	var declared int64
	if err := ddb.QueryRow("SELECT num_rows FROM parquet_file_metadata('" + path + "')").Scan(&declared); err != nil || declared != 16 {
		t.Fatalf("the patched footer declares %d rows (err %v), want 16", declared, err)
	}
	// 4: the pages end exactly on the footer's 16, so only the check past the
	// end sees the rest. 5: the last page runs over and returns 20.
	for page, want := range map[int64]string{4: "7 rows past the 16 its footer declares", 5: "read 20 rows of the 16"} {
		withScanPage(t, page)
		var stats mergeStats
		err := scanBaselinePass(context.Background(), ddb, mergeCore{LocalBaselinePath: path, Schema: "mydb", Table: "orders", PKCols: pkColsIntID()},
			map[string]*query.ResultRow{}, nil, func(map[string]any) error { return nil }, &stats)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("page %d: err = %v, want %q", page, err, want)
		}
	}
}

// The point of the change: an ordinary table is read by pages. The other
// tests compare against the single read, which gives the same rows and bytes,
// so they would pass if paging were switched off by mistake.
func TestBaselineScanPages_pagesAnOrdinaryTableOnly(t *testing.T) {
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	lit := func(p string) string { return "'" + strings.ReplaceAll(p, "'", "''") + "'" }
	plain := writeZooRows(t, t.TempDir(), 23, 4, zooCreateTableSQL)
	total, paged, err := baselineScanPages(context.Background(), ddb, lit(plain), "mydb", "orders")
	if err != nil || !paged || total != 23 {
		t.Fatalf("ordinary table: total=%d paged=%v err=%v, want 23 rows read by pages", total, paged, err)
	}
	clash := writeZooRows(t, t.TempDir(), 9, 4, strings.Replace(zooCreateTableSQL, "`name` varchar(64)", "`FILE_ROW_NUMBER` varchar(64)", 1))
	if _, paged, err := baselineScanPages(context.Background(), ddb, lit(clash), "mydb", "orders"); err != nil || paged {
		t.Fatalf("table with a file_row_number column: paged=%v err=%v, want one query", paged, err)
	}

	// And the one-query read says so, naming the table.
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	mergedRows(t, clash, 2, nil)
	if !strings.Contains(buf.String(), "reading a backup file in one query") || !strings.Contains(buf.String(), "table=orders") {
		t.Fatalf("no warning for the one-query read; log: %q", buf.String())
	}
	buf.Reset()
	mergedRows(t, plain, 2, nil)
	if buf.Len() != 0 {
		t.Fatalf("an ordinary table logged %q", buf.String())
	}
}

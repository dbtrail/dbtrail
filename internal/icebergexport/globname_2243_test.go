package icebergexport

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// TestWriteBaselineRows_aTableNamedLikeAGlob (#2243): parquet_scan takes a
// path as a glob, and "order[st]" as a pattern matches "orders" and not
// itself: the export of the one loaded the rows of the other.
func TestWriteBaselineRows_aTableNamedLikeAGlob(t *testing.T) {
	createSQL := "CREATE TABLE `order[st]` (\n  `k` int NOT NULL,\n  `v` varchar(10) DEFAULT NULL,\n  PRIMARY KEY (`k`)\n) ENGINE=InnoDB;\n"
	bcols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "order[st].parquet")
	// The table's two rows, and three in the file of its neighbour.
	for p, rows := range map[string][][]string{
		path:                                 {{"1", "one"}, {"2", "two"}},
		filepath.Join(dir, "orders.parquet"): {{"7", "x"}, {"8", "y"}, {"9", "z"}},
	} {
		w, err := baseline.NewWriter(p, bcols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			if err := w.WriteRow(r, []bool{false, false}); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	pk := []metadata.ColumnMeta{{Name: "k", IsPK: true, DataType: "int", ColumnType: "int"}}
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
	tbl, err := cat.CreateTable(ctx, catalog.ToIdentifier("shop", "ordersx"), icebergSchema(cols), catalog.WithProperties(tableProperties()))
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
		t.Fatalf("rows = %d, files = %d, want the table's 2 rows", rows, len(files))
	}
}

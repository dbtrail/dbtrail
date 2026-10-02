package cli

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// writeRealTable1733 writes a baseline table with its CREATE TABLE in the
// footer, rows of (id, "v<id>"), the way `bintrail baseline` does.
func writeRealTable1733(t *testing.T, path, createSQL string, ids ...string) []baseline.Column {
	t.Helper()
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if err := w.WriteRow([]string{id, "v" + id}, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return cols
}

// TestRunViews_realFootersReachTheChainRule_1733 drives the command over real
// baseline files, so the footer read is what decides each table's body, not a
// hand-built Input. An ordinary table must get the chain-aware body and answer
// with a chain that appears after generation; a table with a column a delta
// reserves must keep reading its file, or the whole file stops loading.
func TestRunViews_realFootersReachTheChainRule_1733(t *testing.T) {
	root := t.TempDir()
	const stamp = "2026-06-10T12-00-00Z"
	snap := filepath.Join(root, stamp)
	orders := filepath.Join(snap, "shop", "orders.parquet")
	cols := writeRealTable1733(t, orders, "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(8) DEFAULT NULL\n);\n", "1", "2")
	writeRealTable1733(t, filepath.Join(snap, "shop", "odd.parquet"),
		"CREATE TABLE `odd` (\n  `id` int NOT NULL,\n  `filename` varchar(8) DEFAULT NULL\n);\n", "1")
	if err := os.WriteFile(filepath.Join(snap, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := baseline.PublishCurrentPointer(snap); err != nil {
		t.Fatal(err)
	}

	sqlText := runViewsOverBaselines(t, root, false)
	if strings.Contains(sqlText, "reads the table file alone") {
		t.Fatalf("a table the footer read cleared is announced as reading its file alone:\n%s", sqlText)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(sqlText); err != nil {
		t.Fatalf("DuckDB rejected the generated file: %v\n%s", err, sqlText)
	}
	state := func(view string) string {
		t.Helper()
		var got string
		if err := db.QueryRow(`SELECT string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) FROM ` + view).Scan(&got); err != nil {
			t.Fatalf("%s: %v", view, err)
		}
		return got
	}
	if got := state("shop.odd"); got != "1" {
		t.Fatalf("shop.odd = %s", got)
	}
	// A refresh writes the table's first chain: row 0 (id 1) replaced, id 3 new.
	err = baseline.WriteTableDeltaPair(orders, 0, cols, nil, []int64{0}, func(emit func([]string, []bool) error) error {
		return emit([]string{"3", "v3", "3", baseline.TableDeltaOpUpsert}, make([]bool, 4))
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := state("shop.orders"); got != "2,3" {
		t.Fatalf("after the first chain: shop.orders = %s, want 2,3", got)
	}
}

package views

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// writeTable1733 writes one baseline table from a CREATE TABLE, every value
// "1", under <root>/<stamp>/shop/<file>, with the snapshot's _SUCCESS marker.
func writeTable1733(t *testing.T, root, stamp, file, createSQL string) string {
	t.Helper()
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, stamp, "shop", file)
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	vals := make([]string, len(cols))
	for i := range vals {
		vals[i] = "1"
	}
	if err := w.WriteRow(vals, make([]bool, len(cols))); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, stamp, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// followInput1733 is a following Input over the given tables of one snapshot.
func followInput1733(t *testing.T, root, stamp string, follow FollowMode, tables ...BaselineTable) Input {
	t.Helper()
	if follow == FollowPointer {
		if err := os.Symlink(stamp, filepath.Join(root, baseline.CurrentLinkName)); err != nil {
			t.Fatal(err)
		}
		for i := range tables {
			tables[i].Path = filepath.Join(root, baseline.CurrentLinkName, "shop", filepath.Base(tables[i].Path))
		}
	}
	return Input{
		GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
		BaselineSource: root, BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Follow: follow, Baselines: tables,
	}
}

var followModes1733 = []struct {
	name   string
	follow FollowMode
}{{"pointer", FollowPointer}, {"newest", FollowNewest}}

// TestFollowingView_reservedColumnKeepsTheFileRead_1733: a table with a column
// a table delta reserves never gets a chain, and the chain-aware SQL does not
// bind over it (DuckDB refuses the filename and file_row_number options on a
// file that already has such a column). Its following view must keep reading
// the file alone, or the whole views file stops loading at that table.
func TestFollowingView_reservedColumnKeepsTheFileRead_1733(t *testing.T) {
	for _, col := range []string{"filename", "file_row_number"} {
		for _, mode := range followModes1733 {
			t.Run(col+"/"+mode.name, func(t *testing.T) {
				root := t.TempDir()
				const stamp = "2026-04-30T03-00-00Z"
				p := writeTable1733(t, root, stamp, "orders.parquet",
					"CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `"+col+"` varchar(8) DEFAULT NULL\n);\n")
				in := followInput1733(t, root, stamp, mode.follow, BaselineTable{Schema: "shop", Table: "orders", Path: p,
					Rel: "shop/orders.parquet", SchemaKnown: true, DeltaReserved: true})
				sqlText := Generate(in)
				if strings.Contains(sqlText, "reads the table file alone") {
					t.Fatalf("a table that never gets a chain is announced as one that will refuse:\n%s", sqlText)
				}
				db := execViews(t, sqlText)
				var n int
				if err := db.QueryRow(`SELECT count(*) FROM shop.orders`).Scan(&n); err != nil || n != 1 {
					t.Fatalf("n=%d err=%v", n, err)
				}
			})
		}
	}
}

// TestFollowingView_guardStaysWhereTheChainBodyIsNotSafe_1733: a table whose
// schema was not read, or with a sibling table named "<table>.<anything>" in
// the same schema, keeps the file-alone body and its guard. The guard refuses
// once a chain appears, which is loud; the chain-aware body could merge the
// sibling's columns into this view without an error.
func TestFollowingView_guardStaysWhereTheChainBodyIsNotSafe_1733(t *testing.T) {
	const create = "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(8) DEFAULT NULL\n);\n"
	const sibling = "CREATE TABLE `orders.old` (\n  `id` varchar(8) NOT NULL,\n  `extra` varchar(8) DEFAULT NULL\n);\n"
	for _, tc := range []struct {
		name       string
		known      bool
		withPrefix string // a sibling table's name, or none
		why        string // what the file says above the view
	}{
		{"schema not read", false, "", "because its schema could not be read. If a refresh"},
		{"sibling named orders.old", true, "orders.old", `a name that starts with "orders.". If a refresh`},
		{"sibling named ORDERS.000000, other case", true, "ORDERS.000000", `a name that starts with "orders.". If a refresh`},
	} {
		for _, mode := range followModes1733 {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				root := t.TempDir()
				const stamp = "2026-04-30T03-00-00Z"
				p := writeTable1733(t, root, stamp, "orders.parquet", create)
				tables := []BaselineTable{{Schema: "shop", Table: "orders", Path: p, Rel: "shop/orders.parquet", SchemaKnown: tc.known}}
				if tc.withPrefix != "" {
					sp := writeTable1733(t, root, stamp, tc.withPrefix+".parquet", sibling)
					tables = append(tables, BaselineTable{Schema: "shop", Table: tc.withPrefix, Path: sp,
						Rel: "shop/" + tc.withPrefix + ".parquet", SchemaKnown: true})
				}
				in := followInput1733(t, root, stamp, mode.follow, tables...)
				sqlText := Generate(in)
				if want := "-- shop.orders: reads the table file alone "; !strings.Contains(sqlText, want) || !strings.Contains(sqlText, tc.why) {
					t.Fatalf("the file does not say why the view reads the file alone (want %q and %q):\n%s", want, tc.why, sqlText)
				}
				db := execViews(t, sqlText)
				rows, err := db.Query(`SELECT * FROM shop.orders LIMIT 0`)
				if err != nil {
					t.Fatal(err)
				}
				got, _ := rows.Columns()
				rows.Close()
				if strings.Join(got, ",") != "id,status" {
					t.Fatalf("columns = %v, want exactly the table's own", got)
				}
				writeDeltaPairAt(t, filepath.Join(root, stamp, "shop", "orders.parquet"), 0, []int64{0}, nil)
				var n int
				err = db.QueryRow(`SELECT count(*) FROM shop.orders`).Scan(&n)
				if err == nil || !strings.Contains(err.Error(), "Generate the views again") {
					t.Fatalf("after a chain appeared: n=%d err=%v, want the guard's refusal", n, err)
				}
			})
		}
	}
}

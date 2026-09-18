package views

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// TestStateView_readsARangePair (#1723): the generated state view over a
// chain holding a range pair, [0, 1-6, 7], in all three following modes.
// Pair 7 overrides the range's version of key 1, the range's tombstone of
// key 2 stands, and a file the glob matches but the name filter must drop
// (seven digits) kills row 2 and tombstones key 3: a filter that let it
// through would lose id 3 in either half of the query.
func TestStateView_readsARangePair(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	want := []string{"1=v7", "3=c"}
	for _, mode := range followModes {
		t.Run(mode.name, func(t *testing.T) {
			root := t.TempDir()
			base := writeSnapshot(t, root, stamp, true, "a", "b", "c") // ids 1,2,3 at rows 0,1,2
			stem := strings.TrimSuffix(base, ".parquet")
			rename := func(from, to string) {
				t.Helper()
				for _, sfx := range []string{baseline.TableDeltaPosdelSuffix, baseline.TableDeltaUpsertsSuffix} {
					if err := os.Rename(stem+from+sfx, stem+to+sfx); err != nil {
						t.Fatal(err)
					}
				}
			}
			writeDeltaPair(t, base, 0, nil, nil)
			// The views read names, not footers, so a plain pair renamed to
			// the range's name is the layout under test.
			writeDeltaPair(t, base, 1, []int64{0, 1}, [][3]string{{"1", "r16", "u"}, {"2", "", "d"}})
			rename(".000001", ".000001-000006")
			writeDeltaPair(t, base, 7, nil, [][3]string{{"1", "v7", "u"}})
			tables := []BaselineTable{{Schema: "shop", Table: "orders", Path: base, Rel: "shop/orders.parquet", SchemaKnown: true}}
			sqlText := generateFor(t, root, stamp, mode.follow, tables)
			if !tables[0].Delta || tables[0].DeltaLegacy {
				t.Fatal("MarkTableDeltas did not read the chain with its range")
			}
			// The decoy, written after the listing so only the SQL sees it.
			writeDeltaPair(t, base, 1, []int64{2}, [][3]string{{"3", "", "d"}})
			rename(".000001", ".0000001")
			if got := stateRows(t, sqlText); !reflect.DeepEqual(got, want) {
				t.Fatalf("state view = %v, want %v\n--- generated ---\n%s", got, want, sqlText)
			}
		})
	}
}

// TestStateView_pinnedNamesTheChainFiles: a pinned view names the chain's
// files exactly (like reconstruct) instead of reading the chain through the
// glob, so a neighbouring table whose name starts with "<table>.<six
// digits>" cannot reach it. Through the glob its pair would be admitted and
// union_by_name would add its COLUMNS to the view's shape at bind time,
// before the name filter drops its rows: with the neighbour below the view
// would gain an "extra_col". A following view keeps the glob (it cannot
// name files that do not exist yet); that residual is documented on
// baseline.TableDeltaGlobs, not pinned here.
func TestStateView_pinnedNamesTheChainFiles(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
	writeDeltaPair(t, base, 0, nil, nil)
	writeDeltaPair(t, base, 1, []int64{0, 1}, [][3]string{{"1", "v2", "u"}})
	// The neighbour: shop.`orders.000001x`, one more column, its own chain.
	schema := filepath.Join(t.TempDir(), "schema.sql")
	if err := os.WriteFile(schema, []byte("CREATE TABLE `orders.000001x` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  `extra_col` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := baseline.ParseSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	nbase := filepath.Join(filepath.Dir(base), "orders.000001x.parquet")
	w, err := baseline.NewWriter(nbase, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"7", "x", "1"}, []bool{false, false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteTableDeltaPair(nbase, 0, cols, nil, nil, func(emit func([]string, []bool) error) error {
		return emit([]string{"7", "y", "2", "7", baseline.TableDeltaOpUpsert}, []bool{false, false, false, false, false})
	}); err != nil {
		t.Fatal(err)
	}
	tables := []BaselineTable{{Schema: "shop", Table: "orders", Path: base, Rel: "shop/orders.parquet", SchemaKnown: true}}
	sqlText := generateFor(t, root, stamp, FollowNone, tables)
	if len(tables[0].DeltaFiles) != 2 {
		t.Fatalf("DeltaFiles = %+v, want the two pairs listed", tables[0].DeltaFiles)
	}
	// The behaviour first (the shape the view binds to), the text second.
	db := execViews(t, sqlText)
	rows, err := db.Query(`SELECT * FROM state_shop_orders LIMIT 0`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got, _ := rows.Columns()
	if !reflect.DeepEqual(got, []string{"id", "status"}) {
		t.Fatalf("state view columns = %v, want exactly the table's: the neighbour's shape leaked in", got)
	}
	if got := stateRows(t, sqlText); !reflect.DeepEqual(got, []string{"1=v2", "3=c"}) {
		t.Fatalf("state view = %v", got)
	}
	if strings.Contains(sqlText, "[0-9][0-9][0-9][0-9][0-9][0-9]*") {
		t.Fatalf("a pinned view reads the chain through the glob:\n%s", sqlText)
	}
}

// TestStateView_guardSeesARangePairAlone: the following views' guard (#1638)
// also refuses once a chain made of one RANGE pair appears beside the
// table, a shape the chain listing accepts.
func TestStateView_guardSeesARangePairAlone(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	for _, mode := range followModes[1:] {
		t.Run(mode.name, func(t *testing.T) {
			root := t.TempDir()
			base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
			path := base
			if mode.follow == FollowPointer {
				if err := os.Symlink(stamp, filepath.Join(root, baseline.CurrentLinkName)); err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(root, baseline.CurrentLinkName, "shop", "orders.parquet")
			}
			sqlText := Generate(Input{
				GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
				BaselineSource: root, BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
				Follow:    mode.follow,
				Baselines: []BaselineTable{{Schema: "shop", Table: "orders", Path: path, Rel: "shop/orders.parquet"}},
			})
			db := execViews(t, sqlText)
			var n int
			if err := db.QueryRow(`SELECT count(*) FROM state_shop_orders`).Scan(&n); err != nil || n != 3 {
				t.Fatalf("before any delta: n=%d err=%v", n, err)
			}
			writeDeltaPair(t, base, 0, []int64{0}, nil)
			stem := strings.TrimSuffix(base, ".parquet")
			for _, sfx := range []string{baseline.TableDeltaPosdelSuffix, baseline.TableDeltaUpsertsSuffix} {
				if err := os.Rename(stem+".000000"+sfx, stem+".000000-000003"+sfx); err != nil {
					t.Fatal(err)
				}
			}
			err := db.QueryRow(`SELECT count(*) FROM state_shop_orders`).Scan(&n)
			if err == nil || !strings.Contains(err.Error(), "shop.orders now has a table delta") {
				t.Fatalf("after a range pair alone appeared: n=%d err=%v, want the guard's refusal", n, err)
			}
		})
	}
}

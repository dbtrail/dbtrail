package views

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// writeDeltaPair writes pair seq of a table's chain beside base through the
// real baseline writer: dead are row numbers into base, upserts are
// (id, status, op) rows, where op is "u" or "d" (a tombstone's columns are
// written NULL).
func writeDeltaPair(t *testing.T, base string, seq int, dead []int64, upserts [][3]string) {
	t.Helper()
	cols, err := baseline.ParseSchema(writeSchemaFile(t))
	if err != nil {
		t.Fatal(err)
	}
	err = baseline.WriteTableDeltaPair(base, seq, cols, nil, dead, func(emit func([]string, []bool) error) error {
		for _, r := range upserts {
			if r[2] == baseline.TableDeltaOpDelete {
				if err := emit([]string{"", "", r[0], r[2]}, []bool{true, true, false, false}); err != nil {
					return err
				}
				continue
			}
			if err := emit([]string{r[0], r[1], r[0], r[2]}, []bool{false, false, false, false}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// writeLegacyDeltaPair writes the v0.83.0 layout: one pair, no sequence, no
// technical columns.
func writeLegacyDeltaPair(t *testing.T, base string, dead []int64, upserts [][2]string) {
	t.Helper()
	stem := strings.TrimSuffix(base, ".parquet")
	posCols, err := baseline.PosdelColumns()
	if err != nil {
		t.Fatal(err)
	}
	pw, err := baseline.NewWriter(stem+baseline.TableDeltaPosdelSuffix, posCols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range dead {
		if err := pw.WriteRow([]string{strconv.FormatInt(p, 10)}, []bool{false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	cols, err := baseline.ParseSchema(writeSchemaFile(t))
	if err != nil {
		t.Fatal(err)
	}
	uw, err := baseline.NewWriter(stem+baseline.TableDeltaUpsertsSuffix, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range upserts {
		if err := uw.WriteRow([]string{r[0], r[1]}, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := uw.Close(); err != nil {
		t.Fatal(err)
	}
}

func stateRows(t *testing.T, sqlText string) []string {
	t.Helper()
	db := execViews(t, sqlText)
	rows, err := db.Query(`SELECT id::VARCHAR || '=' || status FROM state_shop_orders ORDER BY id`)
	if err != nil {
		t.Fatalf("query the state view: %v\n--- generated ---\n%s", err, sqlText)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

var followModes = []struct {
	name   string
	follow FollowMode
}{{"pinned", FollowNone}, {"pointer", FollowPointer}, {"newest", FollowNewest}}

func generateFor(t *testing.T, root, stamp string, follow FollowMode, tables []BaselineTable) string {
	t.Helper()
	if err := MarkTableDeltas(context.Background(), tables); err != nil {
		t.Fatalf("MarkTableDeltas: %v", err)
	}
	if follow == FollowPointer {
		if err := os.Symlink(stamp, filepath.Join(root, baseline.CurrentLinkName)); err != nil {
			t.Fatal(err)
		}
		for i := range tables {
			tables[i].Path = filepath.Join(root, baseline.CurrentLinkName, "shop", "orders.parquet")
		}
	}
	return Generate(Input{
		GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
		BaselineSource: root, BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Follow: follow, Baselines: tables,
	})
}

// TestStateView_readsTheTableDelta runs the generated view against a real
// chain in all three following modes. The base holds ids 1..3; the chain
// kills rows 0 and 1 (ids 1, 2), brings 1 back changed and adds 9. A view that
// read the base alone returns the THREE original rows, which no assertion
// below accepts, so a lost mark cannot pass as a smaller result.
func TestStateView_readsTheTableDelta(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	want := []string{"1=changed", "3=c", "9=new"}
	for _, mode := range followModes {
		t.Run(mode.name, func(t *testing.T) {
			root := t.TempDir()
			base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
			writeDeltaPair(t, base, 0, nil, nil)
			writeDeltaPair(t, base, 1, []int64{0, 1}, [][3]string{{"1", "changed", "u"}, {"9", "new", "u"}})
			tables := []BaselineTable{{Schema: "shop", Table: "orders", Path: base, Rel: "shop/orders.parquet", SchemaKnown: true}}
			sqlText := generateFor(t, root, stamp, mode.follow, tables)
			if !tables[0].Delta || tables[0].DeltaLegacy {
				t.Fatal("MarkTableDeltas did not find the chain beside the table")
			}
			if got := stateRows(t, sqlText); !reflect.DeepEqual(got, want) {
				t.Fatalf("state view = %v, want %v", got, want)
			}
		})
	}
}

// TestStateView_chainLastVersionWinsAndTombstonesHide (#1718): across the
// pairs of a chain the newest version of a key wins, a tombstone hides a row
// an earlier pair added, and a base row killed in one pair and re-added in a
// later one comes back with the later image. The three modes read the same
// files, so one wrong ordering would show in all of them.
func TestStateView_chainLastVersionWinsAndTombstonesHide(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	want := []string{"1=v3", "2=back", "3=c"}
	for _, mode := range followModes {
		t.Run(mode.name, func(t *testing.T) {
			root := t.TempDir()
			base := writeSnapshot(t, root, stamp, true, "a", "b", "c") // ids 1,2,3 at rows 0,1,2
			writeDeltaPair(t, base, 0, nil, nil)
			writeDeltaPair(t, base, 1, []int64{0, 1}, [][3]string{{"1", "v2", "u"}, {"2", "", "d"}, {"9", "nine", "u"}})
			writeDeltaPair(t, base, 2, []int64{0}, [][3]string{{"1", "v3", "u"}, {"9", "", "d"}, {"2", "back", "u"}})
			// Sequence 10 sorts after 2 under zero padding, and not before it.
			writeDeltaPair(t, base, 10, nil, nil)
			tables := []BaselineTable{{Schema: "shop", Table: "orders", Path: base, Rel: "shop/orders.parquet", SchemaKnown: true}}
			sqlText := generateFor(t, root, stamp, mode.follow, tables)
			if got := stateRows(t, sqlText); !reflect.DeepEqual(got, want) {
				t.Fatalf("state view = %v, want %v\n--- generated ---\n%s", got, want, sqlText)
			}
		})
	}
}

// TestStateView_nullPositionDoesNotEmptyTheTable: verified against DuckDB, one
// NULL in a NOT IN subquery makes every base row vanish. The writer never
// writes one; the state SQL filters them so a file it did not write cannot
// empty the table silently.
func TestStateView_nullPositionDoesNotEmptyTheTable(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
	writeDeltaPair(t, base, 0, nil, nil)
	writeDeltaPair(t, base, 1, []int64{1}, nil)
	// A posdel with a NULL, written outside the writer (which refuses NULLs).
	posdel, _ := baseline.TableDeltaPaths(base, 2)
	_, ups := baseline.TableDeltaPaths(base, 2)
	posCols, _ := baseline.PosdelColumns()
	pw, err := baseline.NewWriter(posdel, posCols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRow([]string{""}, []bool{true}); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	cols, _ := baseline.ParseSchema(writeSchemaFile(t))
	upsCols, _ := baseline.TableDeltaColumns(cols)
	uw, err := baseline.NewWriter(ups, upsCols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := uw.Close(); err != nil {
		t.Fatal(err)
	}
	tables := []BaselineTable{{Schema: "shop", Table: "orders", Path: base, Rel: "shop/orders.parquet", SchemaKnown: true}}
	sqlText := generateFor(t, root, stamp, FollowNone, tables)
	if got, want := stateRows(t, sqlText), []string{"1=a", "3=c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state view = %v, want %v (a NULL position emptied the base?)", got, want)
	}
}

// TestStateView_legacyPairStillRenders: a snapshot written by v0.83.0 (one
// pair, no sequence) is described with the legacy body while it is retained.
func TestStateView_legacyPairStillRenders(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
	writeLegacyDeltaPair(t, base, []int64{0, 1}, [][2]string{{"1", "changed"}, {"9", "new"}})
	tables := []BaselineTable{{Schema: "shop", Table: "orders", Path: base, Rel: "shop/orders.parquet", SchemaKnown: true}}
	sqlText := generateFor(t, root, stamp, FollowNone, tables)
	if !tables[0].Delta || !tables[0].DeltaLegacy {
		t.Fatalf("legacy pair marked as Delta=%v Legacy=%v", tables[0].Delta, tables[0].DeltaLegacy)
	}
	if got, want := stateRows(t, sqlText), []string{"1=changed", "3=c", "9=new"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state view over a legacy pair = %v, want %v", got, want)
	}
}

// TestStateView_withoutADeltaIsUnchanged: a table with no pair renders exactly
// as it did before #1638, in a snapshot where a sibling has one.
func TestStateView_withoutADeltaIsUnchanged(t *testing.T) {
	root := t.TempDir()
	base := writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "a", "b")
	tables := []BaselineTable{{Schema: "shop", Table: "orders", Path: base, Rel: "shop/orders.parquet"}}
	if err := MarkTableDeltas(context.Background(), tables); err != nil {
		t.Fatal(err)
	}
	if tables[0].Delta {
		t.Fatal("a table with no pair was marked")
	}
	sqlText := Generate(Input{GeneratedAt: time.Now(), Version: "test", BaselineSource: root, Baselines: tables})
	if strings.Contains(sqlText, "file_row_number") {
		t.Fatalf("a table with no delta got the delta body:\n%s", sqlText)
	}
}

// TestMarkTableDeltas_refusesHalfAPair: dead positions with no upserts would
// delete every updated row from the view, silently. At any sequence.
func TestMarkTableDeltas_refusesHalfAPair(t *testing.T) {
	root := t.TempDir()
	base := writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "a", "b")
	writeDeltaPair(t, base, 0, nil, nil)
	writeDeltaPair(t, base, 1, []int64{0}, nil)
	writeDeltaPair(t, base, 2, []int64{1}, nil)
	_, ups := baseline.TableDeltaPaths(base, 1)
	if err := os.Remove(ups); err != nil {
		t.Fatal(err)
	}
	tables := []BaselineTable{{Schema: "shop", Table: "orders", Path: base, Rel: "shop/orders.parquet"}}
	if err := MarkTableDeltas(context.Background(), tables); !errors.Is(err, baseline.ErrHalfTableDelta) {
		t.Fatalf("err = %v, want ErrHalfTableDelta", err)
	}
	if _, err := snapshotTables(filepath.Join(root, "2026-04-30T03-00-00Z")); !errors.Is(err, baseline.ErrHalfTableDelta) {
		t.Fatalf("snapshotTables err = %v, want ErrHalfTableDelta", err)
	}
}

// TestFollowingStateView_refusesOnceADeltaAppears: a following view generated
// while the table had no delta must not keep answering from the table's file
// once a refresh starts writing a chain beside it. It reads fine before, and
// names the table and the remedy after. A pinned view carries no guard: the
// files it names never change.
func TestFollowingStateView_refusesOnceADeltaAppears(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	for _, mode := range []struct {
		name   string
		follow FollowMode
	}{{"pointer", FollowPointer}, {"newest", FollowNewest}} {
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
			// A sibling table's chain must not trip this one's guard.
			writeDeltaPairAt(t, filepath.Join(root, stamp, "shop", "orders_archive.parquet"), 0, nil, nil)
			if err := db.QueryRow(`SELECT count(*) FROM state_shop_orders`).Scan(&n); err != nil || n != 3 {
				t.Fatalf("a delta beside ANOTHER table tripped the guard: n=%d err=%v", n, err)
			}
			writeDeltaPair(t, base, 0, []int64{0}, nil)
			err := db.QueryRow(`SELECT count(*) FROM state_shop_orders`).Scan(&n)
			if err == nil || !strings.Contains(err.Error(), "shop.orders now has a table delta") || !strings.Contains(err.Error(), "Generate the views again") {
				t.Fatalf("after the delta appeared: n=%d err=%v, want a refusal naming the table and the remedy", n, err)
			}
		})
	}
	// Pinned: no guard in the text at all.
	root := t.TempDir()
	base := writeSnapshot(t, root, stamp, true, "a")
	pinned := Generate(Input{GeneratedAt: time.Now(), Version: "test", BaselineSource: root,
		Baselines: []BaselineTable{{Schema: "shop", Table: "orders", Path: base, Rel: "shop/orders.parquet"}}})
	if strings.Contains(pinned, "now has a table delta") {
		t.Fatal("a pinned view carries the guard; its files never change")
	}
}

// writeDeltaPairAt writes a pair for a table file that need not exist.
func writeDeltaPairAt(t *testing.T, base string, seq int, dead []int64, upserts [][3]string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(base), 0o755); err != nil {
		t.Fatal(err)
	}
	writeDeltaPair(t, base, seq, dead, upserts)
}

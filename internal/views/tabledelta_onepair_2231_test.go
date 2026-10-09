package views

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2231: a pinned view over a chain of exactly one pair reads it through
// baseline.TableDeltaOnePairStateSQL, with no aggregate and no join; every
// other view of a chain keeps the join. latestMark is the join's name in the
// generated SQL (tableDeltaStateSQL's CTE).
const latestMark = "bintrail_latest"

// onePairTables is one table of a snapshot whose chain is the single pair the
// caller wrote.
func onePairTables(base string) []BaselineTable {
	return []BaselineTable{{Schema: "shop", Table: "orders", Path: base, Rel: "shop/orders.parquet", SchemaKnown: true}}
}

// TestStateView_onePairSkipsTheJoin: the pinned view of a one-pair chain has
// no join, the following views of the same chain keep it (a later snapshot
// may hold more pairs), and all three read the same rows. The base holds ids
// 1..3; the pair kills rows 0 and 1, brings id 1 back changed, adds 9, and
// carries a tombstone for id 2 and for an id 7 the base never had.
func TestStateView_onePairSkipsTheJoin(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	want := []string{"1=changed", "3=c", "9=new"}
	for _, mode := range followModes {
		t.Run(mode.name, func(t *testing.T) {
			root := t.TempDir()
			base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
			writeDeltaPair(t, base, 0, []int64{0, 1}, [][3]string{{"1", "changed", "u"}, {"2", "", "d"}, {"7", "", "d"}, {"9", "new", "u"}})
			tables := onePairTables(base)
			sqlText := generateFor(t, root, stamp, mode.follow, tables)
			if len(tables[0].DeltaFiles) != 1 {
				t.Fatalf("the chain has %d pairs, the case needs one", len(tables[0].DeltaFiles))
			}
			if joined, wantJoin := strings.Contains(sqlText, latestMark), mode.follow != FollowNone; joined != wantJoin {
				t.Fatalf("the view reads through the join = %v, want %v\n%s", joined, wantJoin, sqlText)
			}
			if got := stateRows(t, sqlText); !reflect.DeepEqual(got, want) {
				t.Fatalf("state view = %v, want %v", got, want)
			}
		})
	}
}

// TestStateView_onePairShapes: the pairs a chain of one can be. The empty pair
// that starts a chain after a rewrite reads as the base; a range pair, which
// is what a merge of a whole chain is named, reads as any pair.
func TestStateView_onePairShapes(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	t.Run("the empty pair that starts a chain", func(t *testing.T) {
		root := t.TempDir()
		base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
		writeDeltaPair(t, base, 0, nil, nil)
		tables := onePairTables(base)
		sqlText := generateFor(t, root, stamp, FollowNone, tables)
		if strings.Contains(sqlText, latestMark) {
			t.Fatalf("a one-pair chain is read through the join\n%s", sqlText)
		}
		if got, want := stateRows(t, sqlText), []string{"1=a", "2=b", "3=c"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("state view = %v, want %v", got, want)
		}
	})
	t.Run("a range pair", func(t *testing.T) {
		root := t.TempDir()
		base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
		// Written as pair 5, then given the name of the range 0-5.
		writeDeltaPair(t, base, 5, []int64{2}, [][3]string{{"3", "", "d"}, {"4", "four", "u"}})
		posdel, upserts := baseline.TableDeltaPaths(base, 5)
		rPosdel, rUpserts := baseline.TableDeltaRangePaths(base, 0, 5)
		for _, mv := range [][2]string{{posdel, rPosdel}, {upserts, rUpserts}} {
			if err := os.Rename(mv[0], mv[1]); err != nil {
				t.Fatal(err)
			}
		}
		tables := onePairTables(base)
		sqlText := generateFor(t, root, stamp, FollowNone, tables)
		if len(tables[0].DeltaFiles) != 1 || !tables[0].DeltaFiles[0].Range() {
			t.Fatalf("chain = %+v, want one range pair", tables[0].DeltaFiles)
		}
		if strings.Contains(sqlText, latestMark) {
			t.Fatalf("a one-pair chain is read through the join\n%s", sqlText)
		}
		if got, want := stateRows(t, sqlText), []string{"1=a", "2=b", "4=four"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("state view = %v, want %v", got, want)
		}
	})
}

// TestStateView_twoPairsKeepTheJoin: one pair more and the pinned view goes
// back to the join. The second pair deletes what the first inserted and
// changes id 1 again, so reading either pair without choosing between
// versions shows id 9 or the older id 1.
func TestStateView_twoPairsKeepTheJoin(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
	writeDeltaPair(t, base, 0, []int64{0}, [][3]string{{"1", "first", "u"}, {"9", "new", "u"}})
	writeDeltaPair(t, base, 1, []int64{0}, [][3]string{{"1", "second", "u"}, {"9", "", "d"}})
	tables := onePairTables(base)
	sqlText := generateFor(t, root, stamp, FollowNone, tables)
	if !strings.Contains(sqlText, latestMark) {
		t.Fatalf("a chain of two pairs is read without the join\n%s", sqlText)
	}
	if got, want := stateRows(t, sqlText), []string{"1=second", "2=b", "3=c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state view = %v, want %v", got, want)
	}
}

// TestStateView_onePairNullPositionDoesNotEmptyTheTable is
// TestStateView_nullPositionDoesNotEmptyTheTable for the one-pair SQL, which
// has its own NOT IN: one NULL in it and every base row is gone.
func TestStateView_onePairNullPositionDoesNotEmptyTheTable(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
	writeDeltaPair(t, base, 0, nil, nil)
	// The posdel again, with a NULL and a real position, written outside the
	// writer (which refuses NULLs).
	posdel, _ := baseline.TableDeltaPaths(base, 0)
	posCols, err := baseline.PosdelColumns()
	if err != nil {
		t.Fatal(err)
	}
	pw, err := baseline.NewWriter(posdel, posCols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []struct {
		v    string
		null bool
	}{{"", true}, {"1", false}} {
		if err := pw.WriteRow([]string{r.v}, []bool{r.null}); err != nil {
			t.Fatal(err)
		}
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	tables := onePairTables(base)
	sqlText := generateFor(t, root, stamp, FollowNone, tables)
	if strings.Contains(sqlText, latestMark) {
		t.Fatalf("a one-pair chain is read through the join\n%s", sqlText)
	}
	if got, want := stateRows(t, sqlText), []string{"1=a", "3=c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state view = %v, want %v", got, want)
	}
}

// TestStateView_onePairOfATableNamedLikeAGlob: read_parquet takes a path as a
// glob, so the files of a table named "or?ers" also match those of "orders".
// The join's name filter kept the neighbour's pair out; the one-pair SQL has
// none, so its three paths are made to match themselves. Without that the
// view below reads the neighbour's table file, its dead row and its inserted
// row too: each of the three shows as a different wrong state.
func TestStateView_onePairOfATableNamedLikeAGlob(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	neighbour := writeSnapshot(t, root, stamp, true, "a", "b", "c")
	// The neighbour kills a row this table keeps: read by mistake, 3=c goes.
	writeDeltaPair(t, neighbour, 0, []int64{2}, [][3]string{{"77", "neighbour", "u"}})
	odd := strings.TrimSuffix(neighbour, "orders.parquet") + "or?ers.parquet"
	raw, err := os.ReadFile(neighbour)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(odd, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	writeDeltaPair(t, odd, 0, []int64{0}, [][3]string{{"1", "changed", "u"}})
	tables := []BaselineTable{{Schema: "shop", Table: "or?ers", Path: odd, Rel: "shop/or?ers.parquet", SchemaKnown: true}}
	sqlText := generateFor(t, root, stamp, FollowNone, tables)
	if len(tables[0].DeltaFiles) != 1 || strings.Contains(sqlText, latestMark) {
		t.Fatalf("the case needs a one-pair chain read without the join (pairs=%d)\n%s", len(tables[0].DeltaFiles), sqlText)
	}
	db := execViews(t, sqlText)
	rows, err := db.Query(`SELECT id::VARCHAR || '=' || status FROM shop."or?ers" ORDER BY id`)
	if err != nil {
		t.Fatalf("%v\n%s", err, sqlText)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		got = append(got, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if want := []string{"1=changed", "2=b", "3=c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state view = %v, want %v", got, want)
	}
}

// TestStateView_resolvedPairSkipsTheJoin: a chain of two pairs whose merge
// into one is kept beside the snapshots (BaselineTable.DeltaResolved) is read
// by a pinned view through the base and that pair, with no join. The pair
// here says something the chain does not (id 1 is "resolved"), so the rows
// show which files were read. A following view ignores it and reads the
// chain: the pair belongs to one snapshot and is gone after it.
func TestStateView_resolvedPairSkipsTheJoin(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	for _, mode := range followModes {
		t.Run(mode.name, func(t *testing.T) {
			root := t.TempDir()
			base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
			writeDeltaPair(t, base, 0, []int64{0}, [][3]string{{"1", "first", "u"}, {"9", "new", "u"}})
			writeDeltaPair(t, base, 1, []int64{0}, [][3]string{{"1", "second", "u"}, {"9", "", "d"}})
			tables := onePairTables(base)
			if err := MarkTableDeltas(context.Background(), tables); err != nil {
				t.Fatal(err)
			}
			MarkResolvedTableDeltas(tables)
			if tables[0].DeltaResolved != nil {
				t.Fatal("a resolved pair is marked with none written")
			}
			// The pair, as the daemon would leave it: written beside a copy
			// of the base under the resolved directory, under the range.
			want, ok := baseline.ResolvedTableDeltaPaths(base, tables[0].DeltaFiles)
			if !ok {
				t.Fatal("a chain of two pairs has no place for a resolved pair")
			}
			stage := filepath.Join(filepath.Dir(want.Upserts), "orders.parquet")
			writeDeltaPairAt(t, stage, 7, []int64{0}, [][3]string{{"1", "resolved", "u"}, {"9", "", "d"}})
			posdel, upserts := baseline.TableDeltaPaths(stage, 7)
			for _, mv := range [][2]string{{posdel, want.Posdel}, {upserts, want.Upserts}} {
				if err := os.Rename(mv[0], mv[1]); err != nil {
					t.Fatal(err)
				}
			}
			MarkResolvedTableDeltas(tables)
			if tables[0].DeltaResolved == nil || *tables[0].DeltaResolved != want {
				t.Fatalf("DeltaResolved = %+v, want %+v", tables[0].DeltaResolved, want)
			}
			if len(tables[0].DeltaFiles) != 2 {
				t.Fatalf("the chain was replaced by the pair: %+v", tables[0].DeltaFiles)
			}
			resolved := *tables[0].DeltaResolved
			// generateFor lists the chain again, which drops the pair; it is
			// set again and the views generated with it, in every mode.
			generateFor(t, root, stamp, mode.follow, tables)
			tables[0].DeltaResolved = &resolved
			sqlText := Generate(Input{
				GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
				BaselineSource: root, BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
				Follow: mode.follow, Baselines: tables,
			})
			wantRows, wantJoin := []string{"1=second", "2=b", "3=c"}, true
			if mode.follow == FollowNone {
				wantRows, wantJoin = []string{"1=resolved", "2=b", "3=c"}, false
			}
			if joined := strings.Contains(sqlText, latestMark); joined != wantJoin {
				t.Fatalf("the view reads through the join = %v, want %v\n%s", joined, wantJoin, sqlText)
			}
			if got := stateRows(t, sqlText); !reflect.DeepEqual(got, wantRows) {
				t.Fatalf("state view = %v, want %v", got, wantRows)
			}
		})
	}
}

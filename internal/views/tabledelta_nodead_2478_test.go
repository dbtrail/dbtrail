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

// #2478: a pinned view over one pair with no dead row numbers reads the base
// without the file_row_number join (baseMinusDeadSQL), which adds up to the
// cost of a second base read and removes nothing. deadJoinMark is that join's
// name in the generated SQL.
const deadJoinMark = "file_row_number"

// generatePinnedMarked lists the chain, marks the resolved pair and the pairs
// without dead rows as a statement's views are marked, and generates the
// pinned views. withoutDead=false skips MarkTableDeltasWithoutDeadRows: the
// views as they were before #2478, to compare rows against.
func generatePinnedMarked(t *testing.T, root string, tables []BaselineTable, withoutDead bool) string {
	t.Helper()
	if err := MarkTableDeltas(context.Background(), tables); err != nil {
		t.Fatalf("MarkTableDeltas: %v", err)
	}
	MarkResolvedTableDeltas(tables)
	if withoutDead {
		MarkTableDeltasWithoutDeadRows(tables)
	}
	return generateMarked(root, FollowNone, tables)
}

// generateMarked generates the views of tables as they are marked, without
// listing the chain again (which would reset the marks).
func generateMarked(root string, follow FollowMode, tables []BaselineTable) string {
	return Generate(Input{
		GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
		BaselineSource: root, BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Follow: follow, Baselines: tables,
	})
}

// TestStateView_pairWithoutDeadRowsSkipsTheDeadJoin: the pairs a statement
// reads through the one-pair SQL, each with and without dead row numbers.
// The views without the join must return exactly the rows of the views with
// it, and only a pair whose posdel holds no row is read without it.
func TestStateView_pairWithoutDeadRowsSkipsTheDeadJoin(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	cases := []struct {
		name     string
		dead     []int64
		upserts  [][3]string
		wantJoin bool
		want     []string
	}{
		{"the empty pair that starts a chain", nil, nil, false, []string{"1=a", "2=b", "3=c"}},
		{"inserts only (an append-only table)", nil, [][3]string{{"9", "new", "u"}, {"10", "newer", "u"}}, false,
			[]string{"1=a", "2=b", "3=c", "9=new", "10=newer"}},
		{"a tombstone with no dead row (a key the base never had)", nil, [][3]string{{"7", "", "d"}}, false,
			[]string{"1=a", "2=b", "3=c"}},
		{"dead rows", []int64{0, 2}, [][3]string{{"1", "changed", "u"}}, true, []string{"1=changed", "2=b"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
			writeDeltaPair(t, base, 0, c.dead, c.upserts)
			tables := onePairTables(base)
			sqlText := generatePinnedMarked(t, root, tables, true)
			if got := tables[0].DeltaNoDead; got == c.wantJoin {
				t.Fatalf("DeltaNoDead = %v with %d dead rows", got, len(c.dead))
			}
			if joined := strings.Contains(sqlText, deadJoinMark); joined != c.wantJoin {
				t.Fatalf("the view reads through the dead-row join = %v, want %v\n%s", joined, c.wantJoin, sqlText)
			}
			got := stateRows(t, sqlText)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("state view = %v, want %v", got, c.want)
			}
			before := stateRows(t, generatePinnedMarked(t, root, onePairTables(base), false))
			if !reflect.DeepEqual(got, before) {
				t.Fatalf("rows changed: %v with the mark, %v without", got, before)
			}
		})
	}
}

// TestStateView_nullPositionKeepsTheDeadJoin: a posdel whose only rows are
// NULL positions removes nothing, but it is not EMPTY, and the mark is about
// the file holding no row at all: anything else keeps the join that has
// always read it. The footer counts rows, not positions.
func TestStateView_nullPositionKeepsTheDeadJoin(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
	writeDeltaPair(t, base, 0, nil, nil)
	posdel, _ := baseline.TableDeltaPaths(base, 0)
	posCols, err := baseline.PosdelColumns()
	if err != nil {
		t.Fatal(err)
	}
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
	tables := onePairTables(base)
	sqlText := generatePinnedMarked(t, root, tables, true)
	if tables[0].DeltaNoDead || !strings.Contains(sqlText, deadJoinMark) {
		t.Fatalf("a posdel with a row is read without the join (DeltaNoDead=%v)\n%s", tables[0].DeltaNoDead, sqlText)
	}
	if got, want := stateRows(t, sqlText), []string{"1=a", "2=b", "3=c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state view = %v, want %v", got, want)
	}
}

// TestStateView_unreadablePosdelKeepsTheDeadJoin: a posdel that cannot be
// read as Parquet (gone, or not Parquet) is never taken for an empty one:
// the view keeps the join, and DuckDB says what is wrong with the file when
// the statement reads it, as before #2478.
func TestStateView_unreadablePosdelKeepsTheDeadJoin(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	for _, shape := range []string{"not parquet", "gone"} {
		t.Run(shape, func(t *testing.T) {
			root := t.TempDir()
			base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
			writeDeltaPair(t, base, 0, nil, nil)
			tables := onePairTables(base)
			if err := MarkTableDeltas(context.Background(), tables); err != nil {
				t.Fatal(err)
			}
			posdel, _ := baseline.TableDeltaPaths(base, 0)
			if shape == "gone" {
				if err := os.Remove(posdel); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(posdel, []byte("PAR1 not a footer"), 0o644); err != nil {
				t.Fatal(err)
			}
			MarkResolvedTableDeltas(tables)
			MarkTableDeltasWithoutDeadRows(tables)
			if tables[0].DeltaNoDead {
				t.Fatal("an unreadable posdel was marked as holding no dead rows")
			}
			if sqlText := generateMarked(root, FollowNone, tables); !strings.Contains(sqlText, deadJoinMark) {
				t.Fatalf("the view dropped the join over an unreadable posdel\n%s", sqlText)
			}
		})
	}
}

// TestStateView_resolvedPairDecidesTheDeadJoin: when a statement reads the
// chain's resolved pair, the mark comes from THAT pair's posdel, not from
// the chain's. Both ways round: a chain with dead rows beside a pair with
// none (no writer produces this, since a base row killed anywhere in the
// chain stays killed in the merged pair; the pair is written by hand so the
// mark can be seen following the resolved pair and not the chain), and a
// chain without any beside a pair with some.
func TestStateView_resolvedPairDecidesTheDeadJoin(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	for _, c := range []struct {
		name         string
		chainDead    []int64
		resolvedDead []int64
		wantJoin     bool
		want         []string
	}{
		{"the chain has dead rows, the pair none", []int64{0}, nil, false, []string{"1=a", "2=b", "3=c", "9=resolved"}},
		{"the chain has none, the pair some", nil, []int64{1}, true, []string{"1=a", "3=c", "9=resolved"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
			writeDeltaPair(t, base, 0, c.chainDead, [][3]string{{"8", "first", "u"}})
			writeDeltaPair(t, base, 1, c.chainDead, [][3]string{{"8", "", "d"}})
			tables := onePairTables(base)
			if err := MarkTableDeltas(context.Background(), tables); err != nil {
				t.Fatal(err)
			}
			want, ok := baseline.ResolvedTableDeltaPaths(base, tables[0].DeltaFiles)
			if !ok {
				t.Fatal("a chain of two pairs has no place for a resolved pair")
			}
			stage := filepath.Join(filepath.Dir(want.Upserts), "orders.parquet")
			writeDeltaPairAt(t, stage, 7, c.resolvedDead, [][3]string{{"9", "resolved", "u"}})
			posdel, upserts := baseline.TableDeltaPaths(stage, 7)
			for _, mv := range [][2]string{{posdel, want.Posdel}, {upserts, want.Upserts}} {
				if err := os.Rename(mv[0], mv[1]); err != nil {
					t.Fatal(err)
				}
			}
			sqlText := generatePinnedMarked(t, root, tables, true)
			if tables[0].DeltaResolved == nil {
				t.Fatal("the resolved pair was not marked")
			}
			if joined := strings.Contains(sqlText, deadJoinMark); joined != c.wantJoin {
				t.Fatalf("the view reads through the dead-row join = %v, want %v\n%s", joined, c.wantJoin, sqlText)
			}
			if got := stateRows(t, sqlText); !reflect.DeepEqual(got, c.want) {
				t.Fatalf("state view = %v, want %v", got, c.want)
			}
		})
	}
}

// TestStateView_chainOfPairsWithoutDeadRowsKeepsItsBody: a chain of two pairs
// with no resolved pair is read through the join that picks each key's
// newest version, whatever its posdels hold. The mark is not set there, so
// nothing reads it as an instruction to drop part of that body.
func TestStateView_chainOfPairsWithoutDeadRowsKeepsItsBody(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
	writeDeltaPair(t, base, 0, nil, [][3]string{{"9", "first", "u"}})
	writeDeltaPair(t, base, 1, nil, [][3]string{{"9", "second", "u"}})
	tables := onePairTables(base)
	sqlText := generatePinnedMarked(t, root, tables, true)
	if tables[0].DeltaNoDead {
		t.Fatal("a chain of two pairs was marked: the mark is about the one pair a statement reads")
	}
	if !strings.Contains(sqlText, latestMark) || !strings.Contains(sqlText, deadJoinMark) {
		t.Fatalf("the chain's body lost a part\n%s", sqlText)
	}
	if got, want := stateRows(t, sqlText), []string{"1=a", "2=b", "3=c", "9=second"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state view = %v, want %v", got, want)
	}
}

// TestStateView_followingViewIgnoresTheMark: a following view outlives the
// files it was generated against, and a later pair beside the table can hold
// dead rows, so even a table marked here keeps the join there.
func TestStateView_followingViewIgnoresTheMark(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	for _, mode := range followModes {
		if mode.follow == FollowNone {
			continue
		}
		t.Run(mode.name, func(t *testing.T) {
			root := t.TempDir()
			base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
			writeDeltaPair(t, base, 0, nil, nil)
			tables := onePairTables(base)
			if err := MarkTableDeltas(context.Background(), tables); err != nil {
				t.Fatal(err)
			}
			MarkTableDeltasWithoutDeadRows(tables)
			if !tables[0].DeltaNoDead {
				t.Fatal("the case needs a marked table")
			}
			if mode.follow == FollowPointer {
				if err := os.Symlink(stamp, filepath.Join(root, baseline.CurrentLinkName)); err != nil {
					t.Fatal(err)
				}
				tables[0].Path = filepath.Join(root, baseline.CurrentLinkName, "shop", "orders.parquet")
			}
			if sqlText := generateMarked(root, mode.follow, tables); !strings.Contains(sqlText, deadJoinMark) {
				t.Fatalf("a following view dropped the dead-row join\n%s", sqlText)
			}
		})
	}
}

// TestMarkTableDeltas_resetsTheNoDeadMark: the mark belongs to one listing.
// A table listed again (its pair now holds dead rows, or it has no chain any
// more) must not keep the mark from the listing before, and neither may a
// table whose resolved pair is looked for again.
func TestMarkTableDeltas_resetsTheNoDeadMark(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
	writeDeltaPair(t, base, 0, nil, nil)
	tables := onePairTables(base)
	generatePinnedMarked(t, root, tables, true)
	if !tables[0].DeltaNoDead {
		t.Fatal("the case needs a marked table")
	}
	if err := MarkTableDeltas(context.Background(), tables); err != nil {
		t.Fatal(err)
	}
	if tables[0].DeltaNoDead {
		t.Fatal("MarkTableDeltas kept the mark of the listing before")
	}
	tables[0].DeltaNoDead = true
	MarkResolvedTableDeltas(tables)
	if tables[0].DeltaNoDead {
		t.Fatal("MarkResolvedTableDeltas kept a mark made for another pair")
	}
}

// TestStateView_posdelReplacedAfterTheMarkFailsTheStatement: the pair is
// replaced under the same name between the footer read and the statement
// (reconstruct.ResolveTableDelta replacing a resolved pair that was not the
// chain's), and the new posdel kills a row. The view generated without the
// dead-row join must fail with TableDeltaPosdelChangedError rather than show
// that row beside the pair's new version of it.
func TestStateView_posdelReplacedAfterTheMarkFailsTheStatement(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshot(t, root, stamp, true, "a", "b", "c")
	writeDeltaPair(t, base, 0, nil, nil)
	tables := onePairTables(base)
	sqlText := generatePinnedMarked(t, root, tables, true)
	if !tables[0].DeltaNoDead || strings.Contains(sqlText, deadJoinMark) {
		t.Fatalf("the case needs a view generated without the join\n%s", sqlText)
	}
	if got, want := stateRows(t, sqlText), []string{"1=a", "2=b", "3=c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("before the replace: state view = %v, want %v", got, want)
	}
	writeDeltaPair(t, base, 0, []int64{0}, [][3]string{{"1", "changed", "u"}})
	db := execViews(t, sqlText)
	rows, err := db.Query(`SELECT id::VARCHAR || '=' || status FROM shop.orders ORDER BY id`)
	if err == nil {
		for rows.Next() {
		}
		err = rows.Err()
		rows.Close()
	}
	if err == nil || !strings.Contains(err.Error(), baseline.TableDeltaPosdelChangedError) {
		t.Fatalf("a view read on after its posdel gained a dead row: err = %v", err)
	}
}

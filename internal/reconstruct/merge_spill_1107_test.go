package reconstruct

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// randomChanges builds a deterministic change map over ids 1..n: updates,
// deletes and inserts, including ids the baseline does not hold.
func randomChanges(n int) map[string]*query.ResultRow {
	rng := rand.New(rand.NewPCG(1107, 2))
	changes := map[string]*query.ResultRow{}
	for id := 1; id <= n; id++ {
		pk := pkStrForInt(id)
		ev := &query.ResultRow{PKValues: pk, EventID: uint64(id), SchemaName: "mydb", TableName: "orders"}
		switch rng.IntN(5) {
		case 0, 4:
			continue
		case 1:
			ev.EventType = event.EventUpdate
		case 2:
			ev.EventType = event.EventDelete
		case 3:
			ev.EventType = event.EventInsert
		}
		if ev.EventType != event.EventDelete {
			ev.RowAfter = map[string]any{"id": json.Number(strconv.Itoa(id)), "status": "changed-" + strconv.Itoa(id)}
		}
		changes[pk] = ev
	}
	return changes
}

func cloneChanges(m map[string]*query.ResultRow) map[string]*query.ResultRow { return maps.Clone(m) }

// rendered is each emitted row as "type:id=type:status", sorted, so two merges
// compare on content and value types but not on order.
func rendered(rows []map[string]any) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, fmt.Sprintf("%T:%v=%T:%v", r["id"], r["id"], r["status"], r["status"]))
	}
	slices.Sort(out)
	return out
}

func noPassCheck(map[string]*query.ResultRow) error { return nil }

// A merge from the spill emits exactly what the in-memory merge emits, with
// the same counts, whether it takes one pass or many.
func TestMergeSpilled_matchesInMemoryMerge(t *testing.T) {
	var base [][]string
	for id := 1; id <= 300; id++ {
		base = append(base, []string{strconv.Itoa(id), "base-" + strconv.Itoa(id)})
	}
	path := writeTestBaseline(t, base)
	changes := randomChanges(400)
	if len(changes) < 150 {
		t.Fatalf("fixture built only %d changes", len(changes))
	}
	ctx := context.Background()

	var want []map[string]any
	wantStats, err := mergeBaselineImages(ctx, mergeCore{
		LocalBaselinePath: path, Schema: "mydb", Table: "orders", PKCols: pkColsIntID(), Changes: cloneChanges(changes),
	}, func(r map[string]any) error { want = append(want, r); return nil })
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		limit      int64
		manyPasses bool
	}{{12, true}, {100000, false}} {
		t.Run(fmt.Sprintf("limit %d", tc.limit), func(t *testing.T) {
			s := spillOf(t, tc.limit, cloneChanges(changes))
			var got []map[string]any
			var checks, checked int
			stats, err := mergeBaselineImages(ctx, mergeCore{
				LocalBaselinePath: path, Schema: "mydb", Table: "orders", PKCols: pkColsIntID(), Spill: s,
				CheckPass: func(m map[string]*query.ResultRow) error {
					checks++
					checked += len(m)
					if int64(len(m)) > 2*tc.limit {
						t.Errorf("a pass held %d changes with a limit of %d", len(m), tc.limit)
					}
					return nil
				},
			}, func(r map[string]any) error { got = append(got, r); return nil })
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(rendered(got), rendered(want)) {
				t.Fatalf("the spilled merge emitted different rows:\n got %v\nwant %v", rendered(got), rendered(want))
			}
			if stats.Passes != checks {
				t.Errorf("reported %d passes, checked %d", stats.Passes, checks)
			}
			stats.Passes = 0
			if stats != wantStats {
				t.Errorf("stats = %+v, want %+v", stats, wantStats)
			}
			if checked != len(changes) {
				t.Errorf("the passes checked %d changes, want all %d", checked, len(changes))
			}
			if tc.manyPasses && checks < 3 {
				t.Errorf("a limit of %d over %d changes took %d passes; the test is not exercising passes", tc.limit, len(changes), checks)
			}
			if !tc.manyPasses && checks != 1 {
				t.Errorf("everything fits in memory but took %d passes", checks)
			}
		})
	}
}

// One group holding more changed rows than the limit refuses: that group
// alone would not fit in memory.
func TestMergeSpilled_groupOverTheLimitRefuses(t *testing.T) {
	path := writeTestBaseline(t, [][]string{{"1", "a"}})
	s := spillOf(t, 1, randomChanges(400))
	_, err := mergeBaselineImages(context.Background(), mergeCore{
		LocalBaselinePath: path, Schema: "mydb", Table: "orders", PKCols: pkColsIntID(), Spill: s, CheckPass: noPassCheck,
	}, func(map[string]any) error { return nil })
	if !errors.Is(err, ErrTouchedRowBudget) {
		t.Fatalf("err = %v, want the changed-rows refusal", err)
	}
}

// The per-pass check (the #602 added-column guard in the writers) runs, and
// its refusal stops the merge. Without one, a spilled merge refuses to run:
// that guard reads the changes, which are on disk and not in a map.
func TestMergeSpilled_passCheck(t *testing.T) {
	path := writeTestBaseline(t, [][]string{{"1", "a"}})
	changes := randomChanges(50)
	t.Run("its refusal stops the merge", func(t *testing.T) {
		_, err := mergeBaselineImages(context.Background(), mergeCore{
			LocalBaselinePath: path, Schema: "mydb", Table: "orders", PKCols: pkColsIntID(), Spill: spillOf(t, 1000, cloneChanges(changes)),
			CheckPass: func(map[string]*query.ResultRow) error { return ErrSchemaChanged },
		}, func(map[string]any) error { return nil })
		if !errors.Is(err, ErrSchemaChanged) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("changes left in memory beside the spill refuse", func(t *testing.T) {
		var emitted int
		_, err := mergeBaselineImages(context.Background(), mergeCore{
			LocalBaselinePath: path, Schema: "mydb", Table: "orders", PKCols: pkColsIntID(),
			Spill: spillOf(t, 1000, cloneChanges(changes)), Changes: randomChanges(5), CheckPass: noPassCheck,
		}, func(map[string]any) error { emitted++; return nil })
		if err == nil || emitted != 0 {
			t.Fatalf("a merge with changes both on disk and in memory ran: err=%v emitted=%d", err, emitted)
		}
	})
	t.Run("missing refuses", func(t *testing.T) {
		var emitted int
		_, err := mergeBaselineImages(context.Background(), mergeCore{
			LocalBaselinePath: path, Schema: "mydb", Table: "orders", PKCols: pkColsIntID(), Spill: spillOf(t, 1000, cloneChanges(changes)),
		}, func(map[string]any) error { emitted++; return nil })
		if err == nil || emitted != 0 {
			t.Fatalf("a spilled merge ran without its per-pass check: err=%v emitted=%d", err, emitted)
		}
	})
}

// The #1158 spelling guard still fires when the two spellings of one row land
// in different groups, merged in different passes.
func TestMergeSpilled_pkSpellingGuardAcrossPasses(t *testing.T) {
	const paddedKey = "11223344556677889900AABB00000000"
	pkCols := binaryPKCols()
	stripped := "0x11223344556677889900AABB"
	padded := "0x" + paddedKey
	if spillBucket(stripped) == spillBucket(padded) {
		t.Fatalf("fixture: both spellings share bucket %d; pick keys that do not", spillBucket(stripped))
	}
	path := binaryPKBaseline(t, t.TempDir(), map[string]string{paddedKey: "baseline"})
	s := spillOf(t, 1, map[string]*query.ResultRow{
		stripped: {EventType: event.EventUpdate, PKValues: stripped,
			RowAfter: map[string]any{"k": mustDecodeHex(t, "11223344556677889900AABB"), "val": "updated"}},
		padded: {EventType: event.EventDelete, PKValues: padded},
	})
	var checks int
	_, err := mergeBaselineImages(context.Background(), mergeCore{
		LocalBaselinePath: path, Schema: "db", Table: "bp", PKCols: pkCols, Spill: s,
		CheckPass: func(map[string]*query.ResultRow) error { checks++; return nil },
	}, func(map[string]any) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "DELETE") || errors.Is(err, ErrTouchedRowBudget) {
		t.Fatalf("err = %v, want the #1158 refusal for the undrained DELETE", err)
	}
	if checks < 2 {
		t.Fatalf("both spellings were merged in %d pass(es); the test needs them apart", checks)
	}
}

// The same guard does not refuse a healthy binary-keyed table merged in passes.
func TestMergeSpilled_healthyBinaryTableMerges(t *testing.T) {
	pkCols := binaryPKCols()
	rows := map[string]string{}
	changes := map[string]*query.ResultRow{}
	for i := range 16 {
		k := strings.Repeat("A7", 16-i) + strings.Repeat("00", i)
		rows[k] = "baseline"
		if i%2 == 0 {
			pk := event.BuildPKValues(pkCols, mustCanonicalize(t, pkCols, mustDecodeHex(t, k)))
			changes[pk] = &query.ResultRow{EventType: event.EventUpdate, PKValues: pk,
				RowAfter: map[string]any{"k": mustDecodeHex(t, k), "val": "updated"}}
		}
	}
	path := binaryPKBaseline(t, t.TempDir(), rows)
	var updated, kept int
	stats, err := mergeBaselineImages(context.Background(), mergeCore{
		LocalBaselinePath: path, Schema: "db", Table: "bp", PKCols: pkCols, Spill: spillOf(t, 2, changes), CheckPass: noPassCheck,
	}, func(r map[string]any) error {
		if r["val"] == "updated" {
			updated++
		} else {
			kept++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("a healthy table refused when merged in passes: %v", err)
	}
	if updated != 8 || kept != 8 || stats.Passes < 2 {
		t.Fatalf("updated=%d kept=%d passes=%d, want 8, 8 and several passes", updated, kept, stats.Passes)
	}
}

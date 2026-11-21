//go:build integration

package console

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #3049 against a real index, through the real pgTailSource: a change that
// leaves a large value as it was is stored with the marker in the row it
// leaves and the key alone in the row before, as capture stores it for a
// table below REPLICA IDENTITY FULL; the tail reads the value from the cut
// it lies over. The table's "large value" is status.

// leaves indexes an UPDATE of id that leaves status as it was and does not
// carry it, committed at lsn.
func (r *pgTailRig) leaves(table, id string, lsn uint64, d time.Duration) {
	r.t.Helper()
	before, _ := json.Marshal(map[string]any{"id": id, "status": map[string]any{event.NotCapturedKey: true}})
	after, _ := json.Marshal(map[string]any{"id": id, "status": map[string]any{event.UnchangedToastKey: true}})
	testutil.InsertEvent(r.t, r.db, pgLSN(lsn), lsn, lsn, r.base.Add(d).Format("2006-01-02 15:04:05"), nil,
		"public", table, uint8(event.EventUpdate), id, nil, before, after)
}

// tailRows is the rows tbl's tail holds, "id=status", read from its own
// files and not through the table: an UPDATE that leaves status as it was
// leaves the table reading the same, and what is checked is that the tail's
// row holds the value and not a NULL or the marker. "no tail" when the
// table has none.
func (r *pgTailRig) tailRows(ctx context.Context, tbl views.BaselineTable) []string {
	r.t.Helper()
	got, err := settledRefresh(r.t, ctx, r.k, []views.BaselineTable{tbl})
	if err != nil {
		r.t.Fatal(err)
	}
	v, ok := got[tailKey(tbl.Schema, tbl.Table)]
	if !ok {
		return []string{"no tail"}
	}
	if len(v.Paths) == 0 {
		return nil
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		r.t.Fatal(err)
	}
	defer db.Close()
	files := make([]string, len(v.Paths))
	for i, p := range v.Paths {
		files[i] = "'" + p + "'"
	}
	// The newest file that holds a key is its row: files are numbered as
	// they are written.
	rs, err := db.Query(`SELECT id || '=' || CASE WHEN "` + baseline.TableDeltaOpColumn + `" = '` + baseline.TableDeltaOpDelete + `' THEN '(deleted)' ELSE coalesce(status, '∅') END
		FROM (SELECT *, row_number() OVER (PARTITION BY id ORDER BY CAST(regexp_extract(filename, '([0-9]+)-[^/]*$', 1) AS BIGINT) DESC) AS n FROM read_parquet([` + strings.Join(files, ", ") + `], filename=true)) WHERE n = 1 ORDER BY id`)
	if err != nil {
		r.t.Fatal(err)
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		var s string
		if err := rs.Scan(&s); err != nil {
			r.t.Fatal(err)
		}
		if strings.Contains(s, event.UnchangedToastKey) {
			r.t.Fatalf("the tail of %s holds the marker of a value that was not sent: %s", tbl.Table, s)
		}
		out = append(out, s)
	}
	if err := rs.Err(); err != nil {
		r.t.Fatal(err)
	}
	return out
}

func TestIntegrationPGTailReadsALargeValueFromTheCopy(t *testing.T) {
	ctx := context.Background()
	r := newPGTailRig(t)

	t.Run("while the tail is built, and then as changes arrive", func(t *testing.T) {
		tbl := r.table(nil, [2]string{"1", "one"}, [2]string{"2", "two"}, [2]string{"3", "three"})
		r.leaves(tbl.Table, "1", 0x600, 20*time.Minute)
		r.change(tbl.Table, event.EventInsert, "9", "nine", 0x608, 20*time.Minute)
		r.checkpoint(0x608)
		// The build reads the copy itself, at the end of its own read of the
		// index: installed, the tail waits for nothing.
		if _, err := r.k.refresh(ctx, []views.BaselineTable{tbl}); err != nil {
			t.Fatal(err)
		}
		r.k.settle()
		e := r.k.tables[tailKey(tbl.Schema, tbl.Table)]
		if e == nil || e.tail == nil || e.refused != "" {
			t.Fatalf("the table's tail was not built: %+v", e)
		}
		if keys, _ := e.tail.(pgCopyTail).CopyWants(); len(keys) != 0 || e.tail.Keys() != 2 {
			t.Fatalf("the tail a build installed holds %d keys and still waits for %v", e.tail.Keys(), keys)
		}
		if got, want := r.tailRows(ctx, tbl), []string{"1=one", "9=nine"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("the tail holds %v, want %v", got, want)
		}
		if got, want := r.state(ctx, tbl), []string{"1=one", "2=two", "3=three", "9=nine"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
		// The tail is in: the next such change is folded by an update.
		r.leaves(tbl.Table, "2", 0x610, 21*time.Minute)
		// And one of a row the tail already holds: from the tail.
		r.leaves(tbl.Table, "9", 0x618, 21*time.Minute)
		r.checkpoint(0x618)
		if got, want := r.tailRows(ctx, tbl), []string{"1=one", "2=two", "9=nine"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("the tail holds %v, want %v", got, want)
		}
		// A real value after it replaces it.
		r.change(tbl.Table, event.EventUpdate, "2", "TWO", 0x620, 22*time.Minute)
		r.checkpoint(0x620)
		if got, want := r.state(ctx, tbl), []string{"1=one", "2=TWO", "3=three", "9=nine"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
	})

	t.Run("the table delta beside the file before the file", func(t *testing.T) {
		tbl := r.table(nil, [2]string{"1", "one"}, [2]string{"2", "two"}, [2]string{"3", "three"})
		// A snapshot update published what changed since the file: row 1
		// changed, row 2 gone, row 7 new.
		r.change(tbl.Table, event.EventUpdate, "1", "ONE", 0x6e0, 23*time.Minute)
		r.change(tbl.Table, event.EventDelete, "2", "two", 0x6e4, 23*time.Minute)
		r.change(tbl.Table, event.EventInsert, "7", "seven", 0x6e8, 23*time.Minute)
		r.pair(tbl, map[string]string{"1": "ONE", "2": "", "7": "seven"}, 0x6e9, nil)
		r.leaves(tbl.Table, "1", 0x700, 24*time.Minute)
		r.leaves(tbl.Table, "3", 0x708, 24*time.Minute)
		r.leaves(tbl.Table, "7", 0x710, 24*time.Minute)
		r.checkpoint(0x710)
		if got, want := r.tailRows(ctx, tbl), []string{"1=ONE", "3=three", "7=seven"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("the tail holds %v, want %v: key 1 and key 7 are the delta's rows, key 3 the file's", got, want)
		}
	})

	t.Run("a row the cut does not hold: the table has no tail, and no NULL is written", func(t *testing.T) {
		tbl := r.table(nil, [2]string{"1", "one"}, [2]string{"2", "two"})
		r.change(tbl.Table, event.EventDelete, "2", "two", 0x7e0, 25*time.Minute)
		r.pair(tbl, map[string]string{"2": ""}, 0x7e1, nil)
		r.leaves(tbl.Table, "2", 0x800, 25*time.Minute)
		r.checkpoint(0x800)
		if got := r.tailRows(ctx, tbl); !reflect.DeepEqual(got, []string{"no tail"}) {
			t.Fatalf("the tail holds %v, want no tail", got)
		}
		e := r.k.tables[tailKey(tbl.Schema, tbl.Table)]
		if e == nil || !strings.Contains(e.refused, "the copy has no row under that key") {
			t.Fatalf("the table's refusal: %+v", e)
		}
	})

	t.Run("after a snapshot update the newer file is read", func(t *testing.T) {
		tbl := r.table(nil, [2]string{"1", "one"}, [2]string{"2", "two"})
		r.change(tbl.Table, event.EventUpdate, "1", "ONE", 0x900, 26*time.Minute)
		r.change(tbl.Table, event.EventUpdate, "2", "TWO", 0x910, 26*time.Minute)
		r.checkpoint(0x910)
		if got, want := r.state(ctx, tbl), []string{"1=ONE", "2=TWO"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("before the update: state %v, want %v", got, want)
		}
		// The update: the table as of just after 0x900, written again. Key
		// 1's change is in it; key 2's is after it.
		next := r.rewritten(tbl, r.at.Add(30*time.Minute), 0x901, nil, [2]string{"1", "ONE"}, [2]string{"2", "two"})
		if got, want := r.tailRows(ctx, next), []string{"2=TWO"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("pruned to the newer file, the tail holds %v, want %v", got, want)
		}
		r.leaves(tbl.Table, "1", 0x920, 27*time.Minute)
		r.checkpoint(0x920)
		if got, want := r.tailRows(ctx, next), []string{"1=ONE", "2=TWO"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("the tail holds %v, want %v: key 1's value is the newer file's, the older one holds 'one'", got, want)
		}
	})
}

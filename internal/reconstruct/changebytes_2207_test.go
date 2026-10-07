package reconstruct

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2207: the fold's change map is bounded in rows (MaxTouchedRows), and a
// row's cost is its width: a wide table's million rows were 2.2 GB live. The
// fold now also moves the changes to disk past a size in bytes, estimated
// from a sample of the map.

// changeShapes are row images as the fold holds them: a decoded after-image
// (strings, json.Number, nil, nested JSON) on a retained event.
var changeShapes = map[string]func(id int) *query.ResultRow{
	"narrow": func(id int) *query.ResultRow {
		return heldEvent(id, map[string]any{"id": json.Number(strconv.Itoa(id)), "v": "x"})
	},
	"stock-like": func(id int) *query.ResultRow {
		after := map[string]any{"s_i_id": json.Number(strconv.Itoa(id)), "s_w_id": json.Number("7"), "s_quantity": json.Number("42"),
			"s_ytd": json.Number("1000"), "s_order_cnt": json.Number("3"), "s_remote_cnt": json.Number("0"),
			"s_data": strings.Repeat("d", 40) + strconv.Itoa(id)}
		for c := 1; c <= 10; c++ {
			after[fmt.Sprintf("s_dist_%02d", c)] = strings.Repeat("z", 20) + strconv.Itoa(id%1000)
		}
		return heldEvent(id, after)
	},
	"wide text": func(id int) *query.ResultRow {
		after := map[string]any{"id": json.Number(strconv.Itoa(id))}
		for c := range 4 {
			after["body"+strconv.Itoa(c)] = strings.Repeat("t", 2000) + strconv.Itoa(id)
		}
		return heldEvent(id, after)
	},
	"json and nulls": func(id int) *query.ResultRow {
		return heldEvent(id, map[string]any{"id": json.Number(strconv.Itoa(id)), "gone": nil,
			"doc": map[string]any{"a": []any{json.Number("1"), "two", map[string]any{"k": "v" + strconv.Itoa(id)}}}})
	},
	"delete": func(id int) *query.ResultRow {
		ev := heldEvent(id, nil)
		ev.EventType = event.EventDelete
		return ev
	},
}

// heldEvent is an event as retainEvent keeps it, with fresh strings the way a
// fetched page has them.
func heldEvent(id int, after map[string]any) *query.ResultRow {
	gtid := "3e11fa47-71ca-11e1-9e33-c80aa9429562:" + strconv.Itoa(id)
	return retainEvent(&query.ResultRow{
		EventID: uint64(id), BinlogFile: "binlog." + strconv.Itoa(100000+id%7), StartPos: uint64(id) * 100,
		GTID: &gtid, SchemaName: "db" + strconv.Itoa(id%3), TableName: "t" + strconv.Itoa(id%3),
		EventType: event.EventUpdate, PKValues: strconv.Itoa(id), ChangedColumns: []string{"v"},
		RowAfter: after,
	})
}

// The estimate tracks what the map really takes on the heap: within a third
// either way for every shape, which is the margin the byte budget is set with.
func TestApproxChangeBytes_tracksTheHeap(t *testing.T) {
	const n = 20000
	for name, mk := range changeShapes {
		t.Run(name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			m := make(map[string]*query.ResultRow, n)
			for id := 1; id <= n; id++ {
				ev := mk(id)
				m[ev.PKValues] = ev
			}
			runtime.GC()
			runtime.ReadMemStats(&after)
			real := float64(after.HeapAlloc-before.HeapAlloc) / n
			var est int64
			for _, ev := range m {
				est += approxChangeBytes(ev)
			}
			got := float64(est) / n
			runtime.KeepAlive(m)
			if got < real*0.75 || got > real*1.33 {
				t.Errorf("estimate %.0f bytes a row, heap %.0f: off by more than a third", got, real)
			}
			t.Logf("estimate %.0f bytes a row, heap %.0f", got, real)
		})
	}
}

// The sample's estimate of the whole map is close to the sum over every entry.
func TestSampledChangeBytes(t *testing.T) {
	m := map[string]*query.ResultRow{}
	for id := 1; id <= 5000; id++ {
		ev := changeShapes["stock-like"](id)
		m[ev.PKValues] = ev
	}
	var exact int64
	for _, ev := range m {
		exact += approxChangeBytes(ev)
	}
	total, perRow := sampledChangeBytes(m)
	if perRow <= 0 || total < exact*9/10 || total > exact*11/10 {
		t.Fatalf("sampled %d (%d a row), exact %d", total, perRow, exact)
	}
	if total, perRow := sampledChangeBytes(map[string]*query.ResultRow{}); total != 0 || perRow != 0 {
		t.Fatalf("empty map: %d, %d", total, perRow)
	}
}

func wideRows(from, to int) map[string]*query.ResultRow {
	m := map[string]*query.ResultRow{}
	for id := from; id <= to; id++ {
		ev := changeShapes["wide text"](id)
		m[ev.PKValues] = ev
	}
	return m
}

func TestFoldResult_admitBytes(t *testing.T) {
	perRow := approxChangeBytes(changeShapes["wide text"](1))

	t.Run("no byte budget never spills on size", func(t *testing.T) {
		r := &foldResult{Changes: wideRows(1, 100)}
		if err := r.admit(1000, 0, 1, true); err != nil || r.Spill != nil {
			t.Fatalf("err=%v spill=%v", err, r.Spill != nil)
		}
	})
	t.Run("under both stays in memory", func(t *testing.T) {
		r := &foldResult{Changes: wideRows(1, 10)}
		if err := r.admit(1000, perRow*100, 1, true); err != nil || r.Spill != nil || len(r.Changes) != 10 {
			t.Fatalf("err=%v spill=%v changes=%d", err, r.Spill != nil, len(r.Changes))
		}
	})
	t.Run("over the bytes, under the rows: to disk, passes sized by bytes", func(t *testing.T) {
		r := &foldResult{Changes: wideRows(1, 100)}
		if err := r.admit(1000, perRow*20, 1, true); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.close)
		if r.Spill == nil || len(r.Changes) != 0 {
			t.Fatalf("spill=%v changes=%d, want every change on disk", r.Spill != nil, len(r.Changes))
		}
		if pr := r.Spill.passRows(); pr < 15 || pr > 25 {
			t.Errorf("passRows = %d, want about 20 (the byte budget over the row size)", pr)
		}
		// The refusal stays on rows: a pass is a memory target, not a limit.
		if r.Spill.limit != 1000 {
			t.Errorf("limit = %d, want the row limit unchanged", r.Spill.limit)
		}
	})
	t.Run("over the bytes without a spill does not refuse", func(t *testing.T) {
		r := &foldResult{Changes: wideRows(1, 100)}
		if err := r.admit(1000, perRow*20, 1, false); err != nil || r.Spill != nil || len(r.Changes) != 100 {
			t.Fatalf("err=%v spill=%v changes=%d: the size budget must only ever move changes to disk",
				err, r.Spill != nil, len(r.Changes))
		}
	})
	t.Run("over the rows with a byte budget: passes take the smaller", func(t *testing.T) {
		r := &foldResult{Changes: wideRows(1, 30)}
		if err := r.admit(25, perRow*10, 1, true); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.close)
		if r.Spill == nil || r.Spill.passRows() < 7 || r.Spill.passRows() > 13 {
			t.Fatalf("spill=%v, want passes of about 10", r.Spill != nil)
		}
	})
	t.Run("a byte budget smaller than one row still passes a row at a time", func(t *testing.T) {
		r := &foldResult{Changes: wideRows(1, 5)}
		if err := r.admit(0, 1, 1, true); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.close)
		if r.Spill == nil || r.Spill.passRows() != 1 {
			t.Fatalf("spill=%v, want passes of 1", r.Spill != nil)
		}
	})
}

// A spill sized by bytes runs more passes than its row limit alone would, and
// a group bigger than a pass is still read whole (the refusal stays on rows).
func TestChangeSpill_passesSizedByBytes(t *testing.T) {
	s := spillOf(t, 1000, pageOf(1, 640, "a"))
	count := func() (passes, rows int) {
		n, err := s.eachPass(t.Context(), func(pass map[string]*query.ResultRow, owns *[spillBuckets]bool) error {
			rows += len(pass)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return n, rows
	}
	if p, rows := count(); p != 1 || rows != 640 {
		t.Fatalf("row limit only: %d passes, %d rows; want 1 pass of 640", p, rows)
	}
	perRow := s.heldBytes / s.records
	s.maxBytes = 100 * perRow
	p, rows := count()
	if rows != 640 || p < 6 || p > 9 {
		t.Fatalf("budget of 100 rows: %d passes, %d rows; want 6-9 passes of all 640", p, rows)
	}
	s.maxBytes = 1 // smaller than any group: one group a pass, none refused
	if p, rows := count(); rows != 640 || p < 50 {
		t.Fatalf("passRows 1: %d passes, %d rows", p, rows)
	}
}

// The budget reaches the fold from its config: foldEventWindow divides
// MaxChangeBytes by the parallelism and spills on it (#2207). Without this, a
// deleted line at the call site keeps every admit test green while no fold
// ever bounds by size (the #2148 class: the function works, nothing calls it).
func TestFoldEventWindow_spillsOnTheByteBudget(t *testing.T) {
	for _, tc := range []struct {
		name     string
		maxBytes int64
		spill    bool
	}{{"under the budget", 1 << 40, false}, {"over the budget", 1, true}} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("FROM schema_snapshots").WillReturnRows(
				sqlmock.NewRows([]string{"snapshot_id", "MIN(snapshot_time)"}))
			mock.ExpectQuery("FROM archive_state").WillReturnRows(
				sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
					AddRow("one", archiveFixtureDir(t), nil, nil))
			for range 3 {
				mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
			}
			fixture := []query.ResultRow{foldEvent(1, "1", event.EventInsert), foldEvent(2, "2", event.EventInsert), foldEvent(3, "3", event.EventInsert)}
			fetcher := func(_ context.Context, opts query.Options, _ string) ([]query.ResultRow, error) {
				var out []query.ResultRow
				for _, r := range fixture {
					if opts.AfterEvent != nil && !(query.EventCursor{Timestamp: r.EventTimestamp, EventID: r.EventID}).After(*opts.AfterEvent) {
						continue
					}
					out = append(out, r)
					if opts.Limit > 0 && len(out) >= opts.Limit {
						break
					}
				}
				return out, nil
			}
			res, err := foldEventWindow(context.Background(), foldConfig{
				DB: db, Engine: query.New(db), Schema: "mydb", Table: "orders", PKCols: pkColsIntID(),
				Opts: query.Options{Schema: "mydb", Table: "orders"}, AllowGaps: true, ArchiveFetcher: fetcher, BatchSize: 2,
				MaxTouchedRows: 1000, MaxChangeBytes: tc.maxBytes, Parallelism: 1, SpillOverBudget: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer res.close()
			if (res.Spill != nil) != tc.spill {
				t.Fatalf("spilled = %v, want %v", res.Spill != nil, tc.spill)
			}
			if res.changeCount() != 3 {
				t.Fatalf("kept %d changes, want 3", res.changeCount())
			}
		})
	}
}

// The passes are sized over every drained row, not the sample that started
// the spill: a few huge rows the sample missed still count (#2207 review).
func TestChangeSpill_passRowsCountsEveryDrainedRow(t *testing.T) {
	small := pageOf(1, 640, "a")
	huge := map[string]*query.ResultRow{}
	for id := 1000; id < 1006; id++ {
		pk := strconv.Itoa(id)
		huge[pk] = &query.ResultRow{EventType: event.EventUpdate, PKValues: pk, RowAfter: map[string]any{"v": strings.Repeat("h", 1<<20)}}
	}
	s := spillOf(t, 1_000_000, small, huge)
	if s.heldBytes < 6<<20 {
		t.Fatalf("held %d bytes; the six 1 MiB rows are not counted", s.heldBytes)
	}
	s.maxBytes = 4 << 20
	want := s.maxBytes / (s.heldBytes / s.records)
	smallOnly := spillOf(t, 1_000_000, pageOf(1, 640, "a"))
	smallOnly.maxBytes = s.maxBytes
	if got := s.passRows(); got != want || got*10 > smallOnly.passRows() {
		t.Fatalf("passRows = %d (want %d), %d without the huge rows: the average must carry them", got, want, smallOnly.passRows())
	}
	var none changeSpill
	if none.passRows() != 0 {
		t.Fatal("no budget must mean no byte-sized passes")
	}
}

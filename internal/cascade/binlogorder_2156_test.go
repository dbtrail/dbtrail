package cascade_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2156 item 4: recover-cascade re-inserts each cascade-deleted child with
// its last image before the parent's DELETE. The scan for children reads each
// child's latest change by statement time (LimitPerPK 1), and event_timestamp
// is the time a change's STATEMENT STARTED: when session B waited on a row
// lock A held, A is in the binary log first and carries the later time, so
// the time pick re-inserts the child with A's value where the database held
// B's. A fetcher that can repick (query.LatestRepicker) is asked for each
// child's latest change in binary log order.

type childFetcher struct {
	timeLatest  []query.ResultRow
	repicked    []query.ResultRow
	order       query.LatestPerPKOrder
	repickErr   error
	repickCalls []query.Options
	repickRows  [][]query.ResultRow
}

func (f *childFetcher) Fetch(_ context.Context, o query.Options) ([]query.ResultRow, error) {
	if len(o.ColumnEq) == 0 {
		return nil, nil // the skew probe
	}
	return append([]query.ResultRow(nil), f.timeLatest...), nil
}

type repickingChildFetcher struct{ *childFetcher }

func (f repickingChildFetcher) RepickLatestInBinlog(_ context.Context, o query.Options, rows []query.ResultRow) ([]query.ResultRow, query.LatestPerPKOrder, error) {
	f.repickCalls = append(f.repickCalls, o)
	f.repickRows = append(f.repickRows, rows)
	if f.repickErr != nil {
		return nil, query.LatestPerPKOrder{}, f.repickErr
	}
	return f.repicked, f.order, nil
}

func childRow(v string, id uint64, ts time.Time) query.ResultRow {
	return query.ResultRow{EventID: id, EventTimestamp: ts, SchemaName: "app", TableName: "child",
		EventType: event.EventUpdate, PKValues: "10",
		RowBefore: map[string]any{"id": float64(10), "pid": float64(1), "v": "x"},
		RowAfter:  map[string]any{"id": float64(10), "pid": float64(1), "v": v}}
}

func victimValue(t *testing.T, res cascade.Result) string {
	t.Helper()
	if len(res.Victims) != 1 {
		t.Fatalf("victims = %+v, want one", res.Victims)
	}
	v, _ := res.Victims[0].RowBefore["v"].(string)
	return v
}

func TestSynthesizeVictims2156_childTakenAtItsLastChangeInBinlogOrder(t *testing.T) {
	fks, parents := twoParentDeletes()
	parents = parents[:1]
	t0 := parents[0].EventTimestamp.Add(-time.Minute)
	a := childRow("wait-A", 11, t0.Add(2*time.Second))
	b := childRow("wait-B", 12, t0)

	t.Run("a plain fetcher keeps the statement-time pick", func(t *testing.T) {
		f := &childFetcher{timeLatest: []query.ResultRow{a}}
		res, err := cascade.SynthesizeVictims(context.Background(), f, fks, parents, cascade.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if got := victimValue(t, res); got != "wait-A" {
			t.Fatalf("victim = %s, want wait-A", got)
		}
	})

	t.Run("a repicking fetcher re-inserts the change the database held", func(t *testing.T) {
		f := repickingChildFetcher{&childFetcher{timeLatest: []query.ResultRow{a}, repicked: []query.ResultRow{b},
			order: query.LatestPerPKOrder{Disagreed: 1, Sorted: 1}}}
		res, err := cascade.SynthesizeVictims(context.Background(), f, fks, parents, cascade.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if got := victimValue(t, res); got != "wait-B" {
			t.Fatalf("victim = %s, want wait-B", got)
		}
		if len(f.repickCalls) != 1 {
			t.Fatalf("%d repicks, want 1", len(f.repickCalls))
		}
		o := f.repickCalls[0]
		if o.LimitPerPK != 1 || len(o.ColumnEq) != 1 || o.ColumnEq[0].Column != "pid" || o.ColumnEq[0].Value != "1" {
			t.Fatalf("repick options %+v: want the scan's own (LimitPerPK 1, pid = 1)", o)
		}
		if len(f.repickRows[0]) != 1 || f.repickRows[0][0].EventID != 11 {
			t.Fatalf("repick rows %+v, want the scan's candidates", f.repickRows[0])
		}
		for _, w := range res.Warnings {
			if strings.Contains(w, "order of changes") {
				t.Fatalf("a proven order carries a warning: %q", w)
			}
		}
	})

	t.Run("an unproven order keeps the time pick and says so", func(t *testing.T) {
		f := repickingChildFetcher{&childFetcher{timeLatest: []query.ResultRow{a}, repicked: []query.ResultRow{a},
			order: query.LatestPerPKOrder{Disagreed: 1, Refused: 1}}}
		res, err := cascade.SynthesizeVictims(context.Background(), f, fks, parents, cascade.Options{})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, w := range res.Warnings {
			found = found || strings.HasPrefix(w, "app.child: order of changes unproven: for 1 row(s)")
		}
		if !found || !res.Complete() {
			t.Fatalf("warnings %q complete=%v: want the order note, advisory only", res.Warnings, res.Complete())
		}
	})

	t.Run("a failed repick is a failed scan", func(t *testing.T) {
		f := repickingChildFetcher{&childFetcher{timeLatest: []query.ResultRow{a}, repickErr: errors.New("boom")}}
		res, err := cascade.SynthesizeVictims(context.Background(), f, fks, parents, cascade.Options{})
		if err == nil || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("err = %v, want the repick's error", err)
		}
		if len(res.Victims) != 0 || res.Complete() {
			t.Fatalf("victims %+v complete=%v: want none, and the result partial", res.Victims, res.Complete())
		}
	})
}

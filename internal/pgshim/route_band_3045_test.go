package pgshim

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"testing"
	"time"
)

func TestCostPolicy_toCopyBanded_3045(t *testing.T) {
	pol := DefaultCostPolicy()
	for _, tc := range []struct {
		name string
		pl   pgPlan
		last string
		want bool
	}{
		{"a new shape over the threshold", pgPlan{cost: 52000}, "", true},
		{"a new shape under it", pgPlan{cost: 48000}, "", false},
		{"the source's, just over", pgPlan{cost: 52000}, RouteSource, false},
		{"the source's, at the band's edge", pgPlan{cost: 55000}, RouteSource, true},
		{"the source's, past the band", pgPlan{cost: 56000}, RouteSource, true},
		{"the copy's, just under", pgPlan{cost: 48000}, RouteCopy, true},
		{"the copy's, at the band's edge", pgPlan{cost: 45000}, RouteCopy, true},
		{"the copy's, past the band", pgPlan{cost: 44999}, RouteCopy, false},
		{"the copy's, a cheap plan", pgPlan{cost: 12}, RouteCopy, false},
		{"the source's, a report", pgPlan{cost: 13351258}, RouteSource, true},
		{"the source's, a scan just over its rows", pgPlan{cost: 10, maxSeqScanRows: 1050000}, RouteSource, false},
		{"the source's, a scan past the band", pgPlan{cost: 10, maxSeqScanRows: 1100000}, RouteSource, true},
		{"the copy's, a scan just under its rows", pgPlan{cost: 10, maxSeqScanRows: 950000}, RouteCopy, true},
		{"the copy's, a scan past the band", pgPlan{cost: 10, maxSeqScanRows: 899999}, RouteCopy, false},
	} {
		if got, why := pol.toCopyBanded(tc.pl, tc.last); got != tc.want {
			t.Errorf("%s: %v (%s), want %v", tc.name, got, why, tc.want)
		}
	}
	// A rule turned off stays off within the band.
	noScan := CostPolicy{CostThreshold: 50000}
	if got, _ := noScan.toCopyBanded(pgPlan{cost: 10, maxSeqScanRows: 5e6}, RouteCopy); got {
		t.Error("a scan rule turned off sent a cheap plan to the copy")
	}
}

// One shape whose estimate moves around the threshold: with the server's
// sides it stays where it went first, on every connection, until it moves
// past the band; without them it changes side with each move (as before).
func TestRouteByCost_3045_aShapeNearTheThreshold(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	plans := maps.Clone(costPlans)
	for _, c := range []int{44000, 48000, 52000, 54000, 56000} {
		plans[fmt.Sprintf("k = %d", c)] = plan(fmt.Sprintf(`{"Node Type": "Index Scan", "Total Cost": %d, "Plan Rows": 10}`, c))
	}
	at := func(c int) string { return fmt.Sprintf("SELECT n FROM near WHERE k = %d", c) }
	src := startFakeSource(t, plans)
	for _, tc := range []struct {
		name  string
		sides *CostSides
		costs []int
		want  []string
	}{
		{"kept by the band", &CostSides{}, []int{48000, 52000, 54000, 48000, 56000, 52000, 48000, 44000, 48000},
			[]string{RouteSource, RouteSource, RouteSource, RouteSource, RouteCopy, RouteCopy, RouteCopy, RouteSource, RouteSource}},
		{"first seen over it", &CostSides{}, []int{52000, 48000, 44000},
			[]string{RouteCopy, RouteCopy, RouteSource}},
		{"without the server's sides", nil, []int{48000, 52000, 48000},
			[]string{RouteSource, RouteCopy, RouteSource}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &costLogin{src: src, sides: tc.sides}
			for i, c := range tc.costs {
				// A connection of its own each time: the side is the server's.
				conn := costPortConn(t, ctx, l)
				got := costSimple(ctx, conn, l, at(c))
				if got.side != tc.want[i] {
					t.Errorf("statement %d (cost %d): %+v, want the %s", i, c, got, tc.want[i])
				}
				conn.Close(ctx)
			}
		})
	}
}

func TestCostSides_bounded(t *testing.T) {
	var c CostSides
	for i := range costSidesMax + 10 {
		c.note(fmt.Sprint(i), RouteSource)
	}
	if n := len(c.sides); n > costSidesMax {
		t.Errorf("%d shapes kept, at most %d", n, costSidesMax)
	}
	var nilSides *CostSides
	nilSides.note("x", RouteCopy)
	if nilSides.last("x") != "" {
		t.Error("nil remembers")
	}
}

// A prepared statement's plan without values keeps a side of its own, and a
// statement read only in part keeps none.
func TestBandedToCopy_3045_keys(t *testing.T) {
	sides := &CostSides{}
	s := &session{bound: &Binding{Forward: &Forward{Sides: sides}}, logger: slog.New(slog.DiscardHandler)}
	pol := DefaultCostPolicy()
	const q = "SELECT n FROM near WHERE k = 1"
	if got, _ := s.bandedToCopy(pol, pgPlan{cost: 48000}, q, false); got {
		t.Fatal("under the threshold went to the copy")
	}
	// The same text, prepared: its own side, decided afresh.
	if got, _ := s.bandedToCopy(pol, pgPlan{cost: 52000}, q, true); !got {
		t.Error("a prepared statement's plan was held by the simple query's side")
	}
	if got, _ := s.bandedToCopy(pol, pgPlan{cost: 52000}, q, false); got {
		t.Error("the simple query lost its side to the prepared statement's")
	}
	before := len(sides.sides)
	unread := "SELECT n FROM near WHERE k = 'never closed"
	s.bandedToCopy(pol, pgPlan{cost: 48000}, unread, false)
	if len(sides.sides) != before {
		t.Error("a statement read only in part was remembered")
	}
}

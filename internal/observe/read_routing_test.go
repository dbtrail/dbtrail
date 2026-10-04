package observe_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dbtrail/dbtrail/internal/observe"
)

// routeDecisionValue reads the published counter for one label set; -1 when
// no such series exists.
func routeDecisionValue(t *testing.T, server, route, reason string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "bintrail_read_routing_decisions_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			if got["server"] == server && got["route"] == route && got["reason"] == reason {
				return m.GetCounter().GetValue()
			}
		}
	}
	return -1
}

// TestObserveRouteDecision pins the metric's name and label set — the
// contract an operator's alert or dashboard is written against — and that
// each (server, route, reason) is its own series.
func TestObserveRouteDecision(t *testing.T) {
	observe.ObserveRouteDecision("srv-a", "copy", "expensive_plan")
	observe.ObserveRouteDecision("srv-a", "copy", "expensive_plan")
	observe.ObserveRouteDecision("srv-a", "mysql", "veto")
	observe.ObserveRouteDecision("srv-b", "mysql", "veto")

	if got := routeDecisionValue(t, "srv-a", "copy", "expensive_plan"); got != 2 {
		t.Errorf("srv-a copy/expensive_plan = %v, want 2", got)
	}
	if got := routeDecisionValue(t, "srv-a", "mysql", "veto"); got != 1 {
		t.Errorf("srv-a mysql/veto = %v, want 1", got)
	}
	if got := routeDecisionValue(t, "srv-b", "mysql", "veto"); got != 1 {
		t.Errorf("srv-b mysql/veto = %v, want 1 (its own series)", got)
	}
	if got := routeDecisionValue(t, "srv-b", "copy", "expensive_plan"); got != -1 {
		t.Errorf("srv-b copy/expensive_plan exists (%v); a series must appear only once observed", got)
	}
}

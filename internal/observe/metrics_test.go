package observe_test

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/dbtrail/dbtrail/internal/observe"
)

func TestForSource_registration(t *testing.T) {
	// promauto registers the vectors in the default registry at init time,
	// but a labeled vector only emits families once a child exists — touch
	// one source first.
	m := observe.ForSource("reg-test")
	m.EventsReceived.Inc()

	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var found int
	for _, mf := range mfs {
		if len(mf.GetName()) >= 8 && mf.GetName()[:8] == "bintrail" {
			found++
		}
	}
	if found == 0 {
		t.Error("expected at least one bintrail_* metric family in default registry")
	}
}

func TestForSource_isolatesSources(t *testing.T) {
	a := observe.ForSource("src-a")
	b := observe.ForSource("src-b")

	a.EventsReceived.Add(3)
	b.EventsReceived.Add(7)

	if got := testutil.ToFloat64(a.EventsReceived); got != 3 {
		t.Errorf("src-a events_received = %v, want 3", got)
	}
	if got := testutil.ToFloat64(b.EventsReceived); got != 7 {
		t.Errorf("src-b events_received = %v, want 7", got)
	}

	// Gauges must not clobber across sources — the whole point of the label.
	a.ReplicationLag.Set(1)
	b.ReplicationLag.Set(99)
	if got := testutil.ToFloat64(a.ReplicationLag); got != 1 {
		t.Errorf("src-a replication_lag = %v, want 1", got)
	}
}

func TestForSource_sameSourceAccumulates(t *testing.T) {
	first := observe.ForSource("src-acc")
	second := observe.ForSource("src-acc")

	first.EventsIndexed.Add(2)
	second.EventsIndexed.Add(5)

	if got := testutil.ToFloat64(second.EventsIndexed); got != 7 {
		t.Errorf("events_indexed = %v, want 7 (same source must share the child)", got)
	}
}

func TestForSource_emptyFallsBackToDefault(t *testing.T) {
	anon := observe.ForSource("")
	named := observe.ForSource("default")

	before := testutil.ToFloat64(named.BatchFlushes)
	anon.BatchFlushes.Inc()
	if got := testutil.ToFloat64(named.BatchFlushes); got != before+1 {
		t.Errorf(`ForSource("") must alias ForSource("default"): got %v, want %v`, got, before+1)
	}
}

func TestForSource_errorsKeepTypeLabel(t *testing.T) {
	m := observe.ForSource("src-err")
	// Incrementing with the remaining "type" label must not panic.
	m.Errors.WithLabelValues("batch_flush").Inc()
	m.Errors.WithLabelValues("checkpoint").Inc()
	m.Errors.WithLabelValues("gtid_update").Inc()

	if got := testutil.ToFloat64(m.Errors.WithLabelValues("checkpoint")); got != 1 {
		t.Errorf("errors{type=checkpoint} = %v, want 1", got)
	}
}

func TestForSource_batchSizeObserve(t *testing.T) {
	m := observe.ForSource("src-hist")
	// Observing histogram values should not panic.
	m.BatchSize.Observe(10)
	m.BatchSize.Observe(500)
	m.BatchSize.Observe(1000)
}

// TestStatementDMLDropped verifies the #776 statement-format DML counter
// increments. It is a global-registry singleton other tests may touch, so the
// assertion is a before/after delta, not an absolute value.
func TestStatementDMLDropped(t *testing.T) {
	read := func() float64 {
		mfs, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			t.Fatalf("Gather: %v", err)
		}
		for _, mf := range mfs {
			if mf.GetName() == "bintrail_statement_dml_dropped_total" {
				return mf.GetMetric()[0].GetCounter().GetValue()
			}
		}
		return 0
	}
	before := read()
	observe.StatementDMLDropped()
	observe.StatementDMLDropped()
	if got := read(); got != before+2 {
		t.Errorf("statement_dml_dropped_total = %v after two increments, want %v", got, before+2)
	}
}

// TestUnhandledRowsDropped verifies the unhandled-row-event drop counter adds
// the per-event ROW count (an unhandled event can carry many rows), rendered
// under the exact top-level name alerts key off. Global-registry singleton, so
// before/after delta like its statement-DML sibling.
func TestUnhandledRowsDropped(t *testing.T) {
	read := func() float64 {
		mfs, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			t.Fatalf("Gather: %v", err)
		}
		for _, mf := range mfs {
			if mf.GetName() == "bintrail_unhandled_rows_dropped_total" {
				return mf.GetMetric()[0].GetCounter().GetValue()
			}
		}
		return 0
	}
	before := read()
	observe.UnhandledRowsDropped(3)
	observe.UnhandledRowsDropped(1)
	if got := read(); got != before+4 {
		t.Errorf("unhandled_rows_dropped_total = %v after adding 3+1, want %v", got, before+4)
	}
}

// phaseHistogram reads the sample count and sum of one phase's series from
// the default registry; zero when the series has no observation yet.
func phaseHistogram(t *testing.T, phase string) (uint64, float64) {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "bintrail_sql_statement_phase_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "phase" && l.GetValue() == phase {
					return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
				}
			}
		}
	}
	return 0, 0
}

// #2026: one observation lands in its own phase's series, in seconds, and
// leaves the other phases alone.
func TestObserveSQLStatementPhase(t *testing.T) {
	qCount, qSum := phaseHistogram(t, "query")
	vCount, _ := phaseHistogram(t, "views")
	observe.ObserveSQLStatementPhase("query", 250*time.Millisecond)
	gotCount, gotSum := phaseHistogram(t, "query")
	if gotCount != qCount+1 || gotSum-qSum < 0.2499 || gotSum-qSum > 0.2501 {
		t.Errorf("query phase: count %d→%d, sum +%v; want +1 and +0.25 s", qCount, gotCount, gotSum-qSum)
	}
	if c, _ := phaseHistogram(t, "views"); c != vCount {
		t.Errorf("views phase moved %d→%d on a query observation", vCount, c)
	}
}

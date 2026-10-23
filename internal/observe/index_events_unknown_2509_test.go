package observe

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// An unknown count (#2509: a young index whose estimates read 0 beside a
// row that exists) leaves bintrail_index_events_total at its last value
// instead of publishing 0, which would read as an empty index.
func TestIndexMetricsSet_unknownEventsKeepTheLastValue(t *testing.T) {
	m := IndexForSource("t2509-unknown")
	now := time.Now()
	m.Set(IndexSnapshot{HaveCoverage: true, Events: 42, MySQLBytes: 7}, now)
	m.Set(IndexSnapshot{HaveCoverage: true, Events: 0, EventsUnknown: true, MySQLBytes: 9}, now)
	if got := testutil.ToFloat64(indexEventsTotal.WithLabelValues("t2509-unknown")); got != 42 {
		t.Fatalf("events_total = %v after an unknown count, want the last value 42", got)
	}
	if got := testutil.ToFloat64(indexStorageBytes.WithLabelValues("t2509-unknown", "mysql")); got != 9 {
		t.Fatalf("storage_bytes = %v, want 9: only the count is withheld", got)
	}
}

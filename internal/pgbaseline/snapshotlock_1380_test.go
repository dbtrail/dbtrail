package pgbaseline

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #1380: a PostgreSQL snapshot reads every table inside one REPEATABLE READ
// transaction, so its footer records a read of one instant.
func TestTableFooter_recordsAConsistentRead(t *testing.T) {
	md := tableFooter(tableInfo{Schema: "public", Table: "orders"}, "2026-06-10T12:00:00Z", 42)
	stamp, ok := md[baseline.MetaKeyLockMode]
	if !ok || stamp != baseline.LockStampPGRepeatableRead {
		t.Fatalf("the footer records %q (present %v), want %q", stamp, ok, baseline.LockStampPGRepeatableRead)
	}
	if got := baseline.ReadConsistencyOfStamp(stamp); got != baseline.ReadConsistent {
		t.Fatalf("the snapshot reads %s, want consistent", got)
	}
	// What was there before is still there.
	for key, want := range map[string]string{
		baseline.MetaKeySnapshotProducer: baseline.ProducerDump,
		baseline.MetaKeyLastDumpAt:       "2026-06-10T12:00:00Z",
		baseline.MetaKeyFoldGeneration:   "0",
		baseline.MetaKeyLSN:              "42",
		"bintrail.source_database":       "public",
		"bintrail.source_table":          "orders",
	} {
		if md[key] != want {
			t.Errorf("%s = %q, want %q", key, md[key], want)
		}
	}
}

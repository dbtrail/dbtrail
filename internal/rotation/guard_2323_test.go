package rotation

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
)

// TestGuardBlocks pins the one decision the loop and every reporting surface
// share (#2323): the upgrade guard trips on history beyond twice the window,
// and only history that accumulated under the index's own record is exempt.
func TestGuardBlocks(t *testing.T) {
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	retain := 48 * time.Hour
	part := func(age time.Duration) partitionInfo {
		return partitionInfo{Name: indexer.PartitionName(now.Add(-age))}
	}
	deep := 10 * 24 * time.Hour

	cases := []struct {
		desc       string
		partitions []partitionInfo
		retain     time.Duration
		recorded   bool
		recordedAt time.Time
		blocked    bool
		predates   bool
	}{
		{"no record, deep history: blocked", []partitionInfo{part(deep), part(time.Hour)}, retain, false, time.Time{}, true, false},
		{"record older than the oldest partition (an outage, not an upgrade): exempt",
			[]partitionInfo{part(deep)}, retain, true, now.Add(-deep - 24*time.Hour), false, false},
		{"record written exactly at the oldest partition: exempt",
			[]partitionInfo{part(deep)}, retain, true, now.Add(-deep).Truncate(time.Hour), false, false},
		{"record NEWER than the history (a restore): blocked, history predates the record",
			[]partitionInfo{part(deep)}, retain, true, now.Add(-time.Hour), true, true},
		{"no record, history inside twice the window: not blocked", []partitionInfo{part(72 * time.Hour)}, retain, false, time.Time{}, false, false},
		{"no record, exactly AT twice the window (strict >): not blocked", []partitionInfo{part(2 * retain)}, retain, false, time.Time{}, false, false},
		{"no record, one hour past twice the window: blocked", []partitionInfo{part(2*retain + time.Hour)}, retain, false, time.Time{}, true, false},
		{"zero window: nothing to measure against", []partitionInfo{part(deep)}, 0, false, time.Time{}, false, false},
		{"no partitions (fresh install)", nil, retain, false, time.Time{}, false, false},
		{"p_future only", []partitionInfo{{Name: "p_future"}}, retain, false, time.Time{}, false, false},
	}
	for _, tc := range cases {
		g := guardBlocks(tc.partitions, tc.retain, tc.recorded, tc.recordedAt, now)
		if g.Blocked != tc.blocked || g.HistoryPredatesRecord != tc.predates {
			t.Errorf("%s: blocked=%v predates=%v, want %v/%v", tc.desc, g.Blocked, g.HistoryPredatesRecord, tc.blocked, tc.predates)
		}
	}
	if g := guardBlocks([]partitionInfo{part(deep), part(time.Hour)}, retain, false, time.Time{}, now); !g.Oldest.Equal(now.Add(-deep).Truncate(time.Hour)) {
		t.Errorf("Oldest = %v, want the deepest partition's hour", g.Oldest)
	}
}

package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #1639: `bintrail status --baseline-dir` graded staleness over whatever the
// walk could read, so an unreadable newest folder turned a current table into
// "broken". A skipped folder at or after the newest readable snapshot makes
// the baselines unavailable; an older one does not.
func TestUnreadableNewestBaseline(t *testing.T) {
	older := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	newest := time.Date(2026, 9, 2, 6, 0, 0, 0, time.UTC)
	readable := []baseline.BaselineInfo{{SnapshotTime: older, Database: "shop", Table: "a"}, {SnapshotTime: newest, Database: "shop", Table: "b"}}
	for _, tc := range []struct {
		name       string
		baselines  []baseline.BaselineInfo
		unreadable []time.Time
		refuse     bool
	}{
		{"nothing skipped", readable, nil, false},
		{"older skipped", readable, []time.Time{older.Add(-time.Hour)}, false},
		{"skipped between two readable ones", readable, []time.Time{older.Add(time.Hour)}, false},
		{"newer skipped", readable, []time.Time{newest.Add(time.Hour)}, true},
		{"schema folder of the newest skipped", readable, []time.Time{newest}, true},
		{"nothing readable, something skipped", nil, []time.Time{older}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := unreadableNewestBaseline(tc.baselines, tc.unreadable)
			if (err != nil) != tc.refuse {
				t.Fatalf("err = %v, refuse = %v", err, tc.refuse)
			}
			if err != nil && !strings.Contains(err.Error(), "could not be read") {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

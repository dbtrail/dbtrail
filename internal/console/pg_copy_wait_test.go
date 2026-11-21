package console

import (
	"strings"
	"testing"
	"time"
)

// #2985: the wait is not begun when capture cannot reach the position in the
// time left, at the pace its saved position moved lately.
func TestPGCopyWaitHopeless(t *testing.T) {
	t0 := time.Unix(1000, 0)
	at := func(d time.Duration, lsn uint64) pgCopySaved { return pgCopySaved{lsn: lsn, ok: true, at: t0.Add(d)} }
	for _, tc := range []struct {
		name    string
		want    uint64
		samples []pgCopySaved
		left    time.Duration
		hope    bool
		says    string
	}{
		{"no reads yet", 100, nil, time.Second, true, ""},
		{"already reached", 100, []pgCopySaved{at(0, 50), at(2*time.Second, 100)}, time.Second, true, ""},
		{"watched too briefly to judge", 1000, []pgCopySaved{at(0, 50), at(500*time.Millisecond, 50)}, time.Second, true, ""},
		{"not moving", 1000, []pgCopySaved{at(0, 50), at(2*time.Second, 50)}, time.Second, false, "has not moved"},
		{"fast enough", 1000, []pgCopySaved{at(0, 0), at(time.Second, 900)}, time.Second, true, ""},
		{"too slow for the time left", 10000, []pgCopySaved{at(0, 0), at(time.Second, 100)}, time.Second, false, "at its pace"},
		{"no time left", 1000, []pgCopySaved{at(0, 0), at(time.Second, 10)}, 0, true, ""},
	} {
		why := pgCopyWaitHopeless(tc.want, tc.samples, tc.left)
		if (why == "") != tc.hope || !strings.Contains(why, tc.says) {
			t.Errorf("%s: %q", tc.name, why)
		}
	}
}

package cli

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/rotation"
)

func TestReplacementsOwed(t *testing.T) {
	cases := []struct {
		name      string
		res       rotation.Result
		noReplace bool
		want      string // "" = no error
	}{
		{"nothing skipped", rotation.Result{Dropped: 3, Added: 3}, false, ""},
		{"dropped, add skipped", rotation.Result{Dropped: 3, Deferred: 1, AddSkipped: 3, AddFutureTarget: 5}, false,
			"dropped 3 partition(s) but could not add 3 future partition(s): binlog_events was in use; run 'bintrail rotate --add-future 5' to add them"},
		// The next run tops up by itself: nothing is owed.
		{"add skipped, nothing dropped", rotation.Result{Deferred: 1, AddSkipped: 3, AddFutureTarget: 3}, false, ""},
		{"no-replace", rotation.Result{Dropped: 3, Deferred: 1, AddSkipped: 2, AddFutureTarget: 2}, true, ""},
	}
	for _, c := range cases {
		err := replacementsOwed(c.res, c.noReplace)
		got := ""
		if err != nil {
			got = err.Error()
		}
		if got != c.want || strings.Contains(got, "%!") {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

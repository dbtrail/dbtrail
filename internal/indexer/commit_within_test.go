package indexer

import (
	"testing"
	"time"
)

func TestCommitWithinFromEnv(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		set         bool
		want        time.Duration
	}{
		{"unset", "", false, DefaultCommitWithin},
		{"empty", "", true, DefaultCommitWithin},
		{"spaces only", "   ", true, DefaultCommitWithin},
		{"off", "off", true, 0},
		{"off in capitals", "OFF", true, 0},
		{"zero", "0", true, 0},
		{"zero with a unit", "0s", true, 0},
		{"milliseconds", "500ms", true, 500 * time.Millisecond},
		{"seconds with spaces around", "  2s ", true, 2 * time.Second},
		{"a number with no unit", "5", true, DefaultCommitWithin},
		{"a word", "soon", true, DefaultCommitWithin},
		{"negative", "-1s", true, DefaultCommitWithin},
		{"two lines", "1s\n2s", true, DefaultCommitWithin},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv(CommitWithinEnv, tc.value)
			} else {
				t.Setenv(CommitWithinEnv, "")
			}
			if got := CommitWithinFromEnv(); got != tc.want {
				t.Fatalf("%s=%q: got %v, want %v", CommitWithinEnv, tc.value, got, tc.want)
			}
		})
	}
}

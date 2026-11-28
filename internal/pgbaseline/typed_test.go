package pgbaseline

import "testing"

func TestTypedCopyOn(t *testing.T) {
	for raw, want := range map[string]bool{"": true, "1": true, "on": true, " TRUE ": true, "yes": true,
		"0": false, "false": false, "OFF": false, " no": false, "maybe": true} {
		if got := typedCopyOn(raw); got != want {
			t.Errorf("%q: %v, want %v", raw, got, want)
		}
	}
}

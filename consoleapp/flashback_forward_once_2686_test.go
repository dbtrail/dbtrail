package consoleapp

import "testing"

// #2686: the router says a server has no forwarding account once, and again
// only when the reason changes, not once per connection.
func TestForwardRefusedOnce(t *testing.T) {
	forwardRefusedSaid.Delete("s1")
	forwardRefusedSaid.Delete("s2")
	t.Cleanup(func() { forwardRefusedSaid.Delete("s1"); forwardRefusedSaid.Delete("s2") })
	steps := []struct {
		id, reason string
		want       bool
	}{
		{"s1", "no account", true},
		{"s1", "no account", false},
		{"s1", "no account", false},
		{"s2", "no account", true},
		{"s1", "the account is the capture one", true},
		{"s1", "the account is the capture one", false},
		{"s1", "no account", true},
	}
	for i, st := range steps {
		if got := forwardRefusedOnce(st.id, st.reason); got != st.want {
			t.Fatalf("step %d (%s, %q): %v, want %v", i, st.id, st.reason, got, st.want)
		}
	}
}

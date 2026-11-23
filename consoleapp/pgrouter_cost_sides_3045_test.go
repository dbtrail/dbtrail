package consoleapp

import "testing"

// #3045: one server's statement shapes share their sides across the port's
// connections, and another server's are its own.
func TestPGCostSides_3045(t *testing.T) {
	var p pgCostSides
	a, b := p.of("a"), p.of("b")
	if a == nil || b == nil {
		t.Fatal("no sides")
	}
	if p.of("a") != a {
		t.Error("a server's connections do not share its sides")
	}
	if a == b {
		t.Error("two servers share their sides")
	}
}

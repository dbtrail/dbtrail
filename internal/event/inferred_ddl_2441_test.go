package event

import (
	"testing"
	"time"
)

func TestParseInferredBracket(t *testing.T) {
	low := time.Date(2026, 6, 21, 11, 0, 0, 0, time.UTC)
	high := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		q    string
		want InferredBracket
		ok   bool
	}{
		{"both bounds", InferredDDLPrefix + " oid=10; instant=at-or-before; low=0/1000@2026-06-21T11:00:00Z; high=0/19DF9E8@2026-06-21T12:00:00Z */ ALTER TABLE s.a: added x (text)",
			InferredBracket{Low: low, HasLow: true, High: high}, true},
		{"no low", InferredDDLPrefix + " oid=10; instant=at-or-before; low=none; high=0/19DF9E8@2026-06-21T12:00:00Z */ CREATE TABLE s.a",
			InferredBracket{High: high}, true},
		// Prose after the marker cannot confuse the parser.
		{"prose mentions high=", InferredDDLPrefix + " oid=10; instant=at-or-before; low=none; high=0/1@2026-06-21T12:00:00Z */ ALTER TABLE s.a: added high=x (text)",
			InferredBracket{High: high}, true},
		{"mysql statement", "ALTER TABLE t ADD c INT", InferredBracket{}, false},
		{"truncated marker", InferredDDLPrefix + " oid=10; low=none; high=0/1@2026-06", InferredBracket{}, false},
		{"bad high", InferredDDLPrefix + " oid=10; low=none; high=garbage */ x", InferredBracket{}, false},
		{"bad low", InferredDDLPrefix + " oid=10; low=0/1@nope; high=0/1@2026-06-21T12:00:00Z */ x", InferredBracket{}, false},
		{"empty", "", InferredBracket{}, false},
	}
	for _, c := range cases {
		got, ok := ParseInferredBracket(c.q)
		if ok != c.ok || !got.Low.Equal(c.want.Low) || got.HasLow != c.want.HasLow || !got.High.Equal(c.want.High) {
			t.Errorf("%s: got %+v ok %v, want %+v ok %v", c.name, got, ok, c.want, c.ok)
		}
	}
}

// The gap mark (#2441): a table added back to the publication, whose changes
// while out were never captured.
func TestParseInferredBracket_gap(t *testing.T) {
	gap := InferredDDLPrefix + " oid=10; gap=uncaptured; instant=exact; at=2026-10-17T10:10:00Z; low=0/1@2026-10-17T10:00:00Z; high=0/2@2026-10-17T10:20:00Z */ ALTER PUBLICATION"
	if b, ok := ParseInferredBracket(gap); !ok || !b.Gap || !b.HasLow {
		t.Fatalf("gap row: %+v, %v", b, ok)
	}
	plain := InferredDDLPrefix + " oid=10; instant=at-or-before; low=none; high=0/2@2026-10-17T10:20:00Z */ ALTER PUBLICATION"
	if b, ok := ParseInferredBracket(plain); !ok || b.Gap {
		t.Fatalf("plain row: %+v, %v", b, ok)
	}
}

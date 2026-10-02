package event

import "testing"

// #2007: a system-versioned table's stored pk_values carries the ROW END
// period column; the fold keys the table on the declared key without it.
func TestDropPKComponent(t *testing.T) {
	for _, c := range []struct {
		name   string
		in     string
		idx, n int
		want   string
		wantOK bool
	}{
		{"implicit: id then row_end", "1|2106-02-07 06:28:15.999999", 1, 2, "1", true},
		{"a history row's key", "1|2026-10-02 06:13:43.628276", 1, 2, "1", true},
		{"composite key, period column in the middle", "a|2038-01-19 03:14:07.999999|b", 1, 3, "a|b", true},
		{"escaped pipe and backslash survive", `x\|y\\|2106-02-07 06:28:15.999999`, 1, 2, `x\|y\\`, true},
		{"empty remaining component", "|2106-02-07 06:28:15.999999", 1, 2, "", true},
		{"fewer components than the key", "1", 1, 2, "", false},
		{"more components than the key", "1|2|3", 1, 2, "", false},
		{"index out of range", "1|2", 2, 2, "", false},
		{"negative index", "1|2", -1, 2, "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, ok := DropPKComponent(c.in, c.idx, c.n)
			if ok != c.wantOK || got != c.want {
				t.Fatalf("DropPKComponent(%q, %d, %d) = %q, %v; want %q, %v", c.in, c.idx, c.n, got, ok, c.want, c.wantOK)
			}
		})
	}
}

// The inverse, for lookups: a declared-key value typed by an operator is
// spelled the way a versioned table stores it, with the period end put back
// at its position in the stored key.
func TestInsertPKComponent(t *testing.T) {
	const m = "2106-02-07 06:28:15.999999"
	for _, c := range []struct {
		name   string
		in     string
		idx, n int
		want   string
		wantOK bool
	}{
		{"single declared column", "2", 1, 2, "2|" + m, true},
		{"composite declared key, end last", "a|b", 2, 3, "a|b|" + m, true},
		{"end in the middle", "a|b", 1, 3, "a|" + m + "|b", true},
		{"end first", "7", 0, 2, m + "|7", true},
		{"escaped pipe stays one component", `x\|y`, 1, 2, `x\|y|` + m, true},
		{"already the stored width", "2|" + m, 1, 2, "", false},
		{"too few components", "a", 2, 3, "", false},
		{"index out of range", "a", 2, 2, "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, ok := InsertPKComponent(c.in, c.idx, c.n, m)
			if ok != c.wantOK || got != c.want {
				t.Fatalf("InsertPKComponent(%q, %d, %d) = %q, %v; want %q, %v", c.in, c.idx, c.n, got, ok, c.want, c.wantOK)
			}
		})
	}
}

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

package console

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestTailBudgetFor pins how the two settings are read (#2494): the keys
// default, a bad value warned and ignored, the bytes default as a share of
// the memory the process may use, none when that is unknown, 0 or off for
// no bound.
func TestTailBudgetFor(t *testing.T) {
	const gib = 1 << 30
	// The default is pinned as a number, not as the constant: a changed
	// default must be a deliberate edit here too.
	if tailDefaultMaxKeys != 500000 {
		t.Fatalf("default keys per tail = %d, want 500000 (and the docs say so)", tailDefaultMaxKeys)
	}
	for _, tc := range []struct {
		keys, bytes string
		host        uint64
		wantKeys    int
		wantBytes   int64
		warnings    int
	}{
		{"", "", 8 * gib, tailDefaultMaxKeys, 8 * gib / tailBytesShare, 0},
		{"", "", 0, tailDefaultMaxKeys, 0, 0},
		{"750000", "", 8 * gib, 750000, 8 * gib / tailBytesShare, 0},
		{" 12 ", "", 8 * gib, 12, 8 * gib / tailBytesShare, 0},
		{"0", "", 8 * gib, tailDefaultMaxKeys, 8 * gib / tailBytesShare, 1},
		{"-5", "", 8 * gib, tailDefaultMaxKeys, 8 * gib / tailBytesShare, 1},
		{"many", "", 8 * gib, tailDefaultMaxKeys, 8 * gib / tailBytesShare, 1},
		{"1e6", "", 8 * gib, tailDefaultMaxKeys, 8 * gib / tailBytesShare, 1},
		{"", "1GiB", 8 * gib, tailDefaultMaxKeys, gib, 0},
		{"", "512MB", 8 * gib, tailDefaultMaxKeys, 512 << 20, 0},
		{"", "0", 8 * gib, tailDefaultMaxKeys, 0, 0},
		{"", "off", 8 * gib, tailDefaultMaxKeys, 0, 0},
		{"", "OFF", 8 * gib, tailDefaultMaxKeys, 0, 0},
		{"", "lots", 8 * gib, tailDefaultMaxKeys, 8 * gib / tailBytesShare, 1},
		{"", "-1GiB", 8 * gib, tailDefaultMaxKeys, 8 * gib / tailBytesShare, 1},
		{"x", "y", 8 * gib, tailDefaultMaxKeys, 8 * gib / tailBytesShare, 2},
	} {
		b, w := tailBudgetFor(tc.keys, tc.bytes, tc.host)
		if b.Keys != tc.wantKeys || b.Bytes != tc.wantBytes || len(w) != tc.warnings {
			t.Errorf("tailBudgetFor(%q, %q, %d) = %+v, %d warnings; want keys %d, bytes %d, %d warnings: %v",
				tc.keys, tc.bytes, tc.host, b, len(w), tc.wantKeys, tc.wantBytes, tc.warnings, w)
		}
		for _, msg := range w {
			if !strings.Contains(msg, "BINTRAIL_SQL_TAIL_MAX_") {
				t.Errorf("a warning must name the setting: %q", msg)
			}
		}
	}
}

// TestTailKeeper_bytesBudget: the memory bound is over every tail together
// (#2494). A build past it is refused with the memory reason; a tail that
// advances past it freezes with that reason and is still served; the
// reason names memory, not keys, so the operator knows which setting to
// raise; 0 is no bound.
func TestTailKeeper_bytesBudget(t *testing.T) {
	ctx := context.Background()
	orders, customers := keeperTable("orders"), keeperTable("customers")
	ko, kc := tailKey("shop", "orders"), tailKey("shop", "customers")
	newKeeper := func(f *fakeIndex, b tailBudget) *tailKeeper {
		f.rowBytes = 1000
		k := newTailKeeper(f, t.TempDir(), b, time.Minute, time.Hour)
		k.retryMin, k.retryMax = 0, 0
		return k
	}
	entry := func(k *tailKeeper, key string) (refused string, frozen bool, why string) {
		k.mu.Lock()
		defer k.mu.Unlock()
		e := k.tables[key]
		if e == nil {
			return "", false, ""
		}
		return e.refused, e.frozen, e.frozenWhy
	}

	t.Run("a build past the bytes budget is refused for memory, not keys", func(t *testing.T) {
		f := &fakeIndex{}
		for _, key := range []string{"1", "2", "3"} {
			f.add("orders", key)
		}
		k := newKeeper(f, tailBudget{Keys: 100, Bytes: 2500}) // 3 keys = 3000 bytes
		if got := settled(t, ctx, k, orders); len(got) != 0 {
			t.Fatalf("served a tail that is over the bytes budget: %+v", got[ko])
		}
		refused, _, _ := entry(k, ko)
		if !strings.Contains(refused, "memory") || strings.Contains(refused, "keys changed") {
			t.Fatalf("refusal = %q, want the memory reason", refused)
		}
	})

	t.Run("a tail that advances past the bytes budget freezes and is still served", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		f.add("orders", "2")
		k := newKeeper(f, tailBudget{Keys: 100, Bytes: 2500})
		if got := settled(t, ctx, k, orders); len(got) != 1 {
			t.Fatalf("two keys, 2000 bytes, under the budget: %+v", got)
		}
		f.add("orders", "3")
		got := settled(t, ctx, k, orders)
		refused, frozen, why := entry(k, ko)
		if refused != "" || !frozen || !strings.Contains(why, "memory") {
			t.Fatalf("refused %q frozen %v why %q; want frozen for memory", refused, frozen, why)
		}
		if len(got) != 1 {
			t.Fatalf("a frozen tail is served as it is: %+v", got)
		}
	})

	t.Run("the bytes budget is over every table's tail together", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		f.add("orders", "2")
		f.add("customers", "1")
		k := newKeeper(f, tailBudget{Keys: 100, Bytes: 2500}) // 3000 together, each alone under
		got := settled(t, ctx, k, orders, customers)
		ro, _, _ := entry(k, ko)
		rc, _, _ := entry(k, kc)
		if (ro == "") == (rc == "") {
			t.Fatalf("exactly one of the two must be refused: orders %q, customers %q, served %d", ro, rc, len(got))
		}
		if !strings.Contains(ro+rc, "memory") {
			t.Fatalf("the refusal names memory: orders %q, customers %q", ro, rc)
		}
	})

	t.Run("bytes 0 is no bound; the keys bound still holds", func(t *testing.T) {
		f := &fakeIndex{}
		for _, key := range []string{"1", "2", "3", "4"} {
			f.add("orders", key)
		}
		k := newKeeper(f, tailBudget{Keys: 100, Bytes: 0})
		if got := settled(t, ctx, k, orders); len(got) != 1 {
			t.Fatalf("no bytes bound: %+v", got)
		}
		k2 := newKeeper(f, tailBudget{Keys: 3, Bytes: 0})
		if got := settled(t, ctx, k2, orders); len(got) != 0 {
			t.Fatalf("over the keys bound at a build: %+v", got)
		}
		if refused, _, _ := entry(k2, ko); !strings.Contains(refused, "keys changed") {
			t.Fatalf("refusal = %q, want the keys reason", refused)
		}
	})
}

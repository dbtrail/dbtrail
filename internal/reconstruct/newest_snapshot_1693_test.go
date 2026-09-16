package reconstruct

import (
	"context"
	"testing"
)

// NewestSnapshot returns the instant the listing already sorted by, so the
// caller folding forward from that snapshot knows how much time it covers.
func TestNewestSnapshot_returnsTheNewestInstant(t *testing.T) {
	root := t.TempDir()
	snapshot1639(t, root, t1639a, "shop.a", "shop.b")
	snapshot1639(t, root, t1639c, "shop.a")
	snapshot1639(t, root, t1639b, "shop.a", "shop.c")

	at, tables, err := NewestSnapshot(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if !at.Equal(t1639c) {
		t.Errorf("instant = %v, want the newest directory %v", at, t1639c)
	}
	if len(tables) != 1 || tables[0] != "shop.a" {
		t.Errorf("tables = %v, want only the newest snapshot's [shop.a]", tables)
	}

	// The wrapper keeps its contract.
	only, err := NewestSnapshotTables(context.Background(), root)
	if err != nil || len(only) != 1 || only[0] != "shop.a" {
		t.Errorf("NewestSnapshotTables = %v, %v", only, err)
	}

	// No snapshot at all: zero instant, nil tables, nil error, as before.
	zt, zs, err := NewestSnapshot(context.Background(), t.TempDir())
	if err != nil || !zt.IsZero() || zs != nil {
		t.Errorf("empty root = %v, %v, %v; want zero, nil, nil", zt, zs, err)
	}
}

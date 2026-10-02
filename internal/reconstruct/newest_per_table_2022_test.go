package reconstruct

import (
	"testing"
	"time"
)

// A table name can hold a dot (#2008): a/b.c and a.b/c are two tables, and
// each keeps its own newest file.
func TestNewestPerTable_dottedNamesDoNotCollide(t *testing.T) {
	now := time.Now().UTC()
	files := []BaselineFile{
		{Schema: "a", Table: "b.c", SnapshotTime: now.Add(-time.Hour)},
		{Schema: "a.b", Table: "c", SnapshotTime: now},
	}
	if got := NewestPerTable(files); len(got) != 2 {
		t.Fatalf("newest = %v, want both tables", got)
	}
}

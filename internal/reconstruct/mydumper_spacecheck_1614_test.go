package reconstruct

import (
	"errors"
	"os"
	"testing"
)

// The .sql backup's disk check (#1614) runs before every chunk file, with the
// output directory and the chunk size, and a refusal stops the table before
// the file exists.
func TestMydumperWriter_spaceCheckBeforeEachChunk(t *testing.T) {
	dir := t.TempDir()
	w, err := NewMydumperWriter(dir, "shop", "a", []string{"id"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	full := errors.New("disk full")
	var calls int
	w.spaceCheck = func(d string, next int64) error {
		calls++
		if d != dir || next != 10 {
			t.Fatalf("checked %q for %d bytes", d, next)
		}
		if calls == 2 {
			return full
		}
		return nil
	}
	if err := w.WriteRow([]any{int64(1)}); err != nil {
		t.Fatalf("first row: %v", err)
	}
	if err := w.WriteRow([]any{int64(2)}); !errors.Is(err, full) {
		t.Fatalf("second chunk: err = %v, want the refusal", err)
	}
	if calls != 2 {
		t.Fatalf("checks = %d, want one per chunk", calls)
	}
	if _, err := os.Stat(dir + "/shop.a.00001.sql"); !os.IsNotExist(err) {
		t.Fatalf("the refused chunk file exists: %v", err)
	}
}

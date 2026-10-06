package reconstruct

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// Only the exact ".<snapshot time>.discarding" shape this package creates is
// one of ours: a folder an operator named ".backups.discarding" is not.
func TestIsDiscardingName_onlyTheShapeItCreates(t *testing.T) {
	for name, want := range map[string]bool{
		".2026-10-06T10-00-00Z.discarding":     true,
		discardingName("2026-10-06T10-00-00Z"): true,
		".backups.discarding":                  false,
		".discarding":                          false,
		"..discarding":                         false,
		"2026-10-06T10-00-00Z.discarding":      false,
		".2026-10-06T10-00-00Z.pruning":        false,
	} {
		if got := isDiscardingName(name); got != want {
			t.Errorf("isDiscardingName(%q) = %v, want %v", name, got, want)
		}
	}
	root := t.TempDir()
	keep := filepath.Join(root, ".backups.discarding")
	if err := os.MkdirAll(keep, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := SweepDiscardedSnapshots(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatal("the sweep removed an operator's folder")
	}
}

// The full-table fold publishes through the marker check too (#2180).
func TestFinalizeCompletenessMarker_refusesWithoutItsMarker(t *testing.T) {
	dir := t.TempDir()
	if err := finalizeCompletenessMarker(dir, nil, nil); err == nil {
		t.Fatal("a folder whose _INCOMPLETE marker was gone was published")
	}
	if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); err == nil {
		t.Fatal("_SUCCESS was written")
	}
}

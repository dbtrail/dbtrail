package consoleapp

import (
	"strings"
	"testing"
)

// A table the server reports no size for adds nothing to the sum, so room for
// the sum is not room for the read: the check says it cannot tell (#2278).
func TestDumpDiskVerdict_unsizedTables_2278(t *testing.T) {
	stage := t.TempDir()
	const cannotTell = "Disk check cannot tell whether this read fits"

	t.Run("one unsized table and room for the sum: not vouched for", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 100 * gib})
		est := dumpEstimate{DataBytes: int64(10 * gib), Bytes: int64(10 * gib), Tables: 4, Unsized: 1}
		check, note, err := dumpDiskVerdict(stage, "", est, nil)
		t.Logf("note: %s", note)
		if err != nil || check != dumpDiskUnchecked {
			t.Fatalf("check = %q, err = %v, want unchecked", check, err)
		}
		if !strings.HasPrefix(note, cannotTell+". By the sizes the server reports: a full read needs about 10.0 GiB free at ") ||
			!strings.Contains(note, "The source gave no size for 1 table(s)") {
			t.Fatalf("note = %q", note)
		}
	})
	t.Run("every table unsized: no need of 0 B is printed", func(t *testing.T) {
		for _, local := range []string{"", t.TempDir()} {
			if local != "" {
				stubSameFS(t, false, nil)
			}
			diskByPath(t, map[string]uint64{stage: 100 * gib, local: 100 * gib})
			check, note, err := dumpDiskVerdict(stage, local, dumpEstimate{Tables: 3, Unsized: 3}, nil)
			t.Logf("note: %s", note)
			if err != nil || check != dumpDiskUnchecked || !strings.HasPrefix(note, cannotTell) {
				t.Fatalf("check = %q, err = %v, note = %q", check, err, note)
			}
			if strings.Contains(note, "0 B") || !strings.Contains(note, "3 table(s)") || !strings.Contains(note, "100.0 GiB free at "+stage) {
				t.Fatalf("note = %q", note)
			}
		}
	})
	// The warnings called the sum a bound, which it is not with a size missing.
	t.Run("short of the sum: a warning with no claim of a bound", func(t *testing.T) {
		local := t.TempDir()
		est := dumpEstimate{DataBytes: int64(10 * gib), Bytes: int64(40 * gib), Tables: 4, Unsized: 1}
		diskByPath(t, map[string]uint64{stage: 20 * gib})
		check, note, err := dumpDiskVerdict(stage, "", est, nil)
		if err != nil || check != dumpDiskLow || strings.Contains(note, "upper bound") {
			t.Fatalf("check = %q, err = %v, note = %q", check, err, note)
		}
		stubSameFS(t, false, nil)
		diskByPath(t, map[string]uint64{stage: 100 * gib, local: gib})
		check, note, err = dumpDiskVerdict(stage, local, est, nil)
		t.Logf("note: %s", note)
		if err != nil || check != dumpDiskLow || strings.Contains(note, "up to") || strings.Contains(note, "The dump itself fits") {
			t.Fatalf("check = %q, err = %v, note = %q", check, err, note)
		}
	})
	t.Run("every table sized: ok, as before", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 100 * gib})
		check, note, err := dumpDiskVerdict(stage, "", dumpEstimate{DataBytes: int64(10 * gib), Bytes: int64(10 * gib), Tables: 4}, nil)
		if err != nil || check != dumpDiskOK || !strings.HasPrefix(note, "Disk check: a full read needs about 10.0 GiB free at ") {
			t.Fatalf("check = %q, err = %v, note = %q", check, err, note)
		}
	})
}

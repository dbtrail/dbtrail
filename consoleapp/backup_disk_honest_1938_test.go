package consoleapp

import (
	"errors"
	"strings"
	"testing"
)

// #1938, the check itself. Three things it got wrong or could not see:
// it refused on a size that counts secondary indexes, which a dump does not
// hold; it called its estimate an upper bound for tables whose reported size
// is their COMPRESSED size; and its query could go on working on the source
// after the read had stopped waiting for it.

// A dump holds no secondary indexes, so a table that is mostly indexes is no
// reason to refuse. The refusal reads the data alone; the warnings still read
// data plus indexes.
func TestDumpDiskVerdict_refusesOnDataAlone_1938(t *testing.T) {
	stage := t.TempDir()
	// 10 GiB of rows under 30 GiB of secondary indexes.
	est := dumpEstimate{DataBytes: int64(10 * gib), Bytes: int64(40 * gib), Tables: 1}

	t.Run("15 GiB free: runs with a warning (it was refused: 15 < half of 40)", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 15 * gib})
		check, note, err := dumpDiskVerdict(stage, "", est, nil)
		if err != nil || check != dumpDiskLow {
			t.Fatalf("check = %q, err = %v, want a warning and no refusal", check, err)
		}
		t.Logf("note: %s", note)
	})
	t.Run("under half the data: refused, with the two numbers a reader needs", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 4 * gib})
		_, _, err := dumpDiskVerdict(stage, "", est, nil)
		if !errors.Is(err, errFoldDiskFull) {
			t.Fatalf("err = %v, want a disk refusal", err)
		}
		t.Logf("refusal: %v", err)
		msg := err.Error()
		for _, want := range []string{
			"the full read was not started",
			"take up about 10.0 GiB on the database server, not counting indexes",
			"the working folder " + stage + " has 4.0 GiB free",
			"Nothing was read from your database, and earlier snapshots are unchanged",
			`"Working folder" setting`, "BINTRAIL_CONSOLE_BASELINE_STAGING",
		} {
			if !strings.Contains(msg, want) {
				t.Errorf("refusal lacks %q:\n%s", want, msg)
			}
		}
		// Not the size with indexes, not the line it was measured against,
		// and none of the check's own reasoning.
		for _, not := range []string{"40.0 GiB", "5.0 GiB", "20.0 GiB", "upper bound", "half", "measured", "dump"} {
			if strings.Contains(msg, not) {
				t.Errorf("refusal says %q:\n%s", not, msg)
			}
		}
	})
	t.Run("exactly half the data is not refused", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 5 * gib})
		if _, _, err := dumpDiskVerdict(stage, "", est, nil); err != nil {
			t.Fatalf("refused at the line: %v", err)
		}
		diskByPath(t, map[string]uint64{stage: 5*gib - 1})
		if _, _, err := dumpDiskVerdict(stage, "", est, nil); !errors.Is(err, errFoldDiskFull) {
			t.Fatalf("one byte under the line was let through: %v", err)
		}
	})
	t.Run("a server that reports no data size never refuses", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 0})
		check, _, err := dumpDiskVerdict(stage, "", dumpEstimate{DataBytes: 0, Bytes: int64(40 * gib), Tables: 1, Unsized: 1}, nil)
		if err != nil || check != dumpDiskLow {
			t.Fatalf("check = %q, err = %v", check, err)
		}
	})
}

// With compressed tables the sizes are not a bound, so no verdict may say
// they are, and "there is room" becomes "the check cannot vouch for it".
func TestDumpDiskVerdict_compressedTables_1938(t *testing.T) {
	stage := t.TempDir()
	est := dumpEstimate{DataBytes: int64(10 * gib), Bytes: int64(10 * gib), Tables: 4, Compressed: 3, CompressedTop: []string{"shop.orders", "shop.events"}}
	const sentence = "3 tables use compressed storage (shop.orders, shop.events and 1 more)"

	t.Run("room by the reported sizes: not vouched for", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 100 * gib})
		check, note, err := dumpDiskVerdict(stage, "", est, nil)
		t.Logf("note: %s", note)
		if err != nil || check != dumpDiskUnchecked {
			t.Fatalf("check = %q, err = %v, want unchecked", check, err)
		}
		// Its first words say what it could not do: after "Snapshot complete"
		// a note that opened "Disk check: a full read needs..." read as a pass.
		if !strings.HasPrefix(note, "Disk check cannot tell whether this read fits. By the sizes the server reports: a full read needs about 10.0 GiB free at ") ||
			!strings.Contains(note, sentence) || !strings.Contains(note, "100.0 GiB free") || strings.Contains(note, "upper bound") {
			t.Fatalf("note = %q", note)
		}
	})
	// The snapshot folder on another disk, the usual local setup: the same.
	t.Run("room on both disks: not vouched for either", func(t *testing.T) {
		local := t.TempDir()
		stubSameFS(t, false, nil)
		diskByPath(t, map[string]uint64{stage: 100 * gib, local: 100 * gib})
		check, note, err := dumpDiskVerdict(stage, local, est, nil)
		if err != nil || check != dumpDiskUnchecked || !strings.HasPrefix(note, "Disk check cannot tell whether this read fits. By the sizes the server reports: the dump needs about 10.0 GiB free at ") ||
			!strings.Contains(note, sentence) {
			t.Fatalf("check = %q, err = %v, note = %q", check, err, note)
		}
		plain := dumpEstimate{DataBytes: int64(10 * gib), Bytes: int64(10 * gib), Tables: 4}
		if check, note, _ := dumpDiskVerdict(stage, local, plain, nil); check != dumpDiskOK || !strings.HasPrefix(note, "Disk check: the dump needs about ") {
			t.Fatalf("without compressed tables: check = %q, note = %q", check, note)
		}
	})
	// The snapshot folder on another disk and short of room: the warning made
	// two claims about a bound ("can take up to", "the dump itself fits").
	t.Run("the other disk is short: a warning with no claim the sizes cannot back", func(t *testing.T) {
		local := t.TempDir()
		stubSameFS(t, false, nil)
		diskByPath(t, map[string]uint64{stage: 100 * gib, local: gib})
		check, note, err := dumpDiskVerdict(stage, local, est, nil)
		t.Logf("note: %s", note)
		if err != nil || check != dumpDiskLow || !strings.Contains(note, sentence) {
			t.Fatalf("check = %q, err = %v, note = %q", check, err, note)
		}
		for _, not := range []string{"up to", "The dump itself fits", "upper bound"} {
			if strings.Contains(note, not) {
				t.Errorf("the note says %q over compressed tables: %s", not, note)
			}
		}
		if !strings.Contains(note, "By those sizes the dump fits at "+stage) {
			t.Errorf("the note no longer says where the dump goes: %s", note)
		}
		// Without compressed tables the sentence is what it was.
		plain := dumpEstimate{DataBytes: int64(10 * gib), Bytes: int64(10 * gib), Tables: 4}
		if _, note, _ := dumpDiskVerdict(stage, local, plain, nil); !strings.Contains(note, "the copy can take up to about 10.0 GiB") || !strings.Contains(note, "The dump itself fits at "+stage) {
			t.Errorf("the plain note changed: %s", note)
		}
	})
	for name, free := range map[string]uint64{"below the sizes": 9 * gib, "below the peak": 15 * gib} {
		t.Run(name+": a warning that does not call the sizes a bound", func(t *testing.T) {
			diskByPath(t, map[string]uint64{stage: free})
			check, note, err := dumpDiskVerdict(stage, "", est, nil)
			if err != nil || check != dumpDiskLow || !strings.Contains(note, sentence) || strings.Contains(note, "upper bound") {
				t.Fatalf("check = %q, err = %v, note = %q", check, err, note)
			}
		})
	}
	t.Run("a refusal names them too", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: gib})
		_, _, err := dumpDiskVerdict(stage, "", est, nil)
		if !errors.Is(err, errFoldDiskFull) || !strings.Contains(err.Error(), sentence) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("without compressed tables the verdicts are what they were", func(t *testing.T) {
		plain := dumpEstimate{DataBytes: int64(10 * gib), Bytes: int64(10 * gib), Tables: 4}
		diskByPath(t, map[string]uint64{stage: 100 * gib})
		if check, note, _ := dumpDiskVerdict(stage, "", plain, nil); check != dumpDiskOK || strings.Contains(note, "compressed storage") {
			t.Fatalf("check = %q, note = %q", check, note)
		}
		diskByPath(t, map[string]uint64{stage: 9 * gib})
		if check, note, _ := dumpDiskVerdict(stage, "", plain, nil); check != dumpDiskLow || !strings.Contains(note, "upper bound") {
			t.Fatalf("check = %q, note = %q", check, note)
		}
	})
}

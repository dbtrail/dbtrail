package archive

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// The stored value is a GROUPING KEY, so canonicalization is not cosmetic: two
// files with the same columns must produce the same string, or they split into
// two groups and each extra group costs a footer read at bind time — the exact
// cost #1535 removes.
func TestColumnSetOf_isCanonical(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []string
		want string
	}{
		{"sorted, not write order", []string{"query_text", "event_id", "commit_ts_us"}, "commit_ts_us,event_id,query_text"},
		{"lowercased", []string{"Event_ID", "QUERY_TEXT"}, "event_id,query_text"},
		{"trimmed", []string{" event_id ", "query_text"}, "event_id,query_text"},
		{"deduplicated", []string{"event_id", "event_id", "query_text"}, "event_id,query_text"},
		{"blanks dropped", []string{"event_id", "", "  "}, "event_id"},
		{"nothing in, nothing out", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ColumnSetOf(tc.in); got != tc.want {
				t.Errorf("ColumnSetOf(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}

	// Two orderings of one set must agree. This is the property, stated
	// directly rather than left to the table above.
	a := ColumnSetOf([]string{"a", "b", "c"})
	b := ColumnSetOf([]string{"c", "a", "b"})
	if a != b {
		t.Errorf("two orderings of one column set gave %q and %q", a, b)
	}
}

// ColumnSet over the REAL archive column list is what rotation records, so this
// pins the two together: a column added to BinlogEventColumns without the
// writer noticing would leave archives recorded under a set they do not have,
// and a group naming a column its files lack fails the whole view.
func TestColumnSet_matchesTheArchiveWriter(t *testing.T) {
	got := ColumnSet(BinlogEventColumns)
	names := strings.Split(got, ",")
	if len(names) != len(BinlogEventColumns) {
		t.Fatalf("the archive column set renders %d name(s) for %d column(s)", len(names), len(BinlogEventColumns))
	}
	for _, c := range BinlogEventColumns {
		if !strings.Contains(","+got+",", ","+strings.ToLower(c.Name)+",") {
			t.Errorf("column %q is written to every archive but is not in the recorded set %q", c.Name, got)
		}
	}
}

// The backfill (#1535): a row with no recorded column set gets one from the
// scanned footer, on a plain reconcile. NOT gated on --deep — the gate keeps S3
// footer reads off the default path, and a set that was not read is empty here,
// so the condition simply never fires on S3 without it. A local scan already
// read the footer for row_count, so the repair costs no extra file open.
func TestDiffRecordsTheArchivedColumnSet(t *testing.T) {
	const set = "commit_ts_us,event_id,query_text"
	withSet := func(f ScannedFile) ScannedFile { f.ColumnSet = set; return f }

	t.Run("absent → recorded, without --deep", func(t *testing.T) {
		f := withSet(localFile("p_2026060510", "abc", "/a/x.parquet", 100, 42))
		rows := []StateRow{{PartitionName: "p_2026060510", BintrailID: "abc",
			LocalPath: nStr("/a/x.parquet"), FileSizeBytes: nInt(100), RowCount: nInt(42), ArchivedAt: tOld}}
		opts := bothScanned() // Deep is false
		rep := Diff([]ScannedFile{f}, rows, opts)
		if rep.Updates != 1 {
			t.Fatalf("want 1 update, got %+v", rep)
		}
		if got := changes(rep.Actions[0])["column_set"]; got != set {
			t.Errorf("column_set = %v, want %q", got, set)
		}
		if !strings.Contains(rep.Actions[0].Reason, "column set") {
			t.Errorf("the reason does not name what is being repaired: %q", rep.Actions[0].Reason)
		}
	})

	t.Run("already recorded → no action", func(t *testing.T) {
		f := withSet(localFile("p_2026060510", "abc", "/a/x.parquet", 100, 42))
		rows := []StateRow{{PartitionName: "p_2026060510", BintrailID: "abc",
			LocalPath: nStr("/a/x.parquet"), FileSizeBytes: nInt(100), RowCount: nInt(42),
			ColumnSet: nStr(set), ArchivedAt: tOld}}
		rep := Diff([]ScannedFile{f}, rows, bothScanned())
		if len(rep.Actions) != 0 || rep.InSync != 1 {
			t.Fatalf("a repaired registry still reports drift, so a cron never goes green again: %+v", rep)
		}
	})

	t.Run("drift → corrected", func(t *testing.T) {
		f := withSet(localFile("p_2026060510", "abc", "/a/x.parquet", 100, 42))
		rows := []StateRow{{PartitionName: "p_2026060510", BintrailID: "abc",
			LocalPath: nStr("/a/x.parquet"), FileSizeBytes: nInt(100), RowCount: nInt(42),
			ColumnSet: nStr("event_id"), ArchivedAt: tOld}}
		rep := Diff([]ScannedFile{f}, rows, bothScanned())
		if rep.Updates != 1 || changes(rep.Actions[0])["column_set"] != set {
			t.Fatalf("a recorded set that no longer matches the file was left in place: %+v", rep)
		}
	})

	t.Run("footer not read → nothing claimed", func(t *testing.T) {
		// An S3 scan without --deep. The row stays unrecorded rather than
		// being recorded as having no columns, which would form a group whose
		// read_parquet names nothing.
		f := s3File("p_2026060510", "abc", "bkt", "k/events.parquet", 100)
		rows := []StateRow{{PartitionName: "p_2026060510", BintrailID: "abc",
			S3Bucket: nStr("bkt"), S3Key: nStr("k/events.parquet"), S3UploadedAt: nTime(tModified),
			FileSizeBytes: nInt(100), ArchivedAt: tOld}}
		rep := Diff([]ScannedFile{f}, rows, DiffOptions{ScannedS3: true, PruneMinAge: 0, Now: tNow})
		for _, a := range rep.Actions {
			if _, ok := changes(a)["column_set"]; ok {
				t.Errorf("a column set was recorded from a footer that was never read: %+v", a)
			}
		}
	})

	t.Run("insert carries it", func(t *testing.T) {
		f := withSet(localFile("p_2026060510", "abc", "/a/x.parquet", 100, 42))
		rep := Diff([]ScannedFile{f}, nil, bothScanned())
		if rep.Inserts != 1 {
			t.Fatalf("want 1 insert, got %+v", rep)
		}
		if got := changes(rep.Actions[0])["column_set"]; got != set {
			t.Errorf("a rebuilt registry row has no column set (%v); every partition would read as unrecorded "+
				"and the views would stay on the per-file bind", got)
		}
	})
}

// The recorded set must be the set that is actually IN the file. Asserting
// ColumnSet(BinlogEventColumns) against the same list it was built from proves
// nothing, so this writes a real Parquet through the real writer and reads the
// names back out of its footer — the same footer `archive reconcile` reads to
// backfill, which is what makes the recorded and the backfilled value one
// thing.
func TestColumnSet_isWhatTheParquetFooterHolds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.parquet")
	w, err := baseline.NewWriter(path, BinlogEventColumns, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 10,
	})
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	var names []string
	for _, fld := range pf.Schema().Fields() {
		names = append(names, fld.Name())
	}
	if got, want := ColumnSetOf(names), ColumnSet(BinlogEventColumns); got != want {
		t.Errorf("the footer holds %q but rotation records %q; a group would name columns its files do not have", got, want)
	}
}

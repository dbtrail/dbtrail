package reconstruct

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// writeAnchoredFile2152 writes an empty table file with a binlog anchor and,
// when archiveCut is not "", the archive-cut key.
func writeAnchoredFile2152(t *testing.T, path, producer, file string, pos int64, archiveCut string) {
	t.Helper()
	cols, err := baseline.ParseSchemaText("CREATE TABLE `x` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n")
	if err != nil {
		t.Fatal(err)
	}
	md := map[string]string{
		baseline.MetaKeyBinlogFile: file,
		baseline.MetaKeyBinlogPos:  strconv.FormatInt(pos, 10),
	}
	if producer != "" {
		md[baseline.MetaKeySnapshotProducer] = producer
	}
	if archiveCut != "" {
		md[baseline.MetaKeyArchiveCut] = archiveCut
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func cutKey(file string, pos uint64) string {
	return encodeArchiveCut(query.BinlogPos{File: file, Pos: pos})
}

const rec = baseline.ProducerReconstruct

// The cut a refresh searched through, read back from the snapshot directory
// it published (#2152): only from the archive-cut key, which a refresh writes
// when it checked the archives. Each case is a way a directory can mislead.
func TestSnapshotCutOf_2152(t *testing.T) {
	t.Run("the newest archive cut, across schemas and deltas", func(t *testing.T) {
		dir := t.TempDir()
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "orders.parquet"), rec, "binlog.000002", 4, cutKey("binlog.000002", 4)) // carried forward
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "items.parquet"), rec, "binlog.999999", 900, cutKey("binlog.999999", 900))
		writeAnchoredFile2152(t, filepath.Join(dir, "crm", "people.upserts.0001.parquet"), rec, "binlog.1000000", 7, cutKey("binlog.1000000", 7)) // rollover
		got := snapshotCutOf(dir)
		if got == nil || *got != (query.BinlogPos{File: "binlog.1000000", Pos: 7}) {
			t.Fatalf("snapshotCutOf = %+v, want binlog.1000000:7", got)
		}
	})
	t.Run("a refresh file from a build that never checked archives is not a cut", func(t *testing.T) {
		dir := t.TempDir()
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "orders.parquet"), rec, "binlog.000002", 50, cutKey("binlog.000002", 50))
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "items.parquet"), rec, "binlog.000009", 4, "") // anchor only
		got := snapshotCutOf(dir)
		if got == nil || *got != (query.BinlogPos{File: "binlog.000002", Pos: 50}) {
			t.Fatalf("snapshotCutOf = %+v, want binlog.000002:50 (the anchor-only file must not raise it)", got)
		}
	})
	t.Run("only anchors, no archive cut: no cut", func(t *testing.T) {
		dir := t.TempDir()
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "orders.parquet"), rec, "binlog.000002", 50, "")
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "items.parquet"), "", "binlog.000009", 4, "")
		if got := snapshotCutOf(dir); got != nil {
			t.Fatalf("snapshotCutOf = %+v, want nil", got)
		}
	})
	t.Run("a key that does not parse is left out", func(t *testing.T) {
		dir := t.TempDir()
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "orders.parquet"), rec, "binlog.000002", 50, cutKey("binlog.000002", 50))
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "items.parquet"), rec, "binlog.000009", 4, "binlog.000009:4")
		got := snapshotCutOf(dir)
		if got == nil || *got != (query.BinlogPos{File: "binlog.000002", Pos: 50}) {
			t.Fatalf("snapshotCutOf = %+v, want binlog.000002:50", got)
		}
	})
	t.Run("a file that does not read is left out: a lower cut only reads more", func(t *testing.T) {
		dir := t.TempDir()
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "orders.parquet"), rec, "binlog.000002", 50, cutKey("binlog.000002", 50))
		if err := os.WriteFile(filepath.Join(dir, "shop", "broken.parquet"), []byte("not parquet"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "_SUCCESS"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		got := snapshotCutOf(dir)
		if got == nil || *got != (query.BinlogPos{File: "binlog.000002", Pos: 50}) {
			t.Fatalf("snapshotCutOf = %+v, want binlog.000002:50", got)
		}
	})
}

func TestArchiveCutEncoding_2152(t *testing.T) {
	for _, p := range []query.BinlogPos{{File: "binlog.000001", Pos: 4}, {File: "mysql-bin.1000000", Pos: 1<<64 - 1}} {
		got, ok := parseArchiveCut(encodeArchiveCut(p))
		if !ok || got != p {
			t.Fatalf("round trip of %+v = %+v, %v", p, got, ok)
		}
	}
	for _, bad := range []string{"", "binlog.000001:4", `{"binlog_file":"","start_pos":4}`, `{"binlog_file":"binlog.000001"}`} {
		if got, ok := parseArchiveCut(bad); ok {
			t.Fatalf("parseArchiveCut(%q) = %+v, want refused", bad, got)
		}
	}
}

func TestArchiveCuts_forBaseline_2152(t *testing.T) {
	root := t.TempDir()
	snap := filepath.Join(root, "2026-03-01T10-00-00Z")
	writeAnchoredFile2152(t, filepath.Join(snap, "shop", "orders.parquet"), rec, "binlog.000001", 4, cutKey("binlog.000001", 4))
	writeAnchoredFile2152(t, filepath.Join(snap, "shop", "items.parquet"), rec, "binlog.000003", 77, cutKey("binlog.000003", 77))
	// A newer snapshot the table is missing from (it failed in that run): its
	// refresh never checked orders, so its cut is not orders' cut.
	newer := filepath.Join(root, "2026-03-02T10-00-00Z")
	writeAnchoredFile2152(t, filepath.Join(newer, "shop", "items.parquet"), rec, "binlog.000099", 1, cutKey("binlog.000099", 1))

	c := newArchiveCuts(false)
	got := c.forBaseline(filepath.Join(snap, "shop", "orders.parquet"))
	if got == nil || *got != (query.BinlogPos{File: "binlog.000003", Pos: 77}) {
		t.Fatalf("forBaseline = %+v, want the directory's cut binlog.000003:77", got)
	}
	// Read once per directory: a file added afterwards is not seen.
	writeAnchoredFile2152(t, filepath.Join(snap, "shop", "later.parquet"), rec, "binlog.000009", 1, cutKey("binlog.000009", 1))
	if again := c.forBaseline(filepath.Join(snap, "shop", "items.parquet")); again == nil || *again != *got {
		t.Fatalf("second table of the same directory = %+v, want the cached %+v", again, got)
	}
	if s3 := c.forBaseline("s3://bucket/x/2026-03-01T10-00-00Z/shop/orders.parquet"); s3 != nil {
		t.Fatalf("an S3 snapshot = %+v, want nil (its footers are not read here)", s3)
	}
	if off := newArchiveCuts(true).forBaseline(filepath.Join(snap, "shop", "orders.parquet")); off != nil {
		t.Fatalf("on an index `bintrail index` also wrote = %+v, want nil", off)
	}
}

// The key is written only when the run checked the archives and its cut is a
// cut: an empty index keeps the source's (possibly a dump's) position, which
// no refresh searched through.
func TestSnapshotFileMetadata_archiveCut_2152(t *testing.T) {
	cut := &query.BinlogPos{File: "binlog.000004", Pos: 900}
	base := mergeInput{SnapshotAt: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
		SourceBaseline: baselineMeta{Metadata: baseline.DumpMetadata{BinlogFile: "binlog.000001", BinlogPos: 4}}}
	for _, tc := range []struct {
		name    string
		cut     *query.BinlogPos
		checked bool
		want    string
	}{
		{"a run that checked the archives, with a cut", cut, true, cutKey("binlog.000004", 900)},
		{"a run that did not check them", cut, false, ""},
		{"an empty index: the source's position is kept, not a cut", nil, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := base
			in.Cut, in.ArchivesChecked = tc.cut, tc.checked
			if got := snapshotFileMetadata(in)[baseline.MetaKeyArchiveCut]; got != tc.want {
				t.Fatalf("archive cut key = %q, want %q", got, tc.want)
			}
		})
	}
}

// Whether a run may write the key at all.
func TestRunChecksArchives_2152(t *testing.T) {
	for _, tc := range []struct {
		name       string
		backfilled bool
		allowGaps  bool
		archErr    error
		want       bool
	}{
		{"a stream-only index, sources found", false, false, nil, true},
		{"`bintrail index` also wrote: the cut does not bound what was indexed late", true, false, nil, false},
		{"--allow-gaps: a source it could not read may have been skipped", false, true, nil, false},
		{"archive discovery failed", false, false, errors.New("boom"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := runChecksArchives(tc.backfilled, tc.allowGaps, tc.archErr); got != tc.want {
				t.Fatalf("runChecksArchives = %v, want %v", got, tc.want)
			}
		})
	}
}

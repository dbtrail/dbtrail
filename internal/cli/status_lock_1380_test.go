package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/status"
)

// The status package repeats the three lock words so it does not import
// internal/baseline. They are the same words.
func TestStatusLockWords_areTheBaselineWords(t *testing.T) {
	for word, c := range map[string]baseline.ReadConsistency{
		status.LockConsistent: baseline.ReadConsistent,
		status.LockUnknown:    baseline.ReadUnknown,
		status.LockTorn:       baseline.ReadTorn,
	} {
		if c.String() != word {
			t.Errorf("status says %q where baseline says %q", word, c.String())
		}
	}
}

// #1380: `bintrail status --baseline-dir` walks a real folder and reports how
// each file was locked: the three cases, and a file whose footer cannot be
// read, which is unknown.
func TestStatusBaselines_sayHowEachFileWasLocked(t *testing.T) {
	root := t.TempDir()
	const snap = "2026-09-27T12-00-00Z"
	cols, err := baseline.ParseSchemaText("CREATE TABLE `t` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n);\n")
	if err != nil {
		t.Fatal(err)
	}
	for table, stamp := range map[string]string{"torn": "no-lock", "locked": "ftwrl", "old": "", "later": "locked-v2"} {
		md := map[string]string{}
		if stamp != "" {
			md[baseline.MetaKeyLockMode] = stamp
		}
		w, err := baseline.NewWriter(filepath.Join(root, snap, "shop", table+".parquet"), cols,
			baseline.WriterConfig{Compression: "none", RowGroupSize: 10, Metadata: md})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, snap, "shop", "broken.parquet"), []byte("not a parquet file"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(filepath.Join(root, snap)); err != nil {
		t.Fatal(err)
	}
	found, _, err := baseline.DiscoverBaselinesReport(root)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, b := range statusBaselines(found) {
		got[b.Table] = b.Lock
	}
	for table, want := range map[string]string{
		"torn": "torn", "locked": "consistent", "old": "unknown", "later": "unknown", "broken": "unknown",
	} {
		if got[table] != want {
			t.Errorf("%s: %q, want %q (all: %v)", table, got[table], want, got)
		}
	}
}

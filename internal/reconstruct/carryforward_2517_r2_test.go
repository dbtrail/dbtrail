package reconstruct

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// #2517 round 2: the other readers that share the memo, and the copy arm.
// Lines are matched by message PREFIX so a rewording cannot break a count.

const integrityReadPrefix = `msg="integrity check read the whole file`
const integritySkipPrefix = `msg="integrity check skipped`

func forgetVerified(t *testing.T) {
	t.Helper()
	baselineintegrity.ForgetVerified()
	t.Cleanup(baselineintegrity.ForgetVerified)
}

// realBaselineParquet converts a two-row dump into a snapshot with a manifest
// and returns its one parquet file (no MySQL; DuckDB only).
func realBaselineParquet(t *testing.T) string {
	t.Helper()
	inputDir, outputDir := t.TempDir(), t.TempDir()
	writeF := func(name, data string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(inputDir, name), []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeF("metadata", "Started dump at: 2025-02-28 00:00:00\nSHOW MASTER STATUS:\n\tLog: binlog.000001\n\tPos: 100\n\tGTID:\n")
	writeF("shop.orders-schema.sql", "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(64) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n")
	writeF("shop.orders.00000.sql", "INSERT INTO `orders` VALUES(1,'shipped'),(2,'pending');\n")
	if _, err := baseline.Run(context.Background(), baseline.Config{InputDir: inputDir, OutputDir: outputDir, Compression: "none", RowGroupSize: 100}); err != nil {
		t.Fatalf("baseline.Run: %v", err)
	}
	var parquetPath string
	_ = filepath.Walk(outputDir, func(p string, info os.FileInfo, err error) error {
		if err == nil && filepath.Ext(p) == ".parquet" {
			parquetPath = p
		}
		return nil
	})
	if parquetPath == "" {
		t.Fatal("no .parquet produced")
	}
	ageFile(t, parquetPath) // outside the memo's racy margin, so one read remembers it
	return parquetPath
}

// materializeBaselineLocalIn is the full-table readers' one validation site
// (CLI reconstruct, the console's reconstruct, the shim's _snapshot, verify,
// the export): two reads of one base in one process = one whole-file read.
func TestMaterializeBaselineLocal_validatesOncePerProcess_2517(t *testing.T) {
	forgetVerified(t)
	logs := captureDebugLog(t)
	root := t.TempDir()
	path := filepath.Join(root, "snap", "demo", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("orders bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := baselineintegrity.WriteManifest(filepath.Join(root, "snap")); err != nil {
		t.Fatal(err)
	}
	baselineintegrity.ForgetVerified() // the writer seeds the memo; this test is about the reader
	ageFile(t, path)                   // outside the memo's racy margin, so one read remembers it
	for range 2 {
		local, cleanup, err := materializeBaselineLocalIn(context.Background(), path, duckdbutil.DefaultTuning(), "", nil)
		if err != nil {
			t.Fatal(err)
		}
		cleanup()
		if local != path {
			t.Fatalf("a local base is read in place; got %s", local)
		}
	}
	out := logs.String()
	if n := strings.Count(out, integrityReadPrefix); n != 1 {
		t.Errorf("two reads of one base read the file %d times, want 1:\n%s", n, out)
	}
	if n := strings.Count(out, integritySkipPrefix); n != 1 {
		t.Errorf("the second read was skipped %d times, want 1:\n%s", n, out)
	}
}

// ReadBaselineRows is the single-row reader (cascade recovery, the console's
// single-row reconstruct): in the daemon it read a whole base end to end for
// one row, every time. Now it shares the memo.
func TestReadBaselineRows_validatesOncePerProcess_2517(t *testing.T) {
	forgetVerified(t)
	logs := captureDebugLog(t)
	parquetPath := realBaselineParquet(t)
	baselineintegrity.ForgetVerified() // baseline.Run's manifest writer seeded it
	for range 2 {
		rows, err := ReadBaselineRows(context.Background(), parquetPath, map[string]string{"id": "1"}, 0)
		if err != nil || len(rows) != 1 {
			t.Fatalf("ReadBaselineRows: err=%v rows=%d", err, len(rows))
		}
	}
	out := logs.String()
	if n := strings.Count(out, integrityReadPrefix); n != 1 {
		t.Errorf("two single-row reads read the whole file %d times, want 1:\n%s", n, out)
	}
	if n := strings.Count(out, integritySkipPrefix); n != 1 {
		t.Errorf("the second read was skipped %d times, want 1:\n%s", n, out)
	}
}

// The copy arm (a cross-device destination): the source is remembered, the
// destination is a NEW inode, so the manifest writer hashes it (no digest to
// reuse) and, having read those bytes, remembers it; the next refresh's check
// of the copy is then a skip, and a copy that is corrupted under a new inode
// is still read and refused.
func TestCarryForward_copyArmIsANewIdentity_2517(t *testing.T) {
	forgetVerified(t)
	logs := captureDebugLog(t)
	_, s1, s2, s3 := carryTwiceFixture(t)
	prevLink := linkFile
	linkFile = func(string, string) error { return syscall.EXDEV }
	t.Cleanup(func() { linkFile = prevLink })

	src1 := filepath.Join(s1, "demo", "orders.parquet")
	linked, err := carryForward(context.Background(), src1, s2, "demo", "orders")
	if err != nil || linked {
		t.Fatalf("first carry: linked=%v err=%v", linked, err)
	}
	if n := strings.Count(logs.String(), integrityReadPrefix); n != 1 {
		t.Fatalf("the source was read %d times, want 1", n)
	}
	src2 := filepath.Join(s2, "demo", "orders.parquet")
	a, _ := os.Stat(src1)
	b, _ := os.Stat(src2)
	if os.SameFile(a, b) {
		t.Fatal("fixture: the copy shares the source's inode")
	}
	ageFile(t, src2) // the copy was written seconds ago; aged so the writer's seed takes
	st, err := baselineintegrity.WriteManifestFrom(s2, []string{s1})
	if err != nil {
		t.Fatal(err)
	}
	if st.Hashed != 1 || st.Reused != 0 {
		t.Fatalf("a copy is new bytes: the writer must hash it, got %+v", st)
	}
	logs.Reset()
	if _, err := carryForward(context.Background(), src2, s3, "demo", "orders"); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(logs.String(), integrityReadPrefix); n != 0 {
		t.Errorf("the copy the writer just hashed was read again %d times", n)
	}
	if n := strings.Count(logs.String(), integritySkipPrefix); n != 1 {
		t.Errorf("the copy's check was skipped %d times, want 1:\n%s", n, logs.String())
	}
	// Without the writer's seed (a manifest written by another process, or
	// the memo lost), the copy is a new identity and is read.
	baselineintegrity.ForgetVerified()
	logs.Reset()
	if err := os.RemoveAll(s3); err != nil {
		t.Fatal(err)
	}
	if _, err := carryForward(context.Background(), src2, s3, "demo", "orders"); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(logs.String(), integrityReadPrefix); n != 1 {
		t.Errorf("a copy new to the process was read %d times, want 1", n)
	}
}

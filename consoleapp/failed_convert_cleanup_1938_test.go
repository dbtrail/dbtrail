package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #1938: a full read into a local folder whose CONVERSION fails (a full disk
// is the usual way) left its snapshot folder behind: marked _INCOMPLETE, with
// the Parquet files of the tables that had finished. Nothing removed it (the
// job's journal is cleared when the job ends, prune and the sweep skip it),
// so every failed scheduled read added one more to the disk that was full.

const dumpHeader = "/*!40101 SET NAMES utf8mb4*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n"

// writeHalfBrokenDump writes what mydumper 1.0.3 leaves for two tables, one
// of them cut in the middle of a statement, the way a full disk cuts it. The
// real baseline.Run converts it: no part of the conversion is faked.
func writeHalfBrokenDump(t *testing.T, dir string) {
	t.Helper()
	table := func(name string) string {
		return dumpHeader + "CREATE TABLE `" + name + "` (\n  `id` int(11) NOT NULL,\n  `label` varchar(30) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
	}
	files := map[string]string{
		"metadata": "# Started dump at: 2026-10-02 01:57:48\n[config]\nquote-character = BACKTICK\n\n[source]\n" +
			"# SOURCE_LOG_FILE = \"binlog.000002\"\n# SOURCE_LOG_POS = 839\n\n# Finished dump at: 2026-10-02 01:57:49\n",
		"shop-schema-create.sql": dumpHeader + "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `shop`;\n",
		"shop.good-schema.sql":   table("good"),
		"shop.good.00000.sql":    dumpHeader + "INSERT INTO `good` (`id`,`label`) VALUES(1,\"uno\")\n,(2,\"dos\")\n;\n",
		"shop.bad-schema.sql":    table("bad"),
		"shop.bad.00000.sql":     dumpHeader + "INSERT INTO `bad` (`id`,`label`) VALUES(1,\"uno\")\n,(2,\"d",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func stubDumpThatWrites(t *testing.T, write func(dir string)) {
	t.Helper()
	prev := runMydumperFunc
	runMydumperFunc = func(_ context.Context, _ string, _ config.SSL, _ []string, dir string, _ baseline.LockMode, _ lockModeSource) error {
		write(dir)
		return nil
	}
	t.Cleanup(func() { runMydumperFunc = prev })
	prevDDL, prevEv := dumpDDLMarkFunc, dumpEventMarkFunc
	dumpDDLMarkFunc = func(console.BaselineRequest) string { return "" }
	dumpEventMarkFunc = func(console.BaselineRequest) string { return "" }
	t.Cleanup(func() { dumpDDLMarkFunc, dumpEventMarkFunc = prevDDL, prevEv })
	stubEstimate(t, dumpEstimate{bytes: 1 << 20, tables: 2}, nil)
	stubSameFS(t, true, nil)
	prevDisk := diskSpaceFn
	diskSpaceFn = func(string) (uint64, uint64, error) { return 100 * gib, 1 << 40, nil }
	t.Cleanup(func() { diskSpaceFn = prevDisk })
}

// snapshotDirsIn lists what a failed read could have left in local: snapshot
// folders, and the hidden folders a discard renames them to first.
func snapshotDirsIn(t *testing.T, local string) []string {
	t.Helper()
	entries, err := os.ReadDir(local)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestExecute_aFailedConversionRemovesTheSnapshotFolderItMade_1938(t *testing.T) {
	stage, local := t.TempDir(), t.TempDir()
	// An older, finished snapshot beside it, and a file of the operator's.
	older := filepath.Join(local, "2026-01-02T03-04-05Z")
	if err := os.MkdirAll(older, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{filepath.Join(older, baseline.SuccessMarker), filepath.Join(older, "shop.good.parquet"), filepath.Join(local, "notes.txt")} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	stubDumpThatWrites(t, func(dir string) { writeHalfBrokenDump(t, dir) })
	s := newBaselineSupervisor(context.Background(), stage, baseline.LockModeFTWRL)

	_, err := s.execute(console.BaselineRequest{ServerID: "s1", ServerName: "shop-db", SourceDSN: "src", LocalDir: local})
	if err == nil || !strings.HasPrefix(err.Error(), "convert: ") {
		t.Fatalf("err = %v, want the conversion's failure", err)
	}
	t.Logf("error: %v", err)
	got := snapshotDirsIn(t, local)
	if len(got) != 2 || got[0] != "2026-01-02T03-04-05Z" || got[1] != "notes.txt" {
		t.Fatalf("the snapshot folder holds %v, want only what was there before the read", got)
	}
	for _, f := range []string{baseline.SuccessMarker, "shop.good.parquet"} {
		if _, err := os.Stat(filepath.Join(older, f)); err != nil {
			t.Fatalf("the older snapshot lost %s: %v", f, err)
		}
	}
	// Nothing in the message claims a folder was left.
	if strings.Contains(err.Error(), "could not be removed") {
		t.Fatalf("the message reports a leftover that is not there: %v", err)
	}
	if left, _ := filepath.Glob(filepath.Join(stage, "*")); len(left) != 0 {
		t.Fatalf("the working folder still holds %v", left)
	}
}

// A snapshot folder that already held files when the read started is not this
// read's alone (a `bintrail baseline` run in the same second writes to the
// same name), so a failure never removes it.
func TestExecute_aFailedConversionLeavesAFolderItDidNotMake_1938(t *testing.T) {
	stage, local := t.TempDir(), t.TempDir()
	var foreign []string
	stubDumpThatWrites(t, func(dir string) {
		writeHalfBrokenDump(t, dir)
	})
	// The read names its folder after the second it started in. Occupy that
	// second and the next ones before it starts.
	now := time.Now().UTC()
	for i := range 4 {
		d := filepath.Join(local, reconstruct.SnapshotDirName(now.Add(time.Duration(i)*time.Second)))
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		f := filepath.Join(d, "someone-elses.parquet")
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		foreign = append(foreign, f)
	}
	s := newBaselineSupervisor(context.Background(), stage, baseline.LockModeFTWRL)
	if _, err := s.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: "src", LocalDir: local}); err == nil {
		t.Fatal("a broken dump converted")
	}
	for _, f := range foreign {
		if _, err := os.Stat(f); err != nil {
			t.Fatalf("a file this read did not write is gone: %v", err)
		}
	}
}

// The rules are the reclaim's: an unfinished folder goes, a finished one and
// a markerless one with files stay, and a folder that will not go is said.
func TestDiscardFailedSnapshot_keepsWhatIsUsableAndSaysWhatStays_1938(t *testing.T) {
	const name = "2026-10-10T13-51-42Z"
	mk := func(t *testing.T, files ...string) (root, p string) {
		root = t.TempDir()
		p = filepath.Join(root, name)
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			if err := os.WriteFile(filepath.Join(p, f), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return root, p
	}
	exists := func(p string) bool { _, err := os.Stat(p); return err == nil }

	t.Run("unfinished: removed", func(t *testing.T) {
		root, p := mk(t, baseline.IncompleteMarker, "shop.good.parquet")
		if left := discardFailedSnapshot(root, name, "s1"); left != "" || exists(p) {
			t.Fatalf("left=%q exists=%v", left, exists(p))
		}
		if rest := snapshotDirsIn(t, root); len(rest) != 0 {
			t.Fatalf("the folder was renamed, not removed: %v", rest)
		}
	})
	t.Run("finished and marked: kept", func(t *testing.T) {
		root, p := mk(t, baseline.SuccessMarker, "shop.good.parquet")
		if left := discardFailedSnapshot(root, name, "s1"); left != "" || !exists(filepath.Join(p, "shop.good.parquet")) {
			t.Fatalf("left=%q, a finished snapshot was touched", left)
		}
	})
	t.Run("files and no marker: kept", func(t *testing.T) {
		root, p := mk(t, "shop.good.parquet")
		if left := discardFailedSnapshot(root, name, "s1"); left != "" || !exists(filepath.Join(p, "shop.good.parquet")) {
			t.Fatalf("left=%q, a markerless snapshot was touched", left)
		}
	})
	t.Run("never created: nothing to do", func(t *testing.T) {
		if left := discardFailedSnapshot(t.TempDir(), name, "s1"); left != "" {
			t.Fatalf("left=%q for a folder that does not exist", left)
		}
	})
	t.Run("will not go: said, with where it is", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root renames anything")
		}
		root, p := mk(t, baseline.IncompleteMarker, "shop.good.parquet")
		if err := os.Chmod(root, 0o555); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(root, 0o755) })
		left := discardFailedSnapshot(root, name, "s1")
		if !exists(p) {
			t.Fatal("the test did not keep the folder")
		}
		if !strings.HasPrefix(left, "The unfinished snapshot folder "+p+" could not be removed (") || !strings.Contains(left, "holds nothing usable") {
			t.Fatalf("left = %q", left)
		}
	})
}

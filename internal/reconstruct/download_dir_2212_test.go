package reconstruct

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// #2212: the daemon hands the fold a download folder so an s3:// previous
// snapshot lands on the disk the fold's space check measures. The download
// folder must be made UNDER that folder, and a folder that cannot be used is
// an error naming it, never a quiet fall back to the system temporary
// directory (which is the disk nobody measured).
func TestBaselineDownloadDir_2212(t *testing.T) {
	t.Run("under the given folder", func(t *testing.T) {
		dir := t.TempDir()
		got, err := baselineDownloadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if filepath.Dir(got) != dir || !strings.HasPrefix(filepath.Base(got), "bintrail-baseline-") {
			t.Fatalf("download folder = %s, want bintrail-baseline-* directly under %s", got, dir)
		}
	})
	t.Run("empty keeps the system temporary directory (the command line)", func(t *testing.T) {
		got, err := baselineDownloadDir("")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(got) })
		if filepath.Dir(got) != filepath.Clean(os.TempDir()) {
			t.Fatalf("download folder = %s, want it under %s", got, os.TempDir())
		}
	})
	t.Run("an unusable folder is an error naming it", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "not-a-dir")
		if err := os.WriteFile(file, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := baselineDownloadDir(file)
		if err == nil || !strings.Contains(err.Error(), file) {
			t.Fatalf("err = %v, want a refusal naming %s", err, file)
		}
		t.Log(err)
	})
}

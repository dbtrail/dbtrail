package console

import (
	"path/filepath"
	"testing"
)

// The one retry of a statement is taken only when the snapshot folder it was
// pinned to is gone (#2678): never for a run that did not reach a snapshot,
// and never while the folder is still there.
func TestSQLCopyGone(t *testing.T) {
	dir := t.TempDir()
	if sqlCopyGone("") {
		t.Error("a run that never reached a snapshot is not a removed snapshot")
	}
	if sqlCopyGone(dir) {
		t.Error("a folder that is there was reported gone")
	}
	if !sqlCopyGone(filepath.Join(dir, "2026-06-10T12-00-00Z")) {
		t.Error("a folder that is not there was not reported gone")
	}
}

package cli

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// unreadableFolder makes path unreadable for the rest of the test. root
// reads it anyway, which would make every case below pass without proving
// anything, so the test is skipped there.
func unreadableFolder(t *testing.T, path string) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root reads a folder whatever its mode")
	}
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(path, 0o755) })
}

// #2684 point 2. A snapshot from at or before the moment that cannot be read
// is skipped by the lookup when a newer snapshot, which lacks the table, was
// read. The lookup then says "no snapshot", while the table may be in the one
// it could not read. The refusal names that snapshot and does not offer
// --allow-changes-only, which would write a partial table where a whole one
// may be possible.
func TestReconstructChangesOnly_anUnreadableSnapshotIsNamedAndTheFlagIsNotOffered(t *testing.T) {
	cases := []struct {
		name       string
		unreadable func(dir string) string // the folder made unreadable
	}{
		{"the snapshot folder", func(dir string) string { return filepath.Join(dir, "2026-08-01T00-00-00Z") }},
		{"the table's schema folder in it", func(dir string) string { return filepath.Join(dir, "2026-08-01T00-00-00Z", "mydb") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetMydumperFlags(t)
			dir := t.TempDir()
			putSnapshot(t, dir, "2026-08-01T00-00-00Z", "mydb", "orders")
			putSnapshot(t, dir, "2026-08-15T00-00-00Z", "mydb", "customers")
			unreadableFolder(t, tc.unreadable(dir))
			recBaselineDir = dir
			recAt = changesOnlyAt.Format(time.RFC3339)
			stubEngine(t, nil, []reconstruct.TableFailure{refusedTable("mydb", "orders", time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC))})

			err := runReconstruct(reconstructCmd, nil)
			if err == nil {
				t.Fatal("the command succeeded")
			}
			if !errors.Is(err, reconstruct.ErrChangesOnly) {
				t.Errorf("the error no longer carries reconstruct.ErrChangesOnly: %v", err)
			}
			msg := strings.ReplaceAll(err.Error(), dir, "<snapshots>")
			t.Logf("rendered:\n%s", msg)
			for _, want := range []string{
				"full-table reconstruct: mydb.orders: no snapshot that could be read was found at or before 2026-09-01T12:00:00Z to start it from, so nothing was written for it.",
				"The snapshot taken at 2026-08-01T00:00:00Z could not be read (" + strings.ReplaceAll(tc.unreadable(dir), dir, "<snapshots>"),
				"and it may hold mydb.orders. Make it readable and run again",
			} {
				if !strings.Contains(msg, want) {
					t.Errorf("refusal does not say %q", want)
				}
			}
			for _, not := range []string{"--allow-changes-only", "CREATE TABLE", "does not include", "there is no snapshot"} {
				if strings.Contains(msg, not) {
					t.Errorf("refusal says %q, advice for a table that has no snapshot", not)
				}
			}
			if strings.Contains(msg, "\n") || strings.Contains(msg, "—") {
				t.Error("the refusal spans lines or holds an em dash")
			}
		})
	}
}

// What does not count: a folder of another schema cannot hold the table, and
// a snapshot after the moment answers nothing about it. The usual advice, the
// flag included, stays.
func TestReconstructChangesOnly_unreadableFoldersThatCannotHoldTheTable(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, dir string)
	}{
		{"another schema's folder", func(t *testing.T, dir string) {
			putSnapshot(t, dir, "2026-08-01T00-00-00Z", "other", "orders")
			putSnapshot(t, dir, "2026-08-01T00-00-00Z", "mydb", "customers")
			unreadableFolder(t, filepath.Join(dir, "2026-08-01T00-00-00Z", "other"))
		}},
		{"a snapshot after the moment", func(t *testing.T, dir string) {
			putSnapshot(t, dir, "2026-08-01T00-00-00Z", "mydb", "customers")
			putSnapshot(t, dir, "2026-09-05T00-00-00Z", "mydb", "orders")
			unreadableFolder(t, filepath.Join(dir, "2026-09-05T00-00-00Z"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetMydumperFlags(t)
			dir := t.TempDir()
			tc.setup(t, dir)
			recBaselineDir = dir
			recAt = changesOnlyAt.Format(time.RFC3339)
			stubEngine(t, nil, []reconstruct.TableFailure{refusedTable("mydb", "orders", time.Time{})})
			err := runReconstruct(reconstructCmd, nil)
			if err == nil {
				t.Fatal("the command succeeded")
			}
			msg := strings.ReplaceAll(err.Error(), dir, "<snapshots>")
			t.Logf("rendered:\n%s", msg)
			if strings.Contains(msg, "could not be read") {
				t.Error("a folder that cannot hold the table is named as one that may")
			}
			if !strings.Contains(msg, "holds 1 snapshot, and it does not include mydb.orders") ||
				!strings.Contains(msg, "To write only those rows anyway, add --allow-changes-only") {
				t.Error("the usual advice is gone")
			}
		})
	}
}

// Single-row reconstruct reads the same cause through the same lookup: the
// real one here, which returns "no baseline" for this layout.
func TestReconstruct1807_anUnreadableOlderSnapshotIsNamed(t *testing.T) {
	dir := t.TempDir()
	putSnapshot(t, dir, "2026-08-01T00-00-00Z", "mydb", "orders")
	putSnapshot(t, dir, "2026-08-15T00-00-00Z", "mydb", "customers")
	unreadableFolder(t, filepath.Join(dir, "2026-08-01T00-00-00Z"))
	setSingleRow(t, dir, false)
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("the command succeeded")
	}
	msg := strings.ReplaceAll(err.Error(), dir, "<snapshots>")
	t.Logf("rendered:\n%s", msg)
	if !errors.Is(err, reconstruct.ErrNoBaseline) {
		t.Errorf("the refusal no longer unwraps to ErrNoBaseline: %v", err)
	}
	want := "The snapshot taken at 2026-08-01T00:00:00Z could not be read (<snapshots>/2026-08-01T00-00-00Z: "
	if !strings.Contains(msg, want) || !strings.Contains(msg, "and it may hold mydb.orders. Make it readable and run again") {
		t.Errorf("the message does not name the unreadable snapshot")
	}
	if strings.Contains(msg, "does not include") {
		t.Error("the message says the table is in no snapshot")
	}
}

// When a readable snapshot of the table from at or before the moment exists
// and the lookup did not use it, an older folder that could not be read is
// not the reason: the refusal says the reason is not known, and does not send
// the operator to fix a permission that changes nothing.
func TestReconstructChangesOnly_anUnreadableFolderIsNotBlamedWhenAReadableSnapshotExists(t *testing.T) {
	resetMydumperFlags(t)
	dir := t.TempDir()
	putSnapshot(t, dir, "2026-08-01T00-00-00Z", "mydb", "orders")
	putSnapshot(t, dir, "2026-08-15T00-00-00Z", "mydb", "orders")
	unreadableFolder(t, filepath.Join(dir, "2026-08-01T00-00-00Z"))
	recBaselineDir = dir
	recAt = changesOnlyAt.Format(time.RFC3339)
	stubEngine(t, nil, []reconstruct.TableFailure{refusedTable("mydb", "orders", time.Time{})})
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("the command succeeded")
	}
	msg := strings.ReplaceAll(err.Error(), dir, "<snapshots>")
	t.Logf("rendered:\n%s", msg)
	if strings.Contains(msg, "could not be read") || strings.Contains(msg, "Make it readable") {
		t.Error("the refusal blames the unreadable folder although a readable snapshot of the table exists")
	}
	unknown := "Why there is none is not known: the location shows a snapshot of it from at or before that moment, and the lookup did not use it. Find that out before adding the flag."
	if !strings.Contains(msg, unknown) {
		t.Errorf("refusal does not say %q", unknown)
	}
}

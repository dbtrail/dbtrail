package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// reconstruct asked for a moment it has no snapshot for (#1807). The old
// answers named only flags ("one of --baseline-dir or --baseline-s3 is
// required") or the raw lookup ("no baseline snapshot found: ..."), and
// neither said the one thing a person needs: the snapshot has to be from
// BEFORE the moment asked for, so taking one now does not answer an earlier
// moment. Each case below runs the real command path; none reaches MySQL,
// because the snapshot is looked for before the history database is opened.

// wantNoSnapshotMessage checks the parts every no-snapshot message carries.
func wantNoSnapshotMessage(t *testing.T, err error, table, at string) {
	t.Helper()
	if err == nil {
		t.Fatal("reconstruct went on without a snapshot")
	}
	msg := err.Error()
	for _, want := range []string{
		"snapshot of " + table,
		"at or before " + at,
		"replays the changes recorded after it",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("the message does not say %q:\n%s", want, msg)
		}
	}
}

func TestReconstruct1807_noLocationGivenSaysWhatIsMissing(t *testing.T) {
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })
	recSQL, recFormat = "", "json"
	recSchema, recTable, recPK, recPKColumns = "mydb", "orders", "42", "id"
	recBaselineDir, recBaselineS3 = "", ""
	recAt = "2026-09-01 10:00:00"

	err := runReconstruct(reconstructCmd, nil)
	wantNoSnapshotMessage(t, err, "mydb.orders", "2026-09-01T10:00:00Z")
	// The flags stay named: they are how the location is given.
	for _, want := range []string{"--baseline-dir", "--baseline-s3", "No snapshot location was given"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message does not say %q:\n%s", want, err)
		}
	}
}

// An --at that does not parse is still refused as such, not reported as a
// missing snapshot location.
func TestReconstruct1807_badAtBeatsMissingLocation(t *testing.T) {
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })
	recSQL, recFormat = "", "json"
	recSchema, recTable, recPK, recPKColumns = "mydb", "orders", "42", "id"
	recBaselineDir, recBaselineS3 = "", ""
	recAt = "not-a-time"

	err := runReconstruct(reconstructCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "--at") {
		t.Fatalf("want the --at refusal, got %v", err)
	}
}

func TestReconstruct1807_fullTableNoLocationGivenSaysWhatIsMissing(t *testing.T) {
	resetMydumperFlags(t)
	recBaselineDir, recBaselineS3 = "", ""
	recAt = "2026-09-01 10:00:00"

	err := runReconstruct(reconstructCmd, nil)
	wantNoSnapshotMessage(t, err, recTables, "2026-09-01T10:00:00Z")
	if !strings.Contains(err.Error(), "No snapshot location was given") {
		t.Errorf("full-table mode does not say the location is missing:\n%s", err)
	}
}

// A folder with no snapshot at all, and a folder whose only snapshot is from
// AFTER the moment asked for, are the same refusal: nothing there is from
// before it. The second is the case the old message hid best, because the
// folder plainly holds a snapshot of the table.
func TestReconstruct1807_noSnapshotBeforeTheMoment(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, dir string)
	}{
		{"empty folder", func(t *testing.T, dir string) {}},
		{"only a later snapshot", func(t *testing.T, dir string) {
			p := filepath.Join(dir, "2026-09-02T00-00-00Z", "mydb")
			if err := os.MkdirAll(p, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(p, "orders.parquet"), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		for _, baselineOnly := range []bool{false, true} {
			name := tc.name
			if baselineOnly {
				name += ", --baseline-only"
			}
			t.Run(name, func(t *testing.T) {
				orig := captureRecFlags()
				t.Cleanup(func() { applyRecFlags(orig) })
				dir := t.TempDir()
				tc.setup(t, dir)
				recSQL, recFormat = "", "json"
				recSchema, recTable, recPK, recPKColumns = "mydb", "orders", "42", "id"
				recBaselineDir, recBaselineS3 = dir, ""
				recBaselineOnly, recHistory = baselineOnly, false
				recIndexDSN = "unused:unused@tcp(127.0.0.1:1)/never_opened"
				recAt = "2026-09-01 10:00:00"

				err := runReconstruct(reconstructCmd, nil)
				wantNoSnapshotMessage(t, err, "mydb.orders", "2026-09-01T10:00:00Z")
				if !errors.Is(err, reconstruct.ErrNoBaseline) {
					t.Errorf("the refusal no longer unwraps to ErrNoBaseline: %v", err)
				}
				for _, want := range []string{dir, "A snapshot taken now only answers moments after it", "that includes this table"} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("the message does not say %q:\n%s", want, err)
					}
				}
			})
		}
	}
}

// The control for the fixture above: the same later snapshot IS found for a
// moment after it, so "only a later snapshot" cannot pass for the reason the
// empty folder does.
func TestReconstruct1807_theLaterSnapshotIsFoundAfterIt(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "2026-09-02T00-00-00Z", "mydb")
	if err := os.MkdirAll(p, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p, "orders.parquet"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	after := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	if _, _, _, err := reconstruct.FindBaseline(context.Background(), dir, "mydb", "orders", after); err != nil {
		t.Fatalf("the fixture's snapshot is not found even after it, so the case above proves nothing: %v", err)
	}
}

// The command that makes a snapshot is named after the binary that failed:
// bintrail-pg has its own baseline command, and the commercial binary
// carries the core's under its own name.
func TestReconstruct1807_namesTheRunningBinary(t *testing.T) {
	for _, root := range []string{"bintrail", "bintrail-pg", "dbtrail-ee"} {
		r := &cobra.Command{Use: root}
		c := &cobra.Command{Use: "reconstruct"}
		r.AddCommand(c)
		if got := snapshotCommand(c); got != root+" baseline" {
			t.Errorf("under %s: got %q", root, got)
		}
	}
	if got := snapshotCommand(&cobra.Command{Use: "reconstruct"}); got != "bintrail baseline" {
		t.Errorf("with no parent: got %q", got)
	}
}

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
// reconstruct_nosnapshot_1807_integration_test.go runs the same cases with a
// real history database named.

const (
	replaySentence   = "it starts from that snapshot and replays the changes recorded after it"
	readOnlySentence = "it reads the row as that snapshot holds it"
)

// wantNoSnapshotMessage checks the parts every no-snapshot message carries,
// and the sentence that says what the mode does with the snapshot.
func wantNoSnapshotMessage(t *testing.T, err error, table, at string, baselineOnly bool) {
	t.Helper()
	if err == nil {
		t.Fatal("reconstruct went on without a snapshot")
	}
	msg := err.Error()
	mode, notMode := replaySentence, readOnlySentence
	if baselineOnly {
		mode, notMode = readOnlySentence, replaySentence
	}
	for _, want := range []string{"snapshot of " + table, "at or before " + at, mode} {
		if !strings.Contains(msg, want) {
			t.Errorf("the message does not say %q:\n%s", want, msg)
		}
	}
	if strings.Contains(msg, notMode) || (baselineOnly && strings.Contains(msg, "replays")) {
		t.Errorf("the message describes the other mode (%q):\n%s", notMode, msg)
	}
}

func setSingleRow(t *testing.T, dir string, baselineOnly bool) {
	t.Helper()
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })
	recSQL, recFormat = "", "json"
	recSchema, recTable, recPK, recPKColumns = "mydb", "orders", "42", "id"
	recBaselineDir, recBaselineS3 = dir, ""
	recBaselineOnly, recHistory = baselineOnly, false
	recIndexDSN = "unused:unused@tcp(127.0.0.1:1)/never_opened"
	recAt = "2026-09-01 10:00:00"
}

func TestReconstruct1807_noLocationGivenSaysWhatIsMissing(t *testing.T) {
	for _, baselineOnly := range []bool{false, true} {
		setSingleRow(t, "", baselineOnly)
		err := runReconstruct(reconstructCmd, nil)
		wantNoSnapshotMessage(t, err, "mydb.orders", "2026-09-01T10:00:00Z", baselineOnly)
		// The flags stay named: they are how the location is given.
		for _, want := range []string{"--baseline-dir", "--baseline-s3", "No snapshot location was given"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("baselineOnly=%v: the message does not say %q:\n%s", baselineOnly, want, err)
			}
		}
	}
}

// An --at that does not parse is still refused as such, not reported as a
// missing snapshot location.
func TestReconstruct1807_badAtBeatsMissingLocation(t *testing.T) {
	setSingleRow(t, "", false)
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
	wantNoSnapshotMessage(t, err, recTables, "2026-09-01T10:00:00Z", false)
	if !strings.Contains(err.Error(), "No snapshot location was given") {
		t.Errorf("full-table mode does not say the location is missing:\n%s", err)
	}
}

// putSnapshot writes a snapshot folder holding one empty table file. The
// listing and the lookup read names only, never the file.
func putSnapshot(t *testing.T, dir, stamp, schema, table string) {
	t.Helper()
	p := filepath.Join(dir, stamp, schema)
	if err := os.MkdirAll(p, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p, table+".parquet"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

// noSnapshotCases are the three ways a location can hold no snapshot of the
// table from at or before the moment, each with the sentence that tells it
// apart. "Too late" is the one an evaluator had to find out alone: a
// snapshot only answers moments at or after it was taken.
var noSnapshotCases = []struct {
	name    string
	setup   func(t *testing.T, dir string)
	want    []string
	mustNot []string
}{
	{
		name:    "empty folder",
		setup:   func(t *testing.T, dir string) {},
		want:    []string{"holds no snapshots", "answers moments from then on"},
		mustNot: []string{"earliest", "includes"},
	},
	{
		name: "every snapshot of the table is later",
		setup: func(t *testing.T, dir string) {
			putSnapshot(t, dir, "2026-09-03T00-00-00Z", "mydb", "orders")
			putSnapshot(t, dir, "2026-09-02T00-00-00Z", "mydb", "orders")
			// An earlier snapshot WITHOUT the table does not make it "none".
			putSnapshot(t, dir, "2026-08-01T00-00-00Z", "mydb", "customers")
		},
		want: []string{
			"are all from after 2026-09-01T10:00:00Z; the earliest is 2026-09-02T00:00:00Z",
			"A snapshot only answers moments at or after it was taken",
		},
		mustNot: []string{"holds no snapshots", "none of"},
	},
	{
		name: "no snapshot includes the table",
		setup: func(t *testing.T, dir string) {
			putSnapshot(t, dir, "2026-08-01T00-00-00Z", "mydb", "customers")
			putSnapshot(t, dir, "2026-09-02T00-00-00Z", "mydb", "customers")
		},
		want:    []string{"holds 2 snapshots, and none of them includes mydb.orders", "answers moments from then on"},
		mustNot: []string{"earliest", "holds no snapshots"},
	},
}

func checkNoSnapshotCase(t *testing.T, err error, dir string, baselineOnly bool, want, mustNot []string) {
	t.Helper()
	wantNoSnapshotMessage(t, err, "mydb.orders", "2026-09-01T10:00:00Z", baselineOnly)
	if !errors.Is(err, reconstruct.ErrNoBaseline) {
		t.Errorf("the refusal no longer unwraps to ErrNoBaseline: %v", err)
	}
	for _, w := range append([]string{dir}, want...) {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("the message does not say %q:\n%s", w, err)
		}
	}
	for _, w := range mustNot {
		if strings.Contains(err.Error(), w) {
			t.Errorf("the message says %q, which belongs to another case:\n%s", w, err)
		}
	}
}

func TestReconstruct1807_noSnapshotBeforeTheMoment(t *testing.T) {
	for _, tc := range noSnapshotCases {
		for _, baselineOnly := range []bool{false, true} {
			name := tc.name
			if baselineOnly {
				name += ", --baseline-only"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				tc.setup(t, dir)
				setSingleRow(t, dir, baselineOnly)
				err := runReconstruct(reconstructCmd, nil)
				checkNoSnapshotCase(t, err, dir, baselineOnly, tc.want, tc.mustNot)
			})
		}
	}
}

// One snapshot without the table reads in the singular.
func TestReconstruct1807_oneSnapshotWithoutTheTable(t *testing.T) {
	dir := t.TempDir()
	putSnapshot(t, dir, "2026-08-01T00-00-00Z", "mydb", "customers")
	setSingleRow(t, dir, false)
	err := runReconstruct(reconstructCmd, nil)
	checkNoSnapshotCase(t, err, dir, false,
		[]string{"holds 1 snapshot, and it does not include mydb.orders"}, []string{"none of them", "earliest"})
}

// When the location cannot be listed, the message claims no case it cannot
// see: it says nothing was found and why a snapshot must be older.
func TestReconstruct1807_unlistableLocationClaimsNoCase(t *testing.T) {
	dir := t.TempDir()
	orig := listSnapshots
	t.Cleanup(func() { listSnapshots = orig })
	listSnapshots = func(context.Context, string) ([]reconstruct.BaselineFile, error) {
		return nil, errors.New("listing refused")
	}
	setSingleRow(t, dir, false)
	err := runReconstruct(reconstructCmd, nil)
	checkNoSnapshotCase(t, err, dir, false,
		[]string{"No snapshot of mydb.orders from at or before", "A snapshot only answers moments at or after it was taken"},
		[]string{"earliest", "holds no snapshots", "none of them"})
}

// The control for the later-snapshot fixture: the same snapshot IS found for
// a moment after it, so that case cannot pass for the reason the empty
// folder does.
func TestReconstruct1807_theLaterSnapshotIsFoundAfterIt(t *testing.T) {
	dir := t.TempDir()
	putSnapshot(t, dir, "2026-09-02T00-00-00Z", "mydb", "orders")
	after := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	if _, _, _, err := reconstruct.FindBaseline(context.Background(), dir, "mydb", "orders", after); err != nil {
		t.Fatalf("the fixture's snapshot is not found even after it, so the case above proves nothing: %v", err)
	}
}

// The command that makes a snapshot is named after the binary that failed:
// bintrail-pg has its own baseline command. A commercial build is not
// covered: the core's root command is always "bintrail", so it prints
// "bintrail baseline" there too.
func TestReconstruct1807_namesTheRunningBinary(t *testing.T) {
	for _, root := range []string{"bintrail", "bintrail-pg"} {
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

// A listing that shows a snapshot of the table from before the moment, which
// the lookup did not accept, is not "too late": the message claims no case.
func TestReconstruct1807_aListedEarlierSnapshotClaimsNoCase(t *testing.T) {
	dir := t.TempDir()
	orig := listSnapshots
	t.Cleanup(func() { listSnapshots = orig })
	listSnapshots = func(context.Context, string) ([]reconstruct.BaselineFile, error) {
		return []reconstruct.BaselineFile{
			{SnapshotTime: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), Schema: "mydb", Table: "orders"},
			{SnapshotTime: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC), Schema: "mydb", Table: "orders"},
		}, nil
	}
	setSingleRow(t, dir, false)
	err := runReconstruct(reconstructCmd, nil)
	checkNoSnapshotCase(t, err, dir, false,
		[]string{"No snapshot of mydb.orders from at or before"},
		[]string{"earliest", "holds no snapshots", "none of them"})
}

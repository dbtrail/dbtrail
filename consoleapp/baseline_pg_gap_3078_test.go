package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

func stubPGCaptureStart(t *testing.T, lsn uint64, ok bool) {
	t.Helper()
	stubPGCaptureStartRead(t, lsn, ok, nil)
}

func stubPGCaptureStartRead(t *testing.T, lsn uint64, ok bool, err error) {
	t.Helper()
	prev := pgCaptureStartFunc
	t.Cleanup(func() { pgCaptureStartFunc = prev })
	pgCaptureStartFunc = func(console.BaselineRequest) (uint64, bool, error) { return lsn, ok, err }
}

// What the index knows of the capture's start against a snapshot's floor
// (#3078): only a start after the floor is a gap.
func TestPGRefuseKnownGap_3078(t *testing.T) {
	const floor = 0x2_64ED_0000
	for _, tc := range []struct {
		name    string
		start   uint64
		known   bool
		refused bool
	}{
		{"no start on record (capture has not saved a position)", 0, false, false},
		{"capture started before the snapshot (the usual case)", floor - 0x1000, true, false},
		{"capture starts at the slot the snapshot created", floor, true, false},
		{"capture started after the snapshot's floor", floor + 0x1E68, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubPGCaptureStart(t, tc.start, tc.known)
			err := pgRefuseKnownGap(console.BaselineRequest{ServerID: "pg1"}, pgbaseline.Stats{DeltaStartLSN: floor})
			if (err != nil) != tc.refused {
				t.Fatalf("err = %v, want refused = %v", err, tc.refused)
			}
			if err == nil {
				return
			}
			for _, want := range []string{"the snapshot is not published", "2/64ED1E68", "2/64ED0000", "Run the snapshot again"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not say %q:\n%v", want, err)
				}
			}
		})
	}
}

// With no index to ask there is nothing to compare.
func TestPGCaptureStartFunc_NoIndex_3078(t *testing.T) {
	if lsn, ok, err := pgCaptureStartFunc(console.BaselineRequest{}); ok || lsn != 0 || err != nil {
		t.Fatalf("no index: got %d, %v, %v", lsn, ok, err)
	}
}

// executePG hands the read its question before the publish, and a refused
// read is a failed read: its folder is removed and the error is the job's.
func TestExecutePG_KnownGapFailsTheRead_3078(t *testing.T) {
	at := time.Date(2026, 11, 21, 14, 38, 55, 0, time.UTC)
	name := reconstruct.SnapshotDirName(at)
	prevRun, prevW := pgBaselineRun, snapshotWriterIDFunc
	t.Cleanup(func() { pgBaselineRun, snapshotWriterIDFunc = prevRun, prevW })
	snapshotWriterIDFunc = func(string) (string, error) { return "w", nil }
	pgBaselineRun = func(_ context.Context, cfg pgbaseline.Config) (pgbaseline.Stats, error) {
		cfg.DirStarting(cfg.OutputDir, name)
		st := pgbaseline.Stats{TablesProcessed: 2, SnapshotTime: at, DeltaStartLSN: 100}
		if cfg.BeforePublish == nil {
			t.Error("executePG runs the read without its question before the publish")
		} else if err := cfg.BeforePublish(st); err != nil {
			writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, name), baseline.IncompleteMarker)
			return st, err
		}
		writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, name), baseline.SuccessMarker)
		return st, nil
	}

	t.Run("capture started after the floor", func(t *testing.T) {
		stubPGCaptureStart(t, 200, true)
		local := t.TempDir()
		out, err := pgSupervisor(t).executePG(pgFullReadRequest(local, ""))
		if err == nil || !strings.Contains(err.Error(), "the snapshot is not published") || !strings.Contains(err.Error(), "was removed") {
			t.Fatalf("err = %v, want the gap and that the folder was removed", err)
		}
		if out.snapDir != "" {
			t.Errorf("a refused read returned the snapshot %q to publish", out.snapDir)
		}
		if _, serr := os.Stat(filepath.Join(local, name)); !os.IsNotExist(serr) {
			t.Errorf("the refused read's folder is still there: %v", serr)
		}
	})
	t.Run("capture started at or before the floor", func(t *testing.T) {
		stubPGCaptureStart(t, 100, true)
		local := t.TempDir()
		out, err := pgSupervisor(t).executePG(pgFullReadRequest(local, ""))
		if err != nil || out.snapDir != filepath.Join(local, name) {
			t.Fatalf("out = %+v, err = %v", out, err)
		}
	})
}

// A slot that never got its position reaches the job as the read's refusal,
// with nothing to remove: no folder was started.
func TestExecutePG_SlotNotReadyIsTheJobsError_3078(t *testing.T) {
	prevRun, prevW := pgBaselineRun, snapshotWriterIDFunc
	t.Cleanup(func() { pgBaselineRun, snapshotWriterIDFunc = prevRun, prevW })
	snapshotWriterIDFunc = func(string) (string, error) { return "w", nil }
	var refusal error = &pgcapture.SlotNotReadyError{Slot: "s", Wait: 5 * time.Minute}
	pgBaselineRun = func(context.Context, pgbaseline.Config) (pgbaseline.Stats, error) { return pgbaseline.Stats{}, refusal }
	local := t.TempDir()
	_, err := pgSupervisor(t).executePG(pgFullReadRequest(local, ""))
	if !errors.Is(err, pgcapture.ErrSlotNotReady) || !strings.Contains(err.Error(), "no table was read") || strings.Contains(err.Error(), "removed") {
		t.Fatalf("err = %v", err)
	}
	if entries, _ := os.ReadDir(local); len(entries) != 0 {
		t.Errorf("the refused read left %v", entries)
	}
}

// The three answers of the index, in the full read (#3078). A read that failed
// is said at warning level and the snapshot is published: the update asks the
// same question per table and refuses while the index cannot answer.
func TestPGRefuseKnownGapIndexAnswers_3078(t *testing.T) {
	const floor = 0x2_64ED_0000
	st := pgbaseline.Stats{DeltaStartLSN: floor}
	req := console.BaselineRequest{ServerID: "pg1"}

	stubPGCaptureStartRead(t, floor+1, true, nil)
	var gap *pgKnownGapError
	if err := pgRefuseKnownGap(req, st); !errors.As(err, &gap) {
		t.Errorf("a start after the floor: err %v, want the known gap", err)
	}
	stubPGCaptureStartRead(t, 0, false, nil)
	if err := pgRefuseKnownGap(req, st); err != nil {
		t.Errorf("nothing recorded: err %v, want the snapshot published", err)
	}
	logs := captureLog(t, slog.LevelWarn)
	stubPGCaptureStartRead(t, 0, false, errors.New("Error 1146 (42S02): Table 'idx.stream_state' doesn't exist"))
	if err := pgRefuseKnownGap(req, st); err != nil {
		t.Errorf("a failed read: err %v, want the snapshot published", err)
	}
	text := logs.String()
	for _, want := range []string{"level=WARN", "could not be read from its index", "each update of the snapshot compares it again", "1146", "pg1"} {
		if !strings.Contains(text, want) {
			t.Errorf("the warning of a failed read does not say %q:\n%s", want, text)
		}
	}
}

// The failure card's text for the two refusals whose full message is longer
// than the card shows (#3078): what happened and what to do, whole, and
// within the card's 300 characters, as executePG wraps the error.
func TestPGSnapshotFailureSummary_3078(t *testing.T) {
	req := pgFullReadRequest(t.TempDir(), "")
	const longSlot = "bintrail_s0_a_slot_name_as_long_as_postgresql_lets_it_be_63_chr"
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"the slot is still being created", fmt.Errorf("pg baseline: %w", &pgcapture.SlotNotReadyError{Slot: "bintrail_s0", Wait: 5 * time.Minute}),
			`Not read: the replication slot "bintrail_s0" was still being created after 5 minutes. PostgreSQL finishes it once the source's open write transactions end (pg_stat_activity, oldest xact_start). Nothing was read: run the snapshot again then.`},
		{"a slot name of the longest kind", fmt.Errorf("pg baseline: %w", &pgcapture.SlotNotReadyError{Slot: longSlot, Wait: 5 * time.Minute}), ""},
		{"capture started after the floor", fmt.Errorf("pg baseline: %w. The partial snapshot was removed.", &pgKnownGapError{from: 0xFFFF_FFFF_FFFF_FFFF, floor: 0xFFFF_FFFF_FFFF_0000}),
			"Not published: capture of this server started at FFFFFFFF/FFFFFFFF, after FFFFFFFF/FFFF0000 where this snapshot starts, so the changes between the two were never captured. Run the snapshot again: it will start where capture reads from."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := snapshotFailureOf(tc.err, req)
			if tc.want != "" && f.Summary != tc.want {
				t.Errorf("summary =\n%s\nwant\n%s", f.Summary, tc.want)
			}
			n := len([]rune(f.Summary))
			t.Logf("%d characters: %s", n, f.Summary)
			if n > 300 || strings.HasSuffix(f.Summary, "...") || !strings.Contains(f.Summary, "un the snapshot again") {
				t.Errorf("the card's text is cut or says no remedy (%d characters): %s", n, f.Summary)
			}
			if cut := refusalSummary(tc.err); !strings.HasSuffix(cut, "...") {
				t.Logf("the full message would fit the card as it is: %s", cut)
			}
		})
	}
	// The slot found being created again AFTER the tables were read also
	// matches ErrSlotNotReady; "nothing was read" would be false of it.
	after := fmt.Errorf("pg baseline: pgbaseline: %w", fmt.Errorf("pgcapture: replication slot %q was removed and is being created again while the snapshot was read: %w", "s", pgcapture.ErrSlotNotReady))
	if f := snapshotFailureOf(after, req); strings.Contains(f.Summary, "Nothing was read") {
		t.Errorf("a refusal after the read reads as nothing read: %s", f.Summary)
	}
	// A MySQL source's card is not given these words.
	mysqlReq := req
	mysqlReq.Flavor = ""
	if f := snapshotFailureOf(&pgKnownGapError{from: 2, floor: 1}, mysqlReq); strings.HasPrefix(f.Summary, "Not published: capture") {
		t.Errorf("a MySQL card got the PostgreSQL text: %s", f.Summary)
	}
}

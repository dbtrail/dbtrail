package streamrun

import (
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// continuesNumbering decides whether the position-mode resume cleanup may
// compare indexed rows against the replay start (#2170). Every "false" here
// skips the delete, so a wrong "false" on the ordinary replay would duplicate
// rows and a wrong "true" on a renumbering would delete real ones.
func TestContinuesNumbering(t *testing.T) {
	for _, tc := range []struct {
		name      string
		savedFile string
		savedPos  uint64
		file      string
		pos       uint64
		want      bool
	}{
		{"same position: the ordinary crash replay", "binlog.000007", 500, "binlog.000007", 500, true},
		{"same file, later position", "binlog.000007", 500, "binlog.000007", 900, true},
		{"later file: a purge advance", "binlog.000007", 500, "binlog.000009", 4, true},
		{"suffix rollover past six digits", "binlog.999999", 500, "binlog.1000000", 4, true},
		{"same file, lower position: the file was regenerated", "binlog.000007", 500, "binlog.000007", 4, false},
		{"earlier file: RESET MASTER", "binlog.000131", 500, "binlog.000001", 4, false},
		{"shorter suffix sorts below", "binlog.1000000", 500, "binlog.999999", 4, false},
		{"shorter base name", "mysql-bin.000007", 500, "bin.000009", 4, false},
		{"longer base name", "bin.000007", 500, "mysql-bin.000009", 4, false},
		{"base name differs only in case", "binlog.000007", 500, "BINLOG.000009", 4, false},
		{"no dot, same name", "binlog", 500, "binlog", 600, true},
		{"no dot, other name", "binlog", 500, "other", 600, false},
		{"dotted against undotted", "binlog.000007", 500, "binlog", 600, false},
		{"empty start against a checkpoint", "binlog.000007", 500, "", 4, false},
		{"no saved file", "", 0, "binlog.000001", 4, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := continuesNumbering(tc.savedFile, tc.savedPos, tc.file, tc.pos); got != tc.want {
				t.Fatalf("continuesNumbering(%s:%d -> %s:%d) = %v, want %v",
					tc.savedFile, tc.savedPos, tc.file, tc.pos, got, tc.want)
			}
		})
	}
}

// The skip notice follows the gap detector's verdict, driven through the
// real detectPositionGap. A checkpoint past the end of a file that still
// exists is a crash that lost the tail OR a renumbering that reused the name:
// the notice must name both, must not assert a renumbering, and must warn of
// duplicates (the rows are kept; after a crash the oldest file is replayed).
// A checkpoint whose file is gone, with the start below it, is a renumbering.
func TestPositionCleanupSkipNotice_followsTheGapVerdict(t *testing.T) {
	for _, tc := range []struct {
		name      string
		logs      [][2]any // name, size
		savedFile string
		savedPos  uint64
		wantLine  []string
		wantWarn  []string
		forbid    string
	}{
		{
			name:      "checkpoint past the end of a file that still exists",
			logs:      [][2]any{{"binlog.000005", 900}, {"binlog.000007", 4500}, {"binlog.000008", 300}},
			savedFile: "binlog.000007", savedPos: 5000,
			wantLine: []string{"Cleanup: skipped, the checkpoint binlog.000007:5000 is past the end of that file",
				"binlog.000005:4", "lost the file's tail", "started over", "indexed events are kept", "indexed twice"},
			wantWarn: []string{"past the end of its binlog file", "lost in a source crash", "started over with the same", "duplicates"},
			forbid:   "the binlog numbering started over (checkpoint",
		},
		{
			name:      "checkpoint file gone, start below it",
			logs:      [][2]any{{"binlog.000001", 300}},
			savedFile: "binlog.000131", savedPos: 5000,
			wantLine: []string{"Cleanup: skipped, the binlog numbering started over (checkpoint binlog.000131:5000, now starting at binlog.000001:4); indexed events are kept"},
			wantWarn: []string{"numbering started over", "capture loss"},
			forbid:   "duplicates",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			rows := sqlmock.NewRows([]string{"Log_name", "File_size"})
			for _, l := range tc.logs {
				rows.AddRow(l[0], l[1])
			}
			mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(rows)
			gap, err := detectPositionGap(db, tc.savedFile, uint32(tc.savedPos), 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if !gap.HasGap || gap.Fillable {
				t.Fatalf("want an unfillable gap, got %+v", gap)
			}
			if continuesNumbering(tc.savedFile, tc.savedPos, gap.EarliestFile, uint64(gap.EarliestPos)) {
				t.Fatalf("start %s:%d continues %s:%d: the skip branch would not run", gap.EarliestFile, gap.EarliestPos, tc.savedFile, tc.savedPos)
			}
			warn, line := positionCleanupSkipNotice(gap.CheckpointPastEnd, tc.savedFile, tc.savedPos, gap.EarliestFile, gap.EarliestPos)
			for _, w := range tc.wantLine {
				if !strings.Contains(line, w) {
					t.Errorf("line %q lacks %q", line, w)
				}
			}
			for _, w := range tc.wantWarn {
				if !strings.Contains(warn, w) {
					t.Errorf("warn %q lacks %q", warn, w)
				}
			}
			if strings.Contains(line+warn, tc.forbid) {
				t.Errorf("notice says %q, which this case must not claim:\n%s\n%s", tc.forbid, line, warn)
			}
		})
	}
}

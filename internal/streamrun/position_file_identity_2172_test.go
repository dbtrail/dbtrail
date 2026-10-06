package streamrun

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// fitsGap is what detectPositionGap returns when the checkpoint's file is
// still listed and long enough: no gap in the current file, or a fillable one.
func fitsGap(fillable bool) *gapResult {
	if fillable {
		return &gapResult{HasGap: true, Fillable: true, RebuildUndetectable: true, EarliestFile: "binlog.000001", EarliestPos: 4,
			Message: "gap detected: checkpoint is at binlog.000003:9000, source is at binlog.000004; replaying missed events"}
	}
	return &gapResult{RebuildUndetectable: true, EarliestFile: "binlog.000001", EarliestPos: 4}
}

func purgedGap() *gapResult {
	return &gapResult{HasGap: true, EarliestFile: "binlog.000005", EarliestPos: 4,
		Message: "binlog gap detected but CANNOT be filled: required file binlog.000003 has been purged"}
}

func pastEndGap() *gapResult {
	return &gapResult{HasGap: true, CheckpointPastEnd: true, EarliestFile: "binlog.000001", EarliestPos: 4,
		Message: "binlog gap: file binlog.000003 exists but checkpoint position 9000 exceeds file size 500"}
}

// TestCheckPositionCheckpointFile is the #2172 verdict over everything a
// position-mode resume knows before it deletes anything: the gap detector's
// view of the checkpoint's file, the identities the checkpoint recorded, the
// source's identity now, and (only when it can decide) the identity of the
// file the source now serves under the checkpoint's name.
func TestCheckPositionCheckpointFile(t *testing.T) {
	const (
		fileA = "fde:1791271554:1"
		fileB = "fde:1791271558:1"
		srvA  = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
		srvB  = "9a5f8c2e-0000-11e1-9e33-c80aa9429562"
	)
	for _, c := range []struct {
		name                 string
		gap                  *gapResult
		savedFile            string
		savedFileID, savedSr string
		srcID                string
		probed               string
		probeErr             error
		wantProbe            bool
		wantErr              bool
		wantVerified         bool
		wantKind             positionRenumberKind
		wantUnfillable       bool
		wantMsg              []string
	}{
		{name: "no file identity, same server: today's path, nothing probed",
			gap: fitsGap(false), savedFile: "binlog.000003", savedSr: srvA, srcID: srvA},
		{name: "no identities at all (older build): today's path",
			gap: fitsGap(false), savedFile: "binlog.000003"},
		{name: "Galera (no source identity) and no file identity: today's path",
			gap: fitsGap(true), savedFile: "binlog.000003"},
		{name: "file identity equal: verified",
			gap: fitsGap(false), savedFile: "binlog.000003", savedFileID: fileA, savedSr: srvA, srcID: srvA,
			probed: fileA, wantProbe: true, wantVerified: true},
		{name: "file identity equal on a fillable gap: verified",
			gap: fitsGap(true), savedFile: "binlog.000003", savedFileID: fileA,
			probed: fileA, wantProbe: true, wantVerified: true},
		{name: "file identity differs: the numbering started over and grew back",
			gap: fitsGap(false), savedFile: "binlog.000003", savedFileID: fileA, savedSr: srvA, srcID: srvA,
			probed: fileB, wantProbe: true, wantKind: positionFileReplaced, wantUnfillable: true,
			wantMsg: []string{"binlog.000003", fileA, fileB, "started over", "binlog.000001", "permanently lost", "keeps every event already indexed"}},
		{name: "file identity differs on a fillable gap",
			gap: fitsGap(true), savedFile: "binlog.000003", savedFileID: fileA,
			probed: fileB, wantProbe: true, wantKind: positionFileReplaced, wantUnfillable: true},
		{name: "file identity differs with no source identity (Galera)",
			gap: fitsGap(false), savedFile: "binlog.000003", savedFileID: fileA,
			probed: fileB, wantProbe: true, wantKind: positionFileReplaced, wantUnfillable: true},
		{name: "probe fails: no verdict is guessed",
			gap: fitsGap(false), savedFile: "binlog.000003", savedFileID: fileA,
			probeErr: errors.New("ERROR 1236"), wantProbe: true, wantErr: true},
		{name: "probe answers nothing: no verdict is guessed",
			gap: fitsGap(false), savedFile: "binlog.000003", savedFileID: fileA,
			probed: "", wantProbe: true, wantErr: true},
		{name: "file identity equal wins over a changed source identity (server_id or server_uuid changed, same files)",
			gap: fitsGap(false), savedFile: "binlog.000003", savedFileID: fileA, savedSr: "server_id:1", srcID: "server_id:2",
			probed: fileA, wantProbe: true, wantVerified: true},
		{name: "no file identity, another server: positions say nothing",
			gap: fitsGap(false), savedFile: "binlog.000003", savedSr: srvA, srcID: srvB,
			wantKind: positionOtherServer, wantUnfillable: true,
			wantMsg: []string{srvA, srvB, "binlog.000003", "binlog.000001", "indexed again", "permanently lost"}},
		{name: "no file identity, checkpoint of unknown server: today's path",
			gap: fitsGap(false), savedFile: "binlog.000003", srcID: srvB},
		{name: "no file identity, source of unknown identity: today's path",
			gap: fitsGap(false), savedFile: "binlog.000003", savedSr: srvA},
		{name: "purged, same server: the purge advance as before, nothing probed",
			gap: purgedGap(), savedFile: "binlog.000003", savedFileID: fileA, savedSr: srvA, srcID: srvA,
			wantUnfillable: true},
		{name: "purged, another server: the advance keeps every row",
			gap: purgedGap(), savedFile: "binlog.000003", savedFileID: fileA, savedSr: srvA, srcID: srvB,
			wantKind: positionOtherServer, wantUnfillable: true,
			wantMsg: []string{"required file binlog.000003 has been purged", srvA, srvB}},
		{name: "past the end: the #2170 path, nothing probed",
			gap: pastEndGap(), savedFile: "binlog.000003", savedFileID: fileA, savedSr: srvA, srcID: srvA,
			wantUnfillable: true},
		{name: "no checkpoint file: nothing to check",
			gap: fitsGap(false), savedFile: "", savedFileID: fileA},
	} {
		t.Run(c.name, func(t *testing.T) {
			probed := 0
			probe := func(file string) (string, error) {
				probed++
				if file != c.savedFile {
					t.Errorf("probed %q, want the checkpoint's file %q", file, c.savedFile)
				}
				return c.probed, c.probeErr
			}
			saved := &streamState{mode: "position", binlogFile: c.savedFile, binlogPos: 9000,
				fileIdentity: c.savedFileID, sourceIdentity: c.savedSr}
			in := *c.gap
			got, err := checkPositionCheckpointFile(&in, saved, c.srcID, probe)
			if (probed > 0) != c.wantProbe {
				t.Errorf("probe called %d times, want called=%v", probed, c.wantProbe)
			}
			if c.wantErr {
				if err == nil {
					t.Fatalf("want an error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.verified != c.wantVerified {
				t.Errorf("verified = %v, want %v", got.verified, c.wantVerified)
			}
			if got.kind != c.wantKind {
				t.Errorf("kind = %v, want %v", got.kind, c.wantKind)
			}
			if unfillable := got.gap.HasGap && !got.gap.Fillable; unfillable != c.wantUnfillable {
				t.Errorf("unfillable = %v, want %v (%+v)", unfillable, c.wantUnfillable, got.gap)
			}
			if c.wantKind == positionFileReplaced || (c.wantKind == positionOtherServer && c.gap.RebuildUndetectable) {
				if got.gap.EarliestFile != c.gap.EarliestFile || got.gap.EarliestPos != 4 {
					t.Errorf("restart = %s:%d, want %s:4", got.gap.EarliestFile, got.gap.EarliestPos, c.gap.EarliestFile)
				}
				if got.gap.RebuildUndetectable {
					t.Error("a verdict that the file is another one still says a rebuild is undetectable")
				}
			}
			if c.wantKind == positionContinues && !c.wantVerified && *got.gap != *c.gap {
				t.Errorf("gap changed without a verdict: %+v, want %+v", got.gap, c.gap)
			}
			for _, w := range c.wantMsg {
				if !strings.Contains(got.gap.Message, w) {
					t.Errorf("message %q lacks %q", got.gap.Message, w)
				}
			}
		})
	}
}

func TestFileIdentities(t *testing.T) {
	var none *fileIdentities
	if got := none.get("binlog.000001"); got != "" {
		t.Errorf("nil registry get = %q, want empty", got)
	}
	none.set("binlog.000001", "fde:1:1") // must not panic

	f := newFileIdentities()
	f.set("binlog.000003", "fde:100:1")
	f.set("binlog.000004", "fde:200:1")
	f.set("binlog.000005", "")
	f.set("binlog.000003", "") // an unknown identity never erases a known one
	if got := f.get("binlog.000003"); got != "fde:100:1" {
		t.Errorf("get(000003) = %q", got)
	}
	if got := f.get("binlog.000005"); got != "" {
		t.Errorf("an empty identity was stored: %q", got)
	}
	if got := f.get("binlog.000009"); got != "" {
		t.Errorf("unknown file = %q, want empty", got)
	}
	// Many rotations (a burst of FLUSH LOGS) while the checkpoint stays on an
	// old file: its entry must survive.
	for i := 0; i < 5000; i++ {
		f.set(fmt.Sprintf("binlog.%06d", 10+i), "fde:1:1")
	}
	if got := f.get("binlog.000003"); got != "fde:100:1" {
		t.Errorf("the checkpoint file's identity was evicted after many rotations: %q", got)
	}
}

// identityArg matches the binlog_file_identity argument of saveCheckpoint.
type identityArg struct{ want any }

func (a identityArg) Match(v driver.Value) bool { return v == a.want }

// TestSaveCheckpoint_recordsTheCheckpointFilesIdentity: the identity stored is
// the one of the file the checkpoint names (safeFile in position mode), not of
// the file the stream reads now; unknown → NULL; and the upsert overwrites it,
// never keeps an old one beside a new file name.
func TestSaveCheckpoint_recordsTheCheckpointFilesIdentity(t *testing.T) {
	for _, c := range []struct {
		name  string
		state func() *streamState
		want  any
	}{
		{"checkpoint file behind the stream's file", func() *streamState {
			s := &streamState{mode: "position", binlogFile: "binlog.000004", binlogPos: 300, safeFile: "binlog.000003", safePos: 9000,
				fileIdents: newFileIdentities()}
			s.fileIdents.set("binlog.000003", "fde:100:1")
			s.fileIdents.set("binlog.000004", "fde:200:1")
			return s
		}, "fde:100:1"},
		{"checkpoint file not seen yet", func() *streamState {
			s := &streamState{mode: "position", binlogFile: "binlog.000003", safeFile: "binlog.000003", safePos: 4, fileIdents: newFileIdentities()}
			return s
		}, nil},
		{"no registry (an advance's own checkpoint)", func() *streamState {
			return &streamState{mode: "position", binlogFile: "binlog.000001", safeFile: "binlog.000001", safePos: 4}
		}, nil},
		{"GTID mode names binlogFile", func() *streamState {
			s := &streamState{mode: "gtid", binlogFile: "binlog.000004", binlogPos: 300, fileIdents: newFileIdentities()}
			s.fileIdents.set("binlog.000004", "fde:200:1")
			return s
		}, "fde:200:1"},
	} {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			args := make([]driver.Value, 0, 13)
			for i := 0; i < 12; i++ {
				args = append(args, sqlmock.AnyArg())
			}
			args = append(args, identityArg{c.want})
			mock.ExpectExec(`(?s)INSERT INTO stream_state.*capture_skips, dedup_floor_event_id, binlog_file_identity\).*` +
				`binlog_file_identity = VALUES\(binlog_file_identity\)`).
				WithArgs(args...).WillReturnResult(sqlmock.NewResult(0, 1))
			if err := saveCheckpoint(db, c.state()); err != nil {
				t.Fatalf("saveCheckpoint: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// TestPositionCheckSkipNotice: the notice names what was found, says every
// row is kept, and never claims a renumbering for another server.
func TestPositionCheckSkipNotice(t *testing.T) {
	warn, line := positionCheckSkipNotice(positionFileReplaced, "binlog.000003", 9000, "binlog.000001", 4)
	for _, w := range []string{"binlog.000003:9000", "binlog.000001:4", "not the file", "kept"} {
		if !strings.Contains(line, w) {
			t.Errorf("file-replaced line %q lacks %q", line, w)
		}
	}
	for _, w := range []string{"started over", "kept"} {
		if !strings.Contains(warn, w) {
			t.Errorf("file-replaced warn %q lacks %q", warn, w)
		}
	}
	warn, line = positionCheckSkipNotice(positionOtherServer, "binlog.000003", 9000, "mysql-bin.000001", 4)
	for _, w := range []string{"binlog.000003:9000", "mysql-bin.000001:4", "another server", "kept"} {
		if !strings.Contains(line, w) {
			t.Errorf("other-server line %q lacks %q", line, w)
		}
	}
	if strings.Contains(warn+line, "started over") {
		t.Errorf("other-server notice claims a renumbering: %s / %s", warn, line)
	}
	if !strings.Contains(warn, "duplicates") {
		t.Errorf("other-server warn %q does not say rows may be indexed twice", warn)
	}
}

func TestFloorlessCleanupNotice(t *testing.T) {
	if warn, line := floorlessCleanupNotice(1); warn != "" || line != "" {
		t.Errorf("a cleanup with a floor is warned about: %q / %q", warn, line)
	}
	warn, line := floorlessCleanupNotice(0)
	for _, w := range []string{"no dedup floor", "0.83.0", "RESET MASTER", "never send them again"} {
		if !strings.Contains(warn, w) {
			t.Errorf("warn %q lacks %q", warn, w)
		}
	}
	for _, w := range []string{"no dedup floor", "older binlog numbering"} {
		if !strings.Contains(line, w) {
			t.Errorf("line %q lacks %q", line, w)
		}
	}
	t.Logf("%s\n%s", warn, line)
}

func TestPurgeAdvanceCleanupNotice(t *testing.T) {
	warn, line := purgeAdvanceCleanupNotice("binlog.000003", 9000, "binlog.000005", 4)
	for _, w := range []string{"purged", "RESET MASTER", "never send them again", "tells the two apart"} {
		if !strings.Contains(warn, w) {
			t.Errorf("warn %q lacks %q", warn, w)
		}
	}
	for _, w := range []string{"binlog.000003:9000", "binlog.000005:4", "reset"} {
		if !strings.Contains(line, w) {
			t.Errorf("line %q lacks %q", line, w)
		}
	}
	t.Logf("%s\n%s\n%s", warn, line, lateIndexWriteNotice)
}

// TestOneWarnsBeforeEveryFloorlessCleanup guards the wiring in One (it needs a
// live source to run): both places that start the resume cleanup name the
// floorless case first.
func TestOneWarnsBeforeEveryFloorlessCleanup(t *testing.T) {
	raw, err := os.ReadFile("streamrun.go")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(raw), "\n")
	starts := 0
	for i, l := range lines {
		if !strings.Contains(l, "done := beginResumeCleanup(") {
			continue
		}
		starts++
		found := false
		for j := max(0, i-6); j < i; j++ {
			if strings.Contains(lines[j], "warnFloorlessCleanup(saved.dedupFloorID)") {
				found = true
			}
		}
		if !found {
			t.Errorf("streamrun.go:%d starts the resume cleanup without naming a missing dedup floor", i+1)
		}
	}
	if starts != 2 {
		t.Errorf("found %d cleanup starts, want 2", starts)
	}
}

// TestDetectPositionGap_fitsNamesTheOldestFile: the check of the checkpoint's
// file restarts at the oldest file when the file is another one, so the gap
// detector must name it on a resume that fits too, not only on a purge.
func TestDetectPositionGap_fitsNamesTheOldestFile(t *testing.T) {
	for _, c := range []struct {
		name string
		file string
	}{{"checkpoint in the current file", "binlog.000004"}, {"checkpoint in an older file", "binlog.000003"}} {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(sqlmock.NewRows([]string{"Log_name", "File_size"}).
				AddRow("binlog.000001", 900).AddRow("binlog.000003", 9000).AddRow("binlog.000004", 9000))
			gap, err := detectPositionGap(db, c.file, 500, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if !gap.RebuildUndetectable || gap.EarliestFile != "binlog.000001" || gap.EarliestPos != 4 {
				t.Errorf("gap = %+v, want RebuildUndetectable with the oldest file binlog.000001:4", gap)
			}
		})
	}
}

func TestPositionAdvanceRenumbered(t *testing.T) {
	for _, c := range []struct {
		name      string
		kind      positionRenumberKind
		savedFile string
		savedPos  uint64
		file      string
		pos       uint32
		want      bool
	}{
		{"forward purge in one numbering", positionContinues, "binlog.000003", 9000, "binlog.000005", 4, false},
		{"start below the checkpoint (#2170)", positionContinues, "binlog.000003", 9000, "binlog.000001", 4, true},
		{"file replaced, oldest file has the checkpoint's name at :4", positionFileReplaced, "binlog.000001", 4, "binlog.000001", 4, true},
		{"file replaced, oldest file sorts above", positionFileReplaced, "binlog.000001", 9000, "binlog.000002", 4, true},
		{"another server whose names sort above", positionOtherServer, "binlog.000003", 9000, "binlog.000009", 4, true},
	} {
		if got := positionAdvanceRenumbered(c.kind, c.savedFile, c.savedPos, c.file, c.pos); got != c.want {
			t.Errorf("%s: positionAdvanceRenumbered = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestRunFileIdentities(t *testing.T) {
	saved := &streamState{mode: "position", binlogFile: "binlog.000003", fileIdentity: "fde:100:1"}
	if got := runFileIdentities(positionFileCheck{verified: true}, saved).get("binlog.000003"); got != "fde:100:1" {
		t.Errorf("a verified checkpoint file starts the run unknown (%q): the first checkpoint would store NULL over it", got)
	}
	if got := runFileIdentities(positionFileCheck{}, saved).get("binlog.000003"); got != "" {
		t.Errorf("an unverified checkpoint file was carried: %q", got)
	}
	if got := runFileIdentities(positionFileCheck{kind: positionFileReplaced}, saved).get("binlog.000003"); got != "" {
		t.Errorf("the identity of a replaced file was carried: %q", got)
	}
	if runFileIdentities(positionFileCheck{}, nil) == nil {
		t.Error("a first run has no registry: its checkpoints would never record an identity")
	}
}

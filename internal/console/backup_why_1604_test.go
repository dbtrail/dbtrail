package console

import (
	"path/filepath"
	"strings"
	"testing"
)

// Each reason a scheduled run takes a full backup maps to one stable code,
// and the mapping is fixed when the record is written (#1604). The table
// ties every constant to its code, so rewording a reason without moving its
// code fails here rather than silently reclassifying new records as "".
func TestBackupWhyCode(t *testing.T) {
	cases := []struct{ why, want string }{
		{"", ""},
		{BackupWhyNoIndex, "no_index"},
		{BackupWhyNoLocalDir, "no_local_dir"},
		{BackupWhyFirstBackup, "first_backup"},
		{BackupWhyUnreadablePrefix + " from the backup destination (boom), so a full backup is taken instead", "previous_unreadable"},
		{BackupWhyFoldRefusedPrefix + " (capture gap)", "fold_refused"},
		{BackupWhyFoldCrashedPrefix + " (internal error: nil map)", "fold_crashed"},
		{"no load on your database", ""},
		{"some wording a newer daemon wrote", ""},
	}
	for _, c := range cases {
		if got := BackupWhyCode(c.why); got != c.want {
			t.Errorf("BackupWhyCode(%q) = %q, want %q", c.why, got, c.want)
		}
	}
	// The strings ChooseBackupMethod and rebuildPossible emit ARE the
	// constants: a reason that never reaches the classifier as written is
	// a code that never fires.
	if err := rebuildPossible(ServerEntry{}); err == nil || err.Error() != BackupWhyNoIndex {
		t.Errorf("rebuildPossible without an index = %v, want the constant", err)
	}
	if err := rebuildPossible(ServerEntry{DSN: "d"}); err == nil || err.Error() != BackupWhyNoLocalDir {
		t.Errorf("rebuildPossible without a local dir = %v, want the constant", err)
	}
}

// The reason survives the history file: written with the run, read back
// unchanged, never recomputed.
func TestBaselineHistory_persistsTheFullBackupReason(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.json")
	h, err := OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	rec := BaselineRunRecord{ServerID: "s", Kind: BaselineRunDump, Trigger: BaselineRunTriggerScheduled,
		StartedAt: "2026-09-13T01:00:00Z", FinishedAt: "2026-09-13T01:05:00Z",
		Why: BackupWhyNoLocalDir, WhyCode: BackupWhyCode(BackupWhyNoLocalDir)}
	if err := h.Append(rec); err != nil {
		t.Fatal(err)
	}
	again, err := OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	got := again.List("s")
	if len(got) != 1 || got[0].Why != BackupWhyNoLocalDir || got[0].WhyCode != "no_local_dir" {
		t.Fatalf("reloaded record = %+v", got)
	}
	// Both renderings of a run carry it: the persisted record and the
	// loop's live view of a job it started.
	if dto := scheduleRunFromRecord(&got[0]); dto.Why != BackupWhyNoLocalDir || dto.WhyCode != "no_local_dir" {
		t.Errorf("scheduleRunFromRecord dropped the reason: %+v", dto)
	}
	st := BackupScheduleState{LastMethod: BackupMethodFull, LastWhy: BackupWhyFirstBackup, Last: &BaselineStatus{State: "succeeded"}}
	if dto := scheduleRunFromStatus(st); dto.Why != BackupWhyFirstBackup || dto.WhyCode != "first_backup" {
		t.Errorf("scheduleRunFromStatus dropped the reason: %+v", dto)
	}
}

// The page's remedy table is keyed by the codes this package emits; a code
// renamed on one side leaves the other rendering the raw reason.
func TestBackupWhyRemedyKeysMatchTheCodes(t *testing.T) {
	js := readAsset(t, "app.js")
	i := strings.Index(js, "const BACKUP_WHY_REMEDY = {")
	if i < 0 {
		t.Fatal("app.js has no BACKUP_WHY_REMEDY")
	}
	block := js[i : i+strings.Index(js[i:], "};")]
	for _, why := range []string{BackupWhyNoIndex, BackupWhyNoLocalDir, BackupWhyFirstBackup} {
		if code := BackupWhyCode(why); !strings.Contains(block, "\n  "+code+":") {
			t.Errorf("BACKUP_WHY_REMEDY has no entry for %q", code)
		}
	}
}

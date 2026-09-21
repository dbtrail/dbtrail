package consoleapp

import (
	"context"
	"errors"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
)

// The schedule's answer to "can a full backup start here?" follows the lock
// mode the next dump would really use. It used to return the refusal this
// process started with, so after an operator fixed a bad lock mode from the
// Backup settings page, dumps went through again while the schedule card,
// the next-run prediction and the loop's own gates still said they could
// not, until a restart.
func TestBackupSchedulerFullBackups_followsTheSavedLockMode(t *testing.T) {
	bootErr := errors.New(`BINTRAIL_CONSOLE_BASELINE_LOCK_MODE: unknown lock mode "lock-everything"`)
	reg := liveRegistry(t)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.reg = reg
	sup.configErr = bootErr
	// ONE scheduler, built before anything is saved, and the setting changed
	// under it: the question is whether it reads the lock mode when asked,
	// and a scheduler built after each save could not tell that from one
	// that read it once when it was built (the original bug, moved).
	b := newBackupScheduler(sup, reg, true, false)
	steps := []struct {
		name    string
		saved   *string // nil: nothing saved (cleared)
		wantErr bool
	}{
		{"nothing saved keeps the boot refusal", nil, true},
		{"a readable saved value clears it", strp("lock-all"), false},
		{"an unreadable saved value falls back to the boot refusal", strp("lock-everything"), true},
		{"a cleared saved value (the built-in) clears it", strp(""), false},
		{"removing the saved value brings the boot refusal back", nil, true},
		{"and fixing it again clears it again", strp("lock-all"), false},
	}
	for _, c := range steps {
		if err := reg.SetBackupSetting(console.BackupSettingLockMode, c.saved); err != nil {
			t.Fatalf("%s: save: %v", c.name, err)
		}
		ok, err := b.FullBackups()
		if !ok {
			t.Fatalf("%s: the creation opt-in was lost", c.name)
		}
		if (err != nil) != c.wantErr {
			t.Fatalf("%s: FullBackups() error = %v, want error %v", c.name, err, c.wantErr)
		}
		// The loop's own gates read the same answer, so a slot fires on
		// what the page shows.
		if g := b.gates(); (g.FullBackupsErr != "") != c.wantErr {
			t.Fatalf("%s: gates().FullBackupsErr = %q, want set %v", c.name, g.FullBackupsErr, c.wantErr)
		}
	}
	// With no boot refusal nothing is invented.
	sup = newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	if _, err := newBackupScheduler(sup, liveRegistry(t), true, false).FullBackups(); err != nil {
		t.Fatalf("no boot refusal and nothing saved: FullBackups() error = %v", err)
	}
}

func strp(s string) *string { return &s }

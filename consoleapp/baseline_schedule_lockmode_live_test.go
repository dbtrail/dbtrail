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
	cases := []struct {
		name    string
		saved   *string // nil: nothing saved
		wantErr bool
	}{
		{"nothing saved keeps the boot refusal", nil, true},
		{"a readable saved value clears it", strp("lock-all"), false},
		{"a cleared saved value (the built-in) clears it", strp(""), false},
		{"an unreadable saved value falls back to the boot refusal", strp("lock-everything"), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reg := liveRegistry(t)
			sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
			sup.reg = reg
			sup.configErr = bootErr
			if c.saved != nil {
				save(t, reg, console.BackupSettingLockMode, *c.saved)
			}
			b := newBackupScheduler(sup, reg, true, false)
			ok, err := b.FullBackups()
			if !ok {
				t.Fatal("the creation opt-in was lost")
			}
			if (err != nil) != c.wantErr {
				t.Fatalf("FullBackups() error = %v, want error %v", err, c.wantErr)
			}
			// The loop's own gates read the same answer, so a slot fires on
			// what the page shows.
			if g := b.gates(); (g.FullBackupsErr != "") != c.wantErr {
				t.Fatalf("gates().FullBackupsErr = %q, want set %v", g.FullBackupsErr, c.wantErr)
			}
		})
	}
	// With no boot refusal nothing is invented.
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	if _, err := newBackupScheduler(sup, liveRegistry(t), true, false).FullBackups(); err != nil {
		t.Fatalf("no boot refusal and nothing saved: FullBackups() error = %v", err)
	}
}

func strp(s string) *string { return &s }

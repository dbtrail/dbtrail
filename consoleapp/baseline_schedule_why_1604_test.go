package consoleapp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// A scheduled full backup persists WHY it was a full backup (#1604), as
// decided when it ran: the first backup has nothing to update from.
func TestBackupScheduler_recordsWhyTheFirstBackupWasFull(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, false)
	e.SourceDSN = "not a dsn" // fails before any network; the record is still written
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	if st.LastWhy != console.BackupWhyFirstBackup {
		t.Fatalf("live view LastWhy = %q, want %q", st.LastWhy, console.BackupWhyFirstBackup)
	}
	run, _ := sup.history.LastScheduled(e.ID)
	if run == nil || run.Kind != console.BaselineRunDump {
		t.Fatalf("no scheduled dump on record: %+v", run)
	}
	if run.Why != console.BackupWhyFirstBackup || run.WhyCode != "first_backup" {
		t.Fatalf("record why = %q code = %q, want the first-backup reason", run.Why, run.WhyCode)
	}
}

// The fallback full backup after a refused update records the refusal as
// its reason, with the fold's own message inside.
func TestBackupScheduler_recordsWhyTheFallbackWasFull(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, errors.New("seam: capture gap")
	})
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	e.SourceDSN = "not a dsn"
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	run, _ := sup.history.LastScheduled(e.ID)
	if run == nil || run.Kind != console.BaselineRunDump {
		t.Fatalf("no fallback dump on record: %+v", run)
	}
	if run.WhyCode != "fold_refused" || !strings.Contains(run.Why, "capture gap") {
		t.Fatalf("record why = %q code = %q, want the refusal with the fold's message", run.Why, run.WhyCode)
	}
}

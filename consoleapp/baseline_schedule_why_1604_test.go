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

// The reasons that stay true until a setting changes (#1604's headline
// cases) reach the record from the scheduler: decided BEFORE any probe of
// the destination, so a server with no index connection records that
// setting, not "first snapshot" or an unreadable bucket.
func TestBackupScheduler_recordsThePermanentReasons(t *testing.T) {
	for _, tc := range []struct {
		name  string
		shape func(e *console.ServerEntry, sup *baselineSupervisor)
		why   string
		code  string
	}{
		{"no index connection", func(e *console.ServerEntry, _ *baselineSupervisor) { e.DSN = "" }, console.BackupWhyNoIndex, "no_index"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, reg, sup := newScheduleFixture(t, true)
			e := addScheduled(t, reg, false)
			e.SourceDSN = "not a dsn"
			tc.shape(&e, sup)
			if err := reg.Update(e); err != nil {
				t.Fatal(err)
			}
			fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
			st := waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
			if !strings.HasPrefix(st.LastWhy, tc.why) {
				t.Fatalf("live view LastWhy = %q, want %q", st.LastWhy, tc.why)
			}
			t.Logf("why: %s", st.LastWhy)
			run, _ := sup.history.LastScheduled(e.ID)
			if run == nil || !strings.HasPrefix(run.Why, tc.why) || run.WhyCode != tc.code {
				t.Fatalf("record = %+v, want why %q code %q", run, tc.why, tc.code)
			}
		})
	}
}

// #2431: a scheduled server with an S3 location and no local folder runs
// nothing: the slot is skipped, saying the folder is what is missing, and no
// job is started.
func TestBackupScheduler_s3OnlyServerIsSkippedNamingTheFolder(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, false)
	e.BaselineDir, e.BaselineS3 = "", "s3://bucket/prefix"
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	b.watchers.Wait()
	st := b.ScheduleState(e.ID)
	if !strings.Contains(st.LastSkipReason, "no local snapshot folder") || st.LastStartedAt != "" || st.Running {
		t.Fatalf("schedule state = %+v, want a skip naming the folder and no job", st)
	}
	if runs := sup.history.List(e.ID); len(runs) != 1 || !strings.Contains(runs[0].SkipReason, "no local snapshot folder") {
		t.Fatalf("history = %+v, want the one skipped slot, naming the folder", runs)
	}
}

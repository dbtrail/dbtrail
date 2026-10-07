package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
// the destination, so an S3-only server whose staging folder cannot be used
// (#2212) records that setting, not "first snapshot" or an unreadable bucket.
func TestBackupScheduler_recordsThePermanentReasons(t *testing.T) {
	for _, tc := range []struct {
		name  string
		shape func(e *console.ServerEntry, sup *baselineSupervisor)
		why   string
		code  string
	}{
		{"S3 only, and the staging folder cannot be used", func(e *console.ServerEntry, sup *baselineSupervisor) {
			e.BaselineDir, e.BaselineS3 = "", "s3://bucket/prefix"
			blocker := filepath.Join(t.TempDir(), "staging-is-a-file")
			if err := os.WriteFile(blocker, nil, 0o644); err != nil {
				t.Fatal(err)
			}
			sup.stagingDir = blocker
		}, console.BackupWhyNoStagingPrefix, "no_staging"},
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

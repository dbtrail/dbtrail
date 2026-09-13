package console

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

func TestBackupSchedule_Parse(t *testing.T) {
	cases := []struct {
		name    string
		in      BackupSchedule
		want    ParsedBackupSchedule
		wantErr string
	}{
		{"daily at 03:00", BackupSchedule{Every: "1d", At: "03:00"},
			ParsedBackupSchedule{Every: 24 * time.Hour, At: 3 * time.Hour}, ""},
		{"default: midnight", BackupSchedule{Every: "6h"},
			ParsedBackupSchedule{Every: 6 * time.Hour}, ""},
		{"one-digit hour", BackupSchedule{Every: "30m", At: "9:15"},
			ParsedBackupSchedule{Every: 30 * time.Minute, At: 9*time.Hour + 15*time.Minute}, ""},
		{"whitespace tolerated", BackupSchedule{Every: " 1d ", At: " 03:00 "},
			ParsedBackupSchedule{Every: 24 * time.Hour, At: 3 * time.Hour}, ""},
		{"floor", BackupSchedule{Every: "4m"}, ParsedBackupSchedule{}, "too often"},
		{"exactly the floor is fine", BackupSchedule{Every: "5m"},
			ParsedBackupSchedule{Every: 5 * time.Minute}, ""},
		{"no unit", BackupSchedule{Every: "6"}, ParsedBackupSchedule{}, "every:"},
		{"seconds are not a unit", BackupSchedule{Every: "900s"}, ParsedBackupSchedule{}, "every:"},
		{"empty every", BackupSchedule{}, ParsedBackupSchedule{}, "every:"},
		{"bad clock", BackupSchedule{Every: "1d", At: "25:00"}, ParsedBackupSchedule{}, "at:"},
		{"clock without minutes", BackupSchedule{Every: "1d", At: "3"}, ParsedBackupSchedule{}, "at:"},
		{"clock with seconds", BackupSchedule{Every: "1d", At: "03:00:00"}, ParsedBackupSchedule{}, "at:"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := c.in.Parse()
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want one containing %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Fatalf("got %+v, want %+v", got, c.want)
			}
		})
	}
}

// The refusal names the floor as an operator would type it; the text and the
// constant must agree or the message lies about what is accepted.
func TestBackupSchedule_minEveryTextMatchesConstant(t *testing.T) {
	p, err := (BackupSchedule{Every: backupScheduleMinEveryText}).Parse()
	if err != nil {
		t.Fatalf("the floor text %q does not parse as an accepted interval: %v", backupScheduleMinEveryText, err)
	}
	if p.Every != BackupScheduleMinEvery {
		t.Fatalf("floor text %q = %s, constant = %s", backupScheduleMinEveryText, p.Every, BackupScheduleMinEvery)
	}
}

func TestBackupSchedule_Normalized(t *testing.T) {
	got, err := (BackupSchedule{Every: " 6h ", Extra: map[string]any{"future": 1}}).Normalized()
	if err != nil {
		t.Fatal(err)
	}
	if got.Every != "6h" || got.At != "00:00" {
		t.Fatalf("defaults were not spelled out: %+v", got)
	}
	if got.Extra["future"] != 1 {
		t.Fatal("Normalized dropped the forward-compat catch-all")
	}
	if _, err := (BackupSchedule{Every: "1m"}).Normalized(); err == nil {
		t.Fatal("Normalized accepted what Parse refuses")
	}
}

// Identity is what the loop keys its observations by: equal for the same
// schedule however it was spelled, different for any edit.
func TestBackupSchedule_Identity(t *testing.T) {
	a := BackupSchedule{Every: "1d", At: "03:00"}
	if a.Identity() != (BackupSchedule{Every: " 1d ", At: "3:00"}).Identity() {
		t.Fatal("the same schedule spelled differently has a different identity")
	}
	for _, edited := range []BackupSchedule{{Every: "6h", At: "03:00"}, {Every: "1d", At: "04:00"}} {
		if edited.Identity() == a.Identity() {
			t.Fatalf("an edit (%+v) kept the identity", edited)
		}
	}
	if (BackupSchedule{Every: "soon"}).Identity() == (BackupSchedule{Every: "later"}).Identity() {
		t.Fatal("unparseable schedules collapsed to one identity")
	}
}

func TestParsedBackupSchedule_slots(t *testing.T) {
	at := func(s string) time.Time {
		v, err := time.Parse("2006-01-02 15:04:05", s)
		if err != nil {
			t.Fatal(err)
		}
		return v.UTC()
	}
	cases := []struct {
		name       string
		sched      BackupSchedule
		now        string
		wantBefore string
		wantNext   string
	}{
		{"daily 03:00, before today's slot", BackupSchedule{Every: "1d", At: "03:00"},
			"2026-08-28 02:59:59", "2026-08-27 03:00:00", "2026-08-28 03:00:00"},
		{"daily 03:00, exactly on the slot", BackupSchedule{Every: "1d", At: "03:00"},
			"2026-08-28 03:00:00", "2026-08-28 03:00:00", "2026-08-29 03:00:00"},
		{"daily 03:00, after", BackupSchedule{Every: "1d", At: "03:00"},
			"2026-08-28 11:43:10", "2026-08-28 03:00:00", "2026-08-29 03:00:00"},
		{"every 6h aligned to 03:00", BackupSchedule{Every: "6h", At: "03:00"},
			"2026-08-28 11:43:10", "2026-08-28 09:00:00", "2026-08-28 15:00:00"},
		{"every 6h at midnight", BackupSchedule{Every: "6h"},
			"2026-08-28 23:59:59", "2026-08-28 18:00:00", "2026-08-29 00:00:00"},
		{"every 15m aligned to :05", BackupSchedule{Every: "15m", At: "00:05"},
			"2026-08-28 11:43:10", "2026-08-28 11:35:00", "2026-08-28 11:50:00"},
		// 7d from the epoch (a Thursday): the grid is fixed, not "a week from
		// when it was saved".
		{"weekly", BackupSchedule{Every: "7d", At: "03:00"},
			"2026-08-28 11:43:10", "2026-08-27 03:00:00", "2026-09-03 03:00:00"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := c.sched.Parse()
			if err != nil {
				t.Fatal(err)
			}
			now := at(c.now)
			if got := p.SlotAtOrBefore(now); !got.Equal(at(c.wantBefore)) {
				t.Errorf("SlotAtOrBefore = %s, want %s", got, c.wantBefore)
			}
			if got := p.NextRun(now); !got.Equal(at(c.wantNext)) {
				t.Errorf("NextRun = %s, want %s", got, c.wantNext)
			}
			if got := p.NextRun(now); !got.After(now) {
				t.Errorf("NextRun %s is not after now %s", got, now)
			}
		})
	}
}

// An interval that does not divide a day still sits on the fixed grid: the
// slot is never after now, the next run is exactly one interval later, and
// the clock time drifts day to day (which the docs say it does).
func TestParsedBackupSchedule_intervalThatDoesNotDivideADay(t *testing.T) {
	p, _ := (BackupSchedule{Every: "5h", At: "03:00"}).Parse()
	now := time.Date(2026, 8, 28, 11, 43, 10, 0, time.UTC)
	slot := p.SlotAtOrBefore(now)
	if slot.After(now) || now.Sub(slot) >= 5*time.Hour {
		t.Fatalf("slot %s is not the one in progress at %s", slot, now)
	}
	if p.NextRun(now).Sub(slot) != 5*time.Hour {
		t.Fatalf("next run %s is not one interval after %s", p.NextRun(now), slot)
	}
	a := p.SlotAtOrBefore(now).Hour()
	b := p.SlotAtOrBefore(now.Add(24 * time.Hour)).Hour()
	if a == b {
		t.Fatalf("a 5h grid did not drift across a day: %d == %d", a, b)
	}
}

// The slot grid must be stable in time: the slot at or before an instant
// never depends on which instant it was asked from, only on the grid.
func TestParsedBackupSchedule_gridIsFixed(t *testing.T) {
	p, _ := (BackupSchedule{Every: "6h", At: "03:00"}).Parse()
	base := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	for _, off := range []time.Duration{0, time.Second, time.Hour, 5*time.Hour + 59*time.Minute} {
		if got := p.SlotAtOrBefore(base.Add(off)); !got.Equal(base) {
			t.Fatalf("at +%s the slot moved to %s", off, got)
		}
	}
	if got := p.SlotAtOrBefore(base.Add(6 * time.Hour)); !got.Equal(base.Add(6 * time.Hour)) {
		t.Fatalf("the next slot did not arrive on time: %s", got)
	}
}

// Runnable means at least one producer can run here. The reason, when
// neither can, names both.
func TestCheckBackupSchedule(t *testing.T) {
	sched := BackupSchedule{Every: "1d"}
	ready := ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: "/b"}
	live := BackupScheduleGates{LoopRunning: true, FullBackups: true}
	cases := []struct {
		name    string
		e       ServerEntry
		gates   BackupScheduleGates
		wantErr string // "" = runnable
	}{
		{"everything on", ready, live, ""},
		{"read-only console", ready, BackupScheduleGates{ReadOnlyConsole: true}, "watch daemon"},
		{"watch without any baseline feature", ready, BackupScheduleGates{}, "BINTRAIL_CONSOLE_BASELINE_TRIGGER is not set to 1 and no refresh interval is set (CLI: --baseline-refresh-interval)"},
		{"creation off but a rebuild is possible", ready, BackupScheduleGates{LoopRunning: true}, ""},
		{"creation off and no local dir", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineS3: "s3://b/"}, BackupScheduleGates{LoopRunning: true}, "BINTRAIL_CONSOLE_BASELINE_TRIGGER is not set to 1); an update from the recorded changes needs a local backup directory"},
		{"lock mode misconfigured but a rebuild is possible", ready, BackupScheduleGates{LoopRunning: true, FullBackups: true, FullBackupsErr: "bad lock mode"}, ""},
		{"lock mode misconfigured, S3-only", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineS3: "s3://b/"}, BackupScheduleGates{LoopRunning: true, FullBackups: true, FullBackupsErr: "bad lock mode"}, "bad lock mode; an update from the recorded changes needs a local backup directory"},
		// S3 AND a local dir: since #1539 the rebuild IS a candidate producer
		// there (it reads the bucket, writes the local directory, uploads),
		// so the creation opt-in being off no longer makes this a timer
		// nothing would honour. Saved, not refused.
		{"creation off, S3 and a local dir", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: "/b", BaselineS3: "s3://b/"}, BackupScheduleGates{LoopRunning: true}, ""},
		{"postgres ignores the lock mode", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineS3: "s3://b/", Flavor: FlavorPostgres, SourceSlot: "s", SourcePublication: "p"},
			BackupScheduleGates{LoopRunning: true, FullBackups: true, FullBackupsErr: "bad lock mode"}, ""},
		{"no source, local dir: rebuild only, fine", ServerEntry{DSN: "idx", BaselineDir: "/b"}, live, ""},
		{"no source, no destination", ServerEntry{DSN: "idx"}, live, "no source configured"},
		{"no index at all", ServerEntry{SourceDSN: "src"}, live, "no baseline location"},
		{"unparseable schedule", ready, live, "every:"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := sched
			if c.name == "unparseable schedule" {
				s = BackupSchedule{Every: "1x"}
			}
			err := CheckBackupSchedule(c.e, s, c.gates)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("accepted, want a refusal")
			}
			if !errors.Is(err, ErrBackupScheduleNotRunnable) {
				t.Fatalf("refusal is not classed: %v", err)
			}
			if !strings.Contains(RefusalReason(err), c.wantErr) {
				t.Fatalf("reason %q does not mention %q", RefusalReason(err), c.wantErr)
			}
			if strings.HasPrefix(RefusalReason(err), ErrBackupScheduleNotRunnable.Error()) {
				t.Fatalf("RefusalReason returned the class prefix too: %q", RefusalReason(err))
			}
		})
	}
}

// fakeSnapshot writes the shape NewestSnapshotTables recognises: one
// timestamped directory with one schema and one table file.
func fakeSnapshot(t *testing.T, dir string) {
	t.Helper()
	d := filepath.Join(dir, reconstruct.SnapshotDirName(time.Date(2026, 8, 27, 3, 0, 0, 0, time.UTC)), "shop")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "orders.parquet"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

// How the next run is made is the daemon's decision, and the rule has to be
// the one the docs state: no local backup directory means full, no previous
// backup means full, otherwise rebuild; a rule that lands on a producer this
// daemon cannot run says so.
func TestChooseBackupMethod(t *testing.T) {
	withSnap := t.TempDir()
	fakeSnapshot(t, withSnap)
	empty := t.TempDir()
	live := BackupScheduleGates{LoopRunning: true, FullBackups: true}
	off := BackupScheduleGates{LoopRunning: true}
	cases := []struct {
		name       string
		e          ServerEntry
		gates      BackupScheduleGates
		wantMethod string
		wantWhy    string
		wantErr    string
		s3Has      []string // the newest snapshot in the bucket, when there is one
		s3Err      error    // what listing the bucket answers with, when it fails
		wantProbe  string   // where the previous backup had to be looked for
	}{
		{"local dir with a backup: rebuild", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: withSnap}, live, BackupMethodRefresh, "no load on your database", "", nil, nil, ""},
		{"local dir with a backup, creation off: still rebuild", ServerEntry{DSN: "idx", BaselineDir: withSnap}, off, BackupMethodRefresh, "no load", "", nil, nil, ""},
		{"local dir, no backup yet: full", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: empty}, live, BackupMethodFull, "no previous backup", "", nil, nil, ""},
		// The directory the first full backup has not created yet is the
		// same case, not an unreadable one.
		{"local dir that does not exist yet: full", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: filepath.Join(empty, "not-yet")}, live, BackupMethodFull, "no previous backup", "", nil, nil, ""},
		{"local dir, no backup yet, creation off: nothing can run", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: empty}, off, BackupMethodFull, "", "no previous backup to update", nil, nil, ""},
		// S3 with no local directory is the one shape that still forces a
		// full backup, and the why names the setting that unlocks the cheap
		// path rather than the destination the operator cannot change.
		{"S3 destination, no local dir: full", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineS3: "s3://b/"}, live, BackupMethodFull, "needs a local backup directory", "", nil, nil, ""},
		// #1539: the previous backup is looked for in the BUCKET, so an
		// S3-backed server whose local directory is empty (every backup it
		// has was uploaded) still rebuilds. Under the old rule this was a
		// full dump of production every slot.
		{"S3 and a local dir, a backup in the bucket: rebuild", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: empty, BaselineS3: "s3://b/"}, live, BackupMethodRefresh, "no load on your database", "", []string{"app.orders"}, nil, "s3://b/"},
		// The mirror: a local snapshot does NOT stand in for an empty
		// bucket. Folding the local one would publish an update of a
		// backup no reader of the bucket has ever seen.
		{"S3 and a local dir, nothing in the bucket yet: full", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: withSnap, BaselineS3: "s3://b/"}, live, BackupMethodFull, "no previous backup", "", nil, nil, "s3://b/"},
		{"S3 destination, creation off, no local dir: nothing can run", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineS3: "s3://b/"}, off, BackupMethodFull, "", "needs a local backup directory", nil, nil, ""},
		// A bucket that will not answer must not cost the slot: before #1539
		// these servers were guaranteed a full backup without touching the
		// network, and a throttled listing that skipped the night would be a
		// worse trade than an expensive backup.
		{"S3 listing fails, a full backup is possible: full, with the real cause", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: empty, BaselineS3: "s3://b/"}, live, BackupMethodFull, "could not be read from the backup destination", "", nil, errors.New("throttled"), "s3://b/"},
		{"S3 listing fails and no full backup is possible: the slot is refused", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: empty, BaselineS3: "s3://b/"}, off, BackupMethodFull, "", "could not be read", nil, errors.New("throttled"), "s3://b/"},
		{"S3 and a local dir, creation off, empty bucket: nothing can run", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: empty, BaselineS3: "s3://b/"}, off, BackupMethodFull, "", "no previous backup to update under s3://b/", nil, nil, "s3://b/"},
		{"no destination at all: nothing can run", ServerEntry{DSN: "idx", SourceDSN: "src"}, live, BackupMethodFull, "", "no baseline location", nil, nil, ""},
		{"lock mode misconfigured with a backup on disk: rebuild", ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: withSnap}, BackupScheduleGates{LoopRunning: true, FullBackups: true, FullBackupsErr: "bad lock"}, BackupMethodRefresh, "no load", "", nil, nil, ""},
	}
	real := newestSnapshotTables
	t.Cleanup(func() { newestSnapshotTables = real })
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Only the bucket is stubbed; a local source still runs the real
			// listing, so the cases that pin the on-disk behaviour keep
			// exercising it. The probed source is recorded because WHERE the
			// previous backup is looked for is the whole of #1539: a stub
			// that answered for any source would let a case pass while the
			// decision read the wrong place.
			var probed string
			newestSnapshotTables = func(ctx context.Context, src string) ([]string, error) {
				if !strings.HasPrefix(src, "s3://") {
					return real(ctx, src)
				}
				probed = src
				return c.s3Has, c.s3Err
			}
			method, why, err := ChooseBackupMethod(context.Background(), c.e, c.gates)
			if probed != c.wantProbe {
				t.Fatalf("looked for the previous backup in %q, want %q", probed, c.wantProbe)
			}
			if method != c.wantMethod {
				t.Fatalf("method = %q, want %q (why=%q err=%v)", method, c.wantMethod, why, err)
			}
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want one containing %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if !strings.Contains(why, c.wantWhy) {
				t.Fatalf("why = %q, want it to mention %q", why, c.wantWhy)
			}
		})
	}
}

// An unreadable backup location is its own verdict, never "no backup yet":
// calling it absent would turn the no-load rebuild into a nightly full read
// of production while the page named a false reason.
//
// A LOCAL one still refuses, and the scope matters: an unreadable directory is
// persistent, and the full backup that would stand in writes into that same
// directory, so degrading would trade a precise alarm for an expensive dump
// that fails on the way out. Only a REMOTE source degrades (#1539), because a
// bucket error is usually transient and those servers were guaranteed a full
// backup before this change without touching the network.
func TestChooseBackupMethod_unreadableDirIsNotNoBackup(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	if os.Getuid() == 0 {
		t.Skip("root reads a mode-000 directory")
	}
	_, why, err := ChooseBackupMethod(context.Background(), ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: dir},
		BackupScheduleGates{LoopRunning: true, FullBackups: true})
	if err == nil || !strings.Contains(err.Error(), "could not be read") || !strings.Contains(err.Error(), dir) {
		t.Fatalf("err = %v (why=%q), want the unreadable directory named", err, why)
	}
	if strings.Contains(err.Error(), "no previous backup") {
		t.Fatalf("err = %v, want the real cause, not the absent-backup one", err)
	}
}

// Same verdict for a path that is a FILE (ENOTDIR): this one runs as root
// too, where a mode-000 directory reads fine and the test above skips.
func TestChooseBackupMethod_fileAsDirIsNotNoBackup(t *testing.T) {
	file := filepath.Join(t.TempDir(), "backups")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, err := ChooseBackupMethod(context.Background(), ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: file},
		BackupScheduleGates{LoopRunning: true, FullBackups: true})
	if err == nil || !strings.Contains(err.Error(), "could not be read") {
		t.Fatalf("err = %v, want the unreadable path named", err)
	}
}

// The precheck the Create backup button runs is the SAME function the
// schedule checker runs, so the two cannot accept different servers.
func TestBaselineTriggerPrecheck_sharedWithTheSchedule(t *testing.T) {
	e := ServerEntry{DSN: "idx", SourceDSN: "src"}
	want := baselineTriggerPrecheck(e)
	if want == nil {
		t.Fatal("fixture is runnable; the test needs a refused entry")
	}
	got := CheckBackupSchedule(e, BackupSchedule{Every: "1d"}, BackupScheduleGates{LoopRunning: true, FullBackups: true})
	// The button's hint "(Backup settings page)" moves to the end of the combined
	// reason and appears once, not once per producer.
	const hint = " (Backup settings page)"
	reason := RefusalReason(got)
	if got == nil || !strings.HasPrefix(reason, strings.TrimSuffix(want.Error(), hint)) {
		t.Fatalf("schedule reason %v does not start with the button's %v", got, want)
	}
	if !strings.HasSuffix(reason, hint) || strings.Count(reason, hint) != 1 {
		t.Fatalf("reason %q should carry the edit hint exactly once, at the end", reason)
	}
}

func TestBaselineRunHistory_scheduledRunsAndSkips(t *testing.T) {
	h, err := OpenBaselineHistory(t.TempDir() + "/h.json")
	if err != nil {
		t.Fatal(err)
	}
	if run, skip := h.LastScheduled("a"); run != nil || skip != nil {
		t.Fatal("an empty history reported something")
	}
	// A manual run is never reported as the schedule's.
	if err := h.Append(BaselineRunRecord{ServerID: "a", Kind: BaselineRunDump, StartedAt: "t1", FinishedAt: "t1"}); err != nil {
		t.Fatal(err)
	}
	if run, _ := h.LastScheduled("a"); run != nil {
		t.Fatalf("a manual run was attributed to the schedule: %+v", run)
	}
	if err := h.Append(BaselineRunRecord{ServerID: "a", Kind: BaselineRunDump, Trigger: BaselineRunTriggerScheduled,
		StartedAt: "t2", FinishedAt: "t2", SnapshotTime: "s2"}); err != nil {
		t.Fatal(err)
	}
	wrote, err := h.AppendSkip(BaselineRunRecord{ServerID: "a", Kind: BaselineRunDump, SkipReason: "busy", StartedAt: "t3", FinishedAt: "t3"})
	if err != nil || !wrote {
		t.Fatalf("first skip: wrote=%v err=%v", wrote, err)
	}
	// The same skip again is folded into the existing record, so a wedged
	// server cannot evict the real runs from the capped history, but its
	// timestamp moves to the latest slot so the page does not report the
	// first missed slot of a streak as the last.
	wrote, err = h.AppendSkip(BaselineRunRecord{ServerID: "a", Kind: BaselineRunDump, SkipReason: "busy", StartedAt: "t4", FinishedAt: "t4"})
	if err != nil || wrote {
		t.Fatalf("repeated skip: wrote=%v err=%v, want folded", wrote, err)
	}
	if _, skip := h.LastScheduled("a"); skip == nil || skip.FinishedAt != "t4" || len(h.List("a")) != 3 {
		t.Fatalf("folded skip = %+v over %d records, want the timestamp moved to t4 and no new record", skip, len(h.List("a")))
	}
	// A different reason is a new fact.
	wrote, _ = h.AppendSkip(BaselineRunRecord{ServerID: "a", Kind: BaselineRunDump, SkipReason: "off", StartedAt: "t5", FinishedAt: "t5"})
	if !wrote {
		t.Fatal("a skip with a new reason was folded into the old one")
	}
	run, skip := h.LastScheduled("a")
	if run == nil || run.SnapshotTime != "s2" {
		t.Fatalf("last scheduled run = %+v, want the t2 run", run)
	}
	if skip == nil || skip.SkipReason != "off" || skip.Trigger != BaselineRunTriggerScheduled {
		t.Fatalf("last skip = %+v, want the t5 skip, stamped scheduled", skip)
	}
	// An empty reason never folds into a run record.
	if _, err := h.AppendSkip(BaselineRunRecord{ServerID: "b", Kind: BaselineRunDump, Trigger: BaselineRunTriggerScheduled, StartedAt: "r", FinishedAt: "r"}); err != nil {
		t.Fatal(err)
	}
	// Skips never join a snapshot: they have none.
	if rec := h.FindBySnapshot("a", ""); rec != nil {
		t.Fatalf("a skip joined a snapshot: %+v", rec)
	}
	// And all of it survives a reload.
	h2, err := OpenBaselineHistory(h.path)
	if err != nil {
		t.Fatal(err)
	}
	if run, skip := h2.LastScheduled("a"); run == nil || skip == nil {
		t.Fatal("the scheduled records did not survive a reload")
	}
}

// The cap keeps the newest scheduled run and skip whatever their age: the
// daemon-wide refresh loop appends a record per cycle for the same server,
// and at 30m that is the whole cap in twenty hours, which used to evict a
// daily schedule's last run before its next one fired.
func TestBaselineRunHistory_capKeepsTheScheduleEvidence(t *testing.T) {
	h, err := OpenBaselineHistory(t.TempDir() + "/h.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Append(BaselineRunRecord{ServerID: "a", Kind: BaselineRunDump, Trigger: BaselineRunTriggerScheduled,
		StartedAt: "s-run", FinishedAt: "s-run", SnapshotTime: "snap"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.AppendSkip(BaselineRunRecord{ServerID: "a", Kind: BaselineRunDump, SkipReason: "busy", StartedAt: "s-skip", FinishedAt: "s-skip"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3*BaselineRunHistoryCap; i++ {
		if err := h.Append(BaselineRunRecord{ServerID: "a", Kind: BaselineRunRefresh, StartedAt: "r", FinishedAt: "r"}); err != nil {
			t.Fatal(err)
		}
	}
	recs := h.List("a")
	if len(recs) != BaselineRunHistoryCap {
		t.Fatalf("%d records, want the cap %d", len(recs), BaselineRunHistoryCap)
	}
	run, skip := h.LastScheduled("a")
	if run == nil || run.SnapshotTime != "snap" || skip == nil || skip.SkipReason != "busy" {
		t.Fatalf("the schedule's evidence was evicted: run=%+v skip=%+v", run, skip)
	}
	if recs[0].Trigger != BaselineRunTriggerScheduled || recs[1].Trigger != BaselineRunTriggerScheduled {
		t.Fatalf("protected records lost their place: %+v %+v", recs[0], recs[1])
	}
	if err := h.Append(BaselineRunRecord{ServerID: "a", Kind: BaselineRunDump, Trigger: BaselineRunTriggerScheduled,
		StartedAt: "s-run-2", FinishedAt: "s-run-2", SnapshotTime: "snap2"}); err != nil {
		t.Fatal(err)
	}
	if run, _ := h.LastScheduled("a"); run.SnapshotTime != "snap2" {
		t.Fatalf("run = %+v, want the newer scheduled run", run)
	}
	for _, r := range h.List("a") {
		if r.SnapshotTime == "snap" {
			t.Fatal("the superseded scheduled run was kept past the cap")
		}
	}
	if n := len(h.List("a")); n != BaselineRunHistoryCap {
		t.Fatalf("%d records after the newer run, want the cap", n)
	}
}

package consoleapp

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
)

func liveRegistry(t *testing.T) *console.Registry {
	t.Helper()
	reg, err := console.LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func save(t *testing.T, reg *console.Registry, key, value string) {
	t.Helper()
	if err := reg.SetBackupSetting(key, &value); err != nil {
		t.Fatal(err)
	}
}

// The point of the whole change: a value saved from the interface is what the
// next job uses, without a restart.
func TestEffectiveLockMode_savedWinsOverTheStartupValue(t *testing.T) {
	reg := liveRegistry(t)
	if got, err := effectiveLockMode(reg, baseline.DefaultLockMode, nil); got != baseline.DefaultLockMode || err != nil {
		t.Fatalf("with nothing saved: %v/%v, want the startup value", got, err)
	}
	save(t, reg, console.BackupSettingLockMode, "lock-all")
	got, err := effectiveLockMode(reg, baseline.DefaultLockMode, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got == baseline.DefaultLockMode {
		t.Errorf("lock mode = %v: the saved value did not win, so the page would report a "+
			"change the next dump ignores", got)
	}
}

// The refusal an operator most needs to clear from a browser: the daemon
// booted with a lock mode its environment got wrong, and every MySQL dump is
// refused until it is fixed — which used to mean a restart.
func TestEffectiveLockMode_aSavedValueClearsTheBootRefusal(t *testing.T) {
	reg := liveRegistry(t)
	bootErr := errors.New("unknown lock mode \"lock-everything\"")

	if _, err := effectiveLockMode(reg, baseline.DefaultLockMode, bootErr); err == nil {
		t.Fatal("the boot refusal vanished with nothing saved")
	}
	save(t, reg, console.BackupSettingLockMode, "lock-all")
	if _, err := effectiveLockMode(reg, baseline.DefaultLockMode, bootErr); err != nil {
		t.Errorf("err = %v: saving a readable lock mode must clear the boot refusal, or the "+
			"setting is editable and useless", err)
	}
}

// A file edited by hand into something unreadable must not stop backups: it
// falls back to what the process started with, and says so.
func TestEffectiveLockMode_unreadableSavedValueFallsBack(t *testing.T) {
	reg := liveRegistry(t)
	// Bypasses the endpoint's validation the way a hand edit would.
	bad := "lock-everything"
	if err := reg.SetBackupSetting(console.BackupSettingLockMode, &bad); err != nil {
		t.Fatal(err)
	}
	got, err := effectiveLockMode(reg, baseline.LockModeNoLock, nil)
	if got != baseline.LockModeNoLock || err != nil {
		t.Errorf("%v/%v, want the startup value and no error", got, err)
	}
}

// Saved-empty is a real answer: stop pruning. It must not read as "nothing
// saved", which would hand the setting back to the flag that turned it on.
func TestEffectiveRetain_savedEmptyStopsPruning(t *testing.T) {
	reg := liveRegistry(t)
	if d, on := effectiveRetain(reg, "7d"); !on || d == 0 {
		t.Fatalf("startup value not in force: %v/%v", d, on)
	}
	save(t, reg, console.BackupSettingBaselineRetain, "")
	if d, on := effectiveRetain(reg, "7d"); on {
		t.Errorf("retention still on (%v) after the operator cleared it — the flag came back", d)
	}
	save(t, reg, console.BackupSettingBaselineRetain, "2d")
	if d, on := effectiveRetain(reg, "7d"); !on || d.Hours() != 48 {
		t.Errorf("retention = %v/%v, want 48h from the saved value", d, on)
	}
}

func TestEffectiveVerifyTables_savedWins(t *testing.T) {
	reg := liveRegistry(t)
	if got := effectiveVerifyTables(reg, "shop.orders"); !slices.Equal(got, []string{"shop.orders"}) {
		t.Fatalf("startup filter = %v", got)
	}
	save(t, reg, console.BackupSettingVerifyTables, "shop.orders, shop.items")
	if got := effectiveVerifyTables(reg, "shop.orders"); !slices.Equal(got, []string{"shop.orders", "shop.items"}) {
		t.Errorf("filter = %v, want both saved tables", got)
	}
	save(t, reg, console.BackupSettingVerifyTables, "")
	if got := effectiveVerifyTables(reg, "shop.orders"); len(got) != 0 {
		t.Errorf("filter = %v, want none: an empty list means every table", got)
	}
}

// The honesty guard. A row may only be reported as live when the loop that
// would pick it up is actually running in THIS process.
func TestBackupSettingsLive_namesOnlyWhatRuns(t *testing.T) {
	if got := backupSettingsLive(false, false, false); len(got) != 0 {
		t.Errorf("a daemon running none of the loops reports %v as live", got)
	}
	got := backupSettingsLive(true, true, true)
	for _, key := range []string{
		console.BackupSettingBaselineRetain,
		console.BackupSettingVerifyTables,
		console.BackupSettingLockMode,
	} {
		if !slices.Contains(got, key) {
			t.Errorf("%s missing from %v with every loop running", key, got)
		}
	}
	if slices.Contains(backupSettingsLive(true, true, true), console.BackupSettingStagingDir) {
		t.Error("the staging folder is reported live: it is read once at boot (the startup " +
			"sweep), and a page that says otherwise sends the operator to change a setting " +
			"nothing will read until a restart")
	}
}

// The supervisor reads the saved mode per job rather than the one it was
// built with — the wiring, not just the function.
func TestSupervisorLockModeNow_readsTheSavedValue(t *testing.T) {
	reg := liveRegistry(t)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.reg = reg

	save(t, reg, console.BackupSettingLockMode, "lock-all")
	got, err := sup.lockModeNow()
	if err != nil {
		t.Fatal(err)
	}
	if got == baseline.DefaultLockMode {
		t.Errorf("the supervisor still uses %v: it was built with the boot value and never "+
			"re-reads, so the setting is inert", got)
	}
}

// The staging folder resolves through the saved value at boot, which is what
// lets it be set from the interface at all.
func TestEffectiveStagingDir(t *testing.T) {
	reg := liveRegistry(t)
	if got := effectiveStagingDir(reg, "/boot"); got != "/boot" {
		t.Fatalf("got %q with nothing saved", got)
	}
	save(t, reg, console.BackupSettingStagingDir, "/data/staging")
	if got := effectiveStagingDir(reg, "/boot"); got != "/data/staging" {
		t.Errorf("got %q, want the saved folder", got)
	}
}

// The wiring, not the function: a retention saved while the daemon is running
// must bound the NEXT sweep. The loop used to capture a parsed duration when
// its goroutine started, which is the exact shape that makes a setting inert.
func TestBaselinePruneSweep_readsTheSavedRetentionEachCycle(t *testing.T) {
	reg := liveRegistry(t)
	dir := t.TempDir()
	if _, err := reg.Add(console.ServerEntry{
		Name: "prod", DSN: "u:p@tcp(127.0.0.1:3306)/idx",
		BaselineDir: dir, BaselineS3: "s3://bucket/prefix/",
	}); err != nil {
		t.Fatal(err)
	}

	var seen []time.Duration
	prune := func(_ context.Context, opts baseline.PruneOptions) (baseline.PruneResult, error) {
		seen = append(seen, opts.Retain)
		return baseline.PruneResult{}, nil
	}

	baselinePruneSweep(context.Background(), reg, "", "", "30d", prune)
	save(t, reg, console.BackupSettingBaselineRetain, "2d")
	baselinePruneSweep(context.Background(), reg, "", "", "30d", prune)

	if len(seen) != 2 {
		t.Fatalf("pruned %d times, want 2: %v", len(seen), seen)
	}
	if seen[0].Hours() != 720 {
		t.Errorf("first sweep used %v, want the startup 30d", seen[0])
	}
	if seen[1].Hours() != 48 {
		t.Errorf("second sweep used %v, want the 2d saved between sweeps — the retention is "+
			"being captured instead of re-read, so the setting is inert until a restart", seen[1])
	}

	// And an emptied setting stops pruning entirely on the next sweep.
	save(t, reg, console.BackupSettingBaselineRetain, "")
	baselinePruneSweep(context.Background(), reg, "", "", "30d", prune)
	if len(seen) != 2 {
		t.Errorf("a sweep ran after the operator cleared the retention: %v", seen)
	}
}

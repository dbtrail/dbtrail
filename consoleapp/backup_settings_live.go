package consoleapp

import (
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/console"
)

// The daemon half of #1682: the settings the interface can save are resolved
// HERE, at the moment they are used, instead of being captured at boot.
//
// The pattern is rotationSettingsProvider's, and for the same reason: a value
// read once at startup makes the page inert — it says the setting changed and
// every consumer keeps using the flag. A saved value wins; nothing saved means
// the flag or environment variable is in force; a saved value this build
// cannot parse falls back with a warning rather than disabling the behaviour,
// because a bad hand-edit of the file must not silently stop backups.
//
// Only the values a running loop can pick up are resolved live. The ones that
// decide whether a goroutine exists at all stay restart-only and SAY SO per
// row — see backupSettingsLive below, which is what the page reports.
func savedBackupSetting(reg *console.Registry, key string) (string, bool) {
	if reg == nil {
		return "", false
	}
	return reg.BackupSettings().Get(key)
}

// lockModeSource is where a dump's lock mode came from (#1986). It decides
// whether the automatic retry may run (only for lockModeAutomatic) and which
// remedy a refusal names: the environment variable, or the saved setting that
// wins over it.
type lockModeSource int

const (
	lockModeAutomatic lockModeSource = iota // nothing chosen: picked from the source host
	lockModeFromEnv                         // BINTRAIL_CONSOLE_BASELINE_LOCK_MODE
	lockModeSaved                           // saved in the settings file (backup_settings.lock_mode)
)

func (s lockModeSource) chosen() bool { return s != lockModeAutomatic }

// effectiveLockMode resolves the lock mode an OPERATOR chose, per dump job,
// and says where it came from (#1986). A saved value wins; nothing saved means
// the environment variable, which counts as a choice only when it was set
// (bootChosen). Nothing chosen is the automatic mode: the caller then picks
// from the source host (lockModeFor), and the mode returned here is the
// default it falls back to.
//
// The boot error rides along: with no saved value, a lock mode the
// environment got wrong still refuses dumps exactly as before, and a SAVED
// value clears that refusal, which is the point (an operator who cannot
// restart the daemon can fix the typo from the browser).
func effectiveLockMode(reg *console.Registry, bootMode baseline.LockMode, bootChosen bool, bootErr error) (baseline.LockMode, lockModeSource, error) {
	boot := lockModeAutomatic
	if bootChosen {
		boot = lockModeFromEnv
	}
	raw, ok := savedBackupSetting(reg, console.BackupSettingLockMode)
	if !ok {
		return bootMode, boot, bootErr
	}
	if raw == "" {
		// Saved empty means "automatic", not "the environment's value": the
		// operator cleared the setting, and automatic is what an unset
		// environment gives. Before #1986 this was the built-in ftwrl, which
		// automatic still is on a host that is not RDS or Aurora.
		return baseline.DefaultLockMode, lockModeAutomatic, nil
	}
	mode, err := baseline.ParseLockMode(raw)
	if err != nil {
		slog.Warn("snapshot settings: ignoring an unreadable saved lock mode; using the value this process started with",
			"saved", raw, "error", err)
		return bootMode, boot, bootErr
	}
	return mode, lockModeSaved, nil
}

// effectiveVerifyTables resolves the verification filter, per cycle.
func effectiveVerifyTables(reg *console.Registry, boot string) []string {
	if raw, ok := savedBackupSetting(reg, console.BackupSettingVerifyTables); ok {
		return console.SplitVerifyTables(raw)
	}
	return console.SplitVerifyTables(boot)
}

// effectiveRetain resolves how long local snapshots are kept, per prune cycle.
// The second return says whether pruning runs at all: empty is a real answer
// (keep everything), and it is why the caller cannot treat a zero duration as
// "unset".
func effectiveRetain(reg *console.Registry, boot string) (time.Duration, bool) {
	raw, saved := savedBackupSetting(reg, console.BackupSettingBaselineRetain)
	if !saved {
		raw = boot
	}
	if raw == "" {
		return 0, false
	}
	d, err := cliutil.ParseRetain(raw)
	if err != nil {
		slog.Warn("snapshot settings: ignoring an unreadable saved retention; keeping every local snapshot",
			"saved", raw, "error", err)
		return 0, false
	}
	return d, true
}

// effectiveStagingDir resolves where exports are assembled. Read at BOOT, not
// per job, and the row says so: the directory is swept once at startup for
// what a previous process left behind, so switching it while running would
// leave those files with nothing looking at them again.
func effectiveStagingDir(reg *console.Registry, boot string) string {
	if raw, ok := savedBackupSetting(reg, console.BackupSettingStagingDir); ok && raw != "" {
		return raw
	}
	return boot
}

// backupSettingsLive names the keys THIS process applies without a restart.
// It is computed from what actually got wired — a loop that never started
// cannot pick anything up — so the page's "restart to change" chip is a
// statement about this daemon rather than about the feature.
func backupSettingsLive(pruneLoop, verifyLoop, dumps bool) []string {
	var out []string
	if dumps {
		out = append(out, console.BackupSettingLockMode)
	}
	if verifyLoop {
		out = append(out, console.BackupSettingVerifyTables)
	}
	if pruneLoop {
		out = append(out, console.BackupSettingBaselineRetain)
	}
	return out
}

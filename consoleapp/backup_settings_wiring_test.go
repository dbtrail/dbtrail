package consoleapp

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// TestUpConsoleConfig_backupSettingsDefaultsReachTheConsole pins the read path
// of the Backup settings page (#1582): daemon flags and env ->
// console.Config -> the read-only rows. The page's whole promise is that each
// row shows what the daemon was TOLD, verbatim; deleting the
// BackupSettingsDefaults block in upConsoleConfig would leave every row
// rendering "not set" over a fully configured daemon, which reads as an
// unconfigured install to the operator standing in front of it.
func TestUpConsoleConfig_backupSettingsDefaultsReachTheConsole(t *testing.T) {
	const dsn = "user:pass@tcp(127.0.0.1:3306)/binlog_index"
	opts := consoleOpts{Listen: "127.0.0.1:8090", Token: "tok"}

	prevRetain, prevEvery := upConsoleBaselineRetain, upBaselineRefreshEvery
	prevLock, prevTrig := upConsoleBaselineLockMode, upConsoleBaselineTrigger
	prevStage, prevVI, prevVT := upBaselineStageDir, upVerifyInterval, upVerifyTables
	t.Cleanup(func() {
		upConsoleBaselineRetain, upBaselineRefreshEvery = prevRetain, prevEvery
		upConsoleBaselineLockMode, upConsoleBaselineTrigger = prevLock, prevTrig
		upBaselineStageDir, upVerifyInterval, upVerifyTables = prevStage, prevVI, prevVT
	})

	upConsoleBaselineRetain, upBaselineRefreshEvery = "7d", "6h"
	upConsoleBaselineLockMode, upConsoleBaselineTrigger = baseline.LockModeNoLock, true
	upBaselineStageDir, upVerifyInterval, upVerifyTables = "/stage", "24h", "shop.orders"

	cfg, err := upConsoleConfig(nil, dsn, opts)
	if err != nil {
		t.Fatalf("upConsoleConfig: %v", err)
	}
	got := cfg.BackupSettingsDefaults
	want := map[string]struct{ got, want string }{
		"BaselineRetain": {got.BaselineRetain, "7d"},
		"RefreshEvery":   {got.RefreshEvery, "6h"},
		"LockMode":       {got.LockMode, string(baseline.LockModeNoLock)},
		"StagingDir":     {got.StagingDir, "/stage"},
		"VerifyInterval": {got.VerifyInterval, "24h"},
		"VerifyTables":   {got.VerifyTables, "shop.orders"},
	}
	for name, w := range want {
		if w.got != w.want {
			t.Errorf("%s = %q, want %q: the daemon value did not reach the console verbatim", name, w.got, w.want)
		}
	}
	if !got.TriggerOn {
		t.Error("TriggerOn = false, want true")
	}
}

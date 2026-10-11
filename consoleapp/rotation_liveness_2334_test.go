package consoleapp

import (
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/rotation"
)

// #2334: "does this daemon run a rotation loop" has one answer, the one
// rotation.StartLoop takes at startup from the settings provider. The web
// interface is handed that answer, not the --rotate-retain flag.
func TestRotationLoopRuns_matchesWhatStartLoopReads(t *testing.T) {
	newReg := func(t *testing.T) *console.Registry {
		t.Helper()
		reg, err := console.LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		return reg
	}
	saved := func(t *testing.T, interval string) *console.Registry {
		t.Helper()
		reg := newReg(t)
		if err := reg.SetRotation(console.RotationConfig{Retain: "7d", Interval: interval, AddFuture: 3}); err != nil {
			t.Fatal(err)
		}
		return reg
	}
	off := rotation.Settings{}

	for _, tc := range []struct {
		name  string
		flags rotation.Settings
		reg   func(*testing.T) *console.Registry
		want  bool
	}{
		{"flag on, nothing saved", flags48h, newReg, true},
		{"flag off, nothing saved: no loop", off, newReg, false},
		{"flag off, 7d saved: the loop runs on it", off, func(t *testing.T) *console.Registry { return saved(t, "1h") }, true},
		{"flag off, an unusable policy saved: no loop", off, func(t *testing.T) *console.Registry { return saved(t, "1x") }, false},
		{"no registry (tests, serve): the flag", flags48h, func(*testing.T) *console.Registry { return nil }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withRotationGlobals(t, tc.flags, "")
			reg := tc.reg(t)
			got := rotationLoopRuns(reg)
			if got != tc.want {
				t.Fatalf("rotationLoopRuns = %v, want %v", got, tc.want)
			}
			// The loop's own start decision, read the same way.
			if reg != nil {
				if startLoop := rotationSettingsProvider(reg)().Enabled; startLoop != got {
					t.Fatalf("StartLoop would read enabled=%v, the surfaces are told %v", startLoop, got)
				}
			}
			// And the web interface is handed exactly that.
			cfg, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/binlog_index", consoleOpts{Listen: "127.0.0.1:8090", Token: "tok"}, reg)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.RotationDefaults.Enabled != tc.want {
				t.Fatalf("console RotationDefaults.Enabled = %v, want %v", cfg.RotationDefaults.Enabled, tc.want)
			}
		})
	}
}

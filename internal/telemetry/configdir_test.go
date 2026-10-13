package telemetry

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

// TestConfigDirHonoursTheOverride: a container's home directory lives in the
// container's own writable layer, so a stack that recreates the container on
// every upgrade loses the consent file with it and an opt-out reverts to the
// default (on). DirEnvVar is what lets the stack keep it on a volume.
func TestConfigDirHonoursTheOverride(t *testing.T) {
	home := t.TempDir()
	vol := filepath.Join(t.TempDir(), "telemetry")
	homeDefault := filepath.Join(home, ".config", "bintrail")

	for _, tc := range []struct {
		name    string
		value   string
		want    string
		wantErr string
	}{
		{name: "unset falls back to the home directory", value: "", want: homeDefault},
		{name: "spaces only is unset, not a folder named spaces", value: "   ", want: homeDefault},
		{name: "an absolute path is used as given", value: vol, want: vol},
		{name: "surrounding spaces are trimmed", value: "  " + vol + "\t", want: vol},
		{name: "trailing separators are cleaned", value: vol + "//", want: vol},
		{name: "a dot segment is cleaned", value: vol + "/./", want: vol},
		// A relative value would resolve against whatever directory the
		// process happened to start in: a different folder per launch, so a
		// recorded opt-out would be read on one start and missed on the next.
		{name: "a relative path is refused", value: "telemetry", wantErr: "absolute"},
		{name: "a dot-relative path is refused", value: "./telemetry", wantErr: "absolute"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", home)
			t.Setenv(DirEnvVar, tc.value)
			got, err := ConfigDir()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ConfigDir() = %q, %v; want an error mentioning %q", got, err, tc.wantErr)
				}
				if !strings.Contains(err.Error(), DirEnvVar) {
					t.Errorf("error %q does not name %s, so the operator cannot tell which setting to fix", err, DirEnvVar)
				}
				return
			}
			if err != nil {
				t.Fatalf("ConfigDir(): %v", err)
			}
			if got != tc.want {
				t.Errorf("ConfigDir() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestOptOutSurvivesALostHomeDirectory is the upgrade itself: the choice is
// recorded, the home directory is replaced by an empty one (a recreated
// container), and the next process must still read "off".
func TestOptOutSurvivesALostHomeDirectory(t *testing.T) {
	clearEnv(t)
	vol := filepath.Join(t.TempDir(), "state", "telemetry") // does not exist yet
	t.Setenv(DirEnvVar, vol)

	t.Setenv("HOME", t.TempDir())
	dir, err := ConfigDir()
	if err != nil {
		t.Fatalf("ConfigDir(): %v", err)
	}
	if err := SetEnabled(dir, false); err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}

	t.Setenv("HOME", t.TempDir()) // the upgrade: a fresh, empty home
	dir, err = ConfigDir()
	if err != nil {
		t.Fatalf("ConfigDir() after the upgrade: %v", err)
	}
	if d := Resolve("", dir); d.Enabled || d.Source != SourceConfig {
		t.Errorf("after the home directory was replaced the decision is %+v, want off by the config file", d)
	}

	// And the control: without the override the same sequence loses the
	// choice, which is the defect, so this test cannot pass for a reason that
	// has nothing to do with the override.
	t.Setenv(DirEnvVar, "")
	t.Setenv("HOME", t.TempDir())
	dir, _ = ConfigDir()
	_ = SetEnabled(dir, false)
	t.Setenv("HOME", t.TempDir())
	dir, _ = ConfigDir()
	if d := Resolve("", dir); !d.Enabled {
		t.Fatalf("control: the home-directory default kept the choice across a replaced home, so this test proves nothing")
	}
}

// TestARefusedOverrideNeverReports: a malformed override must not quietly fall
// back to the home directory, where a different (or no) consent file lives.
func TestARefusedOverrideNeverReports(t *testing.T) {
	clearEnv(t)
	t.Setenv("HOME", t.TempDir())
	t.Setenv(DirEnvVar, "relative/telemetry")
	c := Init(Config{Version: "v0.1.0", Endpoint: "http://127.0.0.1:1/"})
	if c.Enabled() {
		t.Error("telemetry is reporting with a refused " + DirEnvVar + "; it must be off")
	}
}

// TestDaemonWarnsOnARefusedOverride: a refused folder leaves telemetry off,
// which is safe, but the operator set it to KEEP a choice somewhere and has to
// be told it is not being kept. A daemon says so once; a missing home
// directory, which nobody set, stays silent.
func TestDaemonWarnsOnARefusedOverride(t *testing.T) {
	for _, tc := range []struct {
		name     string
		override string
		home     string
		wantWarn bool
	}{
		{name: "a relative override is reported", override: "telemetry", home: "x", wantWarn: true},
		{name: "no override and no home is silent", override: "", home: "", wantWarn: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			if tc.home != "" {
				t.Setenv("HOME", t.TempDir())
			} else {
				t.Setenv("HOME", "")
			}
			t.Setenv(DirEnvVar, tc.override)

			var logs bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(prev) })

			c := Init(Config{Version: "v0.1.0", Endpoint: "http://127.0.0.1:1/", Stderr: &bytes.Buffer{}})
			c.RunDaemon(context.Background(), "watch") // returns at once: nothing can report

			got := strings.Contains(logs.String(), DirEnvVar)
			if got != tc.wantWarn {
				t.Errorf("warning naming %s logged = %v, want %v\nlog: %s", DirEnvVar, got, tc.wantWarn, logs.String())
			}
		})
	}
}

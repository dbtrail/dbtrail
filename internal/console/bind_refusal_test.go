package console

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// TestBindRefusalNamesTheCommandsFlags: the credential-less non-loopback
// refusal must name the flags of the command that is running. watch spells
// them --console-token / --console-allow-setup; telling a watch user to pass
// --allow-setup made the fix itself fail with "unknown flag". It also names
// the env var both commands read, and reaches telemetry as config_invalid.
func TestBindRefusalNamesTheCommandsFlags(t *testing.T) {
	cases := []struct {
		name            string
		tokenFlag       string
		allowFlag       string
		want, forbidden []string
	}{
		{"serve defaults", "", "", []string{"--token / BINTRAIL_CONSOLE_TOKEN", "--allow-setup", "BINTRAIL_CONSOLE_ALLOW_SETUP=1", "set-password"}, nil},
		{"watch spelling", "--console-token", "--console-allow-setup",
			[]string{"--console-token / BINTRAIL_CONSOLE_TOKEN", "--console-allow-setup", "BINTRAIL_CONSOLE_ALLOW_SETUP=1", "set-password"},
			[]string{"set --token", "pass --allow-setup"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(Config{
				Listen:         "0.0.0.0:8090",
				AuthPath:       filepath.Join(t.TempDir(), "absent.yaml"),
				TokenFlag:      tc.tokenFlag,
				AllowSetupFlag: tc.allowFlag,
			})
			if err == nil {
				t.Fatal("credential-less non-loopback bind accepted")
			}
			msg := err.Error()
			for _, w := range tc.want {
				if !strings.Contains(msg, w) {
					t.Errorf("refusal %q lacks %q", msg, w)
				}
			}
			for _, f := range tc.forbidden {
				if strings.Contains(msg, f) {
					t.Errorf("refusal %q names another command's flag: %q", msg, f)
				}
			}
			if got := telemetry.ClassifyError(err); got != telemetry.ClassConfigInvalid {
				t.Errorf("telemetry class = %q, want %q", got, telemetry.ClassConfigInvalid)
			}
		})
	}
}

package telemetry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// Source names which control decided the resolved state. Reported by
// `bintrail telemetry status` so an operator never has to guess why telemetry
// is on or off on a given box.
type Source string

const (
	SourceDoNotTrack Source = "DO_NOT_TRACK"
	SourceFlag       Source = "flag"
	SourceEnv        Source = "BINTRAIL_TELEMETRY"
	SourceConfig     Source = "config file"
	SourceDefault    Source = "default"
)

// Decision is the resolved consent state and what decided it.
type Decision struct {
	Enabled bool
	Source  Source
}

// EnvVar is the environment control. Read directly rather than through
// internal/cli's flag-to-env bindings: those resolve against a specific
// command's flag set, and telemetry is resolved once at root level before any
// subcommand's flags exist. Same direct-read pattern the console uses for
// BINTRAIL_CONSOLE_LISTEN.
const EnvVar = "BINTRAIL_TELEMETRY"

// ciEnvVars are the CI markers checked as defense in depth. A CI run is not a
// human deciding anything, so it never sends — regardless of consent state.
// These NEVER enable telemetry; they only suppress it.
var ciEnvVars = []string{
	"CI", "GITHUB_ACTIONS", "TF_BUILD", "TRAVIS",
	"CIRCLECI", "JENKINS_URL", "BUILDKITE", "GITLAB_CI",
}

// CIEnvVars returns a copy of the CI marker variables IsCI checks. Exposed for
// the telemetrytest helpers, which neutralize them so the daemon wiring tests
// behave identically on a laptop and in CI.
func CIEnvVars() []string { return slices.Clone(ciEnvVars) }

// IsCI reports whether the process looks like it is running in CI.
func IsCI() bool {
	for _, v := range ciEnvVars {
		if val := os.Getenv(v); val != "" && val != "0" && !strings.EqualFold(val, "false") {
			return true
		}
	}
	return false
}

// state is the on-disk consent record. It carries NO identifier and never goes
// on the wire — the only thing telemetry ever writes outside the spool.
type state struct {
	// Enabled is nil when the operator has never made an explicit choice, which
	// is distinct from an explicit "on": only the former shows the notice.
	Enabled     *bool  `json:"enabled,omitempty"`
	NoticeShown bool   `json:"notice_shown"`
	DecidedAt   string `json:"decided_at,omitempty"`
}

// DirEnvVar moves the directory telemetry keeps its two things in: the consent
// file and the spool. It exists for a process whose home directory does not
// outlive it. A container's home sits in the container's own writable layer,
// so a stack that recreates the container on every upgrade would lose the
// consent file each time and an opt-out would revert to the default (on); the
// bundled docker-compose.yml points this at its state volume for that reason.
//
// It names where telemetry's own files live and nothing else: it is not a
// consent control and can never turn reporting on.
const DirEnvVar = "BINTRAIL_TELEMETRY_DIR"

// ConfigDir returns the directory holding the consent file and the spool:
// DirEnvVar when set, otherwise ~/.config/bintrail. An error here (no home
// directory: distroless, systemd DynamicUser, a scrubbed environment) disables
// telemetry entirely and silently — mirroring how the env-file loader skips
// rather than failing a command the operator actually asked for.
//
// A DirEnvVar that is not an absolute path is an error too, and deliberately
// NOT a fallback to the home directory: a relative value resolves against
// wherever the process was started, and falling back would read a different
// consent file than the one the operator pointed at. Refusing leaves telemetry
// off, which is the only safe reading of a setting that cannot be honoured.
func ConfigDir() (string, error) {
	if v := strings.TrimSpace(os.Getenv(DirEnvVar)); v != "" {
		if !filepath.IsAbs(v) {
			return "", fmt.Errorf("%s must be an absolute path, got %q", DirEnvVar, v)
		}
		return filepath.Clean(v), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "bintrail"), nil
}

// StatePath returns the consent file path inside dir.
func StatePath(dir string) string { return filepath.Join(dir, "telemetry.json") }

// loadState reads the consent file. A missing, empty or corrupt file is "no
// decision recorded", never an error: telemetry must not be able to fail a
// command, and a hand-edited file should degrade to the default rather than
// breaking the CLI.
func loadState(dir string) state {
	var s state
	data, err := os.ReadFile(StatePath(dir))
	if err != nil || len(data) == 0 {
		return s
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return state{}
	}
	return s
}

// saveState writes the consent file atomically: temp file in the same
// directory, fsync, rename — the pattern internal/console/registry.go uses for
// the server registry. Unlike the spool (a hot path, plain appends), this is
// written at most once per operator decision, so the fsync is free.
func saveState(dir string, s state) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create config directory %s: %w", dir, err)
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal telemetry state: %w", err)
	}
	data = append(data, '\n')

	tmp, err := os.CreateTemp(dir, ".telemetry-*.json")
	if err != nil {
		return fmt.Errorf("create temp telemetry state: %w", err)
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod temp telemetry state: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write telemetry state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync telemetry state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp telemetry state: %w", err)
	}
	if err := os.Rename(tmp.Name(), StatePath(dir)); err != nil {
		return fmt.Errorf("replace telemetry state %s: %w", StatePath(dir), err)
	}
	return nil
}

// SetEnabled records an explicit operator decision. Backs `bintrail telemetry
// on|off`.
func SetEnabled(dir string, enabled bool) error {
	s := loadState(dir)
	s.Enabled = &enabled
	s.NoticeShown = true // an explicit choice makes the first-run notice moot
	s.DecidedAt = time.Now().UTC().Format(time.RFC3339)
	return saveState(dir, s)
}

// parseOnOff accepts the spellings an operator is likely to type. Returns
// ok=false for anything else, so a typo falls through to the next control
// rather than silently meaning "off".
func parseOnOff(v string) (enabled, ok bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "on", "1", "true", "yes", "enabled":
		return true, true
	case "off", "0", "false", "no", "disabled":
		return false, true
	}
	return false, false
}

// Resolve determines the telemetry state. Highest control wins:
//
//  1. DO_NOT_TRACK  — hard off, checked before any file I/O
//  2. flagValue     — --telemetry=on|off
//  3. BINTRAIL_TELEMETRY
//  4. the consent file
//  5. default: ON
//
// dir may be "" when no home directory is available; the file step is then
// skipped and the default applies.
func Resolve(flagValue, dir string) Decision {
	if v := os.Getenv("DO_NOT_TRACK"); v != "" && v != "0" && !strings.EqualFold(v, "false") {
		return Decision{Enabled: false, Source: SourceDoNotTrack}
	}
	if enabled, ok := parseOnOff(flagValue); ok {
		return Decision{Enabled: enabled, Source: SourceFlag}
	}
	if enabled, ok := parseOnOff(os.Getenv(EnvVar)); ok {
		return Decision{Enabled: enabled, Source: SourceEnv}
	}
	if dir != "" {
		if s := loadState(dir); s.Enabled != nil {
			return Decision{Enabled: *s.Enabled, Source: SourceConfig}
		}
	}
	return Decision{Enabled: true, Source: SourceDefault}
}

// Notice is the first-run disclosure. It states plainly that telemetry is on
// and how to turn it off, and is shown before this machine has delivered
// anything: a command only appends to the local spool, and delivery happens on
// a LATER run, so an operator who reads this and runs `telemetry off` has sent
// nothing.
const Notice = `bintrail sends metadata-only usage stats — command names, version,
OS/arch, and success/error class. No identifier is stored or sent, and
NEVER your data, schemas, tables, DSNs, hostnames, IPs, or file paths.

Telemetry is currently ON.
  bintrail telemetry off     turn it off
  bintrail telemetry show    see exactly what is sent (sends nothing)
  DO_NOT_TRACK=1             disable telemetry entirely

Details: https://github.com/dbtrail/dbtrail/blob/main/docs/TELEMETRY.md
`

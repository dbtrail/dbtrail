package console

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// BackupSettings is the console-editable half of the daemon-wide backup
// configuration (#1682): values that used to exist ONLY as flags and
// environment variables of the process, which on this product means that
// changing one costs a restart — and a restart of this process stops change
// capture.
//
// It lives in the registry envelope rather than in a new file. The premise
// that the registry is per server by construction is not true: the envelope
// already carries two daemon-wide sections the interface writes (`rotation`
// and `baseline_refresh`), with the atomic-write discipline, the version gate
// and the Extra catch-all that a settings file would have had to reinvent. A
// second file would have meant two homes for one question.
//
// Every field is a POINTER, and that is the whole tri-state: "nothing saved"
// (the flag or environment variable is in force) has to be distinguishable
// from "saved as empty" (the operator turned it off), or clearing a value in
// the interface would silently hand the setting back to the flag that set it.
// Same reasoning as BaselineRefreshConfig's pointer, one level deeper: here
// each row is saved on its own, so the tri-state has to be per row.
type BackupSettings struct {
	// BaselineRetain prunes local snapshots older than this once a durable
	// copy exists elsewhere (--baseline-retain).
	BaselineRetain *string `yaml:"baseline_retain,omitempty"`
	// LockMode is the lock mydumper takes for a full backup
	// (BINTRAIL_CONSOLE_BASELINE_LOCK_MODE).
	LockMode *string `yaml:"lock_mode,omitempty"`
	// StagingDir is where a .sql export is assembled before it is served
	// (BINTRAIL_CONSOLE_BASELINE_STAGING).
	StagingDir *string `yaml:"staging_dir,omitempty"`
	// VerifyTables restricts the verification loop to these tables
	// (--verify-tables).
	VerifyTables *string `yaml:"verify_tables,omitempty"`
	// Extra preserves keys a newer binary stored here, the same way
	// ServerEntry.Extra and the envelope's own Extra do.
	Extra map[string]any `yaml:",inline"`
}

// The saved keys, as the wire names them. They are the API's vocabulary and
// the YAML's, so they must not drift: backupSettingKeys is what the endpoint
// accepts and what the page's rows are keyed by.
const (
	BackupSettingBaselineRetain = "baseline_retain"
	BackupSettingLockMode       = "lock_mode"
	BackupSettingStagingDir     = "staging_dir"
	BackupSettingVerifyTables   = "verify_tables"
)

// ErrUnknownBackupSetting rejects a key this build does not model. Refusing is
// deliberate: silently accepting a key would store a value nothing reads, and
// the interface would show a setting in force that governs nothing.
var ErrUnknownBackupSetting = errors.New("unknown backup setting")

// backupSettingFields maps each key to its field inside the struct. One table,
// so a key added to the constants above and forgotten here fails a test rather
// than becoming a silently-ignored row.
func backupSettingFields(s *BackupSettings) map[string]**string {
	return map[string]**string{
		BackupSettingBaselineRetain: &s.BaselineRetain,
		BackupSettingLockMode:       &s.LockMode,
		BackupSettingStagingDir:     &s.StagingDir,
		BackupSettingVerifyTables:   &s.VerifyTables,
	}
}

// BackupSettingKeys lists the editable keys in wire order.
func BackupSettingKeys() []string {
	return []string{
		BackupSettingBaselineRetain,
		BackupSettingLockMode,
		BackupSettingStagingDir,
		BackupSettingVerifyTables,
	}
}

// Get returns the saved value for a key and whether one is saved. A saved
// empty string is a REAL answer (the operator cleared the setting), which is
// why the second return is presence, not emptiness.
func (s BackupSettings) Get(key string) (string, bool) {
	p, ok := backupSettingFields(&s)[key]
	if !ok || *p == nil {
		return "", false
	}
	return **p, true
}

// clone deep-copies so a caller cannot mutate the registry's state through a
// returned pointer — the reason every accessor here returns a value.
func (s BackupSettings) clone() BackupSettings {
	out := BackupSettings{Extra: maps.Clone(s.Extra)}
	from, to := backupSettingFields(&s), backupSettingFields(&out)
	for _, key := range slices.Collect(maps.Keys(from)) {
		if v := *from[key]; v != nil {
			copied := *v
			*to[key] = &copied
		}
	}
	return out
}

// BackupSettings returns the saved daemon-wide backup settings. Always a
// value: an absent section is an empty one, and every accessor answers
// "nothing saved" for every key.
func (r *Registry) BackupSettings() BackupSettings {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file.BackupSettings == nil {
		return BackupSettings{}
	}
	return r.file.BackupSettings.clone()
}

// SetBackupSetting saves one key, or clears it when value is nil — clearing is
// what hands the setting back to the flag or environment variable, and without
// it the interface would be a one-way door (see SetBaselineRefresh, same
// reasoning). The in-memory value rolls back if the write fails, so the file
// and this process never disagree.
//
// The value is stored verbatim, trimmed. Grammar is the caller's to validate:
// the endpoint refuses what the daemon cannot parse BEFORE it gets here, so a
// value that reaches the file is one some consumer can read.
func (r *Registry) SetBackupSetting(key string, value *string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readOnly {
		return ErrRegistryReadOnly
	}
	if _, ok := backupSettingFields(&BackupSettings{})[key]; !ok {
		return fmt.Errorf("%w: %q", ErrUnknownBackupSetting, key)
	}
	prev := r.file.BackupSettings
	next := BackupSettings{}
	if prev != nil {
		next = prev.clone()
	}
	field := backupSettingFields(&next)[key]
	if value == nil {
		*field = nil
	} else {
		trimmed := strings.TrimSpace(*value)
		*field = &trimmed
	}
	r.file.BackupSettings = &next
	if err := r.save(); err != nil {
		r.file.BackupSettings = prev // roll back
		return err
	}
	return nil
}

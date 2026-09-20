package console

import (
	"errors"
	"os"
	"strings"
	"testing"
)

// The tri-state, which is the whole reason these fields are pointers: a value
// saved as empty ("stop pruning") must not read as "nothing saved", or the
// next cycle would hand the setting back to the flag that turned it on.
func TestBackupSettings_savedEmptyIsNotUnsaved(t *testing.T) {
	r, _ := tmpRegistry(t)

	if _, ok := r.BackupSettings().Get(BackupSettingBaselineRetain); ok {
		t.Fatal("a fresh registry reports a saved value")
	}
	empty := ""
	if err := r.SetBackupSetting(BackupSettingBaselineRetain, &empty); err != nil {
		t.Fatal(err)
	}
	v, ok := r.BackupSettings().Get(BackupSettingBaselineRetain)
	if !ok || v != "" {
		t.Fatalf("saved empty reads as %q/%v, want \"\"/true — an operator who cleared this "+
			"setting would otherwise get the flag's value back on the next cycle", v, ok)
	}
	if err := r.SetBackupSetting(BackupSettingBaselineRetain, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.BackupSettings().Get(BackupSettingBaselineRetain); ok {
		t.Error("nil did not clear the override — without a way back, the interface is a " +
			"one-way door and the flag can never be heard again")
	}
}

func TestBackupSettings_roundTripsAndTrims(t *testing.T) {
	r, path := tmpRegistry(t)
	val := "  7d  "
	if err := r.SetBackupSetting(BackupSettingBaselineRetain, &val); err != nil {
		t.Fatal(err)
	}
	mode := "lock-all"
	if err := r.SetBackupSetting(BackupSettingLockMode, &mode); err != nil {
		t.Fatal(err)
	}

	r2, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := r2.BackupSettings().Get(BackupSettingBaselineRetain); !ok || v != "7d" {
		t.Errorf("baseline_retain = %q/%v across save+load, want 7d/true", v, ok)
	}
	if v, ok := r2.BackupSettings().Get(BackupSettingLockMode); !ok || v != "lock-all" {
		t.Errorf("lock_mode = %q/%v, want lock-all/true", v, ok)
	}
	// Saving one key must not drop another: each row is saved on its own.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "backup_settings:") {
		t.Errorf("the file has no backup_settings section:\n%s", data)
	}
}

// An unknown key is refused rather than stored: a stored key nothing reads
// would show in the interface as a setting in force that governs nothing.
func TestBackupSettings_unknownKeyRefused(t *testing.T) {
	r, _ := tmpRegistry(t)
	v := "x"
	err := r.SetBackupSetting("not_a_setting", &v)
	if !errors.Is(err, ErrUnknownBackupSetting) {
		t.Fatalf("err = %v, want ErrUnknownBackupSetting", err)
	}
}

// Every key in the wire list must be storable. This is the guard for the one
// mistake the field table invites: adding a constant and forgetting the map,
// which would make the row silently unsavable.
func TestBackupSettings_everyKeyIsStorable(t *testing.T) {
	for _, key := range BackupSettingKeys() {
		r, _ := tmpRegistry(t)
		v := "value-for-" + key
		if err := r.SetBackupSetting(key, &v); err != nil {
			t.Errorf("%s: %v", key, err)
			continue
		}
		if got, ok := r.BackupSettings().Get(key); !ok || got != v {
			t.Errorf("%s = %q/%v, want %q/true", key, got, ok, v)
		}
	}
}

// A returned struct is a copy: a caller that mutates it must not reach into
// the registry's state.
func TestBackupSettings_returnedValueIsACopy(t *testing.T) {
	r, _ := tmpRegistry(t)
	v := "7d"
	if err := r.SetBackupSetting(BackupSettingBaselineRetain, &v); err != nil {
		t.Fatal(err)
	}
	s := r.BackupSettings()
	*s.BaselineRetain = "mutated"
	if got, _ := r.BackupSettings().Get(BackupSettingBaselineRetain); got != "7d" {
		t.Errorf("the registry now holds %q: a caller mutated it through a returned pointer", got)
	}
}

// A file written by a newer bintrail loads read-only, so a save through this
// build's narrower schema cannot drop what that build stored.
func TestBackupSettings_refusedOnANewerFile(t *testing.T) {
	r, path := tmpRegistry(t)
	if err := os.MkdirAll(strings.TrimSuffix(path, "/console-servers.yaml"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("version: 99\nservers: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	v := "7d"
	if err := r.SetBackupSetting(BackupSettingBaselineRetain, &v); !errors.Is(err, ErrRegistryReadOnly) {
		t.Fatalf("err = %v, want ErrRegistryReadOnly", err)
	}
}

package console

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
)

// ValidateBackupSetting refuses a value the daemon could not act on, BEFORE it
// reaches the file. Storing an unparseable value would be worse than refusing
// it: the page would show it as the setting in force while every consumer fell
// back to the flag, which is a setting that reads as changed and is not.
//
// The grammars are the daemon's own parsers, not copies. A second spelling of
// "7d" or of a lock mode would drift, and the drift would only show as a
// warning in a log nobody is reading while the page says the value is live.
//
// An empty value is always accepted: emptiness is how a setting is turned off,
// and each consumer already defines what off means (no pruning, a temporary
// directory, no table filter).
func ValidateBackupSetting(key, value string) error {
	if value == "" {
		return nil
	}
	switch key {
	case BackupSettingBaselineRetain:
		if _, err := cliutil.ParseRetain(value); err != nil {
			return fmt.Errorf("keep local snapshots for: %w", err)
		}
	case BackupSettingLockMode:
		if _, err := baseline.ParseLockMode(value); err != nil {
			return fmt.Errorf("lock mode: %w", err)
		}
	case BackupSettingStagingDir:
		if !filepath.IsAbs(value) {
			// Relative to WHAT is the question with no answer here: the
			// daemon's working directory is not something the operator can
			// see from a browser, and a relative path that resolved
			// somewhere unexpected would put a multi-gigabyte export there.
			return fmt.Errorf("staging folder: %q is relative; give a full path starting with /", value)
		}
	case BackupSettingVerifyTables:
		if err := ValidateVerifyTables(value); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: %q", ErrUnknownBackupSetting, key)
	}
	return nil
}

// SplitVerifyTables parses the comma-separated verify-tables list; empty
// entries are dropped, and an empty list means no filter (nil).
//
// Exported from here, and called by the daemon's flag parsing, so the two
// halves cannot disagree about what the list means: a filter the page accepts
// and the loop reads differently would silently verify the wrong tables.
func SplitVerifyTables(raw string) []string {
	var out []string
	for _, t := range strings.Split(raw, ",") {
		if t = strings.TrimSpace(t); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// ValidateVerifyTables refuses a list whose entries are not schema.table. A
// typo here does not fail loudly at run time — the table simply never matches
// and verification quietly covers less than the operator believes.
func ValidateVerifyTables(raw string) error {
	for _, t := range SplitVerifyTables(raw) {
		schema, table, ok := strings.Cut(t, ".")
		if !ok || strings.TrimSpace(schema) == "" || strings.TrimSpace(table) == "" {
			return fmt.Errorf("tables to check: %q is not schema.table", t)
		}
	}
	return nil
}

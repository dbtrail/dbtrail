package metadata

import "testing"

// @@innodb_native_foreign_keys reads back as 0/1, SHOW VARIABLES as OFF/ON;
// no variable at all (MySQL before 9.6, MariaDB) is "".
func TestSourceFKLogging_LogsCascades(t *testing.T) {
	for v, want := range map[string]bool{"OFF": true, "off": true, "0": true, "ON": false, "1": false, "": false} {
		if got := (SourceFKLogging{NativeForeignKeys: v}).LogsCascades(); got != want {
			t.Errorf("LogsCascades(%q) = %v, want %v", v, got, want)
		}
	}
}

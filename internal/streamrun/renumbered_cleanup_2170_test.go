package streamrun

import "testing"

// continuesNumbering decides whether the position-mode resume cleanup may
// compare indexed rows against the replay start (#2170). Every "false" here
// skips the delete, so a wrong "false" on the ordinary replay would duplicate
// rows and a wrong "true" on a renumbering would delete real ones.
func TestContinuesNumbering(t *testing.T) {
	for _, tc := range []struct {
		name      string
		savedFile string
		savedPos  uint64
		file      string
		pos       uint64
		want      bool
	}{
		{"same position: the ordinary crash replay", "binlog.000007", 500, "binlog.000007", 500, true},
		{"same file, later position", "binlog.000007", 500, "binlog.000007", 900, true},
		{"later file: a purge advance", "binlog.000007", 500, "binlog.000009", 4, true},
		{"suffix rollover past six digits", "binlog.999999", 500, "binlog.1000000", 4, true},
		{"same file, lower position: the file was regenerated", "binlog.000007", 500, "binlog.000007", 4, false},
		{"earlier file: RESET MASTER", "binlog.000131", 500, "binlog.000001", 4, false},
		{"shorter suffix sorts below", "binlog.1000000", 500, "binlog.999999", 4, false},
		{"shorter base name", "mysql-bin.000007", 500, "bin.000009", 4, false},
		{"longer base name", "bin.000007", 500, "mysql-bin.000009", 4, false},
		{"base name differs only in case", "binlog.000007", 500, "BINLOG.000009", 4, false},
		{"no dot, same name", "binlog", 500, "binlog", 600, true},
		{"no dot, other name", "binlog", 500, "other", 600, false},
		{"dotted against undotted", "binlog.000007", 500, "binlog", 600, false},
		{"empty start against a checkpoint", "binlog.000007", 500, "", 4, false},
		{"no saved file", "", 0, "binlog.000001", 4, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := continuesNumbering(tc.savedFile, tc.savedPos, tc.file, tc.pos); got != tc.want {
				t.Fatalf("continuesNumbering(%s:%d -> %s:%d) = %v, want %v",
					tc.savedFile, tc.savedPos, tc.file, tc.pos, got, tc.want)
			}
		})
	}
}

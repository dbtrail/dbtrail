package rotation

import (
	"errors"
	"testing"
	"time"
)

// DropWindow has to name the window the loop drops on, never a longer one: a
// caller relaxes a safety margin on its answer (#2121), and a window believed
// longer than the real one would leave a chain unprotected.
func TestDropWindow(t *testing.T) {
	recorded := Effective{Retain: 48 * time.Hour, Raw: "48h", Source: RetainRecorded}
	legacy := Effective{Retain: legacyRetainDur, Raw: LegacyRetain, Source: RetainLegacy}
	unreadable := Effective{Retain: legacyRetainDur, Raw: LegacyRetain, Source: RetainUnreadable, Err: errors.New("SELECT command denied")}
	for _, tc := range []struct {
		name      string
		settings  Settings
		effective Effective
		want      time.Duration
		known     bool
		asksIndex bool
	}{
		{"rotation off: this loop drops nothing, and who else does is not known",
			Settings{}, recorded, 0, false, false},
		{"the operator chose a window: every index drops on it",
			Settings{Enabled: true, Retain: 6 * time.Hour, Explicit: true}, recorded, 6 * time.Hour, true, false},
		{"no choice: the index's own record, not the daemon default",
			Settings{Enabled: true, Retain: 12 * time.Hour}, recorded, 48 * time.Hour, true, true},
		{"no choice, no record: the legacy window the loop keeps",
			Settings{Enabled: true, Retain: 48 * time.Hour}, legacy, legacyRetainDur, true, true},
		{"no choice, record unreadable: the loop's own read may succeed and drop on a shorter window",
			Settings{Enabled: true, Retain: 48 * time.Hour}, unreadable, 0, false, true},
		{"no choice, a record of no window at all",
			Settings{Enabled: true, Retain: 48 * time.Hour}, Effective{Source: RetainRecorded}, 0, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			asked := false
			got, known := DropWindow(tc.settings, func() Effective { asked = true; return tc.effective })
			if got != tc.want || known != tc.known {
				t.Errorf("DropWindow = %v, %v; want %v, %v", got, known, tc.want, tc.known)
			}
			if asked != tc.asksIndex {
				t.Errorf("asked the index = %v, want %v", asked, tc.asksIndex)
			}
		})
	}
}

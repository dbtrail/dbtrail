package console

import (
	"slices"
	"testing"
)

// The monitor login's password is among the passwords a front end derives
// ahead of a login (AllFlashbackPasswords), while the monitor login is
// accepted and only then: left out, it would be derived at the login of the
// monitor's username alone.
func TestAllFlashbackPasswords_holdsTheMonitorPassword(t *testing.T) {
	s, reg, _ := newMonitorTestServer(t)
	addLoginTestServer(t, reg, "prod")
	if got := s.AllFlashbackPasswords(); slices.Contains(got, "monpw") {
		t.Fatalf("before a monitor login is saved: %q", got)
	}
	before := len(s.AllFlashbackPasswords())
	if code, _, body := doFlashback(t, s, "PUT", "/api/flashback/monitor-login", `{"user":"monitor","password":"monpw"}`); code != 200 {
		t.Fatalf("PUT = %d %s", code, body)
	}
	got := s.AllFlashbackPasswords()
	if !slices.Contains(got, "monpw") || len(got) != before+1 {
		t.Fatalf("with a monitor login: %d passwords, the monitor's among them %v; want %d and true", len(got), slices.Contains(got, "monpw"), before+1)
	}
	// What the port's own passwords are does not change.
	if own := s.FlashbackPasswords(); slices.Contains(own, "monpw") {
		t.Errorf("the monitor's password is one of the port's own: %q", own)
	}

	// A monitor login that cannot be used (its username is a server's, a
	// hand edit) is checked against nothing, and is not derived.
	s.flashback.mu.Lock()
	s.flashback.saved.MonitorUser = "prod"
	s.flashback.mu.Unlock()
	if got := s.AllFlashbackPasswords(); slices.Contains(got, "monpw") {
		t.Errorf("a monitor login named like a server: its password is still derived ahead")
	}
	s.flashback.mu.Lock()
	s.flashback.saved.MonitorUser = "monitor"
	s.flashback.mu.Unlock()

	// A closed port has no passwords at all.
	s.flashback.mu.Lock()
	s.flashback.followsSwitch, s.flashback.saved.Enabled = true, false
	s.flashback.mu.Unlock()
	if got := s.AllFlashbackPasswords(); len(got) != 0 {
		t.Errorf("port off: %d passwords", len(got))
	}
}

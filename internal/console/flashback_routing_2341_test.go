package console

import (
	"errors"
	"os"
	"strings"
	"testing"
)

// Read routing is a saved switch the web interface sets (#2341): PUT
// /api/flashback takes "routing" alone, with the port on or off, and the
// status says what the file holds. The port itself is not touched.
func TestFlashbackRouting_isSavedAloneAndKeptAcrossThePortSwitch(t *testing.T) {
	s, ctl, path := newManagedFlashback(t, Config{})
	fileRouting := func() bool {
		t.Helper()
		f, err := LoadFlashbackFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return f.Routing
	}

	// With the port off, and no file yet: saved, the port stays off, and no
	// password is created or returned.
	code, got, raw := doFlashback(t, s, "PUT", "/api/flashback", `{"routing":true}`)
	if code != 200 || got["routing_on"] != true || got["enabled"] != false || strings.Contains(raw, flashbackPasswordPrefix) {
		t.Fatalf("routing on with the port off = %d %s", code, raw)
	}
	if !fileRouting() || len(ctl.calls) != 0 {
		t.Fatalf("file routing = %v, port calls = %v; want saved and the port untouched", fileRouting(), ctl.calls)
	}
	if f, _ := LoadFlashbackFile(path); f.Enabled || f.Password != "" {
		t.Fatalf("saving routing turned the port on or made a password: %+v", f)
	}

	// Turning the port on keeps it; so does turning it off.
	if code, got, raw = doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":true}`); code != 200 || got["routing_on"] != true || !fileRouting() {
		t.Fatalf("turn on = %d %s (file routing %v)", code, raw, fileRouting())
	}
	calls := len(ctl.calls)
	if code, got, raw = doFlashback(t, s, "PUT", "/api/flashback", `{"routing":false}`); code != 200 || got["routing_on"] != false || got["enabled"] != true || fileRouting() {
		t.Fatalf("routing off with the port on = %d %s (file routing %v)", code, raw, fileRouting())
	}
	if len(ctl.calls) != calls {
		t.Errorf("changing routing touched the port: %v", ctl.calls[calls:])
	}
	if strings.Contains(raw, flashbackPasswordPrefix) {
		t.Errorf("changing routing returned a password: %s", raw)
	}

	// Both in one request: the port and the routing.
	if code, got, raw = doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":false,"routing":true}`); code != 200 || got["enabled"] != false || got["routing_on"] != true || !fileRouting() {
		t.Fatalf("off with routing on = %d %s", code, raw)
	}

	// The status reads the file, which the router follows: a change made
	// there (another process, a hand edit) is what it says.
	f, _ := LoadFlashbackFile(path)
	f.Routing = false
	if err := saveFlashbackFile(path, f); err != nil {
		t.Fatal(err)
	}
	if _, got, raw = doFlashback(t, s, "GET", "/api/flashback", ""); got["routing_on"] != false {
		t.Errorf("status after the file changed = %s, want routing_on false", raw)
	}
}

// What is not a yes or a no is refused and writes nothing.
func TestFlashbackRouting_badRequests(t *testing.T) {
	s, ctl, path := newManagedFlashback(t, Config{})
	for _, body := range []string{`{"routing":"yes"}`, `{"routing":1}`, `{"routing":null}`, `{"routing":null,"listen":"127.0.0.1:3309"}`} {
		if code, _, raw := doFlashback(t, s, "PUT", "/api/flashback", body); code != 400 {
			t.Errorf("PUT %s = %d %s, want 400", body, code, raw)
		}
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) || len(ctl.calls) != 0 {
		t.Errorf("a refused request wrote the file (%v) or reached the port (%v)", err, ctl.calls)
	}
}

// A settings file this console cannot change refuses the switch like every
// other change, and the routing it holds stays.
func TestFlashbackRouting_refusedWhereThePortIsNotManagedHere(t *testing.T) {
	s, _, path := newManagedFlashback(t, Config{})
	if code, _, raw := doFlashback(t, s, "PUT", "/api/flashback", `{"routing":true}`); code != 200 {
		t.Fatalf("setup = %d %s", code, raw)
	}
	s.flashback.mu.Lock()
	s.flashback.loadErr = "yaml: line 1: did not find expected key"
	s.flashback.mu.Unlock()
	code, _, raw := doFlashback(t, s, "PUT", "/api/flashback", `{"routing":false}`)
	if code != 409 || !strings.Contains(raw, "could not be read") {
		t.Fatalf("with an unreadable file = %d %s, want 409 and why", code, raw)
	}
	if f, err := LoadFlashbackFile(path); err != nil || !f.Routing {
		t.Errorf("the refused change reached the file: %+v (%v)", f, err)
	}
}

// The notice (why nothing answers on a port that is on) travels with the
// status only while the port is saved as on.
func TestFlashbackStatus_carriesTheNoticeOnlyWhileThePortIsOn(t *testing.T) {
	s, _, _ := newManagedFlashback(t, Config{FlashbackNotice: "NOTHING ANSWERS ON IT (port 3309)."})
	if _, got, raw := doFlashback(t, s, "GET", "/api/flashback", ""); got["notice"] != nil {
		t.Fatalf("notice with the port off: %s", raw)
	}
	if _, got, raw := doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":true}`); got["notice"] != "NOTHING ANSWERS ON IT (port 3309)." {
		t.Fatalf("turn on = %s, want the notice", raw)
	}
	if _, got, raw := doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":false}`); got["notice"] != nil {
		t.Fatalf("notice after turning it off: %s", raw)
	}
	plain, _, _ := newManagedFlashback(t, Config{})
	if _, got, raw := doFlashback(t, plain, "PUT", "/api/flashback", `{"enabled":true}`); got["notice"] != nil {
		t.Fatalf("a notice where none was given: %s", raw)
	}
}

// Every change starts from what the file holds now. A port turned off in the
// file since this process read it must not come back on because read routing
// (or the password) was changed from the web interface; and a file that can
// no longer be read is not written over from memory.
func TestFlashbackChange_startsFromTheFileNotFromMemory(t *testing.T) {
	s, _, path := newManagedFlashback(t, Config{})
	if code, _, raw := doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":true}`); code != 200 {
		t.Fatalf("turn on = %d %s", code, raw)
	}
	f, err := LoadFlashbackFile(path)
	if err != nil {
		t.Fatal(err)
	}
	f.Enabled = false
	if err := saveFlashbackFile(path, f); err != nil {
		t.Fatal(err)
	}
	if code, _, raw := doFlashback(t, s, "PUT", "/api/flashback", `{"routing":true}`); code != 200 {
		t.Fatalf("routing on = %d %s", code, raw)
	}
	if got, err := LoadFlashbackFile(path); err != nil || got.Enabled || !got.Routing {
		t.Fatalf("after changing routing the file holds %+v (%v): the port turned off in the file came back on, or routing was not saved", got, err)
	}

	if err := os.WriteFile(path, []byte("enabled: [not a yes or no\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`{"routing":false}`, `{"enabled":true}`, `{"enabled":false}`} {
		if code, _, raw := doFlashback(t, s, "PUT", "/api/flashback", body); code != 409 || !strings.Contains(raw, "could not be read") {
			t.Errorf("PUT %s over an unreadable file = %d %s, want 409 and why", body, code, raw)
		}
	}
	if b, _ := os.ReadFile(path); string(b) != "enabled: [not a yes or no\n" {
		t.Errorf("an unreadable file was written over: %q", b)
	}
}

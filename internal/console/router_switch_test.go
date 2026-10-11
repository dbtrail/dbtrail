package console

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// routerSwitchServer is a console that saves the router's setting in a
// directory of the test's, started with or without an access token.
func routerSwitchServer(t *testing.T, token string, path string) *Server {
	t.Helper()
	own := t.TempDir()
	s, err := New(Config{Listen: "127.0.0.1:8090", Token: token, RouterSwitchPath: path,
		AuthPath: filepath.Join(own, "auth.yaml"), MCPTokenPath: filepath.Join(own, "mcp.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	if r := s.SQLSandbox(); r != nil {
		t.Cleanup(r.Close)
	}
	return s
}

type routerChange struct {
	Access   *routerAccessDTO `json:"access"`
	Password string           `json:"password"`
	Error    string           `json:"error"`
}

// changeRouter calls one of the two handlers directly, as a session with no
// data access policy unless pol is given.
func changeRouter(t *testing.T, s *Server, what, body string, pol *ext.AccessPolicy) (int, routerChange) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/api/router", strings.NewReader(body))
	if pol != nil {
		req = req.WithContext(withPolicy(pol))
	}
	if what == "password" {
		s.handleRouterPassword(rec, req)
	} else {
		s.handleRouterPut(rec, req)
	}
	var got routerChange
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("%s: undecodable answer %q: %v", what, rec.Body, err)
	}
	return rec.Code, got
}

func savedRouterFile(t *testing.T, path string) *FlashbackFile {
	t.Helper()
	f, err := LoadFlashbackFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// Turning the router on for the first time makes its password, hands it over
// in that one answer, and saves both for the router to follow. Nothing after
// that answer carries the password again.
func TestRouterSwitch_onMakesThePasswordOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), RouterFileName)
	s := routerSwitchServer(t, "", path)

	if a := s.routerAccess(); a == nil || !a.CanManage || a.Enabled || a.HasPassword || a.Token {
		t.Fatalf("before anything is saved: %+v, want manageable, off, no password", a)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("looking at the switch wrote the file: %v", err)
	}

	code, on := changeRouter(t, s, "put", `{"enabled":true}`, nil)
	if code != 200 || !strings.HasPrefix(on.Password, routerPasswordPrefix) || len(on.Password) != len(routerPasswordPrefix)+48 {
		t.Fatalf("turn on = %d, password %q (%s)", code, on.Password, on.Error)
	}
	if on.Access == nil || !on.Access.Enabled || !on.Access.HasPassword || on.Access.PasswordCreatedAt == "" {
		t.Fatalf("turn on, the state in the answer: %+v", on.Access)
	}
	f := savedRouterFile(t, path)
	if !f.Enabled || f.Password != on.Password || f.Listen != "" {
		t.Fatalf("saved %+v, want on, the password just handed over, no address", f)
	}
	if st, err := os.Stat(path); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("the file holding the password: %v %v, want 0600", st.Mode().Perm(), err)
	}
	rec := httptest.NewRecorder()
	s.handleRouterGet(rec, httptest.NewRequest("GET", "/api/router", nil))
	if strings.Contains(rec.Body.String(), on.Password) || !strings.Contains(rec.Body.String(), `"has_password":true`) {
		t.Fatalf("GET /api/router after turning on: %s", rec.Body)
	}

	// Off keeps the password, on again does not make or show another.
	code, off := changeRouter(t, s, "put", `{"enabled":false}`, nil)
	if code != 200 || off.Password != "" || off.Access.Enabled || !off.Access.HasPassword {
		t.Fatalf("turn off = %d %+v password %q", code, off.Access, off.Password)
	}
	if f := savedRouterFile(t, path); f.Enabled || f.Password != on.Password {
		t.Fatalf("after off, saved %+v", f)
	}
	code, again := changeRouter(t, s, "put", `{"enabled":true}`, nil)
	if code != 200 || again.Password != "" || !again.Access.Enabled {
		t.Fatalf("turn on again = %d %+v password %q, want on and no password in the answer", code, again.Access, again.Password)
	}
	if f := savedRouterFile(t, path); !f.Enabled || f.Password != on.Password {
		t.Fatalf("after on again, saved %+v, want the same password", f)
	}

	// A new password replaces it, on or off untouched.
	code, np := changeRouter(t, s, "password", ``, nil)
	if code != 200 || !strings.HasPrefix(np.Password, routerPasswordPrefix) || np.Password == on.Password || !np.Access.Enabled {
		t.Fatalf("new password = %d %q %+v", code, np.Password, np.Access)
	}
	if f := savedRouterFile(t, path); f.Password != np.Password || !f.Enabled {
		t.Fatalf("after a new password, saved %+v", f)
	}

	// A restart reads back what was saved.
	back := routerSwitchServer(t, "", path)
	if a := back.routerAccess(); a == nil || !a.CanManage || !a.Enabled || !a.HasPassword {
		t.Fatalf("after a restart: %+v", a)
	}
}

// A request that does not say on or off changes nothing.
func TestRouterSwitch_aRequestThatSaysNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), RouterFileName)
	s := routerSwitchServer(t, "", path)
	for _, body := range []string{``, `{}`, `{"enabled":null}`, `{"enabled":"yes"}`, `not json`} {
		code, got := changeRouter(t, s, "put", body, nil)
		if code != 400 || got.Error == "" || got.Password != "" {
			t.Errorf("PUT %q = %d %+v, want 400", body, code, got)
		}
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("a refused request wrote the file: %v", err)
	}
}

// Every case where this console must not write the setting: each one is
// refused with its reason and leaves the file as it was.
func TestRouterSwitch_refusals(t *testing.T) {
	restricted := &ext.AccessPolicy{Profile: "analyst", Permissions: ext.AllPermissions()}
	cases := []struct {
		name    string
		token   string
		file    string // written before the console starts; "" = none
		noPath  bool
		pol     *ext.AccessPolicy
		code    int
		says    string
		manages bool
	}{
		{name: "started with an access token", token: "tok", code: 409, says: "access token"},
		{name: "a file from a newer version", file: "version: 99\nenabled: true\npassword: x\n", code: 409, says: "newer bintrail"},
		{name: "a file that is not a setting", file: "enabled: [oops\n", code: 409, says: "could not be read"},
		{name: "a console that manages no router", noPath: true, code: 409, says: "does not manage a router"},
		{name: "a session with a data access policy", pol: restricted, code: 403, says: "data access policy", manages: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), RouterFileName)
			if tc.file != "" {
				if err := os.WriteFile(path, []byte(tc.file), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cfgPath := path
			if tc.noPath {
				cfgPath = ""
			}
			s := routerSwitchServer(t, tc.token, cfgPath)
			a := s.routerAccess()
			if tc.noPath {
				if a != nil {
					t.Fatalf("a console that manages no router reports a switch: %+v", a)
				}
			} else if a == nil || a.CanManage != tc.manages || (!tc.manages && !strings.Contains(a.Refusal, tc.says)) || a.Token != (tc.token != "") {
				t.Fatalf("the switch as reported: %+v", a)
			}
			for _, what := range []string{"put", "password"} {
				code, got := changeRouter(t, s, what, `{"enabled":true}`, tc.pol)
				if code != tc.code || !strings.Contains(got.Error, tc.says) || got.Password != "" {
					t.Errorf("%s = %d %q password %q, want %d saying %q", what, code, got.Error, got.Password, tc.code, tc.says)
				}
			}
			now, err := os.ReadFile(path)
			if tc.file == "" && !os.IsNotExist(err) {
				t.Errorf("a refused change wrote the file: %q %v", now, err)
			}
			if tc.file != "" && string(now) != tc.file {
				t.Errorf("a refused change rewrote the file: %q", now)
			}
		})
	}
}

// A setting that says on with no password opens nothing, and is reported
// off: turning it on makes the password.
func TestRouterSwitch_onWithNoPasswordIsOff(t *testing.T) {
	path := filepath.Join(t.TempDir(), RouterFileName)
	if err := os.WriteFile(path, []byte("version: 1\nenabled: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := routerSwitchServer(t, "", path)
	if a := s.routerAccess(); a == nil || a.Enabled || a.HasPassword || !a.CanManage {
		t.Fatalf("on with no password: %+v, want reported off", a)
	}
	code, on := changeRouter(t, s, "put", `{"enabled":true}`, nil)
	if code != 200 || on.Password == "" || !on.Access.Enabled {
		t.Fatalf("turn on = %d %+v", code, on)
	}
}

// The switch is in the answer of a console that reads no router's numbers:
// installed from the packages there is no address by default, and the
// password still has to be made somewhere.
func TestRouterSwitch_reportedWithNoAddress(t *testing.T) {
	s := routerSwitchServer(t, "", filepath.Join(t.TempDir(), RouterFileName))
	rec := httptest.NewRecorder()
	s.handleRouterGet(rec, httptest.NewRequest("GET", "/api/router", nil))
	var got routerChange
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || got.Access == nil || !got.Access.CanManage {
		t.Fatalf("GET /api/router with no address = %s (%v)", rec.Body, err)
	}
	if !strings.Contains(rec.Body.String(), `"configured":false`) {
		t.Fatalf("no address must still read as not configured: %s", rec.Body)
	}
}

// The router follows the file, not this process: a file put there, changed
// or removed while this process runs is what the panel reports and what a
// change starts from. A change made from what this process last wrote would
// put a new password over one applications are connected with.
func TestRouterSwitch_readsTheFileEveryTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), RouterFileName)
	s := routerSwitchServer(t, "", path)
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// Put there by hand: on, with a password this process never made.
	write("version: 1\nenabled: true\npassword: kept-from-before\n")
	if a := s.routerAccess(); !a.Enabled || !a.HasPassword || !a.CanManage {
		t.Fatalf("a file put there while running: %+v, want reported on", a)
	}
	code, on := changeRouter(t, s, "put", `{"enabled":true}`, nil)
	if code != 200 || on.Password != "" {
		t.Fatalf("turn on over a password already there = %d, password %q; want none made", code, on.Password)
	}
	if f := savedRouterFile(t, path); f.Password != "kept-from-before" || !f.Enabled {
		t.Fatalf("the password applications use was written over: %+v", f)
	}

	// Removed: reported off, and no password.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if a := s.routerAccess(); a.Enabled || a.HasPassword || !a.CanManage {
		t.Fatalf("a file removed while running: %+v, want off with no password", a)
	}

	// Broken, then mended: refused with the reason, then manageable again
	// with no restart.
	write("enabled: [oops\n")
	if a := s.routerAccess(); a.CanManage || !strings.Contains(a.Refusal, "could not be read") || a.Enabled {
		t.Fatalf("a file broken while running: %+v", a)
	}
	if code, got := changeRouter(t, s, "password", ``, nil); code != 409 || got.Password != "" {
		t.Fatalf("a new password over a broken file = %d %+v", code, got)
	}
	write("version: 1\nenabled: false\npassword: mended\n")
	if a := s.routerAccess(); !a.CanManage || a.Enabled || !a.HasPassword {
		t.Fatalf("a file mended while running: %+v", a)
	}
}

package console

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// fakeFlashbackControl records what the server asks of the port and refuses
// when told to.
type fakeFlashbackControl struct {
	calls []string
	fail  map[string]error
}

func (f *fakeFlashbackControl) Apply(listen string) error {
	f.calls = append(f.calls, listen)
	return f.fail[listen]
}

const fbTok = "secret-tok"

// newManagedFlashback builds a watch-shaped server: a settings file path and
// a controller, the port not set at startup.
func newManagedFlashback(t *testing.T, cfg Config) (*Server, *fakeFlashbackControl, string) {
	t.Helper()
	if cfg.Listen == "" {
		cfg.Listen = "127.0.0.1:8090"
	}
	if cfg.FlashbackPath == "" {
		cfg.FlashbackPath = filepath.Join(t.TempDir(), "state", FlashbackFileName)
	}
	if cfg.Token == "" {
		cfg.Token = fbTok
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctl := &fakeFlashbackControl{fail: map[string]error{}}
	s.ManageFlashback(ctl)
	return s, ctl, cfg.FlashbackPath
}

func doFlashback(t *testing.T, s *Server, method, path, body string) (int, map[string]any, string) {
	t.Helper()
	req := httptest.NewRequest(method, "http://127.0.0.1:8090"+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+fbTok)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	s.mux.ServeHTTP(rec, req)
	var got map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	return rec.Code, got, rec.Body.String()
}

func TestNormalizeFlashbackListen(t *testing.T) {
	const web = "127.0.0.1:8090"
	cases := []struct{ in, want string }{
		{"127.0.0.1:3309", "127.0.0.1:3309"},
		{"  127.0.0.1:3309\n", "127.0.0.1:3309"},
		{":3309", ":3309"},
		{"0.0.0.0:3309", "0.0.0.0:3309"},
		{"LOCALHOST:3309", "LOCALHOST:3309"},
		{"[::1]:3309", "[::1]:3309"},
		{"127.0.0.1:03309", "127.0.0.1:3309"},
		{"", ""},
		{"   ", ""},
		{"127.0.0.1", ""},
		{"3309", ""},
		{"127.0.0.1:", ""},
		{"127.0.0.1:0", ""},
		{"127.0.0.1:-1", ""},
		{"127.0.0.1:70000", ""},
		{"127.0.0.1:mysql", ""},
		{"my host:3309", ""},
		{"127.0.0.1:3309/", ""},
		{"tcp://127.0.0.1:3309", ""},
		{"127.0.0.1:3309\n10.0.0.1:1", ""},
		{"127.0.0.1:8090", ""}, // the web interface's own port
		{"0.0.0.0:8090", ""},   // same port, another host: still taken
	}
	for _, tc := range cases {
		got, err := NormalizeFlashbackListen(tc.in, web)
		if tc.want == "" {
			if err == nil {
				t.Errorf("%q: accepted as %q, want a refusal", tc.in, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%q: got %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
}

func TestDefaultFlashbackListen(t *testing.T) {
	for in, want := range map[string]string{
		"127.0.0.1:8090": "127.0.0.1:3309",
		"localhost:8090": "127.0.0.1:3309",
		"LocalHost:8090": "127.0.0.1:3309",
		"0.0.0.0:8090":   "0.0.0.0:3309",
		":8090":          "0.0.0.0:3309",
		"[::]:8090":      "0.0.0.0:3309",
		"10.0.0.5:8090":  "10.0.0.5:3309",
		"not-an-address": "127.0.0.1:3309",
	} {
		if got := defaultFlashbackListen(in); got != want {
			t.Errorf("web interface on %q: offered %q, want %q", in, got, want)
		}
	}
}

func TestFlashbackFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", FlashbackFileName)
	f, err := LoadFlashbackFile(path)
	if err != nil || f.Enabled || f.Password != "" || f.ReadOnly() {
		t.Fatalf("missing file: %+v, %v; want an empty setting", f, err)
	}
	f.Enabled, f.Listen, f.Password = true, "127.0.0.1:3309", "bfp_x"
	f.Extra = map[string]any{"from_a_newer_build": "kept"}
	if err := saveFlashbackFile(path, f); err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(path); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("file mode = %v, %v; want 0600 (it holds a password)", st.Mode().Perm(), err)
	}
	if st, _ := os.Stat(filepath.Dir(path)); st.Mode().Perm() != 0o700 {
		t.Errorf("directory mode = %v, want 0700", st.Mode().Perm())
	}
	back, err := LoadFlashbackFile(path)
	if err != nil || !back.Enabled || back.Listen != "127.0.0.1:3309" || back.Password != "bfp_x" || back.Extra["from_a_newer_build"] != "kept" {
		t.Fatalf("round trip: %+v, %v", back, err)
	}

	// A newer version loads, is honoured, and refuses changes.
	if err := os.WriteFile(path, []byte("version: 99\nenabled: true\nlisten: 127.0.0.1:3309\npassword: bfp_new\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	newer, err := LoadFlashbackFile(path)
	if err != nil || !newer.ReadOnly() || newer.Password != "bfp_new" {
		t.Fatalf("newer file: %+v, %v; want read-only with its fields", newer, err)
	}
	if err := saveFlashbackFile(path, newer); !errors.Is(err, ErrFlashbackFileReadOnly) {
		t.Fatalf("save over a newer file: %v, want ErrFlashbackFileReadOnly", err)
	}

	if err := os.WriteFile(path, []byte("{not yaml"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFlashbackFile(path); err == nil {
		t.Fatal("corrupt file loaded without an error")
	}
}

// TestFlashbackManage_Lifecycle drives on, move, new password and off through
// the real routes, and checks the password is shown exactly when created.
func TestFlashbackManage_Lifecycle(t *testing.T) {
	s, ctl, path := newManagedFlashback(t, Config{})

	code, got, raw := doFlashback(t, s, "GET", "/api/flashback", "")
	if code != 200 || got["enabled"] != false || got["can_manage"] != true || got["suggested_listen"] != "127.0.0.1:3309" {
		t.Fatalf("fresh status = %d %s", code, raw)
	}
	if _, has := got["has_password"]; has {
		t.Fatalf("fresh status claims a password: %s", raw)
	}
	if !slices.Equal(s.FlashbackPasswords(), []string{fbTok}) {
		t.Fatalf("passwords before = %v", s.FlashbackPasswords())
	}

	code, got, raw = doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":true}`)
	pw, _ := got["password"].(string)
	if code != 200 || !strings.HasPrefix(pw, flashbackPasswordPrefix) || len(pw) != len(flashbackPasswordPrefix)+48 {
		t.Fatalf("turn on = %d %s; want a generated password", code, raw)
	}
	if got["enabled"] != true || got["listen"] != "127.0.0.1:3309" || got["source"] != "saved" || got["has_password"] != true {
		t.Fatalf("turn on status = %s", raw)
	}
	if !slices.Equal(ctl.calls, []string{"127.0.0.1:3309"}) {
		t.Fatalf("controller calls = %v", ctl.calls)
	}
	if !slices.Equal(s.FlashbackPasswords(), []string{fbTok, pw}) {
		t.Fatalf("passwords after on = %v", s.FlashbackPasswords())
	}
	saved, err := LoadFlashbackFile(path)
	if err != nil || !saved.Enabled || saved.Listen != "127.0.0.1:3309" || saved.Password != pw {
		t.Fatalf("saved = %+v, %v", saved, err)
	}

	// The status never carries the password again.
	if _, _, raw = doFlashback(t, s, "GET", "/api/flashback", ""); strings.Contains(raw, pw) || strings.Contains(raw, fbTok) {
		t.Fatalf("GET leaks a password: %s", raw)
	}

	// Moving the port keeps the password and does not show it.
	code, got, raw = doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":true,"listen":" 127.0.0.1:3310 "}`)
	if code != 200 || got["listen"] != "127.0.0.1:3310" || strings.Contains(raw, pw) {
		t.Fatalf("move = %d %s", code, raw)
	}

	code, got, raw = doFlashback(t, s, "POST", "/api/flashback/password", "")
	pw2, _ := got["password"].(string)
	if code != 200 || pw2 == pw || !strings.HasPrefix(pw2, flashbackPasswordPrefix) {
		t.Fatalf("new password = %d %s", code, raw)
	}
	if !slices.Equal(s.FlashbackPasswords(), []string{fbTok, pw2}) {
		t.Fatalf("passwords after replace = %v; the old one must stop working", s.FlashbackPasswords())
	}

	code, got, raw = doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":false}`)
	if code != 200 || got["enabled"] != false || got["has_password"] != true || got["suggested_listen"] != "127.0.0.1:3310" {
		t.Fatalf("turn off = %d %s", code, raw)
	}
	if _, has := got["listen"]; has {
		t.Fatalf("off state carries an address: %s", raw)
	}
	if !slices.Equal(ctl.calls, []string{"127.0.0.1:3309", "127.0.0.1:3310", ""}) {
		t.Fatalf("controller calls = %v", ctl.calls)
	}
	saved, _ = LoadFlashbackFile(path)
	if saved.Enabled || saved.Password != pw2 || saved.Listen != "127.0.0.1:3310" {
		t.Fatalf("saved after off = %+v; off keeps the address and the password", saved)
	}

	// On again: the same password, not shown.
	code, _, raw = doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":true}`)
	if code != 200 || strings.Contains(raw, pw2) || ctl.calls[len(ctl.calls)-1] != "127.0.0.1:3310" {
		t.Fatalf("on again = %d %s, calls %v", code, raw, ctl.calls)
	}
}

func TestFlashbackManage_BadRequests(t *testing.T) {
	s, ctl, path := newManagedFlashback(t, Config{})
	for _, body := range []string{``, `{`, `{}`, `{"listen":"127.0.0.1:3309"}`, `{"enabled":true,"listen":"nope"}`, `{"enabled":true,"listen":"127.0.0.1:8090"}`} {
		if code, _, raw := doFlashback(t, s, "PUT", "/api/flashback", body); code != 400 {
			t.Errorf("PUT %q = %d %s, want 400", body, code, raw)
		}
	}
	if len(ctl.calls) != 0 {
		t.Errorf("a refused request reached the port: %v", ctl.calls)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a refused request wrote the settings file (%v)", err)
	}
}

// TestFlashbackManage_AddressRefused: an address that cannot be opened changes
// nothing, on disk or in what authenticates.
func TestFlashbackManage_AddressRefused(t *testing.T) {
	s, ctl, path := newManagedFlashback(t, Config{})
	ctl.fail["127.0.0.1:3309"] = errors.New("cannot bind 127.0.0.1:3309: address already in use")
	code, got, raw := doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":true}`)
	if code != 409 || !strings.Contains(raw, "address already in use") || !strings.Contains(raw, "127.0.0.1:3309") {
		t.Fatalf("refused bind = %d %s", code, raw)
	}
	if _, has := got["password"]; has {
		t.Fatalf("a refused bind handed out a password: %s", raw)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused bind saved the setting (%v)", err)
	}
	if !slices.Equal(s.FlashbackPasswords(), []string{fbTok}) {
		t.Fatalf("a refused bind left a password behind: %v", s.FlashbackPasswords())
	}
	if _, got, _ = doFlashback(t, s, "GET", "/api/flashback", ""); got["enabled"] != false {
		t.Fatalf("status after a refused bind = %v", got)
	}
}

// TestFlashbackManage_SaveFails: a port that opened but could not be saved is
// closed again; one that a restart would silently drop must not stay open.
func TestFlashbackManage_SaveFails(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "state")
	if err := os.WriteFile(blocker, []byte("a file where the directory should be"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, ctl, _ := newManagedFlashback(t, Config{FlashbackPath: filepath.Join(blocker, FlashbackFileName)})
	code, _, raw := doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":true}`)
	if code != 500 {
		t.Fatalf("unsaveable setting = %d %s, want 500", code, raw)
	}
	if !slices.Equal(ctl.calls, []string{"127.0.0.1:3309", ""}) {
		t.Fatalf("controller calls = %v; want the port opened and closed again", ctl.calls)
	}
	if !slices.Equal(s.FlashbackPasswords(), []string{fbTok}) {
		t.Fatalf("passwords = %v", s.FlashbackPasswords())
	}
	if _, got, _ := doFlashback(t, s, "GET", "/api/flashback", ""); got["enabled"] != false {
		t.Fatalf("status = %v", got)
	}
}

// TestFlashbackManage_StartupDecides: an address given at startup is not the
// web interface's to change, and a password saved earlier does not open it.
func TestFlashbackManage_StartupDecides(t *testing.T) {
	path := filepath.Join(t.TempDir(), FlashbackFileName)
	if err := saveFlashbackFile(path, &FlashbackFile{Enabled: true, Listen: "127.0.0.1:3309", Password: "bfp_saved"}); err != nil {
		t.Fatal(err)
	}
	s, ctl, _ := newManagedFlashback(t, Config{FlashbackListen: "127.0.0.1:3308", FlashbackPath: path})
	if len(ctl.calls) != 0 {
		t.Fatalf("the saved setting was applied over the startup one: %v", ctl.calls)
	}
	if !slices.Equal(s.FlashbackPasswords(), []string{fbTok}) {
		t.Fatalf("passwords = %v; only the token opens a port set at startup", s.FlashbackPasswords())
	}
	_, got, raw := doFlashback(t, s, "GET", "/api/flashback", "")
	if got["enabled"] != true || got["source"] != "startup" || got["can_manage"] != false || got["listen"] != "127.0.0.1:3308" {
		t.Fatalf("status = %s", raw)
	}
	if _, has := got["has_password"]; has {
		t.Fatalf("status reports a saved password that does not apply: %s", raw)
	}
	for _, rq := range [][2]string{{"PUT", "/api/flashback"}, {"POST", "/api/flashback/password"}} {
		if code, _, raw := doFlashback(t, s, rq[0], rq[1], `{"enabled":false}`); code != 409 || !strings.Contains(raw, "where DBTrail is started") {
			t.Errorf("%s %s = %d %s, want 409 naming the startup setting", rq[0], rq[1], code, raw)
		}
	}
}

// TestFlashbackManage_NoPortHere: serve runs no port, so there is nothing to
// turn on.
func TestFlashbackManage_NoPortHere(t *testing.T) {
	s, err := New(Config{Listen: "127.0.0.1:8090", Token: fbTok})
	if err != nil {
		t.Fatal(err)
	}
	if _, got, raw := doFlashback(t, s, "GET", "/api/flashback", ""); got["can_manage"] != false || got["enabled"] != false {
		t.Fatalf("status = %s", raw)
	}
	if code, _, raw := doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":true}`); code != 409 {
		t.Fatalf("PUT on serve = %d %s, want 409", code, raw)
	}
	// A path with no controller is the same: nothing can open the port.
	s2, err := New(Config{Listen: "127.0.0.1:8090", Token: fbTok, FlashbackPath: filepath.Join(t.TempDir(), FlashbackFileName)})
	if err != nil {
		t.Fatal(err)
	}
	if code, _, raw := doFlashback(t, s2, "POST", "/api/flashback/password", ""); code != 409 {
		t.Fatalf("POST with no controller = %d %s, want 409", code, raw)
	}
}

// TestManageFlashback_AtStartup: what a restart does with the saved setting.
func TestManageFlashback_AtStartup(t *testing.T) {
	write := func(t *testing.T, body string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), FlashbackFileName)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	boot := func(t *testing.T, path string, fail error) (*Server, *fakeFlashbackControl, map[string]any) {
		t.Helper()
		// No token: a port saved in the web interface needs none.
		s, err := New(Config{Listen: "127.0.0.1:8090", FlashbackPath: path})
		if err != nil {
			t.Fatal(err)
		}
		ctl := &fakeFlashbackControl{fail: map[string]error{"127.0.0.1:3309": fail}}
		s.ManageFlashback(ctl)
		var got map[string]any
		raw, _ := json.Marshal(s.flashbackStatus())
		_ = json.Unmarshal(raw, &got)
		return s, ctl, got
	}
	const on = "version: 1\nenabled: true\nlisten: 127.0.0.1:3309\npassword: bfp_saved\n"

	t.Run("saved on comes up, with no token", func(t *testing.T) {
		s, ctl, got := boot(t, write(t, on), nil)
		if !slices.Equal(ctl.calls, []string{"127.0.0.1:3309"}) || got["enabled"] != true || got["source"] != "saved" {
			t.Fatalf("calls %v, status %v", ctl.calls, got)
		}
		if !slices.Equal(s.FlashbackPasswords(), []string{"bfp_saved"}) {
			t.Fatalf("passwords = %v", s.FlashbackPasswords())
		}
	})
	t.Run("address taken: reported, port off, still manageable", func(t *testing.T) {
		_, _, got := boot(t, write(t, on), errors.New("cannot bind 127.0.0.1:3309: address already in use"))
		if got["enabled"] != false || got["can_manage"] != true || !strings.Contains(got["error"].(string), "address already in use") {
			t.Fatalf("status = %v", got)
		}
	})
	t.Run("saved off stays off", func(t *testing.T) {
		_, ctl, got := boot(t, write(t, "version: 1\nenabled: false\nlisten: 127.0.0.1:3309\npassword: bfp_saved\n"), nil)
		if len(ctl.calls) != 0 || got["enabled"] != false || got["has_password"] != true {
			t.Fatalf("calls %v, status %v", ctl.calls, got)
		}
	})
	t.Run("on with no password never opens", func(t *testing.T) {
		s, ctl, _ := boot(t, write(t, "version: 1\nenabled: true\nlisten: 127.0.0.1:3309\n"), nil)
		if len(ctl.calls) != 0 || len(s.FlashbackPasswords()) != 0 {
			t.Fatalf("calls %v, passwords %v; a port with no password must not open", ctl.calls, s.FlashbackPasswords())
		}
	})
	t.Run("unreadable setting is reported", func(t *testing.T) {
		_, ctl, got := boot(t, write(t, "{not yaml"), nil)
		if len(ctl.calls) != 0 || got["error"] == nil {
			t.Fatalf("calls %v, status %v", ctl.calls, got)
		}
	})
	t.Run("written by a newer version: honoured, not changeable", func(t *testing.T) {
		s, ctl, got := boot(t, write(t, strings.Replace(on, "version: 1", "version: 99", 1)), nil)
		if !slices.Equal(ctl.calls, []string{"127.0.0.1:3309"}) || got["can_manage"] != false {
			t.Fatalf("calls %v, status %v", ctl.calls, got)
		}
		req := httptest.NewRequest("PUT", "http://127.0.0.1:8090/api/flashback", strings.NewReader(`{"enabled":false}`))
		rec := httptest.NewRecorder()
		s.handleFlashbackPut(rec, req)
		if rec.Code != 409 {
			t.Fatalf("change over a newer file = %d %s, want 409", rec.Code, rec.Body.String())
		}
	})
}

// TestFlashbackManage_RefusesDataRestrictedSession: the port filters nothing,
// so a session that sees redacted data cannot turn it on or take its password,
// whatever permissions it holds.
func TestFlashbackManage_RefusesDataRestrictedSession(t *testing.T) {
	s, ctl, path := newManagedFlashback(t, Config{})
	pol := &ext.AccessPolicy{Profile: "analyst", Permissions: ext.AllPermissions()}
	for name, h := range map[string]func(*httptest.ResponseRecorder, *http.Request){
		"turn on":      func(w *httptest.ResponseRecorder, r *http.Request) { s.handleFlashbackPut(w, r) },
		"new password": func(w *httptest.ResponseRecorder, r *http.Request) { s.handleFlashbackPassword(w, r) },
	} {
		req := httptest.NewRequest("PUT", "http://127.0.0.1:8090/api/flashback", strings.NewReader(`{"enabled":true}`))
		req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{}, pol))
		rec := httptest.NewRecorder()
		h(rec, req)
		if rec.Code != 403 || strings.Contains(rec.Body.String(), flashbackPasswordPrefix) {
			t.Errorf("%s from a data-restricted session = %d %s, want 403 and no password", name, rec.Code, rec.Body.String())
		}
	}
	if len(ctl.calls) != 0 {
		t.Errorf("the port was touched: %v", ctl.calls)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the settings file was written (%v)", err)
	}
}

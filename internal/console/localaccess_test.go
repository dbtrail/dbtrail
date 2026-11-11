package console

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestLocalAccessPathSitsBesideTheLoginFile(t *testing.T) {
	if got, want := LocalAccessPath("/var/lib/bintrail/console-auth.yaml"), "/var/lib/bintrail/console-cli.json"; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
	if got, want := LocalAccessPath(""), filepath.Join(filepath.Dir(DefaultAuthPath()), LocalAccessFileName); got != want {
		t.Errorf("default: got %s, want %s", got, want)
	}
}

func TestLocalAccessURL(t *testing.T) {
	for _, c := range []struct {
		listen string
		tls    bool
		want   string
	}{
		{"127.0.0.1:8090", false, "http://127.0.0.1:8090"},
		{"0.0.0.0:8090", false, "http://127.0.0.1:8090"},
		{":9000", false, "http://127.0.0.1:9000"},
		{"[::]:8090", false, "http://[::1]:8090"},
		{"10.0.0.5:8443", true, "https://10.0.0.5:8443"},
	} {
		if got := localAccessURL(c.listen, c.tls); got != c.want {
			t.Errorf("localAccessURL(%q, %v) = %q, want %q", c.listen, c.tls, got, c.want)
		}
	}
}

func TestNewWritesLocalAccessAndAcceptsItsToken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", LocalAccessFileName) // the folder does not exist yet
	srv, err := New(Config{Listen: "0.0.0.0:8090", Token: "secret", LocalAccessPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("New wrote the file: it is written once the port is open, by Serve")
	}
	srv.publishLocalAccess()
	acc, err := ReadLocalAccess(path)
	if err != nil {
		t.Fatalf("ReadLocalAccess: %v", err)
	}
	if acc.URL != "http://127.0.0.1:8090" {
		t.Errorf("url = %q", acc.URL)
	}
	if len(acc.Token) != 64 || acc.Token == "secret" {
		t.Errorf("token = %q, want 64 hex characters of its own", acc.Token)
	}
	if runtime.GOOS != "windows" {
		st, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm() != 0o600 {
			t.Errorf("mode = %v, want 0600: the file holds a token", st.Mode().Perm())
		}
	}

	call := func(set func(*http.Request)) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/servers", nil)
		set(req)
		srv.Handler().ServeHTTP(rec, req)
		return rec.Code
	}
	if code := call(func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+acc.Token) }); code != http.StatusOK {
		t.Errorf("the file's token as a Bearer: code = %d, want 200", code)
	}
	if code := call(func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+acc.Token+"x") }); code != http.StatusUnauthorized {
		t.Errorf("a wrong token: code = %d, want 401", code)
	}
	// It is a command's token: a browser cookie carrying it is not let in.
	if code := call(func(r *http.Request) { r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: acc.Token}) }); code != http.StatusUnauthorized {
		t.Errorf("the file's token as a cookie: code = %d, want 401", code)
	}

	// A second DBTrail started on the same folder that never gets to serve
	// (its port is busy) must leave the running one's file alone.
	if _, err := New(Config{Listen: "0.0.0.0:8090", Token: "secret", LocalAccessPath: path}); err != nil {
		t.Fatal(err)
	}
	if again, err := ReadLocalAccess(path); err != nil || again.Token != acc.Token {
		t.Errorf("a start that never served changed the file: %+v, %v", again, err)
	}

	// One that does serve replaces the token: the old one stops working there.
	srv2, err := New(Config{Listen: "0.0.0.0:8090", Token: "secret", LocalAccessPath: path})
	if err != nil {
		t.Fatal(err)
	}
	srv2.publishLocalAccess()
	acc2, err := ReadLocalAccess(path)
	if err != nil {
		t.Fatal(err)
	}
	if acc2.Token == acc.Token {
		t.Error("a new start kept the old token")
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/servers", nil)
	req.Header.Set("Authorization", "Bearer "+acc.Token)
	srv2.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("the previous run's token on the new run: code = %d, want 401", rec.Code)
	}

	// The first one stopping must not remove the file of the one that
	// replaced it; the owner stopping removes it.
	srv.withdrawLocalAccess()
	if _, err := ReadLocalAccess(path); err != nil {
		t.Errorf("a stopping DBTrail removed another one's file: %v", err)
	}
	srv2.withdrawLocalAccess()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the file outlived the DBTrail that wrote it: %v", err)
	}
}

// Serve is what publishes and withdraws: the file exists while it serves.
func TestServeWritesAndRemovesLocalAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), LocalAccessFileName)
	srv, err := New(Config{Listen: "127.0.0.1:0", Token: "secret", LocalAccessPath: path})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := ReadLocalAccess(path); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Serve did not write the file")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the file is still there after Serve returned: %v", err)
	}
}

// The audit trail tells a command on the machine from the operator's token.
func TestLocalAccessHasItsOwnAuditActor(t *testing.T) {
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "secret", LocalAccessPath: filepath.Join(t.TempDir(), LocalAccessFileName)})
	if err != nil {
		t.Fatal(err)
	}
	srv.publishLocalAccess()
	actor := func(tok string) (got string) {
		h := srv.tokenMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got = consoleActor(r) }))
		req := httptest.NewRequest("GET", "/api/x", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		h.ServeHTTP(httptest.NewRecorder(), req)
		return got
	}
	if got := actor(srv.localToken); got != localCLIActor {
		t.Errorf("local token: actor = %q, want %q", got, localCLIActor)
	}
	if got := actor("secret"); got != tokenActor {
		t.Errorf("operator token: actor = %q, want %q", got, tokenActor)
	}
}

// With no path, nothing is written and no local token exists. The empty
// token must not be a token: ConstantTimeCompare("", "") is 1.
func TestNoLocalAccessPathAcceptsNoLocalToken(t *testing.T) {
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if srv.localToken != "" {
		t.Fatalf("localToken = %q with no path", srv.localToken)
	}
	for _, auth := range []string{"", "Bearer ", "Bearer x"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/servers", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("auth %q: code = %d, want 401", auth, rec.Code)
		}
	}
}

// The local token is not the operator's token: it must not close first-run
// setup, which a configured Token does.
func TestLocalAccessDoesNotCloseFirstRunSetup(t *testing.T) {
	dir := t.TempDir()
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", AuthPath: filepath.Join(dir, "console-auth.yaml"),
		LocalAccessPath: filepath.Join(dir, LocalAccessFileName),
	})
	if err != nil {
		t.Fatal(err)
	}
	srv.publishLocalAccess()
	if srv.localToken == "" || srv.localAccessPath == "" {
		t.Fatal("no local token was made")
	}
	if !srv.setupAllowed() {
		t.Error("first-run setup closed because a local access file was written")
	}
}

// A file that cannot be written is a warning: the daemon starts, and simply
// accepts no local token.
func TestUnwritableLocalAccessDoesNotStopTheDaemon(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "a-file")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "secret", LocalAccessPath: filepath.Join(blocker, LocalAccessFileName)})
	if err != nil {
		t.Fatalf("New refused to start: %v", err)
	}
	srv.publishLocalAccess()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/servers", nil)
	req.Header.Set("Authorization", "Bearer "+srv.localToken)
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("a token that was never written is being accepted: code = %d", rec.Code)
	}
}

func TestReadLocalAccessRefusesHalfAFile(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"empty":    "",
		"not json": "url: x",
		"no token": `{"url":"http://127.0.0.1:8090"}`,
		"no url":   `{"token":"abc"}`,
	} {
		p := filepath.Join(dir, strings.ReplaceAll(name, " ", "-"))
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if a, err := ReadLocalAccess(p); err == nil {
			t.Errorf("%s: read as %+v, want an error", name, a)
		}
	}
	if _, err := ReadLocalAccess(filepath.Join(dir, "missing")); !os.IsNotExist(err) {
		t.Errorf("a missing file: %v, want not-exist", err)
	}
}

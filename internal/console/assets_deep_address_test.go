package console

import (
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
)

// assetRefRE reads the src= and href= values in index.html; the test keeps
// the ones that name a file (an extension), which leaves out links out,
// in-page anchors and the nav's route links.
var assetRefRE = regexp.MustCompile(`\s(?:src|href)="([^"#][^"]*)"`)

// TestShellLoadsFromADeepAddress: the shell is served for any address without
// a dot in its last segment (/events/, /storage/x), and the browser resolves
// the files the shell names against that address. A relative "app.js" opened
// from /storage/ asked for /storage/app.js, which the server refuses on
// purpose (a missing asset must not be answered with the page), so the page
// came up blank: no script, no styles, no error on screen. Every file the
// shell loads must resolve to itself from any address the shell is served at.
func TestShellLoadsFromADeepAddress(t *testing.T) {
	raw, err := os.ReadFile("assets/index.html")
	if err != nil {
		t.Fatal(err)
	}
	var refs []string
	for _, m := range assetRefRE.FindAllStringSubmatch(string(raw), -1) {
		if strings.Contains(m[1], ":") || path.Ext(strings.SplitN(m[1], "?", 2)[0]) == "" {
			continue
		}
		refs = append(refs, m[1])
	}
	for _, want := range []string{"app.js", "style.css"} {
		found := false
		for _, r := range refs {
			found = found || strings.HasSuffix(r, want)
		}
		if !found {
			t.Fatalf("index.html loads no %s that this guard can see (read %q): the pattern broke, not the page", want, refs)
		}
	}
	srv := newTestServer(t)
	for _, page := range []string{"/", "/events", "/events/", "/storage/", "/storage/x", "/recover/a/b"} {
		base, err := url.Parse("http://127.0.0.1:8090" + page)
		if err != nil {
			t.Fatal(err)
		}
		shell := httptest.NewRecorder()
		srv.Handler().ServeHTTP(shell, httptest.NewRequest("GET", base.String(), nil))
		if shell.Code != 200 {
			t.Fatalf("%s: the shell answered %d", page, shell.Code)
		}
		for _, ref := range refs {
			u, err := url.Parse(ref)
			if err != nil {
				t.Fatal(err)
			}
			abs := base.ResolveReference(u)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, httptest.NewRequest("GET", abs.String(), nil))
			if rec.Code != 200 || strings.Contains(rec.Body.String(), "<!DOCTYPE html") {
				t.Errorf("opened at %s, the shell's %q resolves to %s: HTTP %d, shell=%v; want the file itself",
					page, ref, abs.Path, rec.Code, strings.Contains(rec.Body.String(), "<!DOCTYPE html"))
			}
		}
	}
}

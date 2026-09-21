package consoleapp

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

// TestBannerURL (#1784): inside a container the console listens on 8090 while
// the host may publish another port, so the banner takes its scheme, host and
// path from BINTRAIL_CONSOLE_URL when set, and keeps the console's own query
// (the ?token= of token mode). A value it cannot use keeps the listen address
// and returns why.
func TestBannerURL(t *testing.T) {
	const listen = "http://127.0.0.1:8090/"
	const listenTok = "http://127.0.0.1:8090/?token=abc"
	for _, tc := range []struct {
		name, listen, public, want string
		wantErr                    bool
	}{
		{"unset", listen, "", listen, false},
		{"blank", listen, "   ", listen, false},
		{"host port, no slash", listen, "http://127.0.0.1:8091", "http://127.0.0.1:8091/", false},
		{"host port, slash", listen, "http://127.0.0.1:8091/", "http://127.0.0.1:8091/", false},
		{"spaces around", listen, "  http://127.0.0.1:8091/ \n", "http://127.0.0.1:8091/", false},
		{"token kept", listenTok, "http://127.0.0.1:8091", "http://127.0.0.1:8091/?token=abc", false},
		{"proxy path", listenTok, "https://dbtrail.example.com/console/", "https://dbtrail.example.com/console/?token=abc", false},
		{"its own query dropped", listen, "http://127.0.0.1:8091/?x=1#top", "http://127.0.0.1:8091/", false},
		{"bare port", listen, "8091", listen, true},
		{"no scheme", listen, "127.0.0.1:8091", listen, true},
		{"other scheme", listen, "ftp://127.0.0.1:8091/", listen, true},
		{"no host", listen, "http:///x", listen, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := bannerURL(tc.listen, tc.public)
			if got != tc.want {
				t.Errorf("bannerURL(%q, %q) = %q, want %q", tc.listen, tc.public, got, tc.want)
			}
			if (err != nil) != tc.wantErr {
				t.Errorf("err = %v, want error %v", err, tc.wantErr)
			}
		})
	}
}

// TestConsoleBannerPrintsTheOpenedAddress checks the wiring: the banner the
// console prints at startup reads BINTRAIL_CONSOLE_URL, and without it prints
// the listen address as before.
func TestConsoleBannerPrintsTheOpenedAddress(t *testing.T) {
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	banner := func() string {
		t.Helper()
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		orig := os.Stderr
		os.Stderr = w
		printConsoleBanner(srv, "headline")
		os.Stderr = orig
		w.Close()
		b, _ := io.ReadAll(r)
		return string(b)
	}

	t.Setenv(consoleURLEnv, "http://127.0.0.1:8095")
	if got := banner(); !strings.Contains(got, "http://127.0.0.1:8095/?token=tok") {
		t.Errorf("banner with %s set:\n%s", consoleURLEnv, got)
	}
	t.Setenv(consoleURLEnv, "")
	if got := banner(); !strings.Contains(got, "http://127.0.0.1:0/?token=tok") {
		t.Errorf("banner without %s:\n%s", consoleURLEnv, got)
	}
}

package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// newFlashbackStatusServer builds a token-authenticated server with the embedded
// time-travel port reported at listen ("" = the port is off, as on serve).
func newFlashbackStatusServer(t *testing.T, listen string) *Server {
	t.Helper()
	s, err := New(Config{Listen: "127.0.0.1:8090", Token: "secret-tok", FlashbackListen: listen})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestFlashbackAPI_Off pins the standalone-serve / not-opted-in shape: the
// port reports off and carries NO address fields, so the Connect page cannot
// render a stale or guessed address as reachable.
func TestFlashbackAPI_Off(t *testing.T) {
	s := newFlashbackStatusServer(t, "")
	rec := doJSON(t, s, "GET", "/api/flashback", "secret-tok")
	if rec.Code != 200 {
		t.Fatalf("code = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body.String())
	}
	if got["enabled"] != false {
		t.Errorf("enabled = %v, want false", got["enabled"])
	}
	for _, k := range []string{"listen", "host", "port", "routing"} {
		if _, present := got[k]; present {
			t.Errorf("off state serializes %q = %v; neither an address nor a routing block may appear when the port is off", k, got[k])
		}
	}
}

// TestFlashbackAPI_On drives the enabled shape through the real route for a
// loopback bind and the three wildcard spellings: the split host/port the
// mysql line needs, with host EMPTY on a wildcard bind (the daemon cannot
// know the name the browser reaches it by).
func TestFlashbackAPI_On(t *testing.T) {
	cases := []struct {
		listen, wantHost, wantPort string
	}{
		{"127.0.0.1:3308", "127.0.0.1", "3308"},
		{"db-host.internal:13308", "db-host.internal", "13308"},
		{":3308", "", "3308"},
		{"0.0.0.0:3308", "", "3308"},
		{"[::]:3308", "", "3308"},
	}
	for _, tc := range cases {
		t.Run(tc.listen, func(t *testing.T) {
			s := newFlashbackStatusServer(t, tc.listen)
			rec := doJSON(t, s, "GET", "/api/flashback", "secret-tok")
			if rec.Code != 200 {
				t.Fatalf("code = %d, want 200: %s", rec.Code, rec.Body.String())
			}
			var got flashbackStatusDTO
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode: %v (%s)", err, rec.Body.String())
			}
			if !got.Enabled {
				t.Errorf("enabled = false, want true for a bound port")
			}
			if got.Listen != tc.listen {
				t.Errorf("listen = %q, want the configured %q", got.Listen, tc.listen)
			}
			if got.Host != tc.wantHost || got.Port != tc.wantPort {
				t.Errorf("host/port = %q/%q, want %q/%q", got.Host, got.Port, tc.wantHost, tc.wantPort)
			}
			// The port authenticates on the console token; the status must
			// describe the rule, never carry the value.
			if strings.Contains(rec.Body.String(), "secret-tok") {
				t.Errorf("response leaks the console token: %s", rec.Body.String())
			}
		})
	}
}

// TestFlashbackAPI_RequiresAuth: the address is daemon configuration behind
// the same credential as every /api route.
func TestFlashbackAPI_RequiresAuth(t *testing.T) {
	s := newFlashbackStatusServer(t, "127.0.0.1:3308")
	if rec := doJSON(t, s, "GET", "/api/flashback", ""); rec.Code != 401 {
		t.Errorf("without token: code = %d, want 401", rec.Code)
	}
}

package consoleapp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v2"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// #2318, the two halves together: a router's console counts decisions and
// serves them where its metrics are; a second console, given that address
// as an operator types it, answers GET /api/router with them. Then the
// router goes away, and the answer is an error with no numbers.
func TestRouterNumbers_reachTheWebInterface(t *testing.T) {
	router, err := console.New(console.Config{Token: "router-token", SQLMaxInFlight: 3})
	if err != nil {
		t.Fatal(err)
	}
	if r := router.SQLSandbox(); r != nil {
		t.Cleanup(r.Close)
	}
	for range 4 {
		router.RecordRouteDecision("srv-a", "copy", "expensive_plan")
	}
	router.RecordRouteDecision("srv-a", "mysql", "cheap_plan")
	router.RecordRouteDecision("srv-a", "refused", "copy_wait_timeout")
	last := &routerCopySnapshots{}
	snap := time.Date(2026, 10, 11, 14, 19, 0, 0, time.UTC)
	last.set(map[string]time.Time{"srv-a": snap}, time.Now())

	addr := freeAddr(t)
	stop, err := streamrun.StartMetricsServerWith(addr, map[string]http.Handler{
		"GET " + routerStatusPath: routerStatusHandler(routerStatusSettings(), router.RouterNumbers, last),
	})
	if err != nil {
		t.Fatal(err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			stop()
		}
	})

	resolved, err := console.ParseRouterURL("http://" + addr)
	if err != nil {
		t.Fatal(err)
	}
	web, err := console.New(console.Config{Listen: "127.0.0.1:8090", Token: "t", RouterURL: resolved})
	if err != nil {
		t.Fatal(err)
	}
	if r := web.SQLSandbox(); r != nil {
		t.Cleanup(r.Close)
	}
	type view struct {
		Configured bool   `json:"configured"`
		URL        string `json:"url"`
		Error      string `json:"error"`
		ReadAt     string `json:"read_at"`
		Status     *struct {
			Listen   string `json:"listen"`
			ReadOnly bool   `json:"read_only"`
			Pool     *struct {
				Workers int `json:"workers"`
			} `json:"pool"`
			Servers map[string]struct {
				Copy, MySQL, Refused uint64
				Reasons              map[string]uint64
				CopySnapshot         string `json:"copy_snapshot"`
			} `json:"servers"`
		} `json:"status"`
	}
	ask := func(token string) (int, view) {
		t.Helper()
		req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/router", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		web.Handler().ServeHTTP(rec, req)
		var v view
		if rec.Code == 200 {
			if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
				t.Fatalf("decode %s: %v", rec.Body, err)
			}
		}
		return rec.Code, v
	}
	if code, _ := ask(""); code != http.StatusUnauthorized {
		t.Errorf("GET /api/router with no credential = %d, want 401", code)
	}
	code, got := ask("t")
	if code != 200 || !got.Configured || got.Error != "" || got.Status == nil || got.ReadAt == "" || got.URL != "http://"+addr+"/status" {
		t.Fatalf("GET /api/router = %d %+v", code, got)
	}
	a := got.Status.Servers["srv-a"]
	if a.Copy != 4 || a.MySQL != 1 || a.Refused != 1 || a.Reasons["copy_wait_timeout"] != 1 || a.CopySnapshot != "2026-10-11T14:19:00Z" {
		t.Errorf("srv-a as the web interface got it = %+v", a)
	}
	if got.Status.Pool == nil || got.Status.Pool.Workers != 3 || got.Status.Listen != "127.0.0.1:3309" || !got.Status.ReadOnly {
		t.Errorf("pool and settings = %+v", got.Status)
	}

	stop()
	stopped = true
	code, got = ask("t")
	if code != 200 || !got.Configured || got.Status != nil || got.ReadAt != "" || !strings.Contains(got.Error, "could not be reached") {
		t.Errorf("with the router stopped: %d %+v, want an error and no numbers", code, got)
	}
}

// --router-url, the environment variable, and a value that cannot be used:
// it is warned about and kept for the page, and never stops the start of a
// process that captures.
func TestResolveRouterURL(t *testing.T) {
	run := func(env string, args ...string) (url, why, warned string) {
		t.Helper()
		t.Setenv("BINTRAIL_CONSOLE_ROUTER_URL", env)
		oldURL, oldErr := upRouterURL, upRouterURLError
		t.Cleanup(func() { upRouterURL, upRouterURLError = oldURL, oldErr })
		upRouterURL, upRouterURLError = "", "left over"
		cmd := &cobra.Command{Use: "watch"}
		cmd.Flags().StringVar(&upRouterURL, "router-url", "", "")
		if err := cmd.ParseFlags(args); err != nil {
			t.Fatal(err)
		}
		var out strings.Builder
		resolveRouterURL(cmd, &out)
		return upRouterURL, upRouterURLError, out.String()
	}
	for _, c := range []struct {
		what, env string
		args      []string
		want      string
	}{
		{"neither", "", nil, ""},
		{"the variable", "http://router:9091", nil, "http://router:9091/status"},
		{"the flag wins", "http://router:9091", []string{"--router-url", "http://127.0.0.1:9191/"}, "http://127.0.0.1:9191/status"},
		{"an empty flag turns the variable off", "http://router:9091", []string{"--router-url", ""}, ""},
	} {
		if got, why, warned := run(c.env, c.args...); got != c.want || why != "" || warned != "" {
			t.Errorf("%s: %q, error %q, warned %q; want %q and nothing said", c.what, got, why, warned, c.want)
		}
	}
	got, why, warned := run("router:9091")
	if got != "" || !strings.Contains(why, "BINTRAIL_CONSOLE_ROUTER_URL: ") || !strings.Contains(why, "http://") || !strings.Contains(warned, "WARNING: "+why) || !strings.Contains(warned, "everything else starts as usual") {
		t.Errorf("a variable with no scheme: url %q, error %q, warned %q; want no address, the reason naming the variable, and a warning", got, why, warned)
	}
	got, why, warned = run("", "--router-url", "http://u:hunter2@router:9091")
	if got != "" || !strings.Contains(why, "--router-url: ") || strings.Contains(why+warned, "hunter2") {
		t.Errorf("a flag with a password: url %q, error %q, warned %q; want the reason naming the flag and no password repeated", got, why, warned)
	}
}

// The shipped compose file gives the capture service the address its own
// router service serves its numbers on: the same host name and port.
func TestComposeRouter_theWebInterfaceIsGivenTheRoutersAddress(t *testing.T) {
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatal(err)
	}
	var doc composeRouterFile
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	url, _ := doc.Services["bintrail"].Environment["BINTRAIL_CONSOLE_ROUTER_URL"].(string)
	if url != "${ROUTER_URL-http://router:9091}" {
		t.Errorf("bintrail's BINTRAIL_CONSOLE_ROUTER_URL = %q: want ${ROUTER_URL-http://router:9091} (unset = the router service; set empty = nothing shown)", url)
	}
	addr, _ := doc.Services["router"].Environment["BINTRAIL_ROUTER_METRICS_ADDR"].(string)
	if addr != "${ROUTER_METRICS_ADDR-:9091}" {
		t.Errorf("router's BINTRAIL_ROUTER_METRICS_ADDR = %q: the port the web interface asks is 9091", addr)
	}
	if _, ok := doc.Services["router"]; !ok {
		t.Error("no service named router: the address names it")
	}
	if resolved, err := console.ParseRouterURL("http://router:9091"); err != nil || resolved != "http://router:9091/status" {
		t.Errorf("the compose default does not resolve: %q, %v", resolved, err)
	}
}

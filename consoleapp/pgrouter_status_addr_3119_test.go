package consoleapp

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
)

// The status address taken by something else does not keep the PostgreSQL
// port closed (#3119): the router says so and goes on without its status.
func TestPGRouterStatusAddrTaken(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	addr := taken.Addr().String()

	var out bytes.Buffer
	stop := startPGRouterStatus(addr, map[string]http.Handler{}, &out)
	if stop == nil {
		t.Fatal("no stop function for a status address that is taken")
	}
	stop() // Nothing was started: stopping it is harmless.
	got := out.String()
	for _, want := range []string{"WARNING", addr, "The PostgreSQL port is served without them", "BINTRAIL_PG_ROUTER_STATUS_ADDR", "--status-addr", "BINTRAIL_CONSOLE_PG_ROUTER_URL", "restart the router"} {
		if !strings.Contains(got, want) {
			t.Errorf("the warning does not say %q:\n%s", want, got)
		}
	}
	if !strings.HasSuffix(got, "\n") || strings.Count(got, "\n") != 1 {
		t.Errorf("the warning is not one line:\n%q", got)
	}
}

// A free status address is served, and nothing is warned about.
func TestPGRouterStatusAddrFree(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	probe.Close()

	var out bytes.Buffer
	stop := startPGRouterStatus(addr, map[string]http.Handler{
		"GET " + routerStatusPath: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "here") }),
	}, &out)
	defer stop()
	if out.Len() != 0 {
		t.Fatalf("a warning for an address that was free:\n%s", out.String())
	}
	resp, err := http.Get("http://" + addr + routerStatusPath)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if body, _ := io.ReadAll(resp.Body); string(body) != "here" {
		t.Fatalf("the status answered %q", body)
	}
}

// The command itself goes through startPGRouterStatus and no longer returns
// an error for the status address.
func TestPGRouterRunDoesNotExitOnStatusAddr(t *testing.T) {
	src, err := os.ReadFile("pgrouter.go")
	if err != nil {
		t.Fatal(err)
	}
	i := strings.Index(string(src), "func runPGRouter(")
	if i < 0 {
		t.Fatal("runPGRouter not found in pgrouter.go")
	}
	body := string(src)[i:]
	if j := strings.Index(body, "\n}\n"); j > 0 {
		body = body[:j]
	}
	if !strings.Contains(body, "startPGRouterStatus(st.StatusAddr,") {
		t.Error("runPGRouter does not start its status through startPGRouterStatus")
	}
	if strings.Contains(body, "cannot serve its status") || strings.Contains(body, "StartMetricsServerWith") {
		t.Error("runPGRouter opens the status address itself, where a failure can stop the command")
	}
}

package installer

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// stubs stand in for the tools install.sh calls, so the script runs whole
// with no Docker and no network. lsof reports a port busy when it is listed
// in BUSY_PORTS; curl serves the repository's docker-compose.yml; docker
// records what it was asked to do.
var stubs = map[string]string{
	"docker": `#!/bin/sh
echo "docker $*" >> "$STUB_LOG"
exit 0
`,
	"lsof": `#!/bin/sh
for a in "$@"; do case "$a" in -iTCP:*) p=${a#-iTCP:};; esac; done
for b in $BUSY_PORTS; do [ "$b" = "$p" ] && exit 0; done
exit 1
`,
	"curl": `#!/bin/sh
out=""; url=""
while [ $# -gt 0 ]; do
  case "$1" in -o) out=$2; shift 2;; -*) shift;; *) url=$1; shift;; esac
done
case "$url" in *docker-compose.yml) cp "$COMPOSE_SRC" "$out";; esac
exit 0
`,
}

type run struct {
	out     string
	failed  bool
	dir     string // the stack directory the installer was pointed at
	upCalls int
}

func install(t *testing.T, env ...string) run {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range stubs {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	dir := filepath.Join(tmp, "stack")
	log := filepath.Join(tmp, "docker.log")
	cmd := exec.Command("sh", filepath.Join(root, "install.sh"))
	cmd.Env = append([]string{
		"PATH=" + bin + ":/usr/bin:/bin",
		"HOME=" + tmp,
		"DBTRAIL_DIR=" + dir,
		"DBTRAIL_NO_OPEN=1",
		"STUB_LOG=" + log,
		"COMPOSE_SRC=" + filepath.Join(root, "docker-compose.yml"),
	}, env...)
	out, err := cmd.CombinedOutput()
	calls, _ := os.ReadFile(log)
	return run{out: string(out), failed: err != nil, dir: dir, upCalls: strings.Count(string(calls), "compose up")}
}

func (r run) compose(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(r.dir, "docker-compose.yml"))
	if err != nil {
		t.Fatalf("no compose file written: %v\n%s", err, r.out)
	}
	return string(b)
}

// A taken console port stops the installer before it touches anything, with
// a command that works when pasted: the variables on `sh`, the side of the
// pipe that reads them (on `curl` they never reach the installer), the full
// URL, and a port that is actually free.
func TestInstaller_aTakenConsolePortSuggestsACommandThatWorks(t *testing.T) {
	r := install(t, "BUSY_PORTS=8090 8091")
	if !r.failed || r.upCalls != 0 {
		t.Fatalf("the installer went on with port 8090 taken (failed=%v up=%d):\n%s", r.failed, r.upCalls, r.out)
	}
	if _, err := os.Stat(r.dir); err == nil {
		t.Error("the installer created the stack directory before refusing")
	}
	want := "curl -fsSL https://raw.githubusercontent.com/dbtrail/dbtrail/main/install.sh | DBTRAIL_DIR=" + r.dir + " DBTRAIL_PORT=8092 sh"
	if !strings.Contains(r.out, want) {
		t.Errorf("the suggestion is not the working command on the first free port.\nwant: %s\ngot:\n%s", want, r.out)
	}
	if strings.Contains(r.out, "DBTRAIL_PORT=9090") {
		t.Error("the suggestion names 9090, the stack's own metrics port")
	}
}

// A taken 9090 (Prometheus's default port) moves the stack's metrics to the
// next free port instead of failing on Docker's bind error, and says so.
func TestInstaller_aTakenMetricsPortMovesTheMetrics(t *testing.T) {
	r := install(t, "BUSY_PORTS=9090")
	if r.failed || r.upCalls != 1 {
		t.Fatalf("install failed with only 9090 taken (up=%d):\n%s", r.upCalls, r.out)
	}
	c := r.compose(t)
	if !strings.Contains(c, `"127.0.0.1:9091:9090"`) || !strings.Contains(c, `"127.0.0.1:8090:8090"`) {
		t.Errorf("want metrics on 9091 and the console on 8090 in the compose file:\n%s", portLines(c))
	}
	if !strings.Contains(r.out, "port 9090 is taken, so metrics are on 9091") {
		t.Errorf("the move is not said:\n%s", r.out)
	}
}

// The console asked for on 9090 would collide with the metrics mapping; the
// metrics step aside.
func TestInstaller_theConsoleOn9090MovesTheMetrics(t *testing.T) {
	r := install(t, "DBTRAIL_PORT=9090")
	if r.failed {
		t.Fatalf("install failed:\n%s", r.out)
	}
	c := r.compose(t)
	if !strings.Contains(c, `"127.0.0.1:9090:8090"`) || !strings.Contains(c, `"127.0.0.1:9091:9090"`) {
		t.Errorf("want the console on 9090 and the metrics on 9091:\n%s", portLines(c))
	}
}

// The console banner names the host port (#1784): the container listens on
// 8090 whatever the host publishes, so the compose file carries the address
// people open, and the installer moves it with the port.
func TestInstaller_aMovedConsoleTellsTheBannerItsPort(t *testing.T) {
	r := install(t, "DBTRAIL_PORT=8095")
	if r.failed {
		t.Fatalf("install failed:\n%s", r.out)
	}
	c := r.compose(t)
	if !strings.Contains(c, "BINTRAIL_CONSOLE_URL: http://127.0.0.1:8095/") {
		t.Errorf("the banner address was not moved to 8095:\n%s", portLines(c))
	}
	if strings.Contains(c, "BINTRAIL_CONSOLE_URL: http://127.0.0.1:8090/") {
		t.Error("the banner address still names 8090")
	}
	r = install(t)
	if r.failed || !strings.Contains(r.compose(t), "BINTRAIL_CONSOLE_URL: http://127.0.0.1:8090/") {
		t.Errorf("with nothing moved the banner address is not the published 8090:\n%s", portLines(r.compose(t)))
	}
}

// A compose file from before #1784 (an older DBTRAIL_REF) has no banner
// address; moving the console port must still install, not die on the missing
// line.
func TestInstaller_anOlderComposeWithoutTheBannerAddressStillInstalls(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for _, l := range strings.Split(string(b), "\n") {
		if !strings.Contains(l, "BINTRAIL_CONSOLE_URL:") {
			kept = append(kept, l)
		}
	}
	old := filepath.Join(t.TempDir(), "docker-compose.yml")
	if err := os.WriteFile(old, []byte(strings.Join(kept, "\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	r := install(t, "DBTRAIL_PORT=8095", "COMPOSE_SRC="+old)
	if r.failed || !strings.Contains(r.compose(t), `"127.0.0.1:8095:8090"`) {
		t.Errorf("an older compose file with the console on 8095 did not install:\n%s", r.out)
	}
}

// A metrics port the operator chose is theirs: taken, it is refused, not moved.
func TestInstaller_aChosenMetricsPortIsNeverMoved(t *testing.T) {
	r := install(t, "BUSY_PORTS=9095", "DBTRAIL_METRICS_PORT=9095")
	if !r.failed || !strings.Contains(r.out, "metrics port 9095 is already in use") {
		t.Errorf("a taken, explicitly chosen metrics port was not refused:\n%s", r.out)
	}
	r = install(t, "DBTRAIL_METRICS_PORT=9095")
	if r.failed || !strings.Contains(r.compose(t), `"127.0.0.1:9095:9090"`) {
		t.Errorf("a free, explicitly chosen metrics port was not used:\n%s", r.out)
	}
}

// With nothing taken the compose file is the published one, untouched.
func TestInstaller_nothingTakenLeavesThePublishedPorts(t *testing.T) {
	r := install(t)
	if r.failed || r.upCalls != 1 {
		t.Fatalf("install failed:\n%s", r.out)
	}
	c := r.compose(t)
	if !strings.Contains(c, `"127.0.0.1:8090:8090"`) || !strings.Contains(c, `"127.0.0.1:9090:9090"`) {
		t.Errorf("the published ports changed:\n%s", portLines(c))
	}
	if strings.Contains(r.out, "metrics are on") {
		t.Error("a metrics move was reported with nothing taken")
	}
}

func portLines(c string) string {
	var b strings.Builder
	for _, l := range strings.Split(c, "\n") {
		if strings.Contains(l, "127.0.0.1:") {
			b.WriteString(l + "\n")
		}
	}
	return b.String()
}

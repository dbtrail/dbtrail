package installer

import (
	"go.yaml.in/yaml/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// stubs stand in for the tools install.sh calls, so the script runs whole
// with no Docker and no network. lsof reports a port busy when it is listed
// in BUSY_PORTS; curl serves the repository's docker-compose.yml; docker
// records what it was asked to do, and plays a stopped Docker
// (STUB_DOCKER_DOWN) or a missing Compose (STUB_NO_COMPOSE) when asked;
// `docker info --format` prints STUB_MEM_TOTAL, the memory Docker reports;
// `docker compose pull` also writes down the BINTRAIL_TAG line .env holds at
// that moment, so a test sees what the pull was run with.
//
// curl writes every URL it is asked for to CURL_LOG and answers GitHub's
// newest-release lookup with STUB_RELEASE_BODY (by default releaseBody), or
// fails the way `curl -f` does on a 403 when STUB_RELEASE_FAIL is set.
var stubs = map[string]string{
	"docker": `#!/bin/sh
echo "docker $*" >> "$STUB_LOG"
case "$1" in
  info) [ -n "$STUB_DOCKER_DOWN" ] && exit 1
        [ "$2" = --format ] && printf '%s\n' "$STUB_MEM_TOTAL" ;;
  compose) [ "$2" = version ] && [ -n "$STUB_NO_COMPOSE" ] && exit 1 ;;
esac
case "$1 $2" in
  "compose pull") [ -f .env ] && grep BINTRAIL_TAG .env | sed 's/^/pull sees /' >> "$STUB_LOG"
                  [ -n "$STUB_PULL_FAIL" ] && exit 1 ;;
esac
exit 0
`,
	"lsof": `#!/bin/sh
for a in "$@"; do case "$a" in -iTCP:*) p=${a#-iTCP:};; esac; done
for b in $BUSY_PORTS; do [ "$b" = "$p" ] && exit 0; done
exit 1
`,
	// ss is the kernel's list of listening sockets, which shows the ports
	// Docker holds even to a user who is not root (#2831): one line per port
	// in SS_PORTS, in the shape `ss -ltn` prints. nc connects to the ports
	// in NC_PORTS and to no other. Both are here so the tests never ask the
	// machine they run on what it listens to.
	"ss": `#!/bin/sh
echo "State  Recv-Q Send-Q Local Address:Port  Peer Address:Port"
for b in $SS_PORTS; do echo "LISTEN 0      4096       127.0.0.1:$b      0.0.0.0:*"; done
exit 0
`,
	"nc": `#!/bin/sh
for a in "$@"; do p=$a; done
for b in $NC_PORTS; do [ "$b" = "$p" ] && exit 0; done
exit 1
`,
	"curl": `#!/bin/sh
out=""; url=""
while [ $# -gt 0 ]; do
  case "$1" in -o) out=$2; shift 2;; --max-time) shift 2;; -*) shift;; *) url=$1; shift;; esac
done
echo "$url" >> "$CURL_LOG"
case "$url" in
  https://api.github.com/repos/dbtrail/dbtrail/releases/latest)
    [ -n "$STUB_RELEASE_FAIL" ] && exit 22
    if [ -n "$out" ]; then printf '%s' "$STUB_RELEASE_BODY" > "$out"; else printf '%s' "$STUB_RELEASE_BODY"; fi;;
  *docker-compose.yml) [ -n "$STUB_COMPOSE_FAIL" ] && exit 22; cp "$COMPOSE_SRC" "$out";;
  *dbtrail.sh) [ -n "$STUB_NO_CLI" ] && exit 22; cp "$CLI_SRC" "$out";;
esac
exit 0
`,
}

type run struct {
	out     string
	failed  bool
	dir     string // the stack directory the installer was pointed at
	upCalls int
	calls   string // every docker call, in order
	urls    string // every URL curl was asked for, one per line
}

// releaseBody is what GitHub's API answers for the newest release, cut to
// the fields around tag_name and laid out the way the API lays it out.
const releaseBody = `{
  "url": "https://api.github.com/repos/dbtrail/dbtrail/releases/1",
  "html_url": "https://github.com/dbtrail/dbtrail/releases/tag/v0.130.0",
  "id": 1,
  "tag_name": "v0.130.0",
  "target_commitish": "main",
  "name": "v0.130.0",
  "draft": false,
  "prerelease": false,
  "body": "## Changelog, with a \"quote\", and commas"
}`

func install(t *testing.T, env ...string) run {
	t.Helper()
	return installWith(t, nil, env...)
}

// installWith runs the installer after prep has put files in the stack
// directory (which exists, empty, when prep runs): an .env the operator
// wrote before installing, or a compose file from an earlier run.
func installWith(t *testing.T, prep func(dir string), env ...string) run {
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
	if prep != nil {
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		prep(dir)
	}
	log := filepath.Join(tmp, "docker.log")
	curlLog := filepath.Join(tmp, "curl.log")
	cmd := exec.Command("sh", filepath.Join(root, "install.sh"))
	cmd.Env = append([]string{
		"PATH=" + bin + ":/usr/bin:/bin",
		"HOME=" + tmp,
		"DBTRAIL_DIR=" + dir,
		"DBTRAIL_NO_OPEN=1",
		"STUB_LOG=" + log,
		"CURL_LOG=" + curlLog,
		"STUB_RELEASE_BODY=" + releaseBody,
		"COMPOSE_SRC=" + filepath.Join(root, "docker-compose.yml"),
		"CLI_SRC=" + filepath.Join(root, "dbtrail.sh"),
	}, env...)
	out, err := cmd.CombinedOutput()
	calls, _ := os.ReadFile(log)
	urls, _ := os.ReadFile(curlLog)
	return run{out: string(out), failed: err != nil, dir: dir, upCalls: strings.Count(string(calls), "compose up"), calls: string(calls), urls: string(urls)}
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

// The compose file carries one memory share per service (#2513): the
// daemon's and the router's, each overridable from .env, so the bundled
// stack adds up on the host instead of each process planning with all of it.
func TestInstaller_theComposeCarriesTheMemoryShares(t *testing.T) {
	r := install(t)
	if r.failed {
		t.Fatalf("install failed:\n%s", r.out)
	}
	c := r.compose(t)
	// Off by default (empty = the whole machine, as before): a share is a
	// choice an operator makes for a host the kernel runs short on, since
	// it also takes memory from the snapshot work and the tails.
	if !strings.Contains(c, "BINTRAIL_MEMORY_SHARE: ${DAEMON_MEMORY_SHARE:-}") {
		t.Error("the bintrail service has no memory share hook (BINTRAIL_MEMORY_SHARE: ${DAEMON_MEMORY_SHARE:-})")
	}
	if !strings.Contains(c, "BINTRAIL_MEMORY_SHARE: ${ROUTER_MEMORY_SHARE:-}") {
		t.Error("the router service has no memory share hook (BINTRAIL_MEMORY_SHARE: ${ROUTER_MEMORY_SHARE:-})")
	}
	// The key index switch (#2496) travels the same way: empty = the
	// daemon's default (on), KEY_INDEX=0 in .env turns it off.
	if !strings.Contains(c, "BINTRAIL_KEY_INDEX: ${KEY_INDEX:-}") {
		t.Error("the bintrail service has no key index hook (BINTRAIL_KEY_INDEX: ${KEY_INDEX:-})")
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

// A machine that pulled "latest" once keeps that image, and `up -d` starts it
// without asking the registry: a re-install on such a machine got a version
// months old with no word about it. The installer pulls before it starts.
func TestInstaller_pullsTheImagesBeforeStarting(t *testing.T) {
	r := install(t)
	if r.failed {
		t.Fatalf("the install failed:\n%s", r.out)
	}
	pull, up := strings.Index(r.calls, "compose pull"), strings.Index(r.calls, "compose up")
	if pull < 0 || up < 0 || pull > up {
		t.Fatalf("want a compose pull before compose up, got calls:\n%s", r.calls)
	}
}

// A pull that fails (offline, a registry hiccup) must not stop an install
// that can run on the images already here; it says what that means.
func TestInstaller_aFailedPullStartsWithTheImagesHere(t *testing.T) {
	r := install(t, "STUB_PULL_FAIL=1")
	if r.failed || r.upCalls != 1 {
		t.Fatalf("a failed pull stopped the install (failed=%v up=%d):\n%s", r.failed, r.upCalls, r.out)
	}
	if !strings.Contains(r.out, "could not download the images") {
		t.Errorf("a failed pull is not said:\n%s", r.out)
	}
}

// The MySQL-protocol port is off until someone turns it on, so a taken 3309
// must not stop the install: the stack comes up without publishing it, and
// the installer says so. With 3309 free the mapping is there.
func TestInstaller_aTakenMySQLPortIsLeftUnpublished(t *testing.T) {
	r := install(t)
	if r.failed || !strings.Contains(r.compose(t), `"127.0.0.1:3309:3309"`) {
		t.Fatalf("with 3309 free the MySQL port must be published on loopback (failed=%v):\n%s", r.failed, portLines(r.compose(t)))
	}
	if strings.Contains(r.out, "3309") {
		t.Errorf("nothing about 3309 should be said when it is free:\n%s", r.out)
	}

	r = install(t, "BUSY_PORTS=3309")
	if r.failed || r.upCalls != 1 {
		t.Fatalf("install failed with only 3309 taken (up=%d):\n%s", r.upCalls, r.out)
	}
	c := r.compose(t)
	if strings.Contains(c, `"127.0.0.1:3309:3309"`) {
		t.Errorf("a taken 3309 is still published:\n%s", portLines(c))
	}
	if !strings.Contains(c, `"127.0.0.1:8090:8090"`) || !strings.Contains(c, `"127.0.0.1:9090:9090"`) {
		t.Errorf("the other ports were disturbed:\n%s", portLines(c))
	}
	if !strings.Contains(r.out, "port 3309 is taken, so the MySQL port is not published") {
		t.Errorf("the skip is not said:\n%s", r.out)
	}
	// The next steps then end on a mysql line that would reach whatever holds
	// 3309, so they say to publish another port and use it after -P (#2637).
	// Putting the same 3309 line back would only fail again.
	if !strings.Contains(r.out, "publish another free port, such as 127.0.0.1:3310:3309") ||
		!strings.Contains(r.out, "use that number after -P") {
		t.Errorf("the next steps do not say how to reach the port when 3309 is taken:\n%s", r.out)
	}
	for _, h := range sentenceHits(t, r.out, r.dir) {
		t.Errorf("banned in a sentence the installer prints: %s", h)
	}
	// Docker Compose refuses a file where a service has a "ports:" key and no
	// entries under it ("services.router.ports must be a array"), and then
	// nothing starts, capture included. The mapping was the router's only one.
	var doc struct {
		Services map[string]map[string]any `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(c), &doc); err != nil {
		t.Fatalf("the rewritten compose file is not YAML: %v", err)
	}
	if len(doc.Services) < 3 {
		t.Fatalf("read %d services from the rewritten compose file", len(doc.Services))
	}
	for name, svc := range doc.Services {
		if ports, has := svc["ports"]; has {
			if list, ok := ports.([]any); !ok || len(list) == 0 {
				t.Errorf("service %s is left with an empty ports: key (%v), which Docker Compose refuses", name, ports)
			}
		}
	}
	// And the service that answers on the port is still there: only its
	// mapping went.
	if !strings.Contains(c, "BINTRAIL_ROUTER_LISTEN: 0.0.0.0:3309") {
		t.Errorf("the router service lost more than its port mapping")
	}
}

// The PostgreSQL-protocol port is off until someone turns it on too, so a
// taken 5433 is handled as a taken 3309 is: the stack comes up without
// publishing it and the installer says so. With 5433 free the mapping is
// there and nothing is said.
func TestInstaller_aTakenPostgreSQLPortIsLeftUnpublished(t *testing.T) {
	r := install(t)
	if r.failed || !strings.Contains(r.compose(t), `"127.0.0.1:5433:5433"`) {
		t.Fatalf("with 5433 free the PostgreSQL port must be published on loopback (failed=%v):\n%s", r.failed, portLines(r.compose(t)))
	}
	if strings.Contains(r.out, "5433") {
		t.Errorf("nothing about 5433 should be said when it is free:\n%s", r.out)
	}

	for _, env := range []string{"BUSY_PORTS=5433", "SS_PORTS=5433"} {
		r = install(t, env)
		if r.failed || r.upCalls != 1 {
			t.Fatalf("%s: install failed with only 5433 taken (up=%d):\n%s", env, r.upCalls, r.out)
		}
		c := r.compose(t)
		if strings.Contains(c, `"127.0.0.1:5433:5433"`) {
			t.Errorf("%s: a taken 5433 is still published:\n%s", env, portLines(c))
		}
		for _, kept := range []string{`"127.0.0.1:8090:8090"`, `"127.0.0.1:9090:9090"`, `"127.0.0.1:3309:3309"`} {
			if !strings.Contains(c, kept) {
				t.Errorf("%s: another port was disturbed (%s):\n%s", env, kept, portLines(c))
			}
		}
		if !strings.Contains(r.out, "port 5433 is taken, so the PostgreSQL port is not published") {
			t.Errorf("%s: the skip is not said:\n%s", env, r.out)
		}
		// The MySQL port's own notice is about 3309 only.
		if strings.Contains(r.out, "3309") {
			t.Errorf("%s: 3309 is free and is named:\n%s", env, r.out)
		}
		for _, h := range sentenceHits(t, r.out, r.dir) {
			t.Errorf("%s: banned in a sentence the installer prints: %s", env, h)
		}
		var doc struct {
			Services map[string]map[string]any `yaml:"services"`
		}
		if err := yaml.Unmarshal([]byte(c), &doc); err != nil {
			t.Fatalf("%s: the rewritten compose file is not YAML: %v", env, err)
		}
		svc, ok := doc.Services["pg-router"]
		if !ok {
			t.Fatalf("%s: the pg-router service is gone, not only its mapping", env)
		}
		if _, has := svc["ports"]; has {
			t.Errorf("%s: pg-router keeps a ports: key (%v); empty, Docker Compose refuses the file", env, svc["ports"])
		}
		if !strings.Contains(c, "BINTRAIL_PG_ROUTER_LISTEN: 0.0.0.0:5433") {
			t.Errorf("%s: the pg-router service lost more than its port mapping", env)
		}
	}

	// Both ports taken: each is left out, each is said.
	r = install(t, "BUSY_PORTS=3309 5433")
	c := r.compose(t)
	if r.failed || strings.Contains(c, `"127.0.0.1:3309:3309"`) || strings.Contains(c, `"127.0.0.1:5433:5433"`) {
		t.Fatalf("with both ports taken (failed=%v):\n%s", r.failed, portLines(c))
	}
	if !strings.Contains(r.out, "port 3309 is taken") || !strings.Contains(r.out, "port 5433 is taken") {
		t.Errorf("both skips must be said:\n%s", r.out)
	}
}

// The pg-router service as the stack ships it: the PostgreSQL port on the
// host loopback, listening on every interface inside its container, reading
// the state volume and never writing it.
func TestCompose_pgRouterService(t *testing.T) {
	var doc struct {
		Services map[string]struct {
			Command     []string          `yaml:"command"`
			Ports       []string          `yaml:"ports"`
			Environment map[string]string `yaml:"environment"`
			Volumes     []string          `yaml:"volumes"`
			Profiles    []string          `yaml:"profiles"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(install(t).compose(t)), &doc); err != nil {
		t.Fatal(err)
	}
	svc, ok := doc.Services["pg-router"]
	if !ok {
		t.Fatal("docker-compose.yml has no pg-router service")
	}
	if len(svc.Command) != 1 || svc.Command[0] != "pg-router" {
		t.Errorf("command = %v", svc.Command)
	}
	if len(svc.Ports) != 1 || svc.Ports[0] != "127.0.0.1:5433:5433" {
		t.Errorf("ports = %v, want the one loopback mapping", svc.Ports)
	}
	// It starts with the stack, as the router does: behind a profile the web
	// interface turns the port on and nothing listens.
	if len(svc.Profiles) != 0 {
		t.Errorf("profiles = %v, want none", svc.Profiles)
	}
	if got := svc.Environment["BINTRAIL_PG_ROUTER_LISTEN"]; got != "0.0.0.0:5433" {
		t.Errorf("BINTRAIL_PG_ROUTER_LISTEN = %q", got)
	}
	for _, k := range []string{"BINTRAIL_CONSOLE_SERVERS", "BINTRAIL_CONSOLE_AUTH", "BINTRAIL_CONSOLE_MCP_TOKEN_FILE"} {
		if svc.Environment[k] != doc.Services["router"].Environment[k] || svc.Environment[k] == "" {
			t.Errorf("%s = %q, want the router's %q", k, svc.Environment[k], doc.Services["router"].Environment[k])
		}
	}
	if len(svc.Volumes) != 1 || svc.Volumes[0] != "bintrail-state:/var/lib/bintrail:ro" {
		t.Errorf("volumes = %v, want the state volume read-only", svc.Volumes)
	}
}

// A second installation beside a first (#2815: an installation serves one
// database family, and the other family goes in an installation of its own):
// the folder and the console's port are named, and the ports the first one
// holds are handled as on any install. This is the command docs/install.md
// gives, with the first installation's four ports taken.
func TestInstaller_aSecondInstallationBesideAFirst(t *testing.T) {
	r := install(t, "DBTRAIL_PORT=8091", "BUSY_PORTS=8090 9090 3309 5433")
	if r.failed || r.upCalls != 1 {
		t.Fatalf("the second installation did not start (failed=%v, up=%d):\n%s", r.failed, r.upCalls, r.out)
	}
	c := r.compose(t)
	if !strings.Contains(c, `"127.0.0.1:8091:8090"`) {
		t.Errorf("the web interface is not on the port asked for:\n%s", portLines(c))
	}
	for _, taken := range []string{`"127.0.0.1:8090:8090"`, `"127.0.0.1:9090:9090"`, `"127.0.0.1:3309:3309"`, `"127.0.0.1:5433:5433"`} {
		if strings.Contains(c, taken) {
			t.Errorf("a port the first installation holds is published again (%s):\n%s", taken, portLines(c))
		}
	}
}

// On Linux, lsof run by a user who is not root does not list the sockets of
// root's docker-proxy, so it calls every port Docker publishes free (#2831).
// The kernel's list (ss) shows them: a port only ss sees is taken all the
// same, and the three ports get what a taken port gets.
func TestInstaller_aPortOnlyTheKernelsListShowsIsTaken(t *testing.T) {
	// The console's port: stopped before anything is touched, with the
	// command on the first port NO probe sees taken.
	r := install(t, "SS_PORTS=8090 8091")
	if !r.failed || r.upCalls != 0 {
		t.Fatalf("the installer went on with 8090 held by Docker (failed=%v up=%d):\n%s", r.failed, r.upCalls, r.out)
	}
	if _, err := os.Stat(r.dir); err == nil {
		t.Error("the installer created the stack directory before refusing")
	}
	if want := "DBTRAIL_PORT=8092 sh"; !strings.Contains(r.out, want) {
		t.Errorf("the suggestion is not on the first free port (%s):\n%s", want, r.out)
	}

	// The metrics port and the MySQL port: moved and left unpublished.
	r = install(t, "SS_PORTS=9090 3309")
	if r.failed || r.upCalls != 1 {
		t.Fatalf("install failed with 9090 and 3309 held by Docker (up=%d):\n%s", r.upCalls, r.out)
	}
	c := r.compose(t)
	if !strings.Contains(c, `"127.0.0.1:9091:9090"`) || strings.Contains(c, `"127.0.0.1:9090:9090"`) {
		t.Errorf("the metrics were not moved off 9090:\n%s", portLines(c))
	}
	if strings.Contains(c, `"127.0.0.1:3309:3309"`) {
		t.Errorf("3309 is published though Docker holds it:\n%s", portLines(c))
	}
}

// Each probe is asked, and one that says "taken" is enough: lsof for the
// user's own listeners, ss for everyone's, a connection attempt where the
// first two are silent. A port none of them sees is free.
func TestInstaller_anyProbeThatSeesAPortMakesItTaken(t *testing.T) {
	for _, env := range []string{"BUSY_PORTS=9090", "SS_PORTS=9090", "NC_PORTS=9090"} {
		r := install(t, env)
		if r.failed {
			t.Fatalf("%s: install failed:\n%s", env, r.out)
		}
		if c := r.compose(t); !strings.Contains(c, `"127.0.0.1:9091:9090"`) {
			t.Errorf("%s: 9090 was not read as taken:\n%s", env, portLines(c))
		}
	}
	// A longer port that ends in the same digits is another port.
	r := install(t, "SS_PORTS=19090 90900 909")
	if r.failed {
		t.Fatalf("install failed:\n%s", r.out)
	}
	if c := r.compose(t); !strings.Contains(c, `"127.0.0.1:9090:9090"`) {
		t.Errorf("9090 was read as taken because 19090 is:\n%s", portLines(c))
	}
}

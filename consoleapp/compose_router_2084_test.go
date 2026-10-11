package consoleapp

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"
)

// #2084: the router service of the shipped compose file. It runs beside the
// capture service and has to agree with it on every path they share, read
// the state volume without being able to write it, and listen where the
// file publishes.

type composeRouterFile struct {
	Services map[string]struct {
		Command     any            `yaml:"command"`
		Environment map[string]any `yaml:"environment"`
		Volumes     []string       `yaml:"volumes"`
		Ports       []string       `yaml:"ports"`
		Profiles    []string       `yaml:"profiles"`
		MemLimit    any            `yaml:"mem_limit"`
		CPUs        any            `yaml:"cpus"`
		ExtraHosts  []string       `yaml:"extra_hosts"`
		Image       string         `yaml:"image"`
		Restart     string         `yaml:"restart"`
	} `yaml:"services"`
}

func TestComposeRouter_agreesWithTheCaptureService(t *testing.T) {
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatal(err)
	}
	var doc composeRouterFile
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	router, ok := doc.Services["router"]
	capture := doc.Services[composeService]
	if !ok {
		t.Fatalf("no router service in %s", composePath)
	}
	// It starts with the stack, from the capture service's image, and runs
	// the router and nothing else.
	if len(router.Profiles) != 0 {
		t.Errorf("the router is behind a profile (%v); it starts with the stack", router.Profiles)
	}
	if router.Image != capture.Image || router.Restart != capture.Restart {
		t.Errorf("image %q restart %q, want the capture service's %q and %q", router.Image, router.Restart, capture.Image, capture.Restart)
	}
	if got := fmt.Sprint(router.Command); got != "[router]" {
		t.Errorf("command = %s, want [router]", got)
	}
	// Every path the two share is the same path: the servers it routes
	// for, and the two files a statement must not read through a copy.
	for _, name := range consoleStateEnvVars {
		if router.Environment[name] == nil || router.Environment[name] != capture.Environment[name] {
			t.Errorf("%s is %v on the router and %v on %s; the router would read, or guard, another file", name, router.Environment[name], capture.Environment[name], composeService)
		}
	}
	// The same settings from the same .env variables, so one file sets both.
	for _, name := range []string{"BINTRAIL_CONSOLE_TOKEN", "BINTRAIL_CONSOLE_SQL_MEMORY", "BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS"} {
		if router.Environment[name] != capture.Environment[name] {
			t.Errorf("%s is %v on the router and %v on %s", name, router.Environment[name], capture.Environment[name], composeService)
		}
	}
	// Settings of the capture service that do not apply to the router are
	// not handed to it: it would say so at every start.
	for _, name := range []string{"BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT", "BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE", "BINTRAIL_METRICS_ADDR", "BINTRAIL_CONSOLE_LISTEN"} {
		if _, set := router.Environment[name]; set {
			t.Errorf("the router is given %s, which it does not read", name)
		}
	}
	// The state volume, where the capture service mounts it, read-only: the
	// router writes nothing there, and here it cannot.
	var captureTarget string
	for _, v := range capture.Volumes {
		if name, rest, _ := strings.Cut(v, ":"); name == composeVolume && !strings.HasSuffix(rest, ":ro") {
			captureTarget = rest
		}
	}
	if captureTarget == "" || len(router.Volumes) != 1 || router.Volumes[0] != composeVolume+":"+captureTarget+":ro" {
		t.Errorf("the router's volumes are %v, want only %s:%s:ro", router.Volumes, composeVolume, captureTarget)
	}
	// It listens where the file publishes, on the host's loopback, and not
	// on the capture service's port.
	listen := fmt.Sprint(router.Environment["BINTRAIL_ROUTER_LISTEN"])
	if listen != "0.0.0.0:3310" || len(router.Ports) != 1 || router.Ports[0] != "127.0.0.1:3310:3310" {
		t.Errorf("listens on %s and publishes %v, want 0.0.0.0:3310 published as 127.0.0.1:3310:3310", listen, router.Ports)
	}
	for _, p := range capture.Ports {
		if strings.Contains(p, "3310") {
			t.Errorf("the capture service publishes %s", p)
		}
	}
	// A source on this machine is reached the way the capture service
	// reaches it.
	if fmt.Sprint(router.ExtraHosts) != fmt.Sprint(capture.ExtraHosts) || len(router.ExtraHosts) == 0 {
		t.Errorf("extra_hosts %v, want the capture service's %v", router.ExtraHosts, capture.ExtraHosts)
	}
	// Read-only unless .env says otherwise. The service starts with the
	// stack and its password is one that could only read before it existed:
	// forwarding writes has to be somebody's decision.
	if got := fmt.Sprint(router.Environment["BINTRAIL_CONSOLE_ROUTE_READ_ONLY"]); got != "${ROUTER_READ_ONLY:-1}" {
		t.Errorf("BINTRAIL_CONSOLE_ROUTE_READ_ONLY is %s on the router, want ${ROUTER_READ_ONLY:-1}: it would forward writes by default", got)
	}
	// No ceiling unless one is set in .env, by these two names.
	if fmt.Sprint(router.MemLimit) != "${ROUTER_MEMORY:-0}" || fmt.Sprint(router.CPUs) != "${ROUTER_CPUS:-0}" {
		t.Errorf("mem_limit %v and cpus %v, want ${ROUTER_MEMORY:-0} and ${ROUTER_CPUS:-0}", router.MemLimit, router.CPUs)
	}
}

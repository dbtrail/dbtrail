package installer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const consoleImage = "ghcr.io/dbtrail/bintrail-console:v0.130.0"

// The memory ceiling per SQL statement (writable-cgroups) is turned on by the
// installer only after this Docker started DBTrail's image with it: an older
// Docker refuses to start a stack that asks for it.
func TestInstaller_turnsOnTheMemoryCeilingWhereDockerTakesIt(t *testing.T) {
	r := install(t, "STUB_IMAGES="+consoleImage)
	if r.failed {
		t.Fatalf("install failed:\n%s", r.out)
	}
	if !strings.Contains(r.calls, "docker run --rm --security-opt writable-cgroups=true --entrypoint true "+consoleImage) {
		t.Fatalf("the installer did not try the ceiling with DBTrail's image:\n%s", r.calls)
	}
	try, up := strings.Index(r.calls, "docker run --rm --security-opt"), strings.Index(r.calls, "compose up")
	if try > up {
		t.Error("the ceiling was tried after the stack was started")
	}
	c := r.compose(t)
	// The capture service, the router and the PostgreSQL router.
	if n := strings.Count(c, "\n    security_opt:\n      - writable-cgroups=true\n"); n != 3 {
		t.Errorf("want the ceiling on 3 services, got %d", n)
	}
	if strings.Contains(c, "#   - writable-cgroups=true") {
		t.Error("a commented ceiling is left")
	}
	if !strings.Contains(r.out, "each SQL statement on the copy runs under a memory ceiling of its own") {
		t.Errorf("not said:\n%s", r.out)
	}
	if _, err := os.Stat(filepath.Join(r.dir, "docker-compose.yml.cgroups")); err == nil {
		t.Error("the copy of the compose file was left behind")
	}
}

func TestInstaller_leavesTheMemoryCeilingOffWhereDockerRefusesIt(t *testing.T) {
	r := install(t, "STUB_IMAGES="+consoleImage, "STUB_RUN_FAIL=1")
	if r.failed || r.upCalls != 1 {
		t.Fatalf("an older Docker stopped the install:\n%s", r.out)
	}
	c := r.compose(t)
	if strings.Contains(c, "\n    security_opt:\n") || !strings.Contains(c, "#   - writable-cgroups=true") {
		t.Error("the ceiling was turned on although this Docker refused it")
	}
	if !strings.Contains(r.out, "it needs Docker Engine 28 or later") {
		t.Errorf("why it is off is not said:\n%s", r.out)
	}
}

// A compose file that does not read back with the ceiling on is put back as
// it was.
func TestInstaller_putsTheComposeFileBackWhenItDoesNotRead(t *testing.T) {
	r := install(t, "STUB_IMAGES="+consoleImage, "STUB_CONFIG_FAIL=1")
	if r.failed {
		t.Fatalf("install failed:\n%s", r.out)
	}
	c := r.compose(t)
	if strings.Contains(c, "\n    security_opt:\n") || !strings.Contains(c, "#   - writable-cgroups=true") {
		t.Error("the compose file was not put back")
	}
	if _, err := os.Stat(filepath.Join(r.dir, "docker-compose.yml.cgroups")); err == nil {
		t.Error("the copy of the compose file was left behind")
	}
	if !strings.Contains(r.out, "did not read back with it") {
		t.Errorf("not said:\n%s", r.out)
	}
}

// With no image to try it with, it is left off, and that is what is said:
// not the version of Docker.
func TestInstaller_noImageToTryLeavesTheCeilingOff(t *testing.T) {
	r := install(t)
	if r.failed {
		t.Fatalf("install failed:\n%s", r.out)
	}
	if strings.Contains(r.calls, "docker run") || strings.Contains(r.compose(t), "\n    security_opt:\n") {
		t.Errorf("tried or turned on with no image:\n%s", r.calls)
	}
	if !strings.Contains(r.out, "could not tell which image to try it with") || strings.Contains(r.out, "Docker Engine 28") {
		t.Errorf("wrong reason:\n%s", r.out)
	}
}

// A compose file that was kept is the operator's: not tried, not edited.
func TestInstaller_aKeptComposeFileIsNotEdited(t *testing.T) {
	root, _ := filepath.Abs(filepath.Join("..", ".."))
	src, err := os.ReadFile(filepath.Join(root, "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	r := installWith(t, func(dir string) {
		if err := os.WriteFile(filepath.Join(dir, "docker-compose.yml"), src, 0o644); err != nil {
			t.Fatal(err)
		}
	}, "STUB_IMAGES="+consoleImage)
	if r.failed {
		t.Fatalf("install failed:\n%s", r.out)
	}
	if strings.Contains(r.calls, "docker run") || r.compose(t) != string(src) {
		t.Error("a kept compose file was tried or edited")
	}
}

func TestCompose_thePostgreSQLRouterHasAShareAndACeiling(t *testing.T) {
	root, _ := filepath.Abs(filepath.Join("..", ".."))
	b, err := os.ReadFile(filepath.Join(root, "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	i := strings.Index(s, "\n  pg-router:\n")
	j := strings.Index(s[i:], "\n  shim:")
	if i < 0 || j < 0 {
		t.Fatal("no pg-router service")
	}
	pg := s[i : i+j]
	if !strings.Contains(pg, "BINTRAIL_MEMORY_SHARE: ${PG_ROUTER_MEMORY_SHARE:-}") {
		t.Error("the pg-router service has no memory share hook")
	}
	if !strings.Contains(pg, "    # security_opt:\n    #   - writable-cgroups=true\n") {
		t.Error("the pg-router service has no ceiling to turn on")
	}
}

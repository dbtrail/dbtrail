package consoleapp

import (
	"os"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"
)

// composeOptionFileMount is how docker-compose.yml mounts the install
// folder's ./dbtrail-config, where the images find /etc/dbtrail/dbtrail.cnf
// (#2671).
const composeOptionFileMount = "./dbtrail-config:/etc/dbtrail-config"

// Every service that runs a DBTrail program reads the option file, so every
// one mounts its folder. index-init runs only a shell over the index's
// secret, and mounts nothing of DBTrail's.
func TestCompose_everyServiceMountsTheOptionFile(t *testing.T) {
	data, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var doc composeRouterFile
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for name, svc := range doc.Services {
		if !strings.HasPrefix(svc.Image, "ghcr.io/dbtrail/") || name == "index-init" {
			continue
		}
		seen++
		if !slices.Contains(svc.Volumes, composeOptionFileMount) {
			t.Errorf("service %s runs a DBTrail program and does not mount %s", name, composeOptionFileMount)
		}
	}
	// Not read-only: a file an older override mounts at
	// /etc/dbtrail/dbtrail.cnf lands inside the folder through the link, and
	// a read-only folder makes that container fail to start.
	if strings.Contains(string(data), "./dbtrail-config:/etc/dbtrail-config:ro") {
		t.Error("the option file's folder is mounted read-only: an older override's file mount inside it would stop the container")
	}
	if seen < 6 {
		t.Fatalf("found %d DBTrail services; the file's shape changed under this test", seen)
	}
	// The images link /etc/dbtrail/dbtrail.cnf into that folder.
	for _, f := range []string{"../build/Dockerfile.bintrail-console", "../build/Dockerfile.bintrail-console.goreleaser",
		"../build/Dockerfile.goreleaser", "../build/Dockerfile.bintrail", "../build/Dockerfile.bintrail-pg.goreleaser"} {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), "ln -s /etc/dbtrail-config/dbtrail.cnf /etc/dbtrail/dbtrail.cnf") {
			t.Errorf("%s does not link /etc/dbtrail/dbtrail.cnf into /etc/dbtrail-config", f)
		}
	}
}

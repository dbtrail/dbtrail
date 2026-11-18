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
// (#2671), in a service that runs as user 999; one that runs as root mounts
// it read-only (composeOptionFileMountRO).
const (
	composeOptionFileMount   = "./dbtrail-config:/etc/dbtrail-config"
	composeOptionFileMountRO = "./dbtrail-config:/etc/dbtrail-config:ro"
)

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
	var users struct {
		Services map[string]struct {
			User string `yaml:"user"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &users); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for name, svc := range doc.Services {
		if !strings.HasPrefix(svc.Image, "ghcr.io/dbtrail/") || name == "index-init" {
			continue
		}
		seen++
		// A service that runs as root could write a writable folder of the
		// host's: it mounts it read-only. One that runs as user 999 mounts
		// it writable, which that user cannot write, so that an older
		// override's file mount inside it still starts the container.
		want := composeOptionFileMount
		if strings.HasPrefix(users.Services[name].User, "0:") || users.Services[name].User == "0" || users.Services[name].User == "root" {
			want = composeOptionFileMountRO
		}
		if !slices.Contains(svc.Volumes, want) {
			t.Errorf("service %s runs a DBTrail program and does not mount %s (it runs as %q)", name, want, users.Services[name].User)
		}
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

package cliapp

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"
)

// The shipped stack can run the Iceberg export (#1466) as a one-shot, so an
// operator gets the tables without installing anything. Two things about that
// service are load-bearing rather than cosmetic, and neither shows up in
// `docker compose config`:
//
//   - it runs the CORE image. The console image ships without the export
//     commands on purpose, and no daemon links the Iceberg writer
//     (TestDaemonBinariesAreIcebergFree above), so pointing this service at
//     the console image would produce a service that cannot work.
//   - it is behind its own profile, so it never runs as part of `up -d`. The
//     export writes a new copy of the data and is kept out of the capture
//     process; a service that started with the stack would put it back in.
const icebergComposePath = "../docker-compose.yml"

type icebergComposeFile struct {
	Volumes  map[string]any `yaml:"volumes"`
	Services map[string]struct {
		Image       string            `yaml:"image"`
		Restart     string            `yaml:"restart"`
		Profiles    []string          `yaml:"profiles"`
		Command     []string          `yaml:"command"`
		Volumes     []string          `yaml:"volumes"`
		Environment map[string]string `yaml:"environment"`
		DependsOn   map[string]struct {
			Condition string `yaml:"condition"`
		} `yaml:"depends_on"`
	} `yaml:"services"`
}

func TestComposeIcebergExportProfile(t *testing.T) {
	data, err := os.ReadFile(icebergComposePath)
	if err != nil {
		t.Fatalf("read %s: %v", icebergComposePath, err)
	}
	var doc icebergComposeFile
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse %s: %v", icebergComposePath, err)
	}
	svc, ok := doc.Services["iceberg-export"]
	if !ok {
		t.Fatalf("no iceberg-export service in %s", icebergComposePath)
	}

	// The core image, by name. bintrail-console is a PREFIX-sharing name, so
	// the check is exact rather than a Contains that both would satisfy.
	image, _, _ := strings.Cut(svc.Image, ":")
	if image != "ghcr.io/dbtrail/bintrail" {
		t.Errorf("iceberg-export runs image %q; the export commands ship only in the core image", svc.Image)
	}
	// Behind a profile, so `docker compose up -d` never starts it.
	if len(svc.Profiles) != 1 || svc.Profiles[0] != "iceberg-export" {
		t.Errorf("iceberg-export profiles = %v, want exactly [iceberg-export] so it stays opt-in", svc.Profiles)
	}
	cmd := strings.Join(svc.Command, "\n")
	if !strings.Contains(cmd, "bintrail export iceberg") {
		t.Error("the iceberg-export service does not run `bintrail export iceberg`")
	}
	if !strings.Contains(cmd, "--warehouse") {
		t.Error("the iceberg-export service names no warehouse, which the command requires")
	}
	// The export folds events out of the index, so it must not start before
	// the index is answering.
	if dep, ok := svc.DependsOn["index-mysql"]; !ok || dep.Condition != "service_healthy" {
		t.Errorf("iceberg-export depends_on index-mysql = %+v, want service_healthy", dep)
	}
	// The warehouse it writes to must be a declared volume mounted WRITABLE,
	// or the tables either vanish with the container or the run fails on a
	// read-only mount. Checked as a path prefix and not a substring: the
	// state volume's /var/lib/bintrail is a substring of the warehouse path
	// and is mounted read-only, so a Contains would pass on the wrong mount.
	dir := composeDefault(svc.Environment["WAREHOUSE_DIR"])
	if dir == "" {
		t.Fatal("iceberg-export sets no WAREHOUSE_DIR, so this guard cannot tell where the tables go")
	}
	var warehouse string
	for _, v := range svc.Volumes {
		name, rest, found := strings.Cut(v, ":")
		if !found {
			continue
		}
		target, opts, _ := strings.Cut(rest, ":")
		target = strings.TrimSuffix(target, "/")
		if _, declared := doc.Volumes[name]; !declared || opts == "ro" {
			continue
		}
		if dir == target || strings.HasPrefix(dir, target+"/") {
			warehouse = name
		}
	}
	if warehouse == "" {
		t.Errorf("no declared, writable volume covers %s; the exported tables would not survive the run", dir)
	}
}

// composeDefault reads the `${VAR:-default}` form back. A value in any other
// shape is returned as written, which is what a plain path is.
func composeDefault(raw string) string {
	if m := regexp.MustCompile(`^\$\{[A-Za-z_][A-Za-z0-9_]*:-([^}]*)\}$`).FindStringSubmatch(raw); m != nil {
		return m[1]
	}
	return raw
}

// TestComposeIcebergExportLoop: the stay-up variant (#2552) is the same
// export on a loop, not a second copy that can drift. It shares the one-shot's
// command, volumes and settings (through YAML anchors), adds only the
// interval and a restart policy, and sits behind its OWN profile, so the
// one-shot's `--profile iceberg-export` never starts it alongside.
func TestComposeIcebergExportLoop(t *testing.T) {
	data, err := os.ReadFile(icebergComposePath)
	if err != nil {
		t.Fatal(err)
	}
	var doc icebergComposeFile
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	one, ok := doc.Services["iceberg-export"]
	if !ok {
		t.Fatal("no iceberg-export service")
	}
	loop, ok := doc.Services["iceberg-export-loop"]
	if !ok {
		t.Fatal("no iceberg-export-loop service")
	}
	if len(loop.Profiles) != 1 || loop.Profiles[0] != "iceberg-export-loop" {
		t.Errorf("iceberg-export-loop profiles = %v, want exactly [iceberg-export-loop]", loop.Profiles)
	}
	if loop.Restart != "unless-stopped" {
		t.Errorf("iceberg-export-loop restart = %q, want unless-stopped", loop.Restart)
	}
	// The one-shot must stay a one-shot: a restart policy would re-run it back
	// to back under `up`, and an interval would turn `run --rm` into a loop.
	if one.Restart != "" {
		t.Errorf("iceberg-export restart = %q; the one-shot must not restart", one.Restart)
	}
	for _, k := range []string{"ICEBERG_EVERY", "BINTRAIL_ICEBERG_EVERY"} {
		if _, ok := one.Environment[k]; ok {
			t.Errorf("the one-shot iceberg-export carries %s, so `run --rm` would never return", k)
		}
	}
	if _, ok := loop.Environment["BINTRAIL_ICEBERG_EVERY"]; ok {
		t.Error("iceberg-export-loop sets BINTRAIL_ICEBERG_EVERY; bintrail does not read it, --every is a flag")
	}
	if !strings.Contains(strings.Join(one.Command, "\n"), `--every "$$ICEBERG_EVERY"`) {
		t.Error("the shared command does not turn ICEBERG_EVERY into --every")
	}
	if loop.Image != one.Image || strings.Join(loop.Command, "\n") != strings.Join(one.Command, "\n") ||
		strings.Join(loop.Volumes, ",") != strings.Join(one.Volumes, ",") {
		t.Error("iceberg-export-loop differs from iceberg-export in image, command or volumes; they must be the same export")
	}
	if dep, ok := loop.DependsOn["index-mysql"]; !ok || dep.Condition != "service_healthy" {
		t.Errorf("iceberg-export-loop depends_on index-mysql = %+v, want service_healthy", dep)
	}
	for k, v := range one.Environment {
		if lv, ok := loop.Environment[k]; !ok || lv != v {
			t.Errorf("iceberg-export-loop %s = %q, the one-shot has %q", k, loop.Environment[k], v)
		}
	}
	every := composeDefault(loop.Environment["ICEBERG_EVERY"])
	if d, err := parseExportEvery(every); err != nil || d == 0 {
		t.Errorf("iceberg-export-loop default interval %q is not one --every accepts: %v", every, err)
	}
}

// TestComposeIcebergExportCatalogPassthrough: each catalog setting the service
// takes reaches a flag, and the REST secrets reach bintrail only as variables,
// never on the command line where `ps` shows them.
func TestComposeIcebergExportCatalogPassthrough(t *testing.T) {
	data, err := os.ReadFile(icebergComposePath)
	if err != nil {
		t.Fatal(err)
	}
	var doc icebergComposeFile
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	svc := doc.Services["iceberg-export"]
	cmd := strings.Join(svc.Command, "\n")
	for env, flag := range map[string]string{
		"ICEBERG_CATALOG":           "--catalog ",
		"ICEBERG_CATALOG_URI":       "--catalog-uri ",
		"ICEBERG_CATALOG_WAREHOUSE": "--catalog-warehouse ",
		"ICEBERG_CATALOG_SIGV4":     "--catalog-sigv4",
	} {
		if _, ok := svc.Environment[env]; !ok {
			t.Errorf("iceberg-export does not take %s", env)
		}
		if !strings.Contains(cmd, "$$"+env) || !strings.Contains(cmd, flag) {
			t.Errorf("%s does not reach %s in the command", env, strings.TrimSpace(flag))
		}
	}
	// An unrecognized ICEBERG_CATALOG_SIGV4 must stop the service, not be
	// dropped: without signing, every run of the loop would fail to sign in.
	if !strings.Contains(cmd, "FATAL: ICEBERG_CATALOG_SIGV4=") {
		t.Error("an unrecognized ICEBERG_CATALOG_SIGV4 value is not refused")
	}
	for _, env := range []string{envCatalogCredential, envCatalogToken} {
		if _, ok := svc.Environment[env]; !ok {
			t.Errorf("iceberg-export does not pass %s through", env)
		}
		if strings.Contains(cmd, env) {
			t.Errorf("the command line names %s; a secret must reach bintrail as a variable only", env)
		}
	}
}

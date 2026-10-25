package cliapp

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// The Iceberg export (#1466) is an OUTPUT, never the storage layer (#1467),
// and the guarantee behind that is mechanical: the Iceberg and Arrow
// libraries are linked by exactly one package, and nothing on the recovery
// path reaches it.
//
// Two tests, two altitudes:
//
//   - TestIcebergLibrariesStayInTheExportPackage walks EVERY package of the
//     module (from `go list` over the module, not a hand-kept list) and fails if any
//     package outside a short allowlist transitively links either library. A
//     package added tomorrow is guarded the day it appears; only the allowlist
//     is something a reviewer has to read.
//   - TestDaemonBinariesAreIcebergFree pins the binaries: the console daemon,
//     the MCP server and bintrail-pg must not carry an unused writer library,
//     while cmd/bintrail MUST, or the command silently fell out of the build.
//
// The allowlist is deliberately the export package, the cliapp root and the
// core binary: registering the command from internal/cli would link the
// library into everything internal/cli reaches (the console, bintrail-pg, the
// whole read plane), and this test names each of those packages when it does.

var icebergBanned = []string{
	"github.com/apache/iceberg-go",
	"github.com/apache/arrow-go",
	// The catalog SDKs the export's Glue and REST catalogs pull in (#2552):
	// the Go CDK's blob IO (with its Azure and GCS drivers) and the Glue
	// client. They belong to the export as much as iceberg-go does.
	"gocloud.dev",
	"github.com/aws/aws-sdk-go-v2/service/glue",
}

// icebergAllowed may link the libraries. Keep it short; every entry widens
// the surface the recovery path could reach the library through.
var icebergAllowed = []string{
	"github.com/dbtrail/dbtrail/internal/icebergexport",
	"github.com/dbtrail/dbtrail/cliapp",
	"github.com/dbtrail/dbtrail/cmd/bintrail",
}

func linksBanned(deps []string) []string {
	var hits []string
	for _, dep := range deps {
		for _, b := range icebergBanned {
			if dep == b || strings.HasPrefix(dep, b+"/") {
				hits = append(hits, dep)
			}
		}
	}
	return hits
}

func TestIcebergLibrariesStayInTheExportPackage(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", "{{.ImportPath}} {{join .Deps \" \"}}", "github.com/dbtrail/dbtrail/...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	seen := map[string]bool{}
	exportLinksIceberg := false
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		pkg, deps := fields[0], fields[1:]
		seen[pkg] = true
		hits := linksBanned(deps)
		if pkg == "github.com/dbtrail/dbtrail/internal/icebergexport" && len(hits) > 0 {
			exportLinksIceberg = true
		}
		if slices.Contains(icebergAllowed, pkg) {
			continue
		}
		if len(hits) > 0 {
			t.Errorf("%s links %v — the Iceberg/Arrow libraries belong to internal/icebergexport only; "+
				"if this package is internal/cli or anything it reaches, the export command was registered from the wrong place",
				pkg, hits)
		}
	}
	// The allowlist must name real packages, or a rename would silently
	// widen nothing and guard nothing.
	for _, a := range icebergAllowed {
		if !seen[a] {
			t.Errorf("allowlisted package %s does not exist; update icebergAllowed", a)
		}
	}
	// And the guard must be seeing the library at all: if the export package
	// stopped linking it, every check above passes for an unrelated reason.
	if !exportLinksIceberg {
		t.Fatal("internal/icebergexport no longer links iceberg-go: this guard covers nothing")
	}
}

func TestDaemonBinariesAreIcebergFree(t *testing.T) {
	mustNot := []string{
		"github.com/dbtrail/dbtrail/cmd/bintrail-console",
		"github.com/dbtrail/dbtrail/cmd/bintrail-mcp",
		"github.com/dbtrail/dbtrail/cmd/bintrail-pg",
	}
	for _, bin := range mustNot {
		out, err := exec.Command("go", "list", "-deps", bin).CombinedOutput()
		if err != nil {
			t.Fatalf("go list -deps %s: %v\n%s", bin, err, out)
		}
		if hits := linksBanned(strings.Fields(string(out))); len(hits) > 0 {
			t.Errorf("%s links %v — a daemon must not carry the Iceberg writer library", bin, hits)
		}
	}
	// The positive twin: the core binary is where the command ships.
	out, err := exec.Command("go", "list", "-deps", "github.com/dbtrail/dbtrail/cmd/bintrail").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps cmd/bintrail: %v\n%s", err, out)
	}
	if hits := linksBanned(strings.Fields(string(out))); len(hits) == 0 {
		t.Fatal("cmd/bintrail does not link iceberg-go: `bintrail export iceberg` fell out of the build")
	}
}

// TestExportIcebergCmd_registered: a subcommand defined but never attached is
// invisible, and nothing else in the build notices.
func TestExportIcebergCmd_registered(t *testing.T) {
	found, _, err := rootCmd.Find([]string{"export", "iceberg"})
	if err != nil || found == nil || found.Name() != "iceberg" {
		t.Fatalf("`export iceberg` is not registered on the root: %v", err)
	}
}

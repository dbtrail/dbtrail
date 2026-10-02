package audittest

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// recordCallSites is the explicit accounted-for list of every `ext.Record(`
// call site in non-test source, keyed by module-relative path. It is the
// source-level backstop CheckCoverage's behavioural coverage structurally
// lacks (#1123): behavioural coverage catches a DELETED emission, but a NEW
// unaudited call site — or a call site whose pair no owner declares, like
// mcp/reconstruct.row was — only trips a test if some contract case happens
// to execute it. Counting call sites trips at BUILD-TEST time instead.
//
// When this test fails because you ADDED a call site: declare it here, add
// its (surface, action) pair to Required, and exercise it in that owner's
// contract test. When it fails because a count DROPPED: an emission was
// deleted or moved — put it back, or remove the pair from Required and fix
// the ext/audit.go docstring in the same change.
//
// What this still cannot catch: a new data-serving MODE that returns
// through an existing command without reaching its existing emission
// (reconstruct --baseline-only's original bug). That class has no
// mechanical guard — adding a mode means adding the emission by hand.
var recordCallSites = map[string]int{
	"internal/cli/query.go":                1, // cli/query.run
	"internal/cli/recover.go":              1, // cli/recover.generate
	"internal/cli/recover_cascade.go":      1, // cli/recover.cascade
	"internal/cli/reconstruct.go":          1, // cli/reconstruct.run (auditReconstruct — all five modes)
	"internal/cli/verify.go":               1, // cli/verify.explain
	"internal/cli/drill.go":                1, // cli/drill.run (auditDrill — per rehearsed table)
	"cliapp/export_iceberg.go":             1, // cli/export.iceberg (auditIcebergExport — per committed table)
	"internal/console/audit.go":            1, // console data reads (recordConsoleAudit helper)
	"internal/console/authz.go":            1, // console/authz.denied + console/profile.denied
	"internal/console/mcp.go":              1, // console/authz.denied for /mcp tool-permission denials (#1124)
	"internal/mcptools/mcptools.go":        2, // mcp|console query.run + recover.generate
	"internal/mcptools/reconstruct.go":     1, // mcp|console reconstruct.row
	"internal/mcptools/recover_cascade.go": 1, // mcp|console recover.cascade
	"internal/shim/handler.go":             1, // shim/timetravel.query (recordTimeTravel — all three serving layers)
	"internal/shim/freesql.go":             1, // shim/sql.run (recordFreeSQL — the console's embedded port)
}

// TestAuditRecordCallSitesAccounted walks the module source tree and asserts
// the set of files containing `ext.Record(` — and the occurrence count per
// file — matches recordCallSites exactly. Plain text counting is deliberate
// (simple, zero dependencies); a comment containing the literal `ext.Record(`
// would be counted too — reword the comment or account for it.
func TestAuditRecordCallSitesAccounted(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]int{}
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			// Skip hidden/underscore dirs (worktrees, editor state), vendored
			// code, fixtures, and build output — none of it is the module's
			// shipping source.
			if path != root && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") ||
				name == "vendor" || name == "testdata" || name == "node_modules" ||
				strings.HasPrefix(name, "dist")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if n := strings.Count(string(src), "ext.Record("); n > 0 {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			found[filepath.ToSlash(rel)] = n
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walking %s: %v", root, walkErr)
	}

	for _, f := range sortedKeys(found) {
		want, declared := recordCallSites[f]
		if !declared {
			t.Errorf("unaccounted ext.Record call site: %s has %d occurrence(s) — declare it in recordCallSites, "+
				"add its (surface, action) pair to Required, and exercise it in that owner's contract test", f, found[f])
			continue
		}
		if found[f] != want {
			t.Errorf("%s has %d ext.Record occurrence(s), recordCallSites declares %d — "+
				"update the declaration and check Required and the owner's contract test still tell the truth",
				f, found[f], want)
		}
	}
	for _, f := range sortedKeys(recordCallSites) {
		if _, ok := found[f]; !ok {
			t.Errorf("declared call site %s no longer contains ext.Record — the emission was deleted or moved; "+
				"restore it, or remove its pair from Required and fix the ext/audit.go docstring in the same change", f)
		}
	}

	// Guard the guard: an empty walk (wrong root after a layout change) must
	// not read as "no call sites, all accounted".
	if len(found) == 0 {
		t.Fatalf("found no ext.Record call sites at all under %s — the walk root is wrong", root)
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestGrantBlockScopesSelectToTheTypedSchemas (#2317): with schemas typed in
// the form, the SQL it shows gives the capture account SELECT on those
// schemas and not on every one, which would cover MySQL's own account
// tables. With none typed the server captures every schema, so the grant
// stays global. Rendered through the real grantBlocks.
func TestGrantBlockScopesSelectToTheTypedSchemas(t *testing.T) {
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	script := functionBody(t, js, "function sqlString(") + "\n" + functionBody(t, js, "function grantBlocks(") + `
const g = (schemas, managed, connect, pw = "Ab3-xyzXYZ789_qq") => grantBlocks("dbtrail", pw, false, !!managed, !!connect, schemas);
console.log(JSON.stringify({
  none: g(""), undef: g(undefined), blanks: g(" , ,"),
  one: g("shop"), many: g(" shop , billing ,,shop"), lines: g("shop\nbilling"),
  backtick: g("we` + "`" + `ird"), wildcard: g("shop_eu"), system: g("information_schema, shop, PERFORMANCE_SCHEMA"),
  onlySystem: g("information_schema"), managed: g("shop", true, true), managedGlobal: g("", true, true),
  noPassword: g("shop", false, false, ""),
}));`
	path := filepath.Join(t.TempDir(), "grants.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var got map[string]map[string]string
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	const global = "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'dbtrail'@'%';"
	const repl = "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'dbtrail'@'%';"
	sel := func(schema string) string { return "GRANT SELECT ON `" + schema + "`.* TO 'dbtrail'@'%';" }
	for _, flavor := range []string{"mysql", "mariadb"} {
		b := func(k string) string {
			t.Helper()
			v, ok := got[k][flavor]
			if !ok {
				t.Fatalf("%s/%s not rendered", k, flavor)
			}
			return v
		}
		t.Logf("%s, one schema:\n%s", flavor, b("one"))
		// No schema typed: the server captures every schema, the grant is
		// global, and nothing speaks of scoping.
		for _, k := range []string{"none", "undef", "blanks", "onlySystem", "managedGlobal"} {
			if !activeLine(b(k), global) || strings.Contains(b(k), "GRANT SELECT ON `") || strings.Contains(b(k), "rds_configuration") {
				t.Errorf("%s/%s: want the global capture grant and no scoped line:\n%s", k, flavor, b(k))
			}
		}
		// Schemas typed: the replication grants stay global (MySQL has no
		// other level for them), SELECT is per schema, and SELECT ON *.*
		// is gone from every line, commented ones included.
		for k, schemas := range map[string][]string{"one": {"shop"}, "many": {"shop", "billing"}, "lines": {"shop", "billing"},
			"backtick": {"we``ird"}, "wildcard": {"shop_eu"}, "system": {"shop"}, "managed": {"shop"}} {
			blk := b(k)
			if !activeLine(blk, repl) {
				t.Errorf("%s/%s: the replication grant is not an active line:\n%s", k, flavor, blk)
			}
			if strings.Contains(blk, "SELECT ON *.*") {
				t.Errorf("%s/%s: SELECT on every schema is still granted:\n%s", k, flavor, blk)
			}
			for _, s := range schemas {
				if n := strings.Count(blk, sel(s)); n != 1 || !activeLine(blk, sel(s)) {
					t.Errorf("%s/%s: %q appears %d time(s) as an active line, want once:\n%s", k, flavor, sel(s), n, blk)
				}
			}
			if n := strings.Count(blk, "GRANT SELECT ON `"); n != len(schemas) {
				t.Errorf("%s/%s: %d scoped SELECT lines, want %d:\n%s", k, flavor, n, len(schemas), blk)
			}
			// The snapshot line is untouched by the scoping.
			if !strings.Contains(blk, "SHOW VIEW ON *.* TO 'dbtrail'@'%';") {
				t.Errorf("%s/%s: the snapshot grant is gone:\n%s", k, flavor, blk)
			}
			// Every line is a comment or one whole statement.
			for _, l := range strings.Split(blk, "\n") {
				if l != "" && !strings.HasPrefix(l, "--") && !(strings.HasSuffix(l, ";") && (strings.HasPrefix(l, "GRANT ") || strings.HasPrefix(l, "CREATE USER "))) {
					t.Errorf("%s/%s: a line that is neither a comment nor one statement: %q", k, flavor, l)
				}
			}
		}
		if strings.Contains(b("system"), "information_schema") || strings.Contains(strings.ToLower(b("system")), "`performance_schema`") {
			t.Errorf("system/%s: grants on a schema MySQL does not let anyone grant on:\n%s", flavor, b("system"))
		}
		// _ and % match other names in a schema-level grant: said, once.
		if !strings.Contains(b("wildcard"), "-- In a GRANT, _ and % in a schema name match any character") {
			t.Errorf("wildcard/%s: nothing says shop_eu also matches other names:\n%s", flavor, b("wildcard"))
		}
		if strings.Contains(b("one"), "match any character") {
			t.Errorf("one/%s: the wildcard note shows for a name with no wildcard:\n%s", flavor, b("one"))
		}
		// Amazon RDS: the health check reads one table of the mysql schema.
		// The live line when the RDS box is ticked, a commented one otherwise.
		const rds = "GRANT SELECT ON mysql.rds_configuration TO 'dbtrail'@'%';"
		if !activeLine(b("managed"), rds) {
			t.Errorf("managed/%s: no active grant on mysql.rds_configuration:\n%s", flavor, b("managed"))
		}
		if activeLine(b("one"), rds) || !strings.Contains(b("one"), "-- "+rds) {
			t.Errorf("one/%s: the RDS line must be there, commented (the table exists on RDS only):\n%s", flavor, b("one"))
		}
		// With no password every line is commented, the scoped ones too.
		for _, l := range strings.Split(b("noPassword"), "\n") {
			if l != "" && !strings.HasPrefix(l, "--") {
				t.Errorf("noPassword/%s: a runnable line with no password: %q", flavor, l)
			}
		}
		if !strings.Contains(b("noPassword"), "-- "+sel("shop")) {
			t.Errorf("noPassword/%s: the scoped line is gone from the commented block:\n%s", flavor, b("noPassword"))
		}
	}
}

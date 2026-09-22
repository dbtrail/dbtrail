package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestServerFormGrantsMatchTheDefaultLockMode (#1658): the grant block on
// + Add server is the last grant list anyone reads before pasting it, and it
// must work with the DEFAULT backup mode. That mode (ftwrl) is checked by
// internal/mydumperlock/privileges.go for RELOAD (or FLUSH_TABLES) and, on
// MySQL and Percona 8.0 or later, BACKUP_ADMIN; LOCK TABLES alone is refused
// there, and is only what lock-all needs. The blocks are built by executing
// the page's own grantBlocks, one per flavor, for the default account.
func TestServerFormGrantsMatchTheDefaultLockMode(t *testing.T) {
	// functionBody, not jsFunctionBody: the latter stops at a "//" comment.
	js := readAsset(t, "app.js")
	if !strings.Contains(functionBody(t, js, "function buildServerForm("), "grantBlocks(") {
		t.Fatal("the add-server form no longer draws its grant blocks from grantBlocks; this guard covers nothing")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	script := functionBody(t, js, "function sqlString(") + "\n" +
		functionBody(t, js, "function grantBlocks(") + `
console.log(JSON.stringify(grantBlocks("dbtrail", "Ab3-xyzXYZ789_qq")));`
	path := filepath.Join(t.TempDir(), "grants.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	got := string(out)
	mysql, mariadb := blockFor(t, got, "mysql"), blockFor(t, got, "mariadb")
	t.Logf("mysql:\n%s\nmariadb:\n%s", mysql, mariadb)

	for flavor, b := range map[string]string{"mysql": mysql, "mariadb": mariadb} {
		if !strings.Contains(b, "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'dbtrail'@'%';") {
			t.Errorf("%s: the capture grant is gone", flavor)
		}
		if !activeLine(b, "GRANT RELOAD") {
			t.Errorf("%s: RELOAD is not an active grant, so the default backup mode is refused on a source set up as written", flavor)
		}
		if activeLine(b, "GRANT LOCK TABLES") {
			t.Errorf("%s: LOCK TABLES is an active line; the default mode does not accept it in place of RELOAD", flavor)
		}
		if !strings.Contains(b, "-- GRANT LOCK TABLES, SHOW VIEW ON *.* TO 'dbtrail'@'%';") {
			t.Errorf("%s: the RDS/Aurora alternative (LOCK TABLES with lock-all) is gone", flavor)
		}
		// The compose file maps BASELINE_LOCK_MODE from .env onto the long
		// name; the long name in .env never reaches the container.
		if !strings.Contains(b, "BASELINE_LOCK_MODE=lock-all in .env") || !strings.Contains(b, "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE otherwise") {
			t.Errorf("%s: the lock-all switch does not name the compose .env variable and the plain one", flavor)
		}
		// mydumper refuses the whole backup at the first view it cannot read.
		for _, l := range strings.Split(b, "\n") {
			if strings.HasPrefix(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(l), "--")), "GRANT RELOAD") ||
				strings.HasPrefix(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(l), "--")), "GRANT LOCK TABLES") {
				if !strings.Contains(l, "SHOW VIEW") {
					t.Errorf("%s: backup grant without SHOW VIEW, so a schema with a view fails the backup: %q", flavor, l)
				}
			}
		}
		if strings.Contains(b, "—") {
			t.Errorf("%s: em dash in page copy", flavor)
		}
	}
	if !strings.Contains(mysql, "-- GRANT RELOAD, SHOW VIEW ON *.* TO 'dbtrail'@'%';") {
		t.Error("mysql: no MySQL 5.7 line; the 8.0 statement fails whole on 5.7, RELOAD included")
	}
	if !activeLine(mysql, "GRANT RELOAD, BACKUP_ADMIN, SHOW VIEW ON *.*") {
		t.Error("mysql: BACKUP_ADMIN is not granted, and MySQL/Percona 8.0+ requires it for the default mode")
	}
	if strings.Contains(mariadb, "BACKUP_ADMIN") {
		t.Error("mariadb: BACKUP_ADMIN does not exist on MariaDB, and the GRANT would fail")
	}
}

// blockFor reads one flavor's text out of the JSON the script printed.
func blockFor(t *testing.T, js, flavor string) string {
	t.Helper()
	var blocks map[string]string
	if err := json.Unmarshal([]byte(js), &blocks); err != nil {
		t.Fatalf("decode %q: %v", js, err)
	}
	b, ok := blocks[flavor]
	if !ok {
		t.Fatalf("no %s block rendered: %s", flavor, js)
	}
	return b
}

// activeLine reports whether a line starting with prefix is present and not
// commented out.
func activeLine(block, prefix string) bool {
	for _, l := range strings.Split(block, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), prefix) {
			return true
		}
	}
	return false
}

package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// TestBackupFoldError_schemaRefusalNamesNoCommand: #1651 makes the schema
// refusal fire on every column type change, not only on added or dropped
// columns, so the Backups page shows it far more often. Its remedy is written
// for a terminal ("bintrail dump + bintrail baseline", "bintrail snapshot"),
// which a console user cannot run. The text comes from the real refusal, not a
// copy of it, so a rewording on the Go side fails here instead of leaking the
// commands back onto the page.
func TestBackupFoldError_schemaRefusalNamesNoCommand(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	createSQL := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `gone` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	tm := &metadata.TableMeta{Schema: "shop", Table: "orders", Columns: []metadata.ColumnMeta{{Name: "id", DataType: "int", ColumnType: "int"}}}
	refusal := reconstruct.CheckBaselineSchemaCurrent(createSQL, tm, "shop", "orders")
	if refusal == nil {
		t.Fatal("a dropped column must refuse")
	}
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	msg, _ := json.Marshal("reconstruct shop.orders: " + refusal.Error())
	script := functionBody(t, string(raw), "function backupFoldError(") + "\nconsole.log(backupFoldError(" + string(msg) + "));\n"
	path := filepath.Join(t.TempDir(), "refusal.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	got := strings.TrimSpace(string(out))
	for _, banned := range []string{"bintrail dump", "bintrail baseline", "bintrail snapshot", "\u2014"} {
		if strings.Contains(got, banned) {
			t.Errorf("rendered refusal still contains %q:\n%s", banned, got)
		}
	}
	for _, want := range []string{"gone since: gone", "Only a full backup taken after that change can be updated from the recorded changes.", "schema changed"} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered refusal lacks %q:\n%s", want, got)
		}
	}
}

//go:build integration

package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The foreign-key cascade finding, from a real server through the real
// doctor, drawn by the page's own "Capture started" notice, and printed the
// way `bintrail doctor` prints it. Run with -v to read both texts.
func TestFKCascadeFindingRendersOnBothSurfaces(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	db, dbName := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, `CREATE TABLE customers (id INT PRIMARY KEY)`)
	testutil.MustExec(t, db, `CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT NOT NULL,
		CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE)`)

	r := doctor.Build(t.Context(), testutil.IntegrationDSN(dbName), "", dbName, 0, doctor.ForUnsavedServer())
	var c *doctor.CheckResult
	for i := range r.Checks {
		if r.Checks[i].Name == doctor.FKCascadeCheckName {
			c = &r.Checks[i]
		}
	}
	if c == nil || c.Status != doctor.StatusWarn {
		t.Fatalf("no warning from %q in %+v", doctor.FKCascadeCheckName, r.Checks)
	}
	want := dbName + ".orders → " + dbName + ".customers, ON DELETE CASCADE"
	if c.Detail != want {
		t.Fatalf("detail = %q, want %q", c.Detail, want)
	}

	var cli strings.Builder
	if err := (&doctor.Report{Checks: []doctor.CheckResult{*c}}).Write(&cli, "text"); err != nil {
		t.Fatal(err)
	}
	t.Logf("bintrail doctor:\n%s", cli.String())

	check, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const lines = (n, out = []) => { if (!n) return out; if (n.nodeType === 3) { out.push(n.textContent); return out; }
  if (["p", "pre", "li"].includes(n.tag) || String(n.className).includes("dc-name")) { out.push((n.tag === "pre" ? "[code] " : "") + n.textContent); return out; }
  (n.children || []).forEach((k) => lines(k, out)); return out; };
const notice = vm.runInContext("connectNotice", ctx)({ started: true, name: "demo", doctor: { checks: [` + string(check) + `] } });
console.log(JSON.stringify({ title: notice.summary, head: notice.lines, body: lines(notice.content[0]) }));
`
	path := filepath.Join(t.TempDir(), "fkcascade.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		Title string   `json:"title"`
		Head  []string `json:"head"`
		Body  []string `json:"body"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("parse %s: %v", raw, err)
	}
	t.Logf("console:\n%s\n%s\n%s", got.Title, strings.Join(got.Head, "\n"), strings.Join(got.Body, "\n\n"))

	text := strings.Join(got.Body, "\n")
	if !strings.Contains(text, doctor.FKCascadeCheckName+": "+want) {
		t.Errorf("the card does not name the constraint:\n%s", text)
	}
	if strings.Contains(text, "[code]") {
		t.Errorf("the advice drew a box to copy, but nothing in it is meant to be run:\n%s", text)
	}
	if strings.Contains(text, "--") {
		t.Errorf("a command-line flag reached the console:\n%s", text)
	}
}

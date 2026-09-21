package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// #1777: doctor wraps its fixes at about 80 columns for a terminal, and the
// console showed them verbatim in a monospace box narrower than that, so each
// prose line broke twice. remediationBlocks reads doctor's layout back into
// paragraphs, lists and code. These fixtures are the shapes doctor writes,
// copied from the real texts (the primary-key one verbatim).

type remBlock struct {
	Kind  string   `json:"kind"`
	Text  string   `json:"text"`
	Items []string `json:"items"`
}

// runRemediationBlocks executes the page's own remediationBlocks over each
// input, in node.
func runRemediationBlocks(t *testing.T, inputs []string) [][]remBlock {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	fn := functionBody(t, readAsset(t, "app.js"), "function remediationBlocks(")
	in, _ := json.Marshal(inputs)
	script := fn + "\nconsole.log(JSON.stringify(" + string(in) + ".map(remediationBlocks)));"
	path := filepath.Join(t.TempDir(), "rem.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var got [][]remBlock
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("parse %s: %v", out, err)
	}
	return got
}

func kinds(bs []remBlock) string {
	k := make([]string, len(bs))
	for i, b := range bs {
		k[i] = b.Kind
	}
	return strings.Join(k, ",")
}

const pkFix = "These tables are NOT captured. bintrail identifies a row by its primary key,\n" +
	"and a table without one is refused rather than captured without identity:\n\n" +
	"  - taking the first snapshot REFUSES outright, and the stream does not start\n" +
	"  - a later snapshot, after a schema change, EXCLUDES them and skips their\n" +
	"    row events, capturing every other table\n\n" +
	"So this is not degraded recovery for those tables. There is no history for\n" +
	"them at all, and none of it can be recovered later by adding the key: only\n" +
	"changes made AFTER the key exists are captured.\n\n" +
	"Give each table a primary key. Any existing column that is UNIQUE and\n" +
	"NOT NULL will do:\n\n" +
	"  ALTER TABLE <schema>.<table> ADD PRIMARY KEY (<unique, NOT NULL column>);\n\n" +
	"When no column qualifies, add a surrogate key, which is what InnoDB is\n" +
	"already keeping internally and invisibly:\n\n" +
	"  ALTER TABLE <schema>.<table> ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;\n\n" +
	"To list them all yourself, in the same scope this check used:\n\n" +
	"  bintrail doctor --source-dsn <dsn> --schemas shop\n\n" +
	"Capture cannot start until they do. This index has no schema snapshot\n" +
	"yet, and the first one refuses such a table, so no table at all would be\n" +
	"captured."

func TestRemediationPrimaryKeyFixReadsAsProse(t *testing.T) {
	got := runRemediationBlocks(t, []string{pkFix})[0]
	if k := kinds(got); k != "p,list,p,p,code,p,code,p,code,p" {
		t.Fatalf("blocks = %s", k)
	}
	for _, b := range got {
		if b.Kind == "p" && (strings.Contains(b.Text, "\n") || strings.Contains(b.Text, "  ")) {
			t.Errorf("paragraph kept a terminal line break: %q", b.Text)
		}
	}
	if got[0].Text != "These tables are NOT captured. bintrail identifies a row by its primary key, and a table without one is refused rather than captured without identity:" {
		t.Errorf("first paragraph = %q", got[0].Text)
	}
	want := []string{
		"taking the first snapshot REFUSES outright, and the stream does not start",
		"a later snapshot, after a schema change, EXCLUDES them and skips their row events, capturing every other table",
	}
	if strings.Join(got[1].Items, "|") != strings.Join(want, "|") {
		t.Errorf("list items = %q, want %q", got[1].Items, want)
	}
	// The commands are copied exactly: no reflow, no leading indent.
	if got[6].Text != "ALTER TABLE <schema>.<table> ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;" {
		t.Errorf("surrogate-key command = %q", got[6].Text)
	}
	if got[8].Text != "bintrail doctor --source-dsn <dsn> --schemas shop" {
		t.Errorf("doctor command = %q", got[8].Text)
	}
	if !strings.HasPrefix(got[9].Text, "Capture cannot start until they do.") || !strings.HasSuffix(got[9].Text, "would be captured.") {
		t.Errorf("verdict = %q", got[9].Text)
	}
}

func TestRemediationShapes(t *testing.T) {
	inputs := []string{
		// 0: prose then a command with no blank line, then prose (capacity).
		"Free space now: shorten retention and rotate immediately —\n" +
			"  bintrail rotate --index-dsn \"...\" --retain 7d --no-replace   # DROP PARTITION reclaims space instantly\n" +
			"(archive first with --archive-dir to keep the history). Then grow the volume or lower --rotate-retain.\n" +
			"Emergency recipe: docs/deployment.md §12; sizing math: docs/capacity.md",
		// 1: a command continued with a backslash on a deeper line (object lock).
		"Set a default retention rule so every uploaded archive is locked automatically:\n" +
			"  aws s3api put-object-lock-configuration --bucket b \\\n" +
			"    --object-lock-configuration 'ObjectLockEnabled=Enabled'\n" +
			"See docs/object-lock.md.",
		// 2: a list right under its lead-in (queryErrorRemediation).
		"The check could not query X. Common causes (most likely first):\n" +
			"  - Connection dropped or timed out: retry once before investigating further\n" +
			"  - User lacks required SELECT privilege on the relevant system table or variable\n" +
			"  - Server overloaded — raise the per-check timeout or check server load",
		// 3: numbered steps with commands under them stay as written (cascade FKs).
		"Options:\n\n" +
			"  1. Drop or change the cascade rules:\n" +
			"     ALTER TABLE <child> DROP FOREIGN KEY <fk_name>;\n\n" +
			"  2. Keep the cascades.",
		// 4: SQL comments start with "--", which is not a list item.
		"  -- MySQL 8.0+:\n  SET PERSIST binlog_row_metadata = 'FULL';",
		// 5: a list item beside a command at the same indent is not a list.
		"  - SET GLOBAL x = 1;\n  SET GLOBAL y = 2;",
		// 6: Windows line endings and trailing spaces.
		"First line   \r\nsecond line\r\n\r\n  SELECT 1;  \r\n",
		// 7: nothing to show.
		"  \n\n \t\n",
		// 8: a tab-indented command.
		"Run:\n\tSELECT 1;",
		// 9: one long line, as the Postgres checks write them.
		"Set wal_level=logical in postgresql.conf and restart the server (this setting is not reloadable).",
		// 10: a run of SQL comments alone is code, not a list of "- " items.
		"  -- MySQL 8.0+: already on\n  -- MariaDB: already on",
	}
	got := runRemediationBlocks(t, inputs)

	for i, want := range []string{"p,code,p", "p,code,p", "p,list", "p,code,code", "code", "code", "p,code", "", "p,code", "p", "code"} {
		if k := kinds(got[i]); k != want {
			t.Errorf("input %d: blocks = %q, want %q", i, k, want)
		}
	}
	if got[0][2].Text != "(archive first with --archive-dir to keep the history). Then grow the volume or lower --rotate-retain. Emergency recipe: docs/deployment.md §12; sizing math: docs/capacity.md" {
		t.Errorf("capacity tail = %q", got[0][2].Text)
	}
	if got[1][1].Text != "aws s3api put-object-lock-configuration --bucket b \\\n  --object-lock-configuration 'ObjectLockEnabled=Enabled'" {
		t.Errorf("continued command lost its shape: %q", got[1][1].Text)
	}
	if len(got[2]) == 2 && len(got[2][1].Items) != 3 {
		t.Errorf("list items = %q", got[2][1].Items)
	}
	if len(got[3]) == 3 && got[3][1].Text != "1. Drop or change the cascade rules:\n   ALTER TABLE <child> DROP FOREIGN KEY <fk_name>;" {
		t.Errorf("numbered step = %q", got[3][1].Text)
	}
	if len(got[6]) == 2 && (got[6][0].Text != "First line second line" || got[6][1].Text != "SELECT 1;") {
		t.Errorf("CRLF input = %+v", got[6])
	}
	if len(got[8]) == 2 && got[8][1].Text != "SELECT 1;" {
		t.Errorf("tab-indented command = %q", got[8][1].Text)
	}
}

// TestDoctorCardsDrawTheFixAsBlocks: the cards draw remediationEl, not the
// verbatim pre the #1777 notice showed, and remediationEl draws each block in
// its own element with textContent (a fix can carry "<schema>" and must never
// become markup).
func TestDoctorCardsDrawTheFixAsBlocks(t *testing.T) {
	js := readAsset(t, "app.js")
	cards := jsFunctionBody(t, js, "doctorCards")
	if !strings.Contains(cards, "remediationEl(chk.remediation)") {
		t.Error("doctorCards no longer draws the fix through remediationEl")
	}
	if strings.Contains(cards, `el("pre", { class: "dc-rem"`) {
		t.Error("doctorCards draws the fix verbatim in a pre again")
	}
	draw := jsFunctionBody(t, js, "remediationEl")
	for _, want := range []string{`el("p", { text: b.text })`, `el("li", { text: it })`, `el("pre", { text: b.text })`} {
		if !strings.Contains(draw, want) {
			t.Errorf("remediationEl does not draw %s", want)
		}
	}
	if strings.Contains(draw, "innerHTML") {
		t.Error("remediationEl writes markup")
	}
}

// TestVerifyRawOutputKeepsItsCodeBox: .dc-rem became the container of a
// drawn fix (#1777), so a pre that borrowed it lost its code box. The verify
// drill-down's raw output is tab-aligned rows and needs the monospace,
// wrapping box it had.
func TestVerifyRawOutputKeepsItsCodeBox(t *testing.T) {
	js := readAsset(t, "app.js")
	if regexp.MustCompile(`el\("pre", \{ class: "[^"]*\bdc-rem\b`).MatchString(js) {
		t.Error("a pre still carries dc-rem, which is now a container and styles no pre of its own")
	}
	if !strings.Contains(js, `el("pre", { class: "vfy-explain-pre", text: ex.rendered })`) {
		t.Error("the verify raw output is no longer a pre.vfy-explain-pre")
	}
	css := readAsset(t, "style.css")
	i := strings.Index(css, "\n.vfy-explain-pre {")
	if i < 0 {
		t.Fatal("no .vfy-explain-pre rule")
	}
	rule := css[i : i+strings.Index(css[i:], "}")]
	for _, want := range []string{"var(--f-mono)", "white-space: pre-wrap", "background:", "padding:"} {
		if !strings.Contains(rule, want) {
			t.Errorf(".vfy-explain-pre lost %q", want)
		}
	}
}

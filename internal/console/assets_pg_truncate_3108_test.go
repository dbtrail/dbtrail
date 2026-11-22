package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// #3108: the PostgreSQL health card for a publication that does not publish
// TRUNCATE, drawn by the real app.js in node. Amber, with the consequence
// and the statement; nothing about TRUNCATE for a snapshot without the
// field (a publication that publishes it, or one written before this).
func TestPGTruncateNotPublishedCard_3108(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	const fix = `ALTER PUBLICATION "dbtrail_pub" SET (publish = 'insert, update, delete, truncate');`
	// The health snapshot as the daemon persists it (pgstreamrun.sourceHealthJSON).
	script := renderHarnessJS + `
const walk = (n, f) => { f(n); for (const c of n.children || []) if (c && c.nodeType === 1) walk(c, f); };
const card = (extra) => { ctx.__h = Object.assign({ exists: true, active: true, wal_status: "reserved", retained_bytes: 16384, safe_wal_size: null,
  replica_identity_not_full: [], not_captured: [], checked_at: new Date().toISOString() }, extra); return vm.runInContext("pgHealthCard(__h)", ctx); };
const classes = (n, out = []) => { walk(n, (x) => { if (/hstat-/.test(x.className)) out.push(x.className.split(" ").filter((c) => /^hstat-/.test(c)).join(" ") + ":" + x.textContent); }); return out; };
const fix = ` + "`" + fix + "`" + `;
console.log(JSON.stringify({
  without: { text: card({}).textContent },
  with: { text: card({ truncate_fix: fix }).textContent, stats: classes(card({ truncate_fix: fix })) },
  probeError: { text: card({ exists: false, probe_error: "recovery is in progress" }).textContent },
  unknown: { text: card({ truncate_check_error: "permission denied for table pg_publication" }).textContent },
}));
`
	path := filepath.Join(t.TempDir(), "truncate.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		Without, ProbeError, Unknown struct{ Text string }
		With                struct {
			Text  string
			Stats []string
		}
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	t.Logf("health card, publication without TRUNCATE: %s", got.With.Text)
	for _, text := range []string{got.Without.Text, got.ProbeError.Text} {
		if strings.Contains(text, "TRUNCATE") {
			t.Errorf("a card without truncate_fix speaks of TRUNCATE: %q", text)
		}
	}
	for _, want := range []string{
		"TRUNCATE⚠ not recorded",
		"The publication does not publish TRUNCATE: the rows a TRUNCATE removes stay in the copy and come back in every rebuild. Inserts, updates and deletes are recorded. Run this on the source, as the publication's owner:",
		fix,
		// The tables are all at FULL here: that stays green beside it.
		"replica identityall FULL ✓",
	} {
		if !strings.Contains(got.With.Text, want) {
			t.Errorf("the card lacks %q:\n%s", want, got.With.Text)
		}
	}
	// A read that failed is not shown as fine (no row at all).
	if !strings.Contains(got.Unknown.Text, "TRUNCATE⚠ could not check") || !strings.Contains(got.Unknown.Text, "permission denied for table pg_publication") {
		t.Errorf("a failed read must say it could not check:\n%s", got.Unknown.Text)
	}
	amber := false
	for _, s := range got.With.Stats {
		amber = amber || s == "hstat-warn:⚠ not recorded"
		if strings.HasPrefix(s, "hstat-err") {
			t.Errorf("a red mark for a publication without TRUNCATE: %v", got.With.Stats)
		}
	}
	if !amber {
		t.Errorf("the TRUNCATE row must be amber (hstat-warn): %v", got.With.Stats)
	}
	if strings.Contains(got.With.Text, "—") {
		t.Errorf("an em dash in the card: %s", got.With.Text)
	}
}

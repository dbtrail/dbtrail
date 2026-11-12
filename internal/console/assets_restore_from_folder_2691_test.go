package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The Restore card of a server with an S3 destination whose bucket holds no
// snapshot, or could not be read, while its folder holds some (#2691): the
// restore starts from the folder, so the card is on and says so, and the
// last restore's line says where it read. Rendered by the REAL page in node.
func TestRestoreCard_startsFromTheFolderWhenS3HasNothing(t *testing.T) {
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
	note := "The S3 location s3://b/p/ could not be read (dial tcp: i/o timeout), so this restore started from the snapshot at 2026-08-28 09:00:00 in this server's local folder. A newer snapshot may be in S3."
	noteJSON, _ := json.Marshal(note)
	script := renderHarnessJS + `
vm.runInContext("capsCache = { baseline_restore: true };", ctx);
const card = vm.runInContext("backupRestoreCard", ctx);
const reg = { id: "s1", kind: "registry", baseline_dir: "/srv/snaps", baseline_s3: "s3://b/p/" };
const dirRow = { time: "2026-08-28 09:00:00", kinds: ["dir"] };
const s3Row = { time: "2026-08-28 08:00:00", kinds: ["s3"] };
const list = (rows, s3err) => ({ configured: true, source: "/srv/snaps", kind: "dir", snapshots: rows,
  sources: [{ source: "/srv/snaps", kind: "dir", count: 1 }, { source: "s3://b/p/", kind: "s3", count: 0, error: s3err || undefined }] });
const shape = (n) => {
  if (!n) return null;
  const all = []; const walk = (x) => { if (!x || !x.children) return; all.push(x); x.children.forEach(walk); };
  walk(n);
  const btn = all.find((x) => x.tag === "button" && x.textContent);
  const input = all.find((x) => x.tag === "input");
  return { why: n.dataset.why || "", text: n.textContent, value: input ? input.value : "",
    disabled: !!(btn && btn.disabled) };
};
const note = ` + string(noteJSON) + `;
console.log(JSON.stringify({
  empty: shape(card(reg, list([dirRow]), null)),
  unreadable: shape(card(reg, list([dirRow], "dial tcp: i/o timeout"), null)),
  both: shape(card(reg, list([s3Row, dirRow]), null)),
  succeeded: shape(card(reg, list([dirRow], "x"), { restore: { state: "succeeded", at: "2026-08-28T10:00:00Z", source_note: note } })),
  notSent: shape(card(reg, list([dirRow], "x"), { restore: { state: "failed", published: true, last_error: "upload: timeout", source_note: note } })),
  plain: shape(card(reg, list([s3Row]), { restore: { state: "succeeded", at: "2026-08-28T10:00:00Z" } })),
}));
`
	path := filepath.Join(t.TempDir(), "restore_from_folder.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type shape struct {
		Why, Text, Value string
		Disabled         bool
	}
	var got map[string]*shape
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	t.Logf("rendered: %s", raw)
	empty := "This server's S3 location holds no snapshot yet, so a restore starts from a snapshot in this server's folder."
	unreadable := "This server's S3 location could not be read, so a restore starts from a snapshot in this server's folder. A newer snapshot may be in S3."
	for name, want := range map[string]string{"empty": empty, "unreadable": unreadable} {
		s := got[name]
		if s == nil || s.Why != "" || s.Disabled || !strings.Contains(s.Text, want) || s.Value != "2026-08-28 09:00:00" {
			t.Errorf("%s: %+v, want an enabled card prefilled with the folder's snapshot saying %q", name, s, want)
		}
	}
	if s := got["both"]; s == nil || strings.Contains(s.Text, "starts from a snapshot in this server's folder") {
		t.Errorf("with a snapshot in S3 the card still says it reads the folder: %+v", s)
	}
	for _, name := range []string{"succeeded", "notSent"} {
		if s := got[name]; s == nil || !strings.Contains(s.Text, note) {
			t.Errorf("%s: %+v, want the last restore's line to carry %q", name, s, note)
		}
	}
	if s := got["plain"]; s == nil || strings.Contains(s.Text, "local folder") {
		t.Errorf("a restore that read S3 mentions the folder: %+v", s)
	}
	for name, s := range got {
		if s != nil && strings.Contains(s.Text, "—") {
			t.Errorf("%s: an em dash in %q", name, s.Text)
		}
	}
}

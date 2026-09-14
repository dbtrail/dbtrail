package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The form half of per-server S3 keys (#1575): two fields, the access key
// prefilled and always sent, the secret never prefilled and sent only when
// typed (blank keeps the saved one).
func TestServerFormCarriesTheS3Keys(t *testing.T) {
	js := readAsset(t, "app.js")
	form := jsFunctionBody(t, js, "buildServerForm")
	for _, field := range []string{
		`srvField("S3 access key", "s3_access_key_id"`,
		`srvField("S3 secret key", "s3_secret_access_key", { type: "password", autocomplete: "new-password"`,
	} {
		if !strings.Contains(form, field) {
			t.Errorf("buildServerForm lacks %s", field)
		}
	}
	show := jsFunctionBody(t, js, "showServerForm")
	if !strings.Contains(show, `"s3_region", "s3_access_key_id"`) {
		t.Error("showServerForm does not prefill s3_access_key_id; an edit would submit it blank and CLEAR both keys")
	}
	if strings.Contains(show, `"s3_secret_access_key"]`) || strings.Contains(show, `"s3_secret_access_key",`) {
		t.Error("showServerForm prefills the secret; the DTO never carries it")
	}
	if !strings.Contains(show, "prefill.has_s3_secret_access_key") {
		t.Error("the secret field does not say a saved secret is kept")
	}
	body := jsFunctionBody(t, js, "serverFormBody")
	for _, send := range []string{
		`s3_access_key_id: f.s3_access_key_id.value.trim()`,
		`if (f.s3_secret_access_key.value !== "") body.s3_secret_access_key = f.s3_secret_access_key.value;`,
	} {
		if !strings.Contains(body, send) {
			t.Errorf("serverFormBody lacks %s", send)
		}
	}
	for _, fn := range []string{"testServerForm", "testServerRow"} {
		if !strings.Contains(jsFunctionBody(t, js, fn), "testResultClass(res)") {
			t.Errorf("%s does not color the result with testResultClass; a failed bucket would show green", fn)
		}
	}
}

// Test connection's text and color, executed with real responses.
func TestTestResultTextExecuted(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	script := strings.Join([]string{
		functionBody(t, js, "function s3TestText("),
		functionBody(t, js, "function testResultText("),
		functionBody(t, js, "function testResultClass("),
	}, "\n") + `
const db = { ok: true, latency_ms: 5, server_version: "8.0.36", has_index: true, schema_current: true };
const cases = {
  noStore: db,
  s3ok: { ...db, s3: [{ bucket: "arch", ok: true, latency_ms: 12 }, { bucket: "bk", ok: true, latency_ms: 3 }] },
  s3fail: { ...db, s3: [{ bucket: "arch", ok: true, latency_ms: 12 }, { bucket: "bk", ok: false, error: "Forbidden", latency_ms: 4 }] },
  needsSecret: { ...db, s3: [{ bucket: "arch", ok: false, needs_secret: true, latency_ms: 0 }] },
  needsKeys: { ...db, s3: [{ bucket: "arch", ok: false, needs_keys: true, latency_ms: 0 }] },
  notApplied: { ...db, s3: [{ bucket: "arch", ok: true, not_applied: true, latency_ms: 7 }] },
  noLocation: { ...db, s3: [{ bucket: "", ok: false, error: "no Archive to S3 or Backups S3 location to test the store with", latency_ms: 0 }] },
  dbDown: { ok: false, error: "dial tcp: refused", latency_ms: 1, s3: [{ bucket: "arch", ok: true, latency_ms: 2 }] },
  pending: { ok: false, provision_pending: true, error: "index database \"x\" not provisioned yet", latency_ms: 1, s3: [{ bucket: "arch", ok: true, latency_ms: 2 }] },
  pendingS3fail: { ok: false, provision_pending: true, latency_ms: 1, s3: [{ bucket: "arch", ok: false, error: "NoSuchBucket", latency_ms: 2 }] },
};
const out = {};
for (const [k, v] of Object.entries(cases)) out[k] = { text: testResultText(v), cls: testResultClass(v) };
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "t.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var got map[string]struct{ Text, Cls string }
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	for k, v := range got {
		t.Logf("%-14s %-7s %s", k, v.Cls, v.Text)
		if strings.Contains(v.Text, "—") {
			t.Errorf("%s: em dash in %q", k, v.Text)
		}
	}
	want := map[string]struct {
		cls      string
		contains []string
		absent   []string
	}{
		"noStore":       {"ok", []string{"✓ ok · 5 ms · MySQL 8.0.36"}, []string{"S3"}},
		"s3ok":          {"ok", []string{"✓ ok · 5 ms", "✓ S3 arch · 12 ms", "✓ S3 bk · 3 ms"}, nil},
		"s3fail":        {"err", []string{"✓ S3 arch · 12 ms", "✗ S3 bk: Forbidden"}, nil},
		"needsSecret":   {"pending", []string{"○ S3 arch: type the S3 secret key to test these keys"}, []string{"✗"}},
		"noLocation":    {"err", []string{"✗ S3 store: no Archive to S3"}, nil},
		"needsKeys":     {"pending", []string{"○ S3 arch: save the server, or type S3 keys, to test a new endpoint"}, []string{"✗"}},
		"notApplied":    {"err", []string{"✓ S3 arch · 7 ms", "! S3 arch is saved but the daemon is not using it; its log says why"}, nil},
		"dbDown":        {"err", []string{"✗ dial tcp: refused", "✓ S3 arch · 2 ms"}, nil},
		"pending":       {"pending", []string{"○ index database", "✓ S3 arch · 2 ms"}, nil},
		"pendingS3fail": {"err", []string{"✗ S3 arch: NoSuchBucket"}, nil},
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Errorf("%s: no output", k)
			continue
		}
		if g.Cls != w.cls {
			t.Errorf("%s: class %q, want %q (%s)", k, g.Cls, w.cls, g.Text)
		}
		for _, c := range w.contains {
			if !strings.Contains(g.Text, c) {
				t.Errorf("%s: %q lacks %q", k, g.Text, c)
			}
		}
		for _, a := range w.absent {
			if strings.Contains(g.Text, a) {
				t.Errorf("%s: %q carries %q", k, g.Text, a)
			}
		}
	}
}

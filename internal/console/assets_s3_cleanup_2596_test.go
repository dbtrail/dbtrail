package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// #2596 part 3b, drawn: the S3 retention block of a server row from what
// the real settings endpoint answers, in each state of "Delete from S3
// after", and the field's save.
func TestS3CleanupBlock_drawn(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	t.Cleanup(resetS3Cleanups)
	srv := newRegistryServer(t)
	srv.localPruneLoop = true
	add := func(name, s3, age string) ServerEntry {
		t.Helper()
		e, err := srv.cm.reg.Add(ServerEntry{Name: name, DSN: "u:p@tcp(h:3306)/" + name, BaselineDir: t.TempDir(), BaselineS3: s3, BaselineS3Retain: age})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	row := func(e ServerEntry) string {
		t.Helper()
		b, err := json.Marshal(srv.backupSettingsServerDTO(e))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	off := row(add("off", "s3://bkt/off", ""))
	on := add("on", "s3://bkt/on", "7d")
	RecordS3Cleanup("s3://bkt/on", S3CleanupRecord{At: "2026-11-01T12:00:00Z", Removed: 3, KeptNewest: 1, Deferred: 2, Stopped: "bound"})
	onRow := row(on)
	failed := add("failed", "s3://bkt/failed", "7d")
	RecordS3Cleanup("s3://bkt/failed", S3CleanupRecord{At: "2026-11-01T12:00:00Z", Removed: 1, Error: "S3 snapshot prune: remove snapshot x: AccessDenied", Snapshot: "x", AccessDenied: true})
	failedRow := row(failed)
	hours := row(add("hours", "s3://bkt/hours", "36h"))
	srv.localPruneLoop = false
	noLoop := row(on)
	srv.localPruneLoop = true
	timeout := add("timeout", "s3://bkt/timeout", "7d")
	RecordS3Cleanup("s3://bkt/timeout", S3CleanupRecord{At: "2026-11-01T12:00:00Z", Error: "S3 snapshot prune: list s3://bkt/timeout: i/o timeout"})
	timeoutRow := row(timeout)
	sharedE := add("shared", "s3://bkt/shared", "7d")
	add("quiet", "s3://bkt/shared", "")
	shared := row(sharedE)

	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const byClass = (n, c, out = []) => { if (!n || typeof n !== "object") return out; if ((" " + (n.className || "") + " ").includes(" " + c + " ")) out.push(n); for (const k of n.children || []) byClass(k, c, out); return out; };
const text = (n) => !n ? "" : n.nodeType === 3 ? n.textContent : (n._text || "") + (n.children || []).map(text).join("");
const find = (n, pred) => { if (!n || typeof n !== "object") return null; if (pred(n)) return n; for (const c of n.children || []) { const f = find(c, pred); if (f) return f; } return null; };
vm.runInContext("capsCache = { backup_schedule: true };", ctx);
const draw = (srv, perms) => {
  vm.runInContext("sessionMay = (p) => " + JSON.stringify(perms) + ".includes(p);", ctx);
  const box = vm.runInContext("s3CleanupBlock", ctx)(srv, false);
  return { red: byClass(box, "err").map(text).filter(Boolean), hints: byClass(box, "form-hint").map(text), all: text(box),
    field: !!find(box, (n) => n.tag === "input") };
};
(async () => {
  const out = {
    off: draw(` + off + `, ["settings:write", "servers:write"]),
    on: draw(` + onRow + `, ["settings:write", "servers:write"]),
    failed: draw(` + failedRow + `, ["settings:write", "servers:write"]),
    shared: draw(` + shared + `, ["settings:write", "servers:write"]),
    operator: draw(` + onRow + `, ["servers:write"]),
    settingsOnly: draw(` + onRow + `, ["settings:write"]),
    noLoop: draw(` + noLoop + `, ["settings:write", "servers:write"]),
    timeout: draw(` + timeoutRow + `, ["settings:write", "servers:write"]),
    firstNoLoop: vm.runInContext("s3RetentionBox", ctx)(` + noLoop + `, [], "").children.map(text)[0],
    noneNoLoop: vm.runInContext("s3ExpiryWords", ctx)({ state: "none" }, ` + noLoop + `, 100),
    noneFailing: vm.runInContext("s3ExpiryWords", ctx)({ state: "none" }, ` + timeoutRow + `, 100),
    first: vm.runInContext("s3RetentionBox", ctx)(` + onRow + `, [], "").children.map(text)[0],
    firstOff: vm.runInContext("s3RetentionBox", ctx)(` + off + `, [], "").children.map(text)[0],
    none: vm.runInContext("s3ExpiryWords", ctx)({ state: "none" }, ` + onRow + `, 100),
    noneOff: vm.runInContext("s3ExpiryWords", ctx)({ state: "none" }, ` + off + `, 100),
  };
  // The save: 14 days, then empty.
  const calls = [];
  ctx.__calls = calls;
  vm.runInContext("api = async (p, o) => { __calls.push({ path: p, body: o.body, method: o.method }); return {}; }; toast = () => {}; renderSnapshots = async () => {}; sessionMay = () => true;", ctx);
  const box = vm.runInContext("s3CleanupBlock", ctx)(` + onRow + `);
  const input = find(box, (n) => n.tag === "input");
  const save = find(box, (n) => n.tag === "button" && n.attrs["data-save"] === "s3-retain");
  out.savedValue = input.value;
  out.disabledAtRest = save.disabled;
  input.value = "14"; await save.onclick();
  input.value = ""; await save.onclick();
  input.value = "0"; await save.onclick();
  // What the browser cannot read is a typing error, not "off".
  input.value = ""; input.validity = { badInput: true }; await save.onclick();
  out.calls = calls;
  out.zeroMsg = byClass(box, "err").map(text).join("|");
  // A saved age in hours: the field is empty and its Save asleep.
  const hb = vm.runInContext("s3CleanupBlock", ctx)(` + hours + `, false);
  const hInput = find(hb, (n) => n.tag === "input");
  const hSave = find(hb, (n) => n.tag === "button" && n.attrs["data-save"] === "s3-retain");
  out.hours = { value: hInput.value, disabled: hSave.disabled, all: text(hb) };
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e); process.exit(1); });
`
	path := filepath.Join(t.TempDir(), "cleanup.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type drawn struct {
		Red   []string
		Hints []string
		All   string
		Field bool
	}
	var got struct {
		Off, On, Failed, Shared, Operator drawn
		SettingsOnly, NoLoop, Timeout     drawn
		FirstNoLoop                       string
		NoneNoLoop, NoneFailing           struct {
			Text string
			Warn bool
		}
		Hours struct {
			Value    string
			Disabled bool
			All      string
		}
		First, FirstOff                   string
		None, NoneOff                     struct {
			Text string
			Warn bool
		}
		SavedValue     string `json:"savedValue"`
		DisabledAtRest bool   `json:"disabledAtRest"`
		Calls          []struct {
			Path, Method string
			Body         map[string]any
		}
		ZeroMsg string `json:"zeroMsg"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	for name, d := range map[string]drawn{"off": got.Off, "on": got.On, "failed": got.Failed, "shared": got.Shared, "operator": got.Operator} {
		t.Logf("%s: %s", name, d.All)
	}
	t.Logf("first: %s", got.First)
	t.Logf("none: %+v / off: %+v", got.None, got.NoneOff)
	has := func(list []string, want string) bool {
		for _, s := range list {
			if strings.Contains(s, want) {
				return true
			}
		}
		return false
	}
	if !strings.HasPrefix(got.First, "DBTrail removes this server's snapshots from S3 once they are older than 7 days, and keeps the newest snapshot of each table however old.") {
		t.Errorf("first line with an age: %q", got.First)
	}
	if !strings.Contains(got.FirstOff, "DBTrail never removes one unless Delete from S3 after is set below") {
		t.Errorf("first line without: %q", got.FirstOff)
	}
	if !has(got.On.Hints, "Last cleanup 2026-11-01 12:00:00 UTC: removed 3 snapshot(s), kept 1 older one(s) that hold a table's newest copy; 2 more go in the next cleanups.") {
		t.Errorf("on: %v", got.On.Hints)
	}
	if !strings.Contains(got.On.All, "DBTrail never removes a version") || !strings.Contains(got.On.All, `"NoncurrentDays": 1`) {
		t.Errorf("on: the versioning notice or its rule is missing: %s", got.On.All)
	}
	if !has(got.Failed.Red, "Last cleanup 2026-11-01 12:00:00 UTC stopped after removing 1 snapshot(s): S3 snapshot prune: remove snapshot x: AccessDenied. Removing needs s3:DeleteObject and s3:PutObject on the prefix.") {
		t.Errorf("failed: %v", got.Failed.Red)
	}
	if !has(got.Shared.Red, "Delete from S3 after is set, but nothing is removed: this destination is shared with quiet, which has no age set.") {
		t.Errorf("shared: %v", got.Shared.Red)
	}
	if got.Off.Field != true || len(got.Off.Red) != 0 || strings.Contains(got.Off.All, "Last cleanup") {
		t.Errorf("off: %+v", got.Off)
	}
	if got.Operator.Field || !strings.Contains(got.Operator.All, "Setting or raising it needs the settings:write permission") {
		t.Errorf("operator: a session without settings:write gets the field: %+v", got.Operator)
	}
	if got.None.Warn || !strings.Contains(got.None.Text, "DBTrail removes those older than 7 days") {
		t.Errorf("no bucket rule with DBTrail cleaning: %+v", got.None)
	}
	if !got.NoneOff.Warn || !strings.Contains(got.NoneOff.Text, "grows without limit") {
		t.Errorf("no bucket rule and no cleanup: %+v", got.NoneOff)
	}
	if got.SettingsOnly.Field {
		t.Errorf("a session without servers:write gets a field whose save the route refuses")
	}
	// serve: no cleanup runs here, so nothing says one does.
	if strings.Contains(got.FirstNoLoop, "DBTrail removes") || !got.NoneNoLoop.Warn || strings.Contains(got.NoLoop.All, "Last cleanup") ||
		!strings.Contains(got.NoLoop.All, "this web interface does not, and cannot tell whether it removes anything") || len(got.NoLoop.Red) != 0 {
		t.Errorf("no loop: first %q, none %+v, block %q", got.FirstNoLoop, got.NoneNoLoop, got.NoLoop.All)
	}
	// Every cleanup failing: the bucket line is not calm, and a timeout is
	// not sent to fix permissions.
	if !got.NoneFailing.Warn {
		t.Errorf("a failing cleanup reads as calm: %+v", got.NoneFailing)
	}
	if strings.Contains(got.Timeout.All, "s3:DeleteObject") {
		t.Errorf("a timeout names permissions: %s", got.Timeout.All)
	}
	if !has(got.Failed.Red, "Removing needs s3:DeleteObject and s3:PutObject on the prefix.") {
		t.Errorf("an AccessDenied does not name the permissions: %v", got.Failed.Red)
	}
	if got.Hours.Value != "" || !got.Hours.Disabled || !strings.Contains(got.Hours.All, "Saved as 36h, which is not a whole number of days") {
		t.Errorf("36h: %+v", got.Hours)
	}
	if got.SavedValue != "7" || !got.DisabledAtRest {
		t.Errorf("field shows %q (save disabled %v), want 7 and asleep", got.SavedValue, got.DisabledAtRest)
	}
	if len(got.Calls) != 2 || got.Calls[0].Method != "PUT" || got.Calls[0].Body["baseline_s3_retain"] != "14d" || got.Calls[1].Body["baseline_s3_retain"] != "" || len(got.Calls[0].Body) != 1 {
		t.Errorf("saves = %+v, want 14d then off, each sending only the age", got.Calls)
	}
	if !strings.Contains(got.ZeroMsg, "Enter a whole number of days, 1 or more") {
		t.Errorf("0 days was not refused on the page: %q", got.ZeroMsg)
	}
	for _, bad := range []string{"undefined", "NaN", "null", "—"} {
		if strings.Contains(string(raw), bad) {
			t.Errorf("drawn %q", bad)
		}
	}
}

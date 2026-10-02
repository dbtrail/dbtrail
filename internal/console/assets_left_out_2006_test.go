package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
)

// #2006: a full read that left a table out (its name cannot be stored) is
// said in red on the schedule card, with the table and why; a session with a
// data profile gets the count only. And a fallback the daily cap held back
// says when the next one is allowed. Real DTO builder, real page code.
func TestBackupScheduleCard_leftOutAndCap_2006(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	rep := &stubScheduleReporter{full: true, state: map[string]BackupScheduleState{}}
	srv, id := newScheduleServer(t, rep)
	e, _ := srv.cm.reg.Get(id)
	fakeSnapshot(t, e.BaselineDir)
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", `{"every":"5m"}`); rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	e, _ = srv.cm.reg.Get(id)
	now := time.Date(2026, 10, 2, 10, 20, 0, 0, time.UTC)
	left, omitted := LeftOutTablesOf([]LeftOut{{Table: "demo.order/items",
		Reason: `the name holds a "/", which a snapshot cannot store as a file name (dump file demo.mydumper_1-schema.sql)`}})
	rep.state[id] = BackupScheduleState{LastStartedAt: "2026-10-02T10:00:05Z", LastMethod: BackupMethodFull,
		Last: &BaselineStatus{State: "succeeded", Since: "2026-10-02T10:00:05Z", FinishedAt: "2026-10-02T10:01:00Z",
			Tables: 4, Published: true, LeftOutTables: left, LeftOutTablesOmitted: omitted}}
	docOf := func(r *http.Request) string {
		b, err := json.Marshal(withholdScheduleTables(r, srv.backupScheduleDTO(context.Background(), e, now)))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	open := httptest.NewRequest("GET", "/x", nil)
	profiled := open.WithContext(context.WithValue(open.Context(), policyCtxKey{},
		&ext.AccessPolicy{Profile: "analyst", Permissions: ext.AllPermissions()}))
	leftDoc, hiddenDoc := docOf(open), docOf(profiled)

	rep.state[id] = BackupScheduleState{LastStartedAt: "2026-10-02T10:00:05Z", LastMethod: BackupMethodRefresh,
		Last: &BaselineStatus{State: "failed", Since: "2026-10-02T10:00:05Z", FinishedAt: "2026-10-02T10:00:30Z",
			LastError: "demo.orders: schema changed since the baseline"},
		LastSkippedAt: "2026-10-02T10:00:31Z",
		LastSkipReason: "the update from the recorded changes was refused (demo.orders: schema changed since the baseline) and no full read is taken in its place: " +
			"DBTrail reads this server in full in place of a failed update at most once a day, and the last such read was less than a day ago; the next one is allowed after 2026-10-03 09:00 UTC"}
	capDoc := docOf(open)

	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
vm.runInContext("capsCache.backup_schedule = true;", ctx);
const walk = (n, f) => { if (!n || n.nodeType !== 1) return; f(n); for (const c of n.children) walk(c, f); };
const text = (n) => { const out = []; walk(n, (x) => { if (x._text) out.push(x._text); }); return out; };
const cur = { id: "s1", kind: "registry", baseline_s3: "" };
const out = {};
for (const [k, doc] of Object.entries({ left: ` + leftDoc + `, hidden: ` + hiddenDoc + `, cap: ` + capDoc + ` })) {
  out[k] = text(ctx.backupScheduleCard(cur, { schedule: doc })).join(" | ");
}
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "card.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string]string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("%v\n%s", err, raw)
	}
	for k, v := range got {
		t.Logf("%s card: %s", k, v)
	}
	for _, want := range []string{"1 table is not in this snapshot", "demo.order/items", "left out", `holds a "/"`} {
		if !strings.Contains(got["left"], want) {
			t.Errorf("left-out card lacks %q", want)
		}
	}
	if !strings.Contains(got["hidden"], "1 table is not in this snapshot") || strings.Contains(got["hidden"], "order/items") {
		t.Errorf("a profiled session must get the count, not the name: %s", got["hidden"])
	}
	if !strings.Contains(got["cap"], "the next one is allowed after 2026-10-03 09:00 UTC") {
		t.Errorf("the cap card does not say when the next full read is allowed: %s", got["cap"])
	}
	for k, v := range got {
		if strings.Contains(v, "—") || strings.Contains(v, " --") {
			t.Errorf("%s card has an em dash or a flag: %s", k, v)
		}
	}
}

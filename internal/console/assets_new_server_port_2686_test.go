package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// #2686: the answer to an added server, as the web interface draws its part
// about the MySQL port. The password is drawn whenever the answer carries
// one, since no later answer does; a port that was already on, or was saved
// as off, draws nothing.
func TestNewServerPortBlock_2686(t *testing.T) {
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
	script := renderHarnessJS + `
const block = vm.runInContext("newServerPortBlock", ctx);
const notice = vm.runInContext("connectNotice", ctx);
const res = (port, name) => ({ ok: true, started: true, name: name || "shop", doctor: { checks: [] }, port });
const cases = {
  turnedOn: res({ enabled: true, turned_on: true, routing_on: true, host: "", port: "3309", password: "bfp_abc123" }),
  turnedOnOtherAddress: res({ enabled: true, turned_on: true, routing_on: true, host: "10.0.0.5", port: "3310", password: "bfp_abc123" }),
  turnedOnNoPassword: res({ enabled: true, turned_on: true, routing_on: true, port: "3309" }),
  turnedOnNotAnswering: res({ enabled: true, turned_on: true, routing_on: true, port: "3309", password: "bfp_x", error: "address already in use" }),
  unsafeName: res({ enabled: true, turned_on: true, routing_on: true, port: "3309", password: "bfp_x" }, "my shop; rm"),
  expandedName: res({ enabled: true, turned_on: true, routing_on: true, port: "3309", password: "bfp_x" }, "a{b,c}$(id)"),
  newlineName: res({ enabled: true, turned_on: true, routing_on: true, port: "3309", password: "bfp_x" }, "shop\nrm -rf x"),
  plainName: res({ enabled: true, turned_on: true, routing_on: true, port: "3309", password: "bfp_x" }, "db1.example.com:3306"),
  alreadyOn: res({ enabled: true, turned_on: false, routing_on: true, port: "3309", has_password: true }),
  savedOff: res({ enabled: false }),
  notTurnedOn: res({ enabled: false, not_turned_on: "its setting could not be saved: read-only file system." }),
  olderDaemon: res(undefined),
  noticeOn: res({ enabled: true, turned_on: true, routing_on: true, port: "3309", password: "bfp_x", notice: "NOTHING ANSWERS ON IT: no router." }),
  noWordOnEnabled: res({ turned_on: true, password: "bfp_x" }),
  withRoute: Object.assign(res({ enabled: true, turned_on: true, routing_on: true, port: "3309", password: "bfp_x" }), { server: { id: "s1", has_route: true } }),
};
const out = {};
for (const [k, r] of Object.entries(cases)) {
  const b = block(r);
  const n = notice(r);
  out[k] = { block: b ? b.textContent : null, inNotice: (n.content || []).map((c) => c.textContent).join("\n") };
}
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "port.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string]struct {
		Block    *string `json:"block"`
		InNotice string  `json:"inNotice"`
	}
	s := strings.TrimSpace(string(raw))
	if err := json.Unmarshal([]byte(s[strings.LastIndex(s, "\n")+1:]), &got); err != nil {
		t.Fatalf("output %q: %v", raw, err)
	}
	text := func(k string) string {
		t.Helper()
		if got[k].Block == nil {
			t.Fatalf("%s: nothing was drawn", k)
		}
		return *got[k].Block
	}
	has := func(k string, wants ...string) {
		t.Helper()
		for _, w := range wants {
			if !strings.Contains(text(k), w) {
				t.Errorf("%s lacks %q:\n%s", k, w, text(k))
			}
		}
	}
	hasNot := func(k string, nots ...string) {
		t.Helper()
		for _, w := range nots {
			if strings.Contains(text(k), w) {
				t.Errorf("%s says %q:\n%s", k, w, text(k))
			}
		}
	}
	has("turnedOn", "The MySQL port is on now, with Engine Routing on.", "mysql -h 127.0.0.1 -P 3309 -u shop -p",
		"Password, shown only now.", "bfp_abc123", "Statements for shop are answered once its first copy is ready.")
	has("turnedOnOtherAddress", "mysql -h 10.0.0.5 -P 3310 -u shop -p")
	has("turnedOnNoPassword", "No password came with the answer.")
	hasNot("turnedOnNoPassword", "shown only now")
	has("turnedOnNotAnswering", "bfp_x", "It is not answering: address already in use")
	has("unsafeName", "-u <server name> -p")
	hasNot("unsafeName", "-u my shop")
	has("expandedName", "-u <server name> -p")
	hasNot("expandedName", "-u a{b")
	has("newlineName", "-u <server name> -p")
	has("plainName", "-u db1.example.com:3306 -p")
	has("withRoute", "Statements for shop are answered once its first copy is ready.")
	hasNot("withRoute", "Until it has a forwarding user")
	has("turnedOn", "Until it has a forwarding user")
	has("noticeOn", "bfp_x", "NOTHING ANSWERS ON IT: no router.")
	has("notTurnedOn", "The MySQL port was not turned on: its setting could not be saved: read-only file system.")
	hasNot("notTurnedOn", "system..", "mysql -h", "shown only now")
	for _, k := range []string{"alreadyOn", "savedOff", "olderDaemon", "noWordOnEnabled"} {
		if got[k].Block != nil {
			t.Errorf("%s drew %q; nothing about the port changed, or the answer does not say", k, *got[k].Block)
		}
	}
	// The password reaches the screen through the notice the Connect screen
	// shows, not only through the function.
	if !strings.Contains(got["turnedOn"].InNotice, "bfp_abc123") {
		t.Errorf("the notice of a started capture does not carry the password:\n%s", got["turnedOn"].InNotice)
	}
	if strings.Contains(got["alreadyOn"].InNotice, "MySQL port") {
		t.Errorf("the notice speaks of a port that did not change:\n%s", got["alreadyOn"].InNotice)
	}
}

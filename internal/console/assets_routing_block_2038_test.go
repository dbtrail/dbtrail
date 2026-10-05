package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestRoutingBlockSaysWhoAnswered renders the "Who answered" block of the
// Connect a SQL client panel (#2038) through the real app.js, one shape per
// state the server can report: routing off (name the flag), on with no
// server picked, on with no decision yet, on with counts and reasons, a
// server whose connections cannot route, a policy with no threshold, and
// the port's mode (#2079): read-write, or read-only with its refusals.
// The duration formatter is pinned over the values that broke its first
// version ("10m0s" read as "1").
func TestRoutingBlockSaysWhoAnswered(t *testing.T) {
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
const flat = (n, out = []) => { if (n == null) return out; if (typeof n === "string") { out.push(n); return out; }
  if (n.nodeType === 3) { out.push(n.textContent); return out; } if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const block = vm.runInContext("routingBlock", ctx);
const fmt = vm.runInContext("fmtGoDuration", ctx);
const draw = (routing, cur) => flat(block({ enabled: true, listen: "127.0.0.1:3308", port: "3308", routing }, cur)).join(" ").replace(/\s+/g, " ");
const srv = { id: "s1", name: "shop", has_source: true, source_user: "repl" };
const fwd = { ...srv, has_route: true, route_user: "report_ro" };
const on = { enabled: true, max_copy_age: "10m0s", cost_threshold: 10000, scan_rows: 100000, since: "2026-10-04T12:00:00Z" };
console.log(JSON.stringify({
  off: draw({ enabled: false }, srv),
  noServer: draw(on, null),
  none: draw(on, srv),
  counts: draw({ ...on, servers: { s1: { copy: 3, mysql: 7, reasons: { expensive_plan: 3, cheap_plan: 5, write: 2 } } } }, srv),
  other: draw({ ...on, servers: { s2: { copy: 3, mysql: 7, reasons: { expensive_plan: 3 } } } }, srv),
  unavailable: draw({ ...on, servers: { s1: { copy: 0, mysql: 0, reasons: {}, unavailable: "the server has no source database to forward to" } } }, srv),
  noRule: draw({ ...on, cost_threshold: 0, scan_rows: 0 }, srv),
  readOnly: draw({ ...on, read_only: true, servers: { s1: { copy: 3, mysql: 7, refused: 2, reasons: { expensive_plan: 3, cheap_plan: 7, read_only: 2 } } } }, srv),
  readOnlyOnlyRefusals: draw({ ...on, read_only: true, servers: { s1: { copy: 0, mysql: 0, refused: 4, reasons: { read_only: 4 } } } }, srv),
  readOnlyNoRule: draw({ ...on, read_only: true, cost_threshold: 0, scan_rows: 0 }, srv),
  forwarding: draw(on, fwd),
  forwardingReadOnly: draw({ ...on, read_only: true }, fwd),
  forwardingNoName: draw(on, { ...srv, has_route: true }),
  noSource: draw(on, { id: "s1", name: "shop" }),
  elsewhere: draw(on, { ...fwd, route_host: "replica.internal", route_port: "3307" }),
  unreadable: draw(on, { ...srv, route_unreadable: true }),
  isCapture: draw(on, { ...srv, route_is_capture: true, route_user: "repl" }),
  refused: draw({ ...on, servers: { s1: { copy: 0, mysql: 4, reasons: { upstream_lost: 4 }, account_refused: "the forwarding account report_ro: MySQL error 1045, Access denied for user 'report_ro'@'10.0.0.5' (using password: YES)" } } }, fwd),
  refusedNoCounts: draw({ ...on, servers: { s1: { copy: 0, mysql: 0, reasons: {}, account_refused: "the source account repl (this server has no forwarding account): MySQL error 1045, Access denied" } } }, srv),
  hint: vm.runInContext("FORWARDING_HINT", ctx),
  durations: ["15m0s", "10m0s", "1h0m0s", "1h30m0s", "1m30s", "30s", "24h0m0s"].map(fmt),
}));
`
	path := filepath.Join(t.TempDir(), "routing.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		Off, NoServer, None, Counts, Other, Unavailable, NoRule string
		ReadOnly, ReadOnlyOnlyRefusals, ReadOnlyNoRule          string
		Forwarding, ForwardingReadOnly, ForwardingNoName        string
		NoSource, Hint                                          string
		Elsewhere, Unreadable, IsCapture                        string
		Refused, RefusedNoCounts                                string
		Durations                                               []string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	for name, text := range map[string]string{"off": got.Off, "no server": got.NoServer, "none": got.None, "counts": got.Counts, "unavailable": got.Unavailable, "no rule": got.NoRule,
		"read-only": got.ReadOnly, "only refusals": got.ReadOnlyOnlyRefusals, "ro no rule": got.ReadOnlyNoRule,
		"forwarding": got.Forwarding, "forwarding ro": got.ForwardingReadOnly, "fwd no name": got.ForwardingNoName, "no source": got.NoSource, "form hint": got.Hint,
		"elsewhere": got.Elsewhere, "unreadable": got.Unreadable, "is capture": got.IsCapture, "refused": got.Refused, "refused 0": got.RefusedNoCounts} {
		t.Logf("%-12s %s", name+":", text)
	}
	must := func(name, text string, wants ...string) {
		t.Helper()
		for _, w := range wants {
			if !strings.Contains(text, w) {
				t.Errorf("%s: missing %q in: %s", name, w, text)
			}
		}
		if strings.Contains(text, "—") || strings.Contains(text, "undefined") || strings.Contains(text, "NaN") {
			t.Errorf("%s: holds an em dash, undefined or NaN: %s", name, text)
		}
	}
	must("off", got.Off, "the copy, always", "Read routing is off", "--route-max-copy-age", "BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE")
	must("no server", got.NoServer, "pick a server in the left sidebar")
	must("none", got.None, "shop", "no statements yet", "at most 10m old", "plan costs at least 10000", "at least 100000 rows", "Refresh")
	must("counts", got.Counts, "7 by MySQL", "3 by the copy", "3 × an expensive plan", "5 × a cheap plan", "2 × a write", "Why each side")
	// Another server's counts never show under the picked one.
	must("other server", got.Other, "no statements yet")
	if strings.Contains(got.Other, "7 by MySQL") {
		t.Errorf("another server's counts rendered under shop: %s", got.Other)
	}
	must("unavailable", got.Unavailable, "the copy, always", "Routing is off for this server", "no source database to forward to")
	if strings.Contains(got.Unavailable, "runs on the copy when") {
		t.Errorf("a server that cannot route still shows the routing rule: %s", got.Unavailable)
	}
	must("no rule", got.NoRule, "No plan threshold is set", "nothing reaches the copy")
	// The port's mode (#2079) is said either way, and the refusals are
	// counted on their own: nobody answered those.
	must("off", got.Off, "--route-read-only", "BINTRAIL_CONSOLE_ROUTE_READ_ONLY")
	must("counts", got.Counts, "This port is read-write.", "Writes sent to it run on MySQL", "--route-read-only")
	must("none", got.None, "This port is read-write.")
	must("no rule", got.NoRule, "This port is read-write.")
	must("read-only", got.ReadOnly, "This port is read-only.", "is refused and never sent to MySQL", "Every other read runs on MySQL",
		"7 by MySQL", "3 by the copy", "2 refused", "2 × refused: not a read, and this port is read-only")
	must("only refusals", got.ReadOnlyOnlyRefusals, "0 by MySQL", "0 by the copy", "4 refused", "Why each side")
	must("ro no rule", got.ReadOnlyNoRule, "This port is read-only.", "No plan threshold is set", "every read runs on MySQL")
	for name, text := range map[string]string{"read-only": got.ReadOnly, "only refusals": got.ReadOnlyOnlyRefusals, "ro no rule": got.ReadOnlyNoRule} {
		if strings.Contains(text, "read-write") || strings.Contains(text, "writes included") {
			t.Errorf("%s: a read-only port is described as taking writes: %s", name, text)
		}
	}
	// Which account runs what MySQL answers (#2079): the forwarding user
	// when the server has one, else the capture account, by user name.
	must("none", got.None, "On MySQL, statements run as repl", "the account DBTrail captures with", "set a forwarding user")
	must("read-only", got.ReadOnly, "On MySQL, statements run as repl", "the permissions of the account below")
	must("forwarding", got.Forwarding, "On MySQL, statements run as report_ro", "this server's forwarding user", "The account DBTrail captures with is not used by this port")
	must("forwarding ro", got.ForwardingReadOnly, "This port is read-only.", "statements run as report_ro")
	must("fwd no name", got.ForwardingNoName, "statements run as this server's forwarding account")
	must("form hint", got.Hint, "Forwarding user", "SELECT only", "Blank forwards with the source user")
	// An account that connects somewhere else than the source says where.
	must("elsewhere", got.Elsewhere, "statements run as report_ro", "replica.internal:3307", "not the source's address")
	if strings.Contains(got.Forwarding, "address") {
		t.Errorf("an account on the source's own address talks about an address: %s", got.Forwarding)
	}
	// A stored account that cannot be read, or that is the capture account,
	// is never presented as a separate account.
	must("unreadable", got.Unreadable, "saved forwarding account cannot be read", "Edit the server")
	must("is capture", got.IsCapture, "On MySQL, statements run as repl", "the account DBTrail captures with", "saved forwarding account is that same account")
	for name, text := range map[string]string{"unreadable": got.Unreadable, "is capture": got.IsCapture} {
		if strings.Contains(text, "is not used by this port") {
			t.Errorf("%s: says the capture account is not used: %s", name, text)
		}
	}
	// What the source said when it turned the port's login away.
	must("refused", got.Refused, "MySQL refused the login of the forwarding account report_ro", "MySQL error 1045", "error 2006", "Test connection")
	must("refused 0", got.RefusedNoCounts, "MySQL refused the login of the source account repl", "source user")
	// Test connection tries the forwarding account, and a login that works
	// clears this; it does not try the source account through the port.
	if strings.Contains(got.RefusedNoCounts, "Test connection") {
		t.Errorf("the source account's refusal points at Test connection, which does not try it: %s", got.RefusedNoCounts)
	}
	if strings.Contains(got.Forwarding, "refused the login") {
		t.Errorf("a server with no refusal shows one: %s", got.Forwarding)
	}
	for name, text := range map[string]string{"forwarding": got.Forwarding, "forwarding ro": got.ForwardingReadOnly, "fwd no name": got.ForwardingNoName} {
		if strings.Contains(text, "repl") || strings.Contains(text, "whatever that account can") {
			t.Errorf("%s: a server with a forwarding account still names the capture account as the one that runs statements: %s", name, text)
		}
	}
	if strings.Contains(got.NoSource, "statements run as") || strings.Contains(got.Unavailable, "statements run as") {
		t.Errorf("a server with no source names an account: %s / %s", got.NoSource, got.Unavailable)
	}
	for name, text := range map[string]string{"counts": got.Counts, "none": got.None, "unavailable": got.Unavailable} {
		if strings.Contains(text, "read-only.") || strings.Contains(text, "refused") {
			t.Errorf("%s: a read-write port is described as read-only: %s", name, text)
		}
	}
	if want := []string{"15m", "10m", "1h", "1h30m", "1m30s", "30s", "24h"}; strings.Join(got.Durations, ",") != strings.Join(want, ",") {
		t.Errorf("durations = %v, want %v", got.Durations, want)
	}
}

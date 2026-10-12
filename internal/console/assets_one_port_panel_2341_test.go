package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestSQLPortAddressIsTheRoutersToSay (#2341): the host and the port of the
// mysql line come from what the router says it listens on (GET /api/router),
// through the real sqlPortAddress. An address on every interface names no
// host, an IPv6 one loses its brackets, and with no usable answer the port
// is its usual one and is said to be unconfirmed.
func TestSQLPortAddressIsTheRoutersToSay(t *testing.T) {
	view := func(listen string) routerViewDTO {
		return routerViewDTO{Configured: true, URL: "http://router:9091/status", ReadAt: "2026-10-11T14:31:00Z",
			Status: &RouterStatusDoc{Version: 1, Listen: listen}}
	}
	type want struct {
		Host, Port string
		Confirmed  bool
	}
	unconfirmed := want{"", "3309", false}
	cases := []struct {
		name string
		rt   any
		want want
	}{
		{"one address", view("127.0.0.1:3309"), want{"127.0.0.1", "3309", true}},
		{"every IPv4 address", view("0.0.0.0:3309"), want{"", "3309", true}},
		{"every IPv6 address", view("[::]:3309"), want{"", "3309", true}},
		{"an IPv6 address", view("[::1]:13399"), want{"::1", "13399", true}},
		{"no host", view(":3309"), want{"", "3309", true}},
		{"a name and another port", view("db.internal:3311"), want{"db.internal", "3311", true}},
		{"no router configured", routerViewDTO{}, unconfirmed},
		{"a router that did not answer", routerViewDTO{Configured: true, URL: "http://router:9091/status", Error: "connection refused"}, unconfirmed},
		{"the asking failed", nil, unconfirmed},
		{"a listen that is no address", view("not-an-address"), unconfirmed},
		{"an empty listen", view(""), unconfirmed},
		{"a listen with no port number", view("127.0.0.1:mysql"), unconfirmed},
		{"a listen with nothing after the colon", view("127.0.0.1:"), unconfirmed},
	}
	views := make([]any, len(cases))
	for i, c := range cases {
		views[i] = c.rt
	}
	arg, err := json.Marshal(views)
	if err != nil {
		t.Fatal(err)
	}
	raw := runNodeConnectArgs(t, renderHarnessJS+`
const at = vm.runInContext("sqlPortAddress", ctx);
const views = JSON.parse(process.argv[4]);
// undefined is "not asked": it cannot travel in JSON, so it is added here.
console.log(JSON.stringify({ cases: views.map((v) => at(v)), notAsked: at(undefined) }));
`, string(arg))
	var got struct {
		Cases    []want
		NotAsked want
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if len(got.Cases) != len(cases) {
		t.Fatalf("%d answers for %d cases: %s", len(got.Cases), len(cases), raw)
	}
	for i, c := range cases {
		if got.Cases[i] != c.want {
			t.Errorf("%s: sqlPortAddress = %+v, want %+v", c.name, got.Cases[i], c.want)
		}
	}
	if got.NotAsked != unconfirmed {
		t.Errorf("not asked: sqlPortAddress = %+v, want %+v", got.NotAsked, unconfirmed)
	}
}

// TestRouterMemoryNote (#2341): the router reads the saved memory when it
// starts, so the memory panel says when the router runs with another one,
// and says nothing when they agree or either is unknown. The router spells
// its memory in MiB and the setting in bytes; 2048MiB, 2GiB and 2GB are the
// same amount here.
func TestRouterMemoryNote(t *testing.T) {
	raw := runNodeConnectArgs(t, renderHarnessJS+`
const note = vm.runInContext("routerMemoryNote", ctx);
const st = { memory_bytes: 2147483648, memory: "2 GB" };
const rt = (each) => ({ configured: true, status: { pool: { memory_each: each } } });
const out = {};
for (const each of ["2048MiB", "2GiB", "2GB", "4096MiB", "4GB", "512MiB", "", "junk"]) out[each] = note(st, rt(each));
out.noPool = note(st, { configured: true, status: {} });
out.down = note(st, { configured: true, error: "connection refused" });
out.failed = note(st, null);
out.notAsked = note(st, undefined);
out.noSetting = note(null, rt("4GB"));
console.log(JSON.stringify(out));
`, "[]")
	var got map[string]string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("%v\n%s", err, raw)
	}
	for _, same := range []string{"2048MiB", "2GiB", "2GB", "", "junk", "noPool", "down", "failed", "notAsked", "noSetting"} {
		if got[same] != "" {
			t.Errorf("%q: %q, want nothing said", same, got[same])
		}
	}
	for _, other := range []string{"4096MiB", "4GB", "512MiB"} {
		for _, want := range []string{"runs with " + other + " for each statement, not 2 GB", "restart the router", "--sql-memory"} {
			if !strings.Contains(got[other], want) {
				t.Errorf("%q: want %q in %q", other, want, got[other])
			}
		}
	}
}

// A router that listens on "localhost" must not put that name in the mysql
// line: the mysql client reads -h localhost as "use the socket file" and
// never tries the port.
func TestSQLPortLineNeverNamesLocalhost(t *testing.T) {
	raw := runNodeConnectArgs(t, renderHarnessJS+`
const host = vm.runInContext("flashbackHost", ctx);
const at = vm.runInContext("sqlPortAddress", ctx);
const from = (listen) => host(at({ configured: true, status: { listen } }));
console.log(JSON.stringify({ lower: from("localhost:3309"), upper: from("LocalHost:3309"), named: from("db.internal:3309"), ip: from("127.0.0.1:3309") }));
`, "[]")
	var got map[string]string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("%v\n%s", err, raw)
	}
	want := map[string]string{"lower": "127.0.0.1", "upper": "127.0.0.1", "named": "db.internal", "ip": "127.0.0.1"}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s: host %q, want %q", k, got[k], w)
		}
	}
}

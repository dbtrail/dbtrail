package console

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
)

// constLine extracts one top-level `const NAME = ...;` statement from app.js,
// for a script that runs functions extracted from it.
func constLine(t *testing.T, js, name string) string {
	t.Helper()
	m := regexp.MustCompile(`(?ms)^const ` + name + ` = .*?;\n`).FindString(js)
	if m == "" {
		t.Fatalf("app.js has no top-level const %s", name)
	}
	return m
}

// connectStep1JS extracts what step 1 says (#1953): the failure sentences,
// the note that makes the flavor a choice, and the tile's title.
func connectStep1JS(t *testing.T) string {
	js := readAsset(t, "app.js")
	// functionBody runs to the next function, so connectNote's brings the
	// HOST_UNBLOCK_SQL constant that sits between it and identifyFailureParts.
	return constLine(t, js, "FLAVOR_LABEL") + constLine(t, js, "MANAGED_LABEL") + constLine(t, js, "PROXY_LABEL") +
		functionBody(t, js, "function shortVersion(") + "\n" +
		functionBody(t, js, "function connectTitle(") + "\n" +
		functionBody(t, js, "function connectNote(") + "\n" +
		functionBody(t, js, "function identifyFailureParts(") + "\n"
}

// TestIdentifyKindsSayEveryKind: every kind doctor.Identify can send is said
// in plain words, short, with the fix it carries. The kinds come from
// doctor.IdentifyKinds, so a new one fails here until the screen says it.
func TestIdentifyKindsSayEveryKind(t *testing.T) {
	cases := map[string]map[string]any{
		doctor.KindNameNotFound:        {"addr": "nope.example:3306"},
		doctor.KindHostUnreachable:     {},
		doctor.KindTimeout:             {"from": "10.0.3.7"},
		doctor.KindPortClosed:          {},
		doctor.KindNotMySQL:            {"answer": "silent"},
		doctor.KindHostBlocked:         {"server_error": 1129},
		doctor.KindLoopbackInContainer: {"suggest": "host.docker.internal:3306"},
	}
	for _, k := range doctor.IdentifyKinds() {
		c, ok := cases[k]
		if !ok {
			t.Fatalf("kind %q has no case in this test; add one with the data it carries", k)
		}
		c["kind"] = k
	}
	// The variants that change the words.
	cases["timeout-rds"] = map[string]any{"kind": doctor.KindTimeout, "managed": "rds", "from": "10.0.3.7"}
	cases["timeout-container"] = map[string]any{"kind": doctor.KindTimeout, "in_container": true}
	cases["timeout-unknown"] = map[string]any{"kind": doctor.KindTimeout}
	cases["closed"] = map[string]any{"kind": doctor.KindNotMySQL, "answer": "closed"}
	cases["pg-port"] = map[string]any{"kind": doctor.KindNotMySQL, "answer": "silent", "port": "5432"}
	cases["unknown"] = map[string]any{"kind": "something_new"}
	in, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	script := connectStep1JS(t) + `
(async () => {
  const { bannedHits, countWords } = await import(process.argv[3]);
  const out = {};
  for (const [k, c] of Object.entries(` + string(in) + `)) {
    const p = identifyFailureParts(c, c.port || "3306");
    const said = p.text + " " + (p.note || "") + " " + (p.label || "");
    out[k] = { ...p, banned: bannedHits(said).map((h) => h.word), words: countWords(p.text + " " + (p.note || "")) };
  }
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
`
	var got map[string]struct {
		Text, Code, Note, Broken, Label, Copy, Use string
		Banned                                     []string
		Words                                      int
	}
	raw := runNodeConnect(t, script)
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	for k, p := range got {
		t.Logf("%s: %s | %s | %s", k, p.Text, p.Note, p.Label)
		if p.Text == "" {
			t.Errorf("%s: no sentence", k)
		}
		if len(p.Banned) > 0 {
			t.Errorf("%s: words the first run bans: %v in %q", k, p.Banned, p.Text)
		}
		if strings.Contains(p.Text+p.Note, "—") {
			t.Errorf("%s: em dash", k)
		}
		if p.Words > 40 {
			t.Errorf("%s: %d words, over the 40 a step allows: %q", k, p.Words, p.Text)
		}
		if !strings.Contains("name route port answer", p.Broken) || p.Broken == "" {
			t.Errorf("%s: broken part %q is not one the drawing knows", k, p.Broken)
		}
	}
	if g := got[doctor.KindNameNotFound]; g.Broken != "name" || !strings.Contains(g.Text, "spelling") {
		t.Errorf("name not found: %+v", g)
	}
	// A timeout names the address to allow, and says NAT may change it.
	if g := got[doctor.KindTimeout]; !strings.Contains(g.Text, "10.0.3.7") || g.Copy != "10.0.3.7" || !strings.Contains(g.Note, "NAT") || !strings.Contains(g.Text, "port 3306") {
		t.Errorf("timeout: %+v", g)
	}
	if g := got["timeout-rds"]; !strings.Contains(g.Text, "security group") || !strings.Contains(g.Text, "AWS") {
		t.Errorf("timeout on RDS: %+v", g)
	}
	// In a container the local address is the wrong one: none is named.
	if g := got["timeout-container"]; g.Copy != "" || strings.Contains(g.Text, "10.0.") || !strings.Contains(g.Text, "the machine DBTrail runs on") {
		t.Errorf("timeout in a container: %+v", g)
	}
	if g := got["timeout-unknown"]; g.Copy != "" || g.Note != "" {
		t.Errorf("timeout with no address: %+v", g)
	}
	if g := got[doctor.KindPortClosed]; !strings.Contains(g.Text, "bind-address") || g.Broken != "port" {
		t.Errorf("port closed: %+v", g)
	}
	// The unblock SQL verified per server: the host cache table (MySQL 8.0
	// and 8.4, where FLUSH HOSTS no longer parses), FLUSH HOSTS for MariaDB,
	// commented so pasting the block runs one statement.
	if g := got[doctor.KindHostBlocked]; !strings.Contains(g.Code, "\nTRUNCATE TABLE performance_schema.host_cache;\n") || !strings.Contains(g.Code, "-- FLUSH HOSTS;") {
		t.Errorf("host blocked: %+v", g)
	}
	if g := got[doctor.KindLoopbackInContainer]; g.Use != "host.docker.internal:3306" || !strings.Contains(g.Text, "host.docker.internal") {
		t.Errorf("loopback: %+v", g)
	}
	if g := got["closed"]; !strings.Contains(g.Text, "port forward") {
		t.Errorf("closed at once: %+v", g)
	}
	if g := got["pg-port"]; !strings.Contains(g.Text, "PostgreSQL") {
		t.Errorf("port 5432: %+v", g)
	}
	if g := got["unknown"]; g.Text == "" {
		t.Errorf("an unknown kind must still say something: %+v", g)
	}
}

// What the found tile says, and when the flavor is a choice instead.
func TestConnectTileWords(t *testing.T) {
	script := connectStep1JS(t) + `
(async () => {
  const { bannedHits } = await import(process.argv[3]);
  const ids = {
    mariaRDS: [{ version: "10.11.6-MariaDB-log", flavor: "mariadb", managed: "rds" }, "mariadb"],
    mysql: [{ version: "8.4.3", flavor: "mysql" }, "mysql"],
    aurora: [{ version: "8.0.39", flavor: "mysql", managed: "aurora" }, "mysql"],
    chosenOther: [{ version: "8.0.11", proxy: "proxysql" }, "mariadb"],
    proxy: [{ version: "8.0.11", proxy: "proxysql" }, "mysql"],
    rdsProxy: [{ version: "8.0.39", proxy: "rds_proxy", managed: "rds" }, "mysql"],
    notAllowed: [{ server_error: 1130 }, "mysql"],
    tidb: [{ version: "8.0.11-TiDB-v7.5.1" }, "mysql"],
  };
  const out = {};
  for (const [k, [id, fl]] of Object.entries(ids)) {
    const note = connectNote(id);
    out[k] = { title: connectTitle(id, fl), note, banned: bannedHits(note).map((h) => h.word) };
  }
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
`
	var got map[string]struct {
		Title, Note string
		Banned      []string
	}
	if err := json.Unmarshal(runNodeConnect(t, script), &got); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]string{
		"mariaRDS": "MariaDB 10.11 on Amazon RDS",
		"mysql":    "MySQL 8.4",
		"aurora":   "MySQL 8.0 on Amazon Aurora",
		// A version the proxy sent is not the server's: no number shown.
		"chosenOther": "MariaDB",
		"proxy":       "MySQL",
		"rdsProxy":    "MySQL on Amazon RDS",
		"notAllowed":  "MySQL",
		"tidb":        "MySQL",
	} {
		if got[k].Title != want {
			t.Errorf("%s: title %q, want %q", k, got[k].Title, want)
		}
	}
	for _, k := range []string{"mariaRDS", "mysql", "aurora"} {
		if got[k].Note != "" {
			t.Errorf("%s: a flavor read from the greeting is offered as a choice: %q", k, got[k].Note)
		}
	}
	for k, word := range map[string]string{"proxy": "ProxySQL", "rdsProxy": "RDS Proxy", "notAllowed": "SQL below", "tidb": "cannot tell"} {
		if !strings.Contains(got[k].Note, word) {
			t.Errorf("%s: note %q does not say %q", k, got[k].Note, word)
		}
		if len(got[k].Banned) > 0 {
			t.Errorf("%s: banned words %v", k, got[k].Banned)
		}
	}
}

// On Amazon RDS or Aurora the block grants what the lock-all mode needs as
// its live line: the default mode cannot work there, so offering it would
// only fail on the first snapshot. Snapshots choose lock-all on their own
// there (#1986), so the block asks for no setting. Everything else stays as
// it was.
func TestGrantBlocksForAManagedServer(t *testing.T) {
	js := readAsset(t, "app.js")
	script := functionBody(t, js, "function sqlString(") + "\n" + functionBody(t, js, "function grantBlocks(") + `
const live = (b) => b.split("\n").filter((l) => l && !l.startsWith("--"));
const m = grantBlocks("dbtrail", "Pw-1", false, true), plain = grantBlocks("dbtrail", "Pw-1", false, false), none = grantBlocks("dbtrail", "", false, true);
console.log(JSON.stringify({ mysql: live(m.mysql), mariadb: live(m.mariadb), plain: live(plain.mysql), none: live(none.mysql), note: m.mysql }));
`
	var got struct {
		MySQL, MariaDB, Plain, None []string
		Note                        string
	}
	if err := json.Unmarshal(runNodeConnect(t, script), &got); err != nil {
		t.Fatal(err)
	}
	for name, lines := range map[string][]string{"mysql": got.MySQL, "mariadb": got.MariaDB} {
		joined := strings.Join(lines, "\n")
		if !strings.Contains(joined, "GRANT LOCK TABLES, SHOW VIEW ON *.* TO 'dbtrail'@'%';") || strings.Contains(joined, "RELOAD") || strings.Contains(joined, "BACKUP_ADMIN") {
			t.Errorf("%s on RDS, live lines:\n%s", name, joined)
		}
		if !strings.Contains(joined, "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'dbtrail'@'%';") {
			t.Errorf("%s on RDS lost the capture grant:\n%s", name, joined)
		}
	}
	// #1986: what the permission is for, in the reader's words, and no
	// setting to change: snapshots pick lock-all on an RDS/Aurora host.
	if !strings.Contains(got.Note, "\n-- Amazon RDS and Aurora: this permission lets each snapshot start every table at the same point-in-time.\nGRANT LOCK TABLES, SHOW VIEW ON *.* TO 'dbtrail'@'%';") {
		t.Errorf("the RDS block does not explain its permission with the agreed line:\n%s", got.Note)
	}
	if !strings.Contains(strings.Join(got.Plain, "\n"), "GRANT RELOAD, BACKUP_ADMIN, SHOW VIEW") {
		t.Errorf("a server that is not managed lost its default grant: %v", got.Plain)
	}
	// No password: nothing runnable, managed or not.
	if len(got.None) != 0 {
		t.Errorf("a managed block with no password has runnable lines: %v", got.None)
	}
}

// #1986: the SQL on both screens (Connect and the full form), managed or not,
// typed or not, never sends anyone to a setting: snapshots pick their mode on
// their own, and the "Lock while dumping" control has not existed since #1846.
// Specific tokens, not an upper-case pattern: BACKUP_ADMIN and FLUSH_TABLES
// are SQL and belong here.
func TestGrantBlocksNameNoSettingOrVariable(t *testing.T) {
	js := readAsset(t, "app.js")
	script := functionBody(t, js, "function sqlString(") + "\n" + functionBody(t, js, "function grantBlocks(") + `
const all = [];
for (const m of [true, false]) for (const [u, p] of [["dbtrail", "Pw-1"], ["", ""], ["dbtrail", ""]]) {
  const b = grantBlocks(u, p, false, m);
  all.push(b.mysql, b.mariadb);
}
console.log(JSON.stringify(all));
`
	var blocks []string
	if err := json.Unmarshal(runNodeConnect(t, script), &blocks); err != nil {
		t.Fatal(err)
	}
	for _, b := range blocks {
		for _, bad := range []string{"Snapshots, Settings", "Lock while dumping", "BASELINE_LOCK_MODE", "BINTRAIL_", "DBTRAIL_", ".env", "lock mode", "lock-all", "—"} {
			if strings.Contains(b, bad) {
				t.Errorf("the SQL names %q:\n%s", bad, b)
			}
		}
	}
}

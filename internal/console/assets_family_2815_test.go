package console

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// The page's side of "one database family per installation" (#2815): what it
// is told, and what it says and offers for it. The page's decisions are
// small functions of app.js, run here with node on the data the server sends.

type familyPage struct {
	Choices map[string][]string `json:"choices"`
	Hints   map[string]string   `json:"hints"`
	Labels  map[string]string   `json:"labels"`
	Notes   map[string]string   `json:"notes"`
	Said    map[string]string   `json:"said"`
	// Banned are the words of the first-run list found in what the Connect
	// screen shows; EmDash says one of those texts holds an em dash.
	Banned []string `json:"banned"`
	EmDash bool     `json:"em_dash"`
}

func runFamilyPage(t *testing.T) familyPage {
	t.Helper()
	js := readAsset(t, "app.js")
	script := functionBody(t, js, "function mixedFamiliesNote(") + "\n" +
		functionBody(t, js, "function familyChoices(") + "\n" +
		functionBody(t, js, "function familyHint(") + "\n" +
		functionBody(t, js, "function connectFullFormLabel(") + "\n" +
		functionBody(t, js, "function connectRefusalSaid(") + `
(async () => {
  const { bannedHits } = await import(process.argv[3]);
  const out = { choices: {}, hints: {}, labels: {}, notes: {}, said: {} };
  for (const f of ["", "mysql", "postgres", "something-else"]) {
    out.choices[f] = familyChoices(f);
    out.hints[f] = familyHint(f);
    out.labels[f] = connectFullFormLabel(f);
  }
  out.notes.none = mixedFamiliesNote(null);
  out.notes.oneFamily = mixedFamiliesNote({ postgres: ["ledger"], mysql: [] });
  out.notes.empty = mixedFamiliesNote({});
  out.notes.mixed = mixedFamiliesNote({ postgres: ["ledger"], mysql: ["shop", "wiki"] });
  out.notes.many = mixedFamiliesNote({ postgres: ["a", "b", "c", "d", "e"], mysql: ["the server this was started with"] });
  const said = "This installation serves MySQL and MariaDB servers, so a PostgreSQL server cannot be added to it.";
  out.said.otherFamily = connectRefusalSaid(said, { status: 409, body: { error: said, code: "other_family" } });
  out.said.duplicate = connectRefusalSaid("A server named shop already exists.", { status: 409, body: { error: "x" } });
  out.said.noBody = connectRefusalSaid("Bad value.", { status: 400 });
  out.said.noErr = connectRefusalSaid("Bad value.", null);
  // What the Connect screen itself shows: the foot button and the refusal's
  // tail. Held to the first-run word list like every sentence of that screen.
  const shown = Object.values(out.labels).concat([out.said.duplicate.replace("A server named shop already exists.", "")]);
  out.banned = shown.flatMap((s) => bannedHits(s).map((h) => h.word));
  out.em_dash = Object.values(out.labels).concat(Object.values(out.hints), Object.values(out.notes), Object.values(out.said)).some((s) => s.includes("—"));
  console.log(JSON.stringify(out));
})();
`
	var got familyPage
	if err := json.Unmarshal(runNodeConnect(t, script), &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestFamilyPage_offersTheInstalledFamilysTypes(t *testing.T) {
	got := runFamilyPage(t)
	for family, want := range map[string]string{
		"":               "mysql,postgres,mariadb", // no server yet: every type, and the hint says what the choice fixes
		"mysql":          "mysql,mariadb",
		"postgres":       "postgres",
		"something-else": "mysql,postgres,mariadb", // a family a newer server might send: nothing is withheld
	} {
		if s := strings.Join(got.Choices[family], ","); s != want {
			t.Errorf("family %q offers %q, want %q", family, s, want)
		}
	}
	if h := got.Hints[""]; !strings.Contains(h, "first server decides") || !strings.Contains(h, "PostgreSQL") || !strings.Contains(h, "MySQL and MariaDB") {
		t.Errorf("the hint with no server: %q", h)
	}
	if h := got.Hints["mysql"]; !strings.Contains(h, "serves MySQL and MariaDB") || !strings.Contains(h, "PostgreSQL goes in an installation of its own") {
		t.Errorf("the hint on a MySQL installation: %q", h)
	}
	if h := got.Hints["postgres"]; !strings.Contains(h, "serves PostgreSQL") || !strings.Contains(h, "MySQL and MariaDB go in an installation of their own") {
		t.Errorf("the hint on a PostgreSQL installation: %q", h)
	}
	// The Connect screen's button does not offer PostgreSQL where the full
	// form would refuse it.
	if strings.Contains(got.Labels["mysql"], "PostgreSQL") {
		t.Errorf("a MySQL installation's Connect screen offers PostgreSQL: %q", got.Labels["mysql"])
	}
	if !strings.Contains(got.Labels[""], "PostgreSQL") {
		t.Errorf("an installation with no server does not say where PostgreSQL is added: %q", got.Labels[""])
	}
	for f, l := range got.Labels {
		if !strings.Contains(l, "Open the full form") {
			t.Errorf("family %q: the button %q does not say what it opens", f, l)
		}
	}
}

func TestFamilyPage_saysTheMixWhileItLasts(t *testing.T) {
	got := runFamilyPage(t)
	for _, k := range []string{"none", "oneFamily", "empty"} {
		if got.Notes[k] != "" {
			t.Errorf("a note with nothing mixed (%s): %q", k, got.Notes[k])
		}
	}
	m := got.Notes["mixed"]
	for _, want := range []string{"both database families", "PostgreSQL (ledger)", "MySQL or MariaDB (shop, wiki)", "keeps working", "can be edited", "No server can be added"} {
		if !strings.Contains(m, want) {
			t.Errorf("the note does not say %q: %q", want, m)
		}
	}
	many := got.Notes["many"]
	if !strings.Contains(many, "a, b, c and 2 more") || !strings.Contains(many, "the server this was started with") {
		t.Errorf("many servers: %q", many)
	}
}

// The other-family refusal is not one the form can fix: it is shown as it
// is. Every other refusal still says to press Check again.
func TestFamilyPage_theRefusalIsNotToldToCheckAgain(t *testing.T) {
	got := runFamilyPage(t)
	if s := got.Said["otherFamily"]; strings.Contains(s, "Check again") || !strings.HasSuffix(s, "cannot be added to it.") {
		t.Errorf("the other-family refusal: %q", s)
	}
	for _, k := range []string{"duplicate", "noBody", "noErr"} {
		if !strings.HasSuffix(got.Said[k], " Press Check again once it is fixed.") {
			t.Errorf("%s lost its next step: %q", k, got.Said[k])
		}
	}
	if len(got.Banned) != 0 {
		t.Errorf("the Connect screen shows words of the first-run list: %v", got.Banned)
	}
	if got.EmDash {
		t.Error("one of the sentences holds an em dash")
	}
}

// The page's functions are wired where they act: the list sets the state,
// the note is drawn from it, the form removes the other family's types, and
// the Connect screen asks the refusal's code.
func TestFamilyPage_isWired(t *testing.T) {
	js := readAsset(t, "app.js")
	for _, want := range []string{
		`installFamily = { family: data.family || "", mixed: data.mixed || null };`,
		`mixedFamiliesNote(installFamily.mixed)`,
		`familyChoices(installFamily.family)`,
		`familyHint(installFamily.family)`,
		`connectFullFormLabel(installFamily.family)`,
		`connectRefusalSaid(said, refused)`,
		`installFamily.family !== "postgres") return showConnectForm(`,
	} {
		if !strings.Contains(js, want) {
			t.Errorf("app.js no longer holds %q", want)
		}
	}
	if strings.Contains(js, `said + " Press Check again once it is fixed.";`+"\n      return;") {
		t.Error("the Connect screen still appends Check again to every refusal by itself")
	}
	if html := readAsset(t, "index.html"); !strings.Contains(html, `id="srv-mixed-note"`) || !strings.Contains(html, `id="srv-mixed-note" role="status" hidden`) {
		t.Error("index.html has no hidden note for the mix")
	}
}

type serversListFamily struct {
	Family string       `json:"family"`
	Mixed  *familiesDTO `json:"mixed"`
}

func listFamily(t *testing.T, srv *Server) serversListFamily {
	t.Helper()
	rec, body := doServersReq(t, srv, "GET", "/api/servers", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list: %d %s", rec.Code, body)
	}
	var out serversListFamily
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// The servers list says what the installation serves, and it is the same
// answer an add would get: no server, one family, both, and the server given
// on the command line counted.
func TestServersList_saysTheInstallationsFamily(t *testing.T) {
	srv := newRegistryServer(t)
	if got := listFamily(t, srv); got.Family != "" || got.Mixed != nil {
		t.Fatalf("with no server: %+v", got)
	}
	if _, err := srv.cm.reg.Add(familyTestEntry("wiki", "mariadb")); err != nil {
		t.Fatal(err)
	}
	if got := listFamily(t, srv); got.Family != FamilyMySQL || got.Mixed != nil {
		t.Fatalf("with a MariaDB server: %+v", got)
	}
	addByHand(t, srv.cm.reg, familyTestEntry("ledger", "postgres"))
	got := listFamily(t, srv)
	if got.Family != "" || got.Mixed == nil || strings.Join(got.Mixed.Postgres, ",") != "ledger" || strings.Join(got.Mixed.MySQL, ",") != "wiki" {
		t.Fatalf("with both families: %+v %+v", got, got.Mixed)
	}

	pg := newRegistryServer(t)
	if _, err := pg.cm.reg.Add(familyTestEntry("ledger", "postgres")); err != nil {
		t.Fatal(err)
	}
	if got := listFamily(t, pg); got.Family != FamilyPostgres || got.Mixed != nil {
		t.Fatalf("with a PostgreSQL server: %+v", got)
	}
	// A MySQL server given on the command line beside it: both, as an add
	// would be told.
	pg.cm.mu.Lock()
	pg.cm.boot = &bundle{}
	pg.cm.mu.Unlock()
	got = listFamily(t, pg)
	if got.Mixed == nil || strings.Join(got.Mixed.MySQL, ",") != bootServerWords {
		t.Fatalf("with a command-line MySQL server beside a PostgreSQL one: %+v %+v", got, got.Mixed)
	}
	if err := pg.newServerFamilyRefusal("postgres"); err == nil {
		t.Fatal("the list says both families and an add is accepted: the two disagree")
	}
}

// The refusal carries a code the page decides by, on both ways in; another
// refusal of an add carries none.
func TestOtherFamilyRefusal_carriesACode(t *testing.T) {
	type coded struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	srv, _ := newSupervisorServer(t)
	if rec, body := doServersReq(t, srv, "POST", "/api/servers/check", checkBody); rec.Code != 200 {
		t.Fatalf("the first server: %d %s", rec.Code, body)
	}
	for _, path := range []string{"/api/servers", "/api/servers/check"} {
		rec, body := doServersReq(t, srv, "POST", path,
			`{"name":"ledger","flavor":"postgres","host":"h","user":"u","password":"p","dbname":"db","source_host":"pg.example.com","source_user":"dbtrail","source_password":"Ab3-xyz","source_database":"app","source_slot":"s","source_publication":"p"}`)
		var got coded
		if rec.Code != http.StatusConflict || json.Unmarshal(body, &got) != nil {
			t.Fatalf("%s: %d %s", path, rec.Code, body)
		}
		if got.Code != OtherFamilyCode || !strings.Contains(got.Error, "This installation serves") {
			t.Errorf("%s: code %q error %q", path, got.Code, got.Error)
		}
	}
	// A refusal that is not this one: no code.
	rec, body := doServersReq(t, srv, "POST", "/api/servers", `{"name":"x"}`)
	var got coded
	_ = json.Unmarshal(body, &got)
	if rec.Code < 400 || got.Code != "" {
		t.Errorf("another refusal: %d code %q", rec.Code, got.Code)
	}
}

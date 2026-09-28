package console

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSharedSnapshotLocations(t *testing.T) {
	const a, b, c = "aaaa-1", "bbbb-2", "cccc-3"
	seen := map[string][]string{
		"/one":       {a},
		"/none":      nil,
		"/two":       {a, b},
		"s3://x/y":   {a, b, c},
		"/strangers": {b, c},
	}
	cases := []struct {
		name    string
		sources []string
		own     string
		want    []snapshotWritersDTO
		asks    int
	}{
		{name: "one writer is not reported and the index is not asked", sources: []string{"/one", "/none"}, own: a},
		{name: "two writers name the other one", sources: []string{"/two"}, own: a, asks: 1,
			want: []snapshotWritersDTO{{Source: "/two", Own: a, Others: []string{b}}}},
		{name: "seen from the other installation, the other one is the first", sources: []string{"/two"}, own: b, asks: 1,
			want: []snapshotWritersDTO{{Source: "/two", Own: b, Others: []string{a}}}},
		{name: "an unknown identity names every writer", sources: []string{"/two"}, own: "", asks: 1,
			want: []snapshotWritersDTO{{Source: "/two", Others: []string{a, b}}}},
		{name: "a server that signed nothing here names every writer", sources: []string{"/strangers"}, own: a, asks: 1,
			want: []snapshotWritersDTO{{Source: "/strangers", Others: []string{b, c}}}},
		{name: "two locations, the index asked once", sources: []string{"/one", "/two", "s3://x/y"}, own: a, asks: 1,
			want: []snapshotWritersDTO{
				{Source: "/two", Own: a, Others: []string{b}},
				{Source: "s3://x/y", Own: a, Others: []string{b, c}},
			}},
		{name: "no sources", own: a},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			asks := 0
			got := sharedSnapshotLocations(tc.sources,
				func(src string) []string { return seen[src] },
				func() string { asks++; return tc.own })
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
			if asks != tc.asks {
				t.Fatalf("the index was asked %d times, want %d", asks, tc.asks)
			}
			// What was seen is the listing's; reporting must not edit it.
			if !reflect.DeepEqual(seen["/two"], []string{a, b}) {
				t.Fatalf("the seen writers were changed: %q", seen["/two"])
			}
		})
	}
}

// The endpoint itself, over real folders: what the page is handed.
func TestBaselinesAPI_sharedWith(t *testing.T) {
	const own = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	const other = "bbbbbbbb-0000-0000-0000-000000000002"
	const d1, d2, d3 = "2026-06-01T00-00-00Z", "2026-06-02T00-00-00Z", "2026-06-03T00-00-00Z"
	folder := func(t *testing.T, markers map[string][]string) string {
		dir := t.TempDir()
		for ts, ms := range markers {
			writeBaselineFixture(t, dir, ts, "shop", "orders.parquet")
			for _, m := range ms {
				writeBaselineFixture(t, dir, ts, m)
			}
		}
		return dir
	}
	get := func(t *testing.T, srv *Server) baselinesResponse {
		t.Helper()
		rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
		if rec.Code != 200 {
			t.Fatalf("code = %d, body = %s", rec.Code, body)
		}
		var got baselinesResponse
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if out := os.Getenv("WRITERS_1762_SAMPLES"); out != "" {
			if err := os.WriteFile(filepath.Join(out, strings.ReplaceAll(t.Name(), "/", "__")+".json"), body, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return got
	}
	withIndex := func(t *testing.T, srv *Server, id any) sqlmock.Sqlmock {
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		mock.MatchExpectationsInOrder(false)
		mock.ExpectQuery("FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"bintrail_id"}).AddRow(id))
		srv.cm.boot.db = db
		return mock
	}

	t.Run("unsigned snapshots and one writer say nothing", func(t *testing.T) {
		dir := folder(t, map[string][]string{d1: nil, d2: {"_SUCCESS"}, d3: {"_SUCCESS", "_WRITER." + own}})
		got := get(t, newBaselineServer(t, dir, true))
		if len(got.SharedWith) != 0 {
			t.Fatalf("shared_with = %+v for a folder with one writer", got.SharedWith)
		}
		if len(got.Snapshots) != 3 {
			t.Fatalf("snapshots = %d, want 3", len(got.Snapshots))
		}
	})
	t.Run("only unsigned snapshots say nothing", func(t *testing.T) {
		dir := folder(t, map[string][]string{d1: nil, d2: {"_SUCCESS"}})
		if got := get(t, newBaselineServer(t, dir, true)); len(got.SharedWith) != 0 {
			t.Fatalf("shared_with = %+v", got.SharedWith)
		}
	})
	t.Run("two writers, this server known, name the other one", func(t *testing.T) {
		dir := folder(t, map[string][]string{d1: {"_SUCCESS", "_WRITER." + own}, d2: {"_SUCCESS", "_WRITER." + other}})
		srv := newBaselineServer(t, dir, true)
		// The index spells its id in upper case; it is the same writer.
		withIndex(t, srv, strings.ToUpper(own))
		got := get(t, srv)
		want := []snapshotWritersDTO{{Source: dir, Own: own, Others: []string{other}}}
		if !reflect.DeepEqual(got.SharedWith, want) {
			t.Fatalf("shared_with = %+v, want %+v", got.SharedWith, want)
		}
		if len(got.Snapshots) != 2 {
			t.Fatalf("snapshots = %d, want 2: the listing is served as before", len(got.Snapshots))
		}
	})
	t.Run("two writers, no index connection, name both", func(t *testing.T) {
		dir := folder(t, map[string][]string{d1: {"_SUCCESS", "_WRITER." + own}, d2: {"_SUCCESS", "_WRITER." + other}})
		got := get(t, newBaselineServer(t, dir, true))
		want := []snapshotWritersDTO{{Source: dir, Others: []string{own, other}}}
		if !reflect.DeepEqual(got.SharedWith, want) {
			t.Fatalf("shared_with = %+v, want %+v", got.SharedWith, want)
		}
	})
	t.Run("an unreadable signature does not fail the listing", func(t *testing.T) {
		dir := folder(t, map[string][]string{d1: {"_SUCCESS", "_WRITER." + own}, d2: {"_SUCCESS", "_WRITER.who?*"}, d3: {"_SUCCESS", "_WRITER."}})
		got := get(t, newBaselineServer(t, dir, true))
		if len(got.SharedWith) != 0 || len(got.Snapshots) != 3 {
			t.Fatalf("shared_with = %+v, snapshots = %d; want none and 3", got.SharedWith, len(got.Snapshots))
		}
	})
}

// The sentence the settings row says, rendered by the REAL row in node from
// what the REAL endpoint answered over real folders.
func TestBackupServerRow_saysTheLocationIsShared(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	const own = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	const other = "bbbbbbbb-0000-0000-0000-000000000002"
	const third = "cccccccc-0000-0000-0000-000000000003"
	answer := func(t *testing.T, ownID string, markers ...[]string) (string, json.RawMessage) {
		dir := t.TempDir()
		for i, ms := range markers {
			ts := fmt.Sprintf("2026-06-%02dT00-00-00Z", i+1)
			writeBaselineFixture(t, dir, ts, "shop", "orders.parquet")
			for _, m := range ms {
				writeBaselineFixture(t, dir, ts, m)
			}
		}
		srv := newBaselineServer(t, dir, true)
		if ownID != "" {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			mock.ExpectQuery("FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"bintrail_id"}).AddRow(ownID))
			srv.cm.boot.db = db
		}
		rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
		if rec.Code != 200 {
			t.Fatalf("code = %d, body = %s", rec.Code, body)
		}
		return dir, body
	}
	signed := func(id string) []string { return []string{"_SUCCESS", "_WRITER." + id} }
	type scene struct {
		Dir      string          `json:"dir"`
		Listing  json.RawMessage `json:"listing"`
		TypedDir string          `json:"typed_dir,omitempty"`
		S3       string          `json:"s3,omitempty"`
	}
	scenes := map[string]scene{}
	add := func(name, ownID string, mod func(*scene), markers ...[]string) {
		dir, body := answer(t, ownID, markers...)
		sc := scene{Dir: dir, Listing: body}
		if mod != nil {
			mod(&sc)
		}
		scenes[name] = sc
	}
	add("oneWriter", own, nil, signed(own), signed(own))
	add("unsignedAndOne", own, nil, nil, []string{"_SUCCESS"}, signed(own))
	add("onlyUnsigned", own, nil, nil, []string{"_SUCCESS"})
	add("twoKnown", own, nil, signed(own), signed(other))
	add("threeKnown", own, nil, signed(own), signed(other), signed(third))
	add("twoUnknown", "", nil, signed(own), signed(other))
	add("twoWithS3", own, func(s *scene) { s.S3 = "s3://b/p/" }, signed(own), signed(other))
	add("twoTypedElsewhere", own, func(s *scene) { s.TypedDir = "/srv/a-new-folder" }, signed(own), signed(other))
	in, err := json.Marshal(scenes)
	if err != nil {
		t.Fatal(err)
	}

	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
FakeEl.prototype.addEventListener = function (t, f) { (this._l = this._l || {})[t] = ((this._l || {})[t] || []).concat(f); };
const fire = (n, t) => { for (const f of ((n._l || {})[t] || [])) f({ key: "" }); };
vm.runInContext("capsCache.monitor = true;", ctx);
const walk = (n, f) => { if (!n || n.nodeType !== 1) return; f(n); for (const c of n.children) walk(c, f); };
const find = (root, pred) => { let hit = null; walk(root, (n) => { if (!hit && pred(n)) hit = n; }); return hit; };
const reds = (root) => { const out = []; const go = (n, hid) => { if (!n || n.nodeType !== 1) return; const h = hid || n.hidden; if (!h && n.tag === "p" && /\berr\b/.test(n.className) && n._text) out.push(n._text); for (const c of n.children) go(c, h); }; go(root, false); return out; };
const scenes = ` + string(in) + `;
const out = {};
for (const [name, sc] of Object.entries(scenes)) {
  // What the page loader does with the listing it read.
  ctx.__listing = sc.listing;
  vm.runInContext("snapSharedWith = (__listing && __listing.shared_with) || [];", ctx);
  const srv = { id: "s1", name: "prod", baseline_dir: sc.dir, baseline_s3: sc.s3 || "", default_dir: "/state/snapshots/s1",
    keep_newest: 0, local_copy: true, prune_loop: true, source: "server" };
  const r = ctx.backupServerRow(srv, false, [], "", true);
  if (sc.typed_dir) { const i = find(r, (n) => n.tag === "input" && n.attrs.name === "baseline_dir"); i.value = sc.typed_dir; fire(i, "input"); }
  out[name] = reds(r);
}
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "row.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string][]string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("node said: %s", raw)
	}
	if len(got) != len(scenes) {
		t.Fatalf("node rendered %d scenes of %d: %s", len(got), len(scenes), raw)
	}
	tail := func(dir string) string {
		return " Their snapshots mix in " + dir + ", and a read can return the other one's data. Give each its own folder or S3 prefix."
	}
	want := map[string][]string{
		"oneWriter":         nil,
		"unsignedAndOne":    nil,
		"onlyUnsigned":      nil,
		"twoKnown":          {"Another DBTrail writes its snapshots here too: " + other + "." + tail(scenes["twoKnown"].Dir)},
		"threeKnown":        {"Other copies of DBTrail write their snapshots here too: " + other + ", " + third + "." + tail(scenes["threeKnown"].Dir)},
		"twoUnknown":        {"More than one DBTrail writes its snapshots here: " + own + ", " + other + "." + tail(scenes["twoUnknown"].Dir)},
		"twoWithS3":         {"Another DBTrail writes its snapshots here too: " + other + "." + tail(scenes["twoWithS3"].Dir)},
		"twoTypedElsewhere": nil,
	}
	banned := regexp.MustCompile(`(?i)\b(backups?|baselines?|index(es)?|sources?|consoles?|daemons?)\b|—`)
	for name, w := range want {
		if !reflect.DeepEqual(got[name], w) && !(len(got[name]) == 0 && len(w) == 0) {
			t.Errorf("%s: the row says in red\n\t%q\nwant\n\t%q", name, got[name], w)
		}
		for _, line := range got[name] {
			t.Logf("%s: %s", name, line)
			if m := banned.FindString(strings.ReplaceAll(line, scenes[name].Dir, "")); m != "" {
				t.Errorf("%s: %q uses %q", name, line, m)
			}
			// Known writer: the row names the OTHER one, never this server.
			if strings.HasPrefix(line, "Another") || strings.HasPrefix(line, "Other") {
				if strings.Contains(line, own) || !strings.Contains(line, other) {
					t.Errorf("%s: %q does not name the other writer and only it", name, line)
				}
			}
		}
	}
}

// The row reads what the page loader kept from the listing. The scenes above
// set that value themselves, so this pins the one line that sets it on the
// real page, and that the row passes it on.
func TestSnapshotsPage_handsTheListingToTheRow(t *testing.T) {
	js := readAsset(t, "app.js")
	for _, want := range []string{
		`snapSharedWith = (baselines && baselines.shared_with) || [];`,
		`shared: asSaved ? snapSharedWith : [] },`,
		`for (const w of was.shared || []) say(sharedLocationWords(w), true);`,
	} {
		if strings.Count(js, want) != 1 {
			t.Errorf("app.js holds %q %d times, want once", want, strings.Count(js, want))
		}
	}
}

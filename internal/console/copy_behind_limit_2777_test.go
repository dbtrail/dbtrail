package console

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// How far behind the source a statement about to run would be (#2777): the
// number the router reports after an answer, asked before it.
func TestBehindBeforeRun_2777(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	through := now.Add(-2 * time.Hour)
	caught := func(ago, read time.Duration, maxID uint64, newest time.Time) captureSample {
		return captureSample{caughtUp: now.Add(-ago), readAt: now.Add(-read), maxID: maxID, newest: newest}
	}
	for _, tc := range []struct {
		name      string
		through   time.Time
		throughID uint64
		sample    captureSample
		have      bool
		want      time.Duration
	}{
		// No sample read: the data's age, never a guess that capture is
		// caught up.
		{"no sample", through, 10, captureSample{}, false, 2 * time.Hour},
		// A quiet source: old data that is still current. Capture is caught
		// up and the data holds every indexed event.
		{"quiet source", through, 10, caught(10*time.Second, 5*time.Second, 10, through), true, 0},
		// The same with a sample read longer ago than the router re-reads:
		// events may have come in since, so the conservative distance.
		{"quiet source, stale sample", through, 10, caught(10*time.Second, 40*time.Second, 10, through), true, 2 * time.Hour},
		// Capture stopped (its caught-up stamp is old): the data's age.
		{"capture stopped", through, 10, caught(10*time.Minute, time.Second, 10, through), true, 2 * time.Hour},
		// Caught up, but the data misses events 20 seconds newer than it.
		{"events missing", through, 9, caught(10*time.Second, time.Second, 10, through.Add(20*time.Second)), true, 20 * time.Second},
		// No caught-up stamp at all (a PostgreSQL source, an older capture).
		{"no stamp", through, 10, captureSample{readAt: now, maxID: 10}, true, 2 * time.Hour},
		// An unknown data id never reaches a known index id.
		{"unknown id", through, 0, caught(10*time.Second, time.Second, 10, through), true, 2 * time.Hour},
		// The source's clock ahead of the router's: never below zero.
		{"clock ahead", now.Add(time.Minute), 10, captureSample{}, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := behindBeforeRun(now, tc.through, tc.throughID, tc.sample, tc.have); got != tc.want {
				t.Errorf("behind = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCheckMaxBehindSource_2777(t *testing.T) {
	for v, ok := range map[int64]bool{-1: false, 0: true, 1: true, 300: true, maxBehindSourceCap: true, maxBehindSourceCap + 1: false} {
		if err := checkMaxBehindSource(v); (err == nil) != ok {
			t.Errorf("checkMaxBehindSource(%d) = %v, want ok %v", v, err, ok)
		}
	}
}

// No limit, nothing carried: the copy's path asks the index nothing more.
func TestWithBehindLimit_zeroIsNone_2777(t *testing.T) {
	ctx := context.Background()
	if _, ok := behindLimitFrom(withBehindLimit(ctx, "a", 0)); ok {
		t.Error("a zero limit is carried")
	}
	if _, ok := behindLimitFrom(withBehindLimit(ctx, "a", -time.Second)); ok {
		t.Error("a negative limit is carried")
	}
	l, ok := behindLimitFrom(withBehindLimit(ctx, "a", time.Minute))
	if !ok || l.id != "a" || l.limit != time.Minute {
		t.Errorf("carried %+v %v", l, ok)
	}
}

// The refusal says the lag and the limit, rounded so a lag is never said
// smaller, and fits a MySQL client's error message (512 bytes).
func TestSQLBehindRefusalMessage_2777(t *testing.T) {
	over := (&sqlBehindRefusal{behind: 7200*time.Second + 200*time.Millisecond, limit: 300 * time.Second}).Error()
	if !strings.Contains(over, "7201 s behind") || !strings.Contains(over, "limit of 300 s") || !strings.Contains(over, "max_behind_source_seconds") {
		t.Errorf("over: %q", over)
	}
	unknown := (&sqlBehindRefusal{limit: 300 * time.Second, unknown: copyAgeWhyNotPlaced}).Error()
	if !strings.Contains(unknown, copyAgeWhyNotPlaced) || !strings.Contains(unknown, "300 s") {
		t.Errorf("unknown: %q", unknown)
	}
	for _, m := range []string{over, unknown} {
		if len(m) > 512 {
			t.Errorf("%d bytes, over a client's 512: %q", len(m), m)
		}
	}
}

// The limit rides on PUT /api/servers/{id}/sql-mode beside the other two
// choices: saved only when sent, refused when it is not a whole number of
// seconds in range, said in the server list and SHOW ROUTER STATUS, and read
// per statement, so a change governs open connections and one server's limit
// is never another's.
func TestSQLModeAPI_maxBehindSource_2777(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	srv := newRegistryServer(t)
	id := createHiddenTestServer(t, srv)
	rec, body := doServersReq(t, srv, "POST", "/api/servers", `{"name":"other","host":"h2","user":"u","password":"p","dbname":"db"}`)
	if rec.Code != 201 {
		t.Fatalf("create other: %d %s", rec.Code, body)
	}
	var other serverDTO
	if err := json.Unmarshal(body, &other); err != nil {
		t.Fatal(err)
	}
	path := "/api/servers/" + id + "/sql-mode"
	put := func(body string) sqlModeDTO {
		t.Helper()
		rec, out := doServersReq(t, srv, "PUT", path, body)
		if rec.Code != 200 {
			t.Fatalf("PUT %s = %d %s", body, rec.Code, out)
		}
		return decodeSQLMode(t, out)
	}
	if srv.maxBehindSourceOf(id) != 0 {
		t.Fatal("a new server has a limit")
	}
	if d := put(`{"like_ignores_case":true,"fresh":false}`); d.MaxBehindSource != 0 {
		t.Fatalf("no limit sent: %+v", d)
	}
	if d := put(`{"max_behind_source_seconds":300}`); d.MaxBehindSource != 300 || !d.LikeIgnoresCase || d.Fresh {
		t.Fatalf("limit alone must keep the others: %+v", d)
	}
	// Read now, not when a connection opened.
	if got := srv.maxBehindSourceOf(id); got != 300*time.Second {
		t.Errorf("limit read = %v, want 5m", got)
	}
	if got := srv.maxBehindSourceOf(other.ID); got != 0 {
		t.Errorf("the other server has %v, want none", got)
	}
	if srv.maxBehindSourceOf(bootServerID) != 0 || srv.maxBehindSourceOf("nope") != 0 {
		t.Error("the boot entry or an unknown id has a limit")
	}
	if d := put(`{"fresh":true}`); d.MaxBehindSource != 300 {
		t.Fatalf("fresh alone must keep the limit: %+v", d)
	}
	rows := map[string]string{}
	for _, p := range srv.RouterStatus(context.Background(), id, nil) {
		rows[p[0]] = p[1]
	}
	if rows["max_behind_source_seconds"] != "300" {
		t.Errorf("SHOW ROUTER STATUS says %q, want 300", rows["max_behind_source_seconds"])
	}
	for _, p := range srv.RouterStatus(context.Background(), other.ID, nil) {
		if p[0] == "max_behind_source_seconds" && p[1] != "off" {
			t.Errorf("the other server's status says %q, want off", p[1])
		}
	}

	rec, out := doServersReq(t, srv, "GET", "/api/servers", "")
	var list struct {
		Servers []serverDTO `json:"servers"`
	}
	if rec.Code != 200 || json.Unmarshal(out, &list) != nil {
		t.Fatalf("list: %d %s", rec.Code, out)
	}
	for _, s := range list.Servers {
		want := int64(0)
		if s.ID == id {
			want = 300
		}
		if s.SQLMaxBehindSource != want {
			t.Errorf("list says %d for %s, want %d", s.SQLMaxBehindSource, s.Name, want)
		}
	}

	// Edits through the server's own form never carry it: it survives them.
	edited := mustEntry(t, srv, id)
	edited.SQLMaxBehindSource = 0
	if err := srv.cm.reg.Update(edited); err != nil {
		t.Fatalf("update: %v", err)
	}
	if e := mustEntry(t, srv, id); e.SQLMaxBehindSource != 300 {
		t.Fatalf("an edit dropped the limit: %+v", e)
	}

	for _, bad := range []string{`{"max_behind_source_seconds":-1}`, `{"max_behind_source_seconds":1.5}`, `{"max_behind_source_seconds":"300"}`,
		`{"max_behind_source_seconds":null}`, `{"max_behind_source_seconds":31536001}`, `{"max_behind_source":60}`, `{}`} {
		rec, out := doServersReq(t, srv, "PUT", path, bad)
		if rec.Code != 400 {
			t.Errorf("body %s answered %d %s, want 400", bad, rec.Code, out)
		}
		if srv.maxBehindSourceOf(id) != 300*time.Second {
			t.Fatalf("body %s changed the saved limit", bad)
		}
	}
	if rec, out := doServersReq(t, srv, "PUT", "/api/servers/"+bootServerID+"/sql-mode", `{"max_behind_source_seconds":60}`); rec.Code != 409 {
		t.Errorf("boot entry: %d %s, want 409", rec.Code, out)
	}

	// 0 takes it off.
	if d := put(`{"max_behind_source_seconds":0}`); d.MaxBehindSource != 0 || srv.maxBehindSourceOf(id) != 0 {
		t.Fatalf("off: %+v", d)
	}
}

// The limit on the copy's path, over the fixture's real snapshot: what the
// statement reads is placed from the server's remembered cut (as the router
// keeps them) and its capture sample; a statement within the limit runs, one
// past it is refused before the statement runs, and one whose lag cannot be
// told is refused as unknown.
func TestSQL_behindSourceLimit_2777(t *testing.T) {
	refs := &sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{{Schema: "shop", Name: "orders"}}}
	const id = "srv-limit"
	const stmt = "SELECT count(*) FROM shop.orders"
	run := func(f *sqlFixture, limit time.Duration) error {
		t.Helper()
		f.expectArchive()
		ctx := withBehindLimit(context.Background(), id, limit)
		_, err := f.s.runSQLVouched(ctx, f.s.cm.boot, "u", stmt, "", 0, sqlsandbox.Session{}, nil)
		return err
	}
	t.Run("no limit asks nothing", func(t *testing.T) {
		runner := &fakeSQLRunner{res: oneRowResult(), refs: refs}
		f := newSQLFixture(t, runner, true)
		if err := run(f, 0); err != nil {
			t.Fatalf("no limit: %v", err)
		}
		if runner.calls() != 1 {
			t.Errorf("ran %d times, want once", runner.calls())
		}
	})

	t.Run("unknown lag", func(t *testing.T) {
		// The fixture's file records no writer and no event mark: where its
		// snapshot was cut cannot be placed.
		runner := &fakeSQLRunner{res: oneRowResult(), refs: refs}
		f := newSQLFixture(t, runner, true)
		err := run(f, time.Minute)
		var refusal *sqlBehindRefusal
		if !errors.As(err, &refusal) || refusal.unknown == "" {
			t.Fatalf("err = %v, want a refusal for an unknown lag", err)
		}
		if runner.calls() != 0 {
			t.Errorf("the statement ran %d time(s) with its lag unknown", runner.calls())
		}
	})

	t.Run("within and past the limit", func(t *testing.T) {
		runner := &fakeSQLRunner{res: oneRowResult(), refs: refs}
		f := newSQLFixture(t, runner, true)
		snapKey := filepath.Join(f.root, sqlSnapshotDirName) + string(filepath.Separator)
		now := time.Now()
		// Cut ten minutes ago, the data holding event 5, and capture caught
		// up with event 5 the newest: a quiet source, 0 behind.
		f.s.copyAges.keepCut(id, snapKey, now.Add(-10*time.Minute), 5)
		f.s.copyAges.keepSample(id, captureSample{caughtUp: now.Add(-5 * time.Second), readAt: now, maxID: 5, newest: now.Add(-10 * time.Minute)})
		if err := run(f, time.Minute); err != nil {
			t.Fatalf("a quiet source within the limit: %v", err)
		}
		if runner.calls() != 1 {
			t.Fatalf("ran %d times, want once", runner.calls())
		}
		// Capture went on: event 9 is the newest, and the data stops at 5.
		f.s.copyAges.keepSample(id, captureSample{caughtUp: now.Add(-5 * time.Second), readAt: time.Now(), maxID: 9, newest: now})
		err := run(f, time.Minute)
		var refusal *sqlBehindRefusal
		if !errors.As(err, &refusal) || refusal.unknown != "" || refusal.behind < 9*time.Minute {
			t.Fatalf("err = %v, want a refusal about ten minutes behind", err)
		}
		if runner.calls() != 1 {
			t.Errorf("the statement past the limit ran")
		}
		// The same statement under a larger limit runs: read per statement.
		if err := run(f, time.Hour); err != nil {
			t.Fatalf("under an hour's limit: %v", err)
		}
		// Another server's limit does not read this one's cut.
		if _, _, ok := f.s.copyAges.knownCut("srv-other", snapKey); ok {
			t.Error("a cut kept for one server is another's")
		}
	})

	t.Run("the change log is not held to it", func(t *testing.T) {
		runner := &fakeSQLRunner{res: oneRowResult(), refs: &sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{{Name: "events"}}}}
		f := newSQLFixture(t, runner, true)
		f.expectArchive()
		ctx := withBehindLimit(context.Background(), id, time.Second)
		if _, err := f.s.runSQLVouched(ctx, f.s.cm.boot, "u", "SELECT count(*) FROM events", "", 0, sqlsandbox.Session{}, nil); err != nil {
			t.Fatalf("events under a limit: %v", err)
		}
	})

	t.Run("the port's error", func(t *testing.T) {
		runner := &fakeSQLRunner{res: oneRowResult(), refs: refs}
		f := newSQLFixture(t, runner, true)
		f.expectArchive()
		q := &SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:default", id: bootServerID}
		// The boot entry has no limit of its own; one carried in by the
		// caller is the copy's path all the same.
		_, err := q.Run(withBehindLimit(context.Background(), id, time.Minute), stmt, "", sqlsandbox.Session{})
		var behind *sqlsandbox.BehindSourceError
		if !errors.As(err, &behind) || !behind.Unknown || !strings.Contains(behind.Reason, "max_behind_source_seconds") {
			t.Fatalf("err = %v, want a BehindSourceError for an unknown lag", err)
		}
	})
}

// The router card says the server's lag limit where it says the copy's age,
// from the server's saved entry, and "whatever its age" without one.
func TestRouterLines_sayTheLagLimit_2777(t *testing.T) {
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
	view := routerViewDTO{Configured: true, URL: "http://router:9091/status", ReadAt: "2026-10-24T14:31:00Z",
		Status: &RouterStatusDoc{Version: 1, Listen: "0.0.0.0:3309", ReadOnly: true, CopySnapshotsReadAt: "2026-10-24T14:30:50Z",
			RouterNumbers: RouterNumbers{CountingSince: "2026-10-24T09:00:00Z", Servers: map[string]RouterServerNumbers{
				"s1": {CopySnapshot: "2026-10-24T14:19:00Z"}}}}}
	payload, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const lines = vm.runInContext("routerLines", ctx);
const view = ` + string(payload) + `;
const out = {};
for (const [k, cur] of Object.entries({ limited: { id: "s1", name: "shop", sql_max_behind_source_seconds: 300 }, open: { id: "s1", name: "shop", sql_max_behind_source_seconds: 0 } })) {
  out[k] = (lines(view, cur) || []).map((l) => l.text).join("\n");
}
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "lines.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string]string
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(raw[strings.LastIndex(strings.TrimSpace(string(raw)), "\n")+1:]))), &got); err != nil {
		t.Fatalf("output %q: %v", raw, err)
	}
	if !strings.Contains(got["limited"], "at most 5m behind the source, this server's limit; past that, MySQL answers.") || strings.Contains(got["limited"], "whatever its age") {
		t.Errorf("with a limit the card says %q", got["limited"])
	}
	if !strings.Contains(got["open"], "answered from it whatever its age.") || strings.Contains(got["open"], "limit") {
		t.Errorf("without a limit the card says %q", got["open"])
	}
}

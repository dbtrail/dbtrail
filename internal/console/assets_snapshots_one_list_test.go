package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The Snapshots card draws the jobs and the list of snapshots as one list:
// a job sits on the line of the snapshot it left, and a job that left none
// (a skip, a failure, a compaction) gets a line of its own between the
// snapshots. This drives the real functions over the payloads the page
// reads: the join, the paging of the loose jobs, and the card.
const oneListHarnessJS = `
const find = (n, cls, out = []) => { if (!n || !n.children) return out; if ((" " + n.className + " ").includes(" " + cls + " ")) out.push(n);
  for (const c of n.children) find(c, cls, out); return out; };
const text = (n) => (n ? n.textContent : "");
const call = (name) => vm.runInContext(name, ctx);
const c = JSON.parse(process.argv[3]);
const rows = call("copyJobsModel")({ jobs: c.jobs }).rows;
const joined = call("copyJobsJoin")(rows, c.snapshots);
const out = { on: {}, loose: joined.loose.map((r) => r.label + "@" + r.at), pages: [], screens: [] };
for (const [i, r] of joined.bySnap) out.on[c.snapshots[i].time] = r.label + "|" + r.took + "|" + r.qty + "|" + r.outcome;
const size = vm.runInContext("BACKUPS_PAGE_SIZE", ctx);
for (let start = 0; start < Math.max(1, c.snapshots.length); start += size) {
  out.pages.push(call("copyJobsBetween")(joined.loose, c.snapshots, start, Math.min(c.snapshots.length, start + size)).map((r) => r.label + "@" + r.at));
}
vm.runInContext("capsCache = { monitor: true }; currentServer = 'a'; defaultServerId = 'a';", ctx);
const b = { configured: true, snapshots: c.snapshots.map((s, i) => Object.assign({ age_hours: i, tables: ["t"] }, s)) };
const lines = (panel) => find(panel, "stg-row").map((r) => r.children.map(text).join("|"));
for (let page = 0; page < out.pages.length; page++) {
  vm.runInContext("backupsPage = { server: 'a', index: " + page + " };", ctx);
  const panel = call("baselinesPanel")(b, [], { jobs: rows, top: call("copyJobsParts")(call("copyJobsModel")({ jobs: c.jobs })) });
  out.screens.push({ lines: lines(panel), all: text(panel), card: panel.className, titles: find(panel, "ov-panel-title").length,
    chevrons: find(panel, "bk-chev").length, grid: find(panel, "snap-row").length });
}
vm.runInContext("backupsPage = { server: 'a', index: 0 };", ctx);
const plain = call("baselinesPanel")(b, [], {});
out.plain = { lines: lines(plain), card: plain.className, titles: find(plain, "ov-panel-title").length, grid: find(plain, "snap-row").length };
process.stdout.write(JSON.stringify(out));
`

type oneListScreen struct {
	Lines                  []string
	All, Card              string
	Titles, Chevrons, Grid int
}

type oneListOut struct {
	On      map[string]string
	Loose   []string
	Pages   [][]string
	Screens []oneListScreen
	Plain   oneListScreen
}

func runOneList(t *testing.T, jobs []map[string]any, snapshots []map[string]any) oneListOut {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed")
	}
	appJS, err := filepath.Abs(filepath.Join("assets", "app.js"))
	if err != nil {
		t.Fatal(err)
	}
	if jobs == nil {
		jobs = []map[string]any{}
	}
	arg, err := json.Marshal(map[string]any{"jobs": jobs, "snapshots": snapshots})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "onelist.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+oneListHarnessJS), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, string(arg)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var out oneListOut
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	return out
}

func snapAt(stamp string) map[string]any { return map[string]any{"time": stamp} }

func TestSnapshotsOneList_aJobSitsOnTheSnapshotItLeft(t *testing.T) {
	type j = map[string]any
	out := runOneList(t, []j{
		// Newest first, as the daemon sends them.
		{"kind": "refresh", "outcome": "ok", "started_at": "2026-10-13T21:40:08Z", "finished_at": "2026-10-13T21:40:12Z", "snapshot_time": "2026-10-13T21:40:08Z", "events": 800},
		// A skip names no snapshot.
		{"outcome": "skipped", "started_at": "2026-10-13T21:35:08Z", "finished_at": "2026-10-13T21:35:08Z", "skip_reason": "another job holds the server"},
		// A restore's snapshot is a PAST moment: it joins by the snapshot it
		// left, never by when the job ran.
		{"kind": "restore", "outcome": "ok", "started_at": "2026-10-13T21:32:00Z", "finished_at": "2026-10-13T21:33:30Z", "snapshot_time": "2026-10-13T09:00:00Z"},
		// Failed, and the snapshot was written anyway: on the snapshot's line.
		{"kind": "refresh", "outcome": "failed", "started_at": "2026-10-13T21:30:08Z", "finished_at": "2026-10-13T21:30:17Z", "snapshot_time": "2026-10-13T21:30:08Z", "published": true, "error": "upload failed"},
		// Failed before publishing: a line of its own.
		{"kind": "refresh", "outcome": "failed", "started_at": "2026-10-13T21:25:08Z", "finished_at": "2026-10-13T21:25:11Z", "error": "capture gap"},
		// An older run that names the SAME snapshot as the newest one: the
		// newest takes the line, this one stays loose.
		{"kind": "refresh", "outcome": "failed", "started_at": "2026-10-13T21:39:00Z", "finished_at": "2026-10-13T21:39:02Z", "snapshot_time": "2026-10-13T21:40:08Z", "published": true, "error": "upload failed"},
		// Its snapshot is not in the list any more. It went well, so it has
		// no line: it is counted in one sentence under the list.
		{"kind": "refresh", "outcome": "ok", "started_at": "2026-10-13T08:00:00Z", "finished_at": "2026-10-13T08:00:05Z", "snapshot_time": "2026-10-13T08:00:00Z", "events": 5},
		// The same, and it failed: that one keeps its line and says so.
		{"kind": "refresh", "outcome": "failed", "started_at": "2026-10-13T07:00:00Z", "finished_at": "2026-10-13T07:00:05Z", "snapshot_time": "2026-10-13T07:00:00Z", "published": true, "error": "upload failed"},
		// A snapshot time that does not read as one, and a job with no time.
		{"kind": "compact", "outcome": "ok", "started_at": "2026-10-13T21:20:00Z", "finished_at": "2026-10-13T21:20:01Z", "snapshot_time": "soon"},
		{"kind": "compact", "outcome": "ok"},
	}, []j{
		// The list's own stamp is the bare form; the job's is RFC 3339.
		snapAt("2026-10-13 21:40:08"), snapAt("2026-10-13 21:30:08"), snapAt("2026-10-13 21:00:00"), snapAt("2026-10-13 09:00:00"),
	})
	want := map[string]string{
		"2026-10-13 21:40:08": "update|4s|800 changes|",
		"2026-10-13 21:30:08": "update|9s||failed",
		"2026-10-13 09:00:00": "restore|1m 30s||",
	}
	if len(out.On) != len(want) {
		t.Errorf("jobs on snapshot lines = %v, want %v", out.On, want)
	}
	for at, w := range want {
		if out.On[at] != w {
			t.Errorf("snapshot %s carries %q, want %q", at, out.On[at], w)
		}
	}
	if len(out.Loose) != 7 {
		t.Errorf("loose jobs = %v, want the seven with no line of their own", out.Loose)
	}
	s := out.Screens[0]
	if !strings.Contains(s.Card, "cj-card") || s.Titles != 0 {
		t.Errorf("the list is not inside the jobs card: class %q, %d panel titles", s.Card, s.Titles)
	}
	// One chevron per snapshot: a loose job opens nothing.
	if s.Chevrons != 4 || s.Grid != 10 || len(s.Lines) != 10 {
		t.Errorf("%d chevrons, %d grid rows, %d lines; want 4, 10, 10:\n%s", s.Chevrons, s.Grid, len(s.Lines), strings.Join(s.Lines, "\n"))
	}
	for _, bad := range []string{"null", "undefined", "NaN", "Invalid"} {
		if strings.Contains(s.All, bad) {
			t.Errorf("the card says %q: %s", bad, s.All)
		}
	}
	// By time, newest first, the job with no time last; the restore is on
	// the line of the past moment it rebuilt, and the snapshot with no job
	// on record says so.
	wantOrder := []string{
		"update|2026-10-13 21:40:08|", "update|2026-10-13 21:39:00|", "scheduled run|2026-10-13 21:35:08|",
		"update|2026-10-13 21:30:08|", "update|2026-10-13 21:25:08|", "compaction|2026-10-13 21:20:00|",
		"snapshot|2026-10-13 21:00:00|", "restore|2026-10-13 09:00:00|", "update|2026-10-13 07:00:00|", "compaction||",
	}
	for i, w := range wantOrder {
		if i >= len(s.Lines) || !strings.HasPrefix(s.Lines[i], w) {
			t.Errorf("line %d does not start %q:\n%s", i, w, strings.Join(s.Lines, "\n"))
			break
		}
	}
	// The restore's own line would be a second line for one event.
	if n := strings.Count(strings.Join(s.Lines, "\n"), "restore|"); n != 1 {
		t.Errorf("the restore is drawn %d times", n)
	}
	all := strings.Join(s.Lines, "\n")
	// Said of the job whose snapshot is gone, and of no other: the older
	// run for the newest snapshot is loose because a newer one took the
	// line, and its snapshot is right there in the list.
	if n := strings.Count(all, "its snapshot is not in this list"); n != 1 || !strings.Contains(s.Lines[8], "upload failed · the copy was written · its snapshot is not in this list") {
		t.Errorf("\"its snapshot is not in this list\" is said %d times, want once, on the 07:00 job:\n%s", n, all)
	}
	if !strings.Contains(s.All, "1 job on this page left a snapshot that is no longer in the list. The bars show it.") {
		t.Errorf("the job that went well and whose snapshot is gone is not counted: %s", s.All)
	}
	// A run that failed and still left its snapshot: the reason is on the
	// snapshot's line as text, not behind a hover.
	if !strings.HasPrefix(s.Lines[3], "update|2026-10-13 21:30:08|") || !strings.Contains(s.Lines[3], "upload failed · the copy was written") {
		t.Errorf("the failed run's reason is not on its snapshot's line: %s", s.Lines[3])
	}
}

// Every loose job is on exactly one page, whatever the page: lost is a
// failure nobody sees, twice is a failure counted twice.
func TestSnapshotsOneList_everyLooseJobIsOnOnePage(t *testing.T) {
	type j = map[string]any
	var jobs, snaps []j
	stamp := func(min int) string {
		return "2026-10-13T" + two(10+min/60) + ":" + two(min%60) + ":00Z"
	}
	// 19 snapshots five minutes apart (three pages of eight), and a failed
	// job two minutes after each, plus one newer than all, one older than
	// all, one at the exact instant of a page's first snapshot, one with no time.
	for i := 18; i >= 0; i-- {
		jobs = append(jobs, j{"kind": "refresh", "outcome": "failed", "started_at": stamp(i*5 + 2), "finished_at": stamp(i*5 + 2), "error": "x"})
		snaps = append(snaps, j{"time": strings.NewReplacer("T", " ", "Z", "").Replace(stamp(i * 5))})
	}
	jobs = append(jobs,
		j{"kind": "compact", "outcome": "ok", "started_at": "2026-10-13T09:00:00Z", "finished_at": "2026-10-13T09:00:01Z"},
		j{"kind": "compact", "outcome": "ok", "started_at": stamp(10 * 5), "finished_at": stamp(10 * 5)},
		j{"kind": "compact", "outcome": "ok"})
	out := runOneList(t, jobs, snaps)
	if len(out.Pages) != 3 {
		t.Fatalf("%d pages, want 3", len(out.Pages))
	}
	seen := map[string]int{}
	total := 0
	for _, p := range out.Pages {
		for _, l := range p {
			seen[l]++
			total++
		}
	}
	if total != len(out.Loose) || len(out.Loose) != 22 {
		t.Errorf("%d loose jobs, %d drawn over the pages", len(out.Loose), total)
	}
	for l, n := range seen {
		if n != 1 {
			t.Errorf("%s is on %d pages", l, n)
		}
	}
	// Drawn, not only counted: each page's card holds its snapshots and its jobs.
	lines := 0
	for i, s := range out.Screens {
		lines += len(s.Lines)
		if strings.Contains(s.All, "null") || strings.Contains(s.All, "NaN") {
			t.Errorf("page %d: %s", i, s.All)
		}
	}
	if lines != 19+22 {
		t.Errorf("%d lines drawn over the pages, want 19 snapshots and 22 jobs", lines)
	}
}

func two(n int) string {
	if n < 10 {
		return "0" + string(rune('0'+n))
	}
	return string(rune('0'+n/10)) + string(rune('0'+n%10))
}

// With no job history (a standalone serve keeps none) the list is the plain
// one, with its own title: no empty job column, no line saying "snapshot".
func TestSnapshotsOneList_withoutJobsItIsThePlainList(t *testing.T) {
	type j = map[string]any
	out := runOneList(t, nil, []j{snapAt("2026-10-13 21:40:08"), snapAt("2026-10-13 21:30:08")})
	if out.Plain.Grid != 0 || out.Plain.Titles != 1 || !strings.Contains(out.Plain.Card, "ov-panel") || len(out.Plain.Lines) != 2 {
		t.Errorf("plain list: %+v", out.Plain)
	}
	// A history that is there and empty still makes the card, and every
	// snapshot says no job is on record for it.
	s := out.Screens[0]
	if s.Grid != 2 || !strings.HasPrefix(s.Lines[0], "snapshot|2026-10-13 21:40:08|") || !strings.Contains(s.All, "No job has run yet.") {
		t.Errorf("empty history: %+v", s)
	}
}

// With no snapshot to sit beside, the jobs are listed all the same: a first
// read that keeps failing has its reason here and nowhere else on the page.
func TestSnapshotsOneList_noSnapshotsStillListsTheJobs(t *testing.T) {
	type j = map[string]any
	out := runOneList(t, []j{
		{"kind": "dump", "outcome": "failed", "started_at": "2026-10-13T21:40:08Z", "finished_at": "2026-10-13T21:41:08Z", "error": "access denied for user"},
		{"outcome": "skipped", "started_at": "2026-10-13T21:35:08Z", "finished_at": "2026-10-13T21:35:08Z", "skip_reason": "no folder to write to"},
	}, []j{})
	s := out.Screens[0]
	all := strings.Join(s.Lines, "\n")
	if len(s.Lines) != 2 || !strings.Contains(all, "access denied for user") || !strings.Contains(all, "no folder to write to") || s.Chevrons != 0 {
		t.Errorf("with no snapshots the jobs are not listed:\n%s", all)
	}
	if !strings.Contains(s.All, "no snapshots found") {
		t.Errorf("the empty state is gone: %s", s.All)
	}
}

// The link the page joins on leaves the server: without it every job is a
// loose line and every snapshot says no job is on record.
func TestCopyJobCarriesTheSnapshotItLeft(t *testing.T) {
	rec := BaselineRunRecord{Kind: BaselineRunRefresh, SnapshotTime: "2026-10-13T21:40:08Z", StartedAt: "2026-10-13T21:40:08Z", FinishedAt: "2026-10-13T21:40:12Z"}
	for _, restricted := range []bool{false, true} {
		d := copyJobFromRecord(rec, restricted)
		if d.SnapshotTime != rec.SnapshotTime || !d.Published {
			t.Errorf("restricted=%v: snapshot_time %q published %v", restricted, d.SnapshotTime, d.Published)
		}
	}
	if d := copyJobFromRecord(BaselineRunRecord{Kind: BaselineRunDump, SkipReason: "busy"}, false); d.SnapshotTime != "" || d.Published {
		t.Errorf("a skip names a snapshot: %+v", d)
	}
}

// The old address of the Jobs tab opens the list the jobs moved into.
func TestSnapshotsJobsAddressOpensVersions(t *testing.T) {
	js := readAsset(t, "app.js")
	if !strings.Contains(js, `jobs: "versions"`) {
		t.Error("/snapshots#jobs no longer opens Versions: a saved link lands on a tab that does not exist")
	}
	if strings.Contains(functionBody(t, js, "function snapshotTabs("), `["jobs", "Jobs"]`) {
		t.Error("the Jobs tab is back beside the list that already draws every job")
	}
}

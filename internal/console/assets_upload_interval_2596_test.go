package console

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2596 part 2, drawn: the S3 interval on the schedule form, the age of the
// newest S3 copy on the Snapshots header, the held snapshot nothing will
// send, and the bucket rule worked out from what reaches S3. Every scene is
// drawn from what the real endpoints answered.
func TestSnapshotsPage_uploadInterval(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	now := time.Now().UTC().Truncate(time.Second)
	s3 := "s3://bkt/snaps"
	// scene answers GET /api/baselines for a server whose local copy is
	// fresh and whose newest S3 copy is s3Age old, the newest local one
	// held when held is set, on the schedule put (none when empty).
	// s3Listing, when set, replaces the bucket's answer.
	sceneWith := func(withS3 bool, put string, s3Age time.Duration, held bool, s3Listing *s3CopyListing, runs ...BaselineRunRecord) (json.RawMessage, json.RawMessage) {
		t.Helper()
		srv, id, dir := s3CopyServerWith(t, &stubScheduleReporter{full: true}, withS3)
		if put != "" {
			if rec, raw := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", put); rec.Code != 200 {
				t.Fatalf("PUT %s: code=%d body=%s", put, rec.Code, raw)
			}
		}
		for _, r := range runs {
			r.ServerID, r.Trigger = id, BaselineRunTriggerScheduled
			if err := srv.baselineHistory.Append(r); err != nil {
				t.Fatal(err)
			}
		}
		listings := map[string]s3CopyListing{dir: {snaps: []time.Time{now.Add(-2 * time.Minute), now.Add(-20 * time.Hour)}}}
		if withS3 {
			listings[s3] = s3CopyListing{snaps: []time.Time{now.Add(-s3Age)}}
			if s3Listing != nil {
				listings[s3] = *s3Listing
			}
		}
		stubListings(listings)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if held {
			if err := baseline.MarkUploadHeld(dir, reconstruct.SnapshotDirName(now.Add(-2*time.Minute))); err != nil {
				t.Fatal(err)
			}
		}
		rec, raw := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
		if rec.Code != 200 {
			t.Fatalf("GET: code=%d body=%s", rec.Code, raw)
		}
		e, _ := srv.cm.reg.Get(id)
		row, err := json.Marshal(srv.backupSettingsServerDTO(e))
		if err != nil {
			t.Fatal(err)
		}
		return raw, row
	}
	scene := func(withS3 bool, put string, s3Age time.Duration, held bool, runs ...BaselineRunRecord) (json.RawMessage, json.RawMessage) {
		return sceneWith(withS3, put, s3Age, held, nil, runs...)
	}
	sched := `{"every":"15m","at":"00:00","upload_every":"6h"}`
	onTime, _ := scene(true, sched, 5*time.Hour, true)
	late, _ := scene(true, sched, 13*time.Hour, true)
	deleted, _ := scene(true, "", 7*time.Hour, true)
	notHeld, _ := scene(true, "", 7*time.Hour, false)
	noS3, _ := scene(false, `{"every":"15m","at":"00:00"}`, 0, false)
	lastRun, _ := scene(true, sched, 5*time.Hour, true, BaselineRunRecord{Kind: BaselineRunRefresh,
		StartedAt: now.Add(-3 * time.Minute).Format(time.RFC3339), FinishedAt: now.Add(-2 * time.Minute).Format(time.RFC3339),
		SnapshotTime: now.Add(-2 * time.Minute).Format(time.RFC3339), Tables: 3, UploadHeld: true})
	emptyBucket, _ := sceneWith(true, sched, 0, true, &s3CopyListing{})
	// No schedule and nothing held: the unread bucket is the only reason to be amber.
	unread, _ := sceneWith(true, "", 0, false, &s3CopyListing{err: errors.New("AccessDenied")})
	_, dailyRow := scene(true, `{"every":"15m","at":"00:00","upload_every":"1d"}`, time.Hour, false)

	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const byClass = (n, c, out = []) => { if (!n || typeof n !== "object") return out; if ((" " + (n.className || "") + " ").includes(" " + c + " ")) out.push(n); for (const k of n.children || []) byClass(k, c, out); return out; };
const text = (n) => !n ? "" : n.nodeType === 3 ? n.textContent : (n._text || "") + (n.children || []).map(text).join("");
const group = (card, label) => byClass(card, "sch-pills").find((g) => g.attrs["aria-label"] === label) || null;
vm.runInContext("capsCache = { backup_schedule: true }; sessionMay = () => true;", ctx);
const cur = { id: "a", name: "a", kind: "registry" };
const draw = (b, caps) => {
  vm.runInContext("capsCache = " + (caps || "{ backup_schedule: true }") + ";", ctx);
  const card = vm.runInContext("backupScheduleCard", ctx)(cur, b);
  const up = group(card, "Send to S3 every");
  const hero = vm.runInContext("snapshotHero", ctx)(b, null, cur, null);
  const tile = byClass(hero, "hero-tile").find((n) => text(n).startsWith("In S3")) || null;
  return {
    state: card ? text(byClass(card, "bk-card-state")[0]) : "",
    red: card ? byClass(card, "err").map(text) : [],
    hints: card ? byClass(card, "form-hint").map(text) : [],
    up: up ? up.value : null,
    upPills: up ? up.children.filter((p) => !p.hidden).map(text) : null,
    tile: tile ? text(tile) : "",
    tileClass: tile ? tile.className : "",
  };
};
// Saving, and changing the update interval with the form open.
const save = async (b, everyTo) => {
  const calls = [];
  ctx.__calls = calls;
  vm.runInContext("api = async (p, o) => { __calls.push(o.body); return { schedule: {} }; }; toast = () => {}; renderSnapshots = () => {};", ctx);
  const card = vm.runInContext("backupScheduleCard", ctx)(cur, b);
  const up = group(card, "Send to S3 every");
  if (everyTo) group(card, "Update the copy every").children.find((p) => text(p) === everyTo).onclick();
  const btn = byClass(card, "btn").find((n) => n.tag === "button" && text(n) === "Save");
  await btn.onclick();
  return { body: calls[0], upPills: up ? up.children.filter((p) => !p.hidden).map(text) : null };
};
(async () => {
  const out = {
    onTime: draw(` + string(onTime) + `),
    late: draw(` + string(late) + `),
    deleted: draw(` + string(deleted) + `),
    notHeld: draw(` + string(notHeld) + `),
    noS3: draw(` + string(noS3) + `),
    lastRun: draw(` + string(lastRun) + `),
    readOnly: draw(` + string(deleted) + `, "{ backup_schedule: false }"),
    emptyBucket: draw(` + string(emptyBucket) + `),
    unread: draw(` + string(unread) + `),
    _caps: vm.runInContext("capsCache = { backup_schedule: true }; 1", ctx),
    save: await save(` + string(onTime) + `),
    saveDaily: await save(` + string(onTime) + `, "24 h"),
    saveNoS3: await save(` + string(noS3) + `),
    expiry: vm.runInContext("s3ExpiryWords", ctx)({ state: "in_force", days: 1 }, ` + string(dailyRow) + `, 0),
  };
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e); process.exit(1); });
`
	path := filepath.Join(t.TempDir(), "upload.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type drawn struct {
		State     string
		Red       []string
		Hints     []string
		Up        *string
		UpPills   []string `json:"upPills"`
		Tile      string
		TileClass string `json:"tileClass"`
	}
	type saved struct {
		Body    map[string]any
		UpPills []string `json:"upPills"`
	}
	var got struct {
		OnTime, Late, Deleted, NotHeld, NoS3, LastRun drawn
		ReadOnly, EmptyBucket, Unread                 drawn
		Save, SaveDaily, SaveNoS3                     saved
		Expiry                                        struct {
			Text string
			Warn bool
		}
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	for name, d := range map[string]drawn{"onTime": got.OnTime, "late": got.Late, "deleted": got.Deleted, "notHeld": got.NotHeld, "noS3": got.NoS3, "lastRun": got.LastRun} {
		t.Logf("%s state: %s | tile: %s (%s) | pills %v", name, d.State, d.Tile, d.TileClass, d.UpPills)
		for _, r := range d.Red {
			t.Logf("%s red:  %s", name, r)
		}
	}
	t.Logf("expiry: %+v", got.Expiry)
	has := func(list []string, want string) bool {
		for _, s := range list {
			if strings.Contains(s, want) {
				return true
			}
		}
		return false
	}

	o := got.OnTime
	if !strings.Contains(o.State, ", sent to S3 every 6h.") {
		t.Errorf("onTime state = %q, want the S3 interval on it", o.State)
	}
	// 15 minutes hides nothing: every choice is at least that.
	if o.Up == nil || *o.Up != "6h" || strings.Join(o.UpPills, "|") != "Each update|1 h|6 h|24 h" {
		t.Errorf("onTime pills: lit %v, shown %v", o.Up, o.UpPills)
	}
	if o.Tile != "In S3newest 5 h old" || strings.Contains(o.TileClass, "warn") {
		t.Errorf("onTime tile = %q (%s), want the age, not amber: 5 h is within two 6 h intervals", o.Tile, o.TileClass)
	}
	if l := got.Late; l.Tile != "In S3newest 13 h old" || !strings.Contains(l.TileClass, "warn") {
		t.Errorf("late tile = %q (%s), want amber past two intervals", l.Tile, l.TileClass)
	}
	d := got.Deleted
	if !strings.Contains(d.TileClass, "warn") || !has(d.Red, "The newest snapshot is only on this machine: the S3 interval kept it here, and no schedule is running to send it. The newest snapshot in S3 is 7 h old. Turn the schedule on to send it.") {
		t.Errorf("deleted: tile %q (%s), red %v", d.Tile, d.TileClass, d.Red)
	}
	if n := got.NotHeld; strings.Contains(n.TileClass, "warn") || has(n.Red, "only on this machine") {
		t.Errorf("notHeld: a newer unmarked snapshot is not held: tile %s, red %v", n.TileClass, n.Red)
	}
	if n := got.NoS3; n.Up != nil || n.Tile != "" {
		t.Errorf("noS3: the S3 row (%v) or tile (%q) is drawn on a server with no S3", n.Up, n.Tile)
	}
	// A read-only console sees every schedule as unable to run, even while
	// the service beside it sends on time: it must not say nothing sends.
	if ro := got.ReadOnly; has(ro.Red, "only on this machine") || strings.Contains(ro.TileClass, "warn") {
		t.Errorf("readOnly: a console that runs no schedule claims none runs: red %v, tile %s", ro.Red, ro.TileClass)
	}
	// A bucket that never got a snapshot while this machine kept them for 20 h.
	if eb := got.EmptyBucket; eb.Tile != "In S3none yet" || !strings.Contains(eb.TileClass, "warn") {
		t.Errorf("emptyBucket tile = %q (%s), want amber: S3 holds nothing after 20 h", eb.Tile, eb.TileClass)
	}
	if u := got.Unread; u.Tile != "In S3could not be read" || !strings.Contains(u.TileClass, "warn") {
		t.Errorf("unread tile = %q (%s), want amber", u.Tile, u.TileClass)
	}
	if !has(got.LastRun.Hints, ", not sent to S3 (snapshots go there every 6h).") {
		t.Errorf("lastRun: the held run is not said: %v", got.LastRun.Hints)
	}
	if v, ok := got.Save.Body["upload_every"]; !ok || v != "6h" {
		t.Errorf("save sent %v, want the saved upload_every 6h", got.Save.Body)
	}
	// Moving the updates to 24 h puts 1 h and 6 h out of reach: they are
	// hidden and the choice falls back to each update instead of a refusal.
	if v := got.SaveDaily.Body["upload_every"]; v != "" || strings.Join(got.SaveDaily.UpPills, "|") != "Each update|24 h" || got.SaveDaily.Body["every"] != "1d" {
		t.Errorf("saveDaily sent %v with pills %v", got.SaveDaily.Body, got.SaveDaily.UpPills)
	}
	if _, ok := got.SaveNoS3.Body["upload_every"]; ok {
		t.Errorf("saveNoS3 sent upload_every %v: with no S3 row, the save must keep what is saved", got.SaveNoS3.Body)
	}
	if !got.Expiry.Warn || !strings.Contains(got.Expiry.Text, "That is too short for snapshots sent to S3 every 1d: ") || !strings.Contains(got.Expiry.Text, "at least 2 days") {
		t.Errorf("expiry = %+v, want a 1-day rule refused against a 1d S3 interval", got.Expiry)
	}
	for _, s := range strings.Split(string(raw), "\"") {
		for _, bad := range []string{"undefined", "NaN", "null h"} {
			if strings.Contains(s, bad) {
				t.Errorf("drawn %q: %q", bad, s)
			}
		}
	}
}

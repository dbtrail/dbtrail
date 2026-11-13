package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #2674, points 1, 4 and 6, on the console side.

// Point 1, at the edges of the margin: with a rate and no count only the
// fixed cost decides, and inside the margin it does not choose.
func TestWindowChoice_theFixedCostWithARateAndNoCount_2674(t *testing.T) {
	now := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	for _, c := range []struct {
		name string
		w    BackupWindow
		full bool
	}{
		{"clearly dearer: a full read", BackupWindow{Anchor: old, Events: -1, FoldFixed: 6 * time.Minute, FoldRate: 4000, LastFull: 2 * time.Minute}, true},
		{"dearer, but inside the margin: the update", BackupWindow{Anchor: old, Events: -1, FoldFixed: 9 * time.Minute, FoldRate: 4000, LastFull: 8 * time.Minute}, false},
		{"twice as long but under 30s more: the update", BackupWindow{Anchor: old, Events: -1, FoldFixed: 50 * time.Second, FoldRate: 4000, LastFull: 20 * time.Second}, false},
		{"clearly dearer, nothing measured since the full read: the update", BackupWindow{Anchor: old, Events: -1, FoldFixed: 6 * time.Minute, FoldRate: 4000, LastFull: 2 * time.Minute, UnmeasuredSinceFull: true}, false},
		{"no fixed cost measured: the update", BackupWindow{Anchor: old, Events: -1, FoldRate: 4000, LastFull: 2 * time.Minute}, false},
		{"no full read on record: the update", BackupWindow{Anchor: old, Events: -1, FoldFixed: 6 * time.Minute, FoldRate: 4000}, false},
		{"a known zero with a rate: the estimate decides, as before", BackupWindow{Anchor: now.Add(-time.Minute), Events: 0, FoldFixed: 6 * time.Minute, FoldRate: 4000, LastFull: 2 * time.Minute}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			method, why := WindowChoice(c.w, 5*time.Minute, now)
			if (method == BackupMethodFull) != c.full {
				t.Fatalf("method=%q why=%q, want full read %v", method, why, c.full)
			}
			if c.full && why != BackupWhyWindowPrefix+": the cheapest recent update took 6m, more than twice and over 30s longer than the last full read, which took 2m" {
				t.Fatalf("the reason reads %q", why)
			}
		})
	}
}

// Point 4: the run's update why reaches the wire from the record and from
// the live view, and the page says it after the run.
func TestScheduleRun_carriesTheUpdateWhy_2674(t *testing.T) {
	why := UnconfirmedUpdateWhy("the capture runs in binlog-position mode, which is not compared",
		"the previous snapshot is close to the oldest change the index still keeps, and could not be updated once rotation drops those hours")
	rec := scheduleRunFromRecord(&BaselineRunRecord{Kind: BaselineRunRefresh, UpdateWhy: why, StartedAt: "2026-11-12T10:00:00Z"})
	live := scheduleRunFromStatus(BackupScheduleState{LastMethod: BackupMethodRefresh, LastUpdateWhy: why, Last: &BaselineStatus{State: "succeeded"}})
	for name, dto := range map[string]*backupScheduleRunDTO{"record": rec, "live": live} {
		raw, err := json.Marshal(dto)
		if err != nil {
			t.Fatal(err)
		}
		var got struct {
			UpdateWhy string `json:"update_why"`
			Why       string `json:"why"`
		}
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if got.UpdateWhy != why || got.Why != "" {
			t.Fatalf("%s: %s, want update_why set and why empty", name, raw)
		}
	}
	// A record from a build before the field reads as no reason.
	var old BaselineRunRecord
	if err := json.Unmarshal([]byte(`{"server_id":"a","kind":"refresh","started_at":"x","finished_at":"y"}`), &old); err != nil || old.UpdateWhy != "" {
		t.Fatalf("an old record: %+v %v", old, err)
	}
}

// The two new lines of the page, generated from the daemon's real texts.
func TestScheduleFollowupsOnThePage_executed_2674(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	why := UnconfirmedUpdateWhy("the capture runs in binlog-position mode, which is not compared",
		"the previous snapshot is close to the oldest change the index still keeps, and could not be updated once rotation drops those hours")
	unread := IndexUnreadUpdateWhy("the index did not answer in time")
	busy := "a snapshot kept on this machine is being sent to S3 since 2026-11-12 10:00:05 UTC; the full read can start once it finishes"
	payload, err := json.Marshal(map[string]string{"why": why, "unread": unread, "busy": busy})
	if err != nil {
		t.Fatal(err)
	}
	script := functionBody(t, js, "function updateWhyLine(") + "\n" + functionBody(t, js, "function createBaselineRefusal(") + `
const c = ` + string(payload) + `;
console.log(JSON.stringify({
  update: updateWhyLine(c.why),
  unread: updateWhyLine(c.unread),
  busy: createBaselineRefusal(Object.assign(new Error(c.busy), { status: 409 })),
  failed: createBaselineRefusal(Object.assign(new Error("this server has no source configured; set the source connection first"), { status: 400 })),
}));
`
	path := filepath.Join(t.TempDir(), "followups.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var got struct{ Update, Unread, Busy, Failed string }
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	for _, l := range []string{got.Update, got.Unread, got.Busy, got.Failed} {
		t.Logf("page: %s", l)
		if strings.Contains(l, "—") || strings.Contains(l, "..") {
			t.Errorf("the line %q carries a dash or a doubled stop", l)
		}
	}
	if got.Update != "This update ran because "+why+"." || got.Unread != "This update ran because "+unread+"." {
		t.Errorf("the update lines read %q and %q", got.Update, got.Unread)
	}
	if got.Busy != "The full read did not start: "+busy+"." {
		t.Errorf("the refusal reads %q", got.Busy)
	}
	if got.Failed != "Snapshot failed: this server has no source configured; set the source connection first" {
		t.Errorf("a failure that is not a refusal reads %q", got.Failed)
	}
	// Both places a run is drawn say it.
	if strings.Count(js, "updateWhyLine(run.update_why)") != 1 || strings.Count(js, "updateWhyLine(d.run.update_why)") != 1 {
		t.Error("the run card or the snapshot's detail does not draw the update's reason")
	}
}

package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// #1528, the corollary written into CONTRIBUTING: a <details> is for a
// detail, and the page's own subject is a card. Putting backups on a
// timetable and restoring to a moment are what the Backups page is named
// after, and both sat behind a line of small caps that had to be clicked,
// with prose above them. A fold reintroduced here compiles, renders, and
// passes every other test in this package, because nothing else asserts on
// the element these functions build.
//
// jsFunctionSpan, NOT jsFunctionBody: these are must-NOT-contain checks, and
// jsFunctionBody cuts every line at its first `//`, so a fold written on the
// same line as any string holding a `//` (a https:// URL is enough, and
// app.js has several) would be truncated out of the needle and the guard
// would pass with the fold on screen. That failure direction is documented
// on the helper itself.
//
// Known limits, so nobody reads more coverage into this than it has: it
// matches source text, so `document.createElement("details")` or single
// quotes would slip past, and the span starts at the opening brace, so the
// header comment above each function is not covered by the loop (the
// separate check below covers the one claim that matters there).
func TestBackupsPageDoesNotFoldItsOwnSubject(t *testing.T) {
	js := readAsset(t, "app.js")
	for _, fn := range []string{"backupScheduleCard", "backupRestoreCard"} {
		span := jsFunctionSpan(t, js, fn)
		if strings.Contains(span, `el("details"`) || strings.Contains(span, `el("summary"`) {
			t.Errorf("%s builds a fold again; the Snapshots page's own subject is a card (#1528)", fn)
		}
		if !strings.Contains(span, `el("section", { class: "ov-panel`) {
			t.Errorf("%s no longer builds a panel section, so it does not look like the cards around it", fn)
		}
		if strings.Contains(span, ".open = true") {
			t.Errorf("%s still opens a fold, so one was reintroduced somewhere this guard cannot see", fn)
		}
	}
	// The header comment is the first thing a reader meets and the last thing
	// a rewrite updates: this one described the fold for the whole of #1528's
	// review. It sits outside jsFunctionSpan, so it needs its own look.
	for _, fn := range []string{"backupScheduleCard", "backupRestoreCard"} {
		i := strings.Index(js, "function "+fn+"(")
		if i < 0 {
			t.Fatalf("%s is gone", fn)
		}
		head := js[max(0, i-1200):i]
		if j := strings.LastIndex(head, "\n\n"); j >= 0 {
			head = head[j:]
		}
		for _, stale := range []string{"opens the card", "opening the card", "Collapsed by default", "The summary line"} {
			if strings.Contains(head, stale) {
				t.Errorf("%s's header comment still describes the fold (%q)", fn, stale)
			}
		}
	}
}

// The alarm survived the unfolding, and it survived it in WORDS. A failed,
// skipped or not-runnable schedule used to force the fold OPEN; with the card
// always visible that signal has to land somewhere, and colour alone is a
// verdict a screen reader cannot read out.
//
// This renders the real function in node rather than searching the source,
// because the source-level version of this guard passed green against
// `if (false) state.classList.add(...)` and against deleting five of the six
// alarm branches: strings.Contains proves a needle EXISTS, never that it
// runs.
func TestBackupScheduleAlarmReachesTheStateLine(t *testing.T) {
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

	cases := []struct {
		name        string
		caps        string
		schedule    map[string]any
		wantAlarm   bool
		wantWords   string // must appear in the state line itself
		rejectWords string // must NOT: a note that contradicts the body
	}{{
		name:      "healthy",
		caps:      "{ backup_schedule: true }",
		schedule:  map[string]any{"every": "1d", "at": "03:00", "runnable": true, "next_run": "2026-09-20T03:00:00Z"},
		wantAlarm: false,
	}, {
		// The case the card exists for: a saved schedule on a daemon that
		// cannot run it. It returns before the run-history block, so if the
		// alarm were raised only there this would render grey.
		name:      "not runnable, read-only daemon",
		caps:      "{ backup_schedule: false }",
		schedule:  map[string]any{"every": "1d", "at": "03:00", "runnable": false, "reason": "snapshot features are off on this daemon"},
		wantAlarm: true,
		wantWords: "Cannot run:",
	}, {
		// Runnable, next run predicted, but the last one failed: the state
		// line used to go red while its words stayed healthy.
		name: "last run failed",
		caps: "{ backup_schedule: true }",
		schedule: map[string]any{"every": "1d", "at": "03:00", "runnable": true, "next_run": "2026-09-20T03:00:00Z",
			"last_run": map[string]any{"method": "refresh", "ok": false, "finished_at": "2026-09-19T03:01:00Z", "error": "disk full"}},
		wantAlarm: true,
		wantWords: "The last run failed.",
	}, {
		name: "a slot was skipped",
		caps: "{ backup_schedule: true }",
		schedule: map[string]any{"every": "1d", "at": "03:00", "runnable": true, "next_run": "2026-09-20T03:00:00Z",
			"last_skipped": map[string]any{"at": "2026-09-19T03:00:00Z", "reason": "no previous snapshot"}},
		wantAlarm: true,
		wantWords: "A scheduled run did not start.",
	}, {
		// TWO facts at once, which every row above is blind to: the note is
		// one line, so the branches compete, and the fallback is recorded
		// when the full backup STARTS. Last-one-wins handed the line to the
		// fallback and said a full backup had run instead, while the body
		// said that same backup failed and nothing was written. A false
		// all-clear, on the backups page.
		name: "a fallback whose full read then failed",
		caps: "{ backup_schedule: true }",
		schedule: map[string]any{"every": "1d", "at": "03:00", "runnable": true, "next_run": "2026-09-20T03:00:00Z",
			"last_fallback": map[string]any{"at": "2026-09-19T03:00:00Z", "reason": "schema changed since the baseline"},
			"last_run":      map[string]any{"method": "dump", "ok": false, "finished_at": "2026-09-19T03:02:00Z", "error": "mydumper: connection refused"}},
		wantAlarm: true,
		wantWords: "The last run failed.",
		rejectWords: "full read ran instead",
	}, {
		// The mirror, and the reason the guard is a recency rule and not a
		// suppression: with the fallback the newest fact it must still reach
		// the line. Suppressing it whenever a run exists would leave the card
		// red with no words, which is what the note exists to prevent.
		name: "a fallback after a run that succeeded",
		caps: "{ backup_schedule: true }",
		schedule: map[string]any{"every": "1d", "at": "03:00", "runnable": true, "next_run": "2026-09-20T03:00:00Z",
			"last_run":      map[string]any{"method": "refresh", "ok": true, "finished_at": "2026-09-18T03:01:00Z", "tables": 4},
			"last_fallback": map[string]any{"at": "2026-09-19T03:00:00Z", "reason": "the recorded changes do not reach that far back"}},
		wantAlarm: true,
		wantWords: "An update was refused, so a full read ran instead.",
	}, {
		// The tie, which is not a corner case: baseline_schedule_loop.go
		// formats ONE stamp and gives it to both the fallback record and the
		// run it starts, so a full backup that fails inside its starting
		// second collides exactly. Two minutes apart (the row above) never
		// exercises it.
		name: "a fallback and its failed snapshot in the same second",
		caps: "{ backup_schedule: true }",
		schedule: map[string]any{"every": "1d", "at": "03:00", "runnable": true, "next_run": "2026-09-20T03:00:00Z",
			"last_fallback": map[string]any{"at": "2026-09-19T03:00:00Z", "reason": "schema changed since the baseline"},
			"last_run":      map[string]any{"method": "dump", "ok": false, "started_at": "2026-09-19T03:00:00Z", "finished_at": "2026-09-19T03:00:00Z", "error": "mydumper: connection refused"}},
		wantAlarm:   true,
		wantWords:   "The last run failed.",
		rejectWords: "full read ran instead",
	}, {
		// Same tie reached the other way: no finished_at, so the run is dated
		// by started_at, which IS the fallback's stamp.
		name: "a fallback and its failed snapshot, dated by started_at",
		caps: "{ backup_schedule: true }",
		schedule: map[string]any{"every": "1d", "at": "03:00", "runnable": true, "next_run": "2026-09-20T03:00:00Z",
			"last_fallback": map[string]any{"at": "2026-09-19T03:00:00Z", "reason": "schema changed since the baseline"},
			"last_run":      map[string]any{"method": "dump", "ok": false, "started_at": "2026-09-19T03:00:00Z", "error": "mydumper: connection refused"}},
		wantAlarm:   true,
		wantWords:   "The last run failed.",
		rejectWords: "full read ran instead",
	}, {
		// The upload-failure variant of the same tie: "ran instead" would
		// read as done and shipped, while the backup never left the machine.
		name: "a fallback whose snapshot could not be sent, same second",
		caps: "{ backup_schedule: true }",
		schedule: map[string]any{"every": "1d", "at": "03:00", "runnable": true, "next_run": "2026-09-20T03:00:00Z",
			"last_fallback": map[string]any{"at": "2026-09-19T03:00:00Z", "reason": "schema changed since the baseline"},
			"last_run": map[string]any{"method": "dump", "ok": false, "started_at": "2026-09-19T03:00:00Z", "finished_at": "2026-09-19T03:00:00Z",
				"snapshot_time": "2026-09-19 03:00:00", "error": "s3: access denied"}},
		wantAlarm:   true,
		wantWords:   "The last run could not send its snapshot.",
		rejectWords: "full read ran instead",
	}, {
		// The three forward-looking and knowledge-gap notes, which arrived
		// with no test of their own: each is a new user-visible string, and
		// deleting any one of them left the whole package green.
		name: "the next run cannot start",
		caps: "{ backup_schedule: true }",
		schedule: map[string]any{"every": "1d", "at": "03:00", "runnable": true, "next_run": "2026-09-20T03:00:00Z",
			"next_method_error": "the previous snapshot could not be read"},
		wantAlarm: true,
		wantWords: "The next run cannot start.",
	}, {
		name: "every run reads the database in full",
		caps: "{ backup_schedule: true }",
		schedule: map[string]any{"every": "1d", "at": "03:00", "runnable": true, "next_run": "2026-09-20T03:00:00Z",
			"next_method": "dump", "next_method_why": "this server has no index connection", "next_method_why_code": "no_index"},
		wantAlarm: true,
		wantWords: "Every run reads your database in full.",
	}, {
		name: "the run history could not be opened",
		caps: "{ backup_schedule: true }",
		schedule: map[string]any{"every": "1d", "at": "03:00", "runnable": true, "next_run": "2026-09-20T03:00:00Z",
			"history_unavailable": true},
		wantAlarm: true,
		wantWords: "The run history could not be opened.",
	}, {
		// Two sentences on one line: the daemon's refusal reasons end bare,
		// so the note used to run straight on from the reason.
		name: "a refusal and a note on the same line",
		caps: "{ backup_schedule: true }",
		schedule: map[string]any{"every": "1d", "at": "03:00", "runnable": false, "reason": "creating snapshots from the web interface is turned off on this daemon (Snapshot settings page)",
			"last_skipped": map[string]any{"at": "2026-09-19T03:00:00Z", "reason": "no previous snapshot"}},
		wantAlarm: true,
		wantWords: "(Snapshot settings page). A scheduled run did not start.",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sched, err := json.Marshal(tc.schedule)
			if err != nil {
				t.Fatal(err)
			}
			script := renderHarnessJS + `
vm.runInContext("capsCache = ` + tc.caps + `;", ctx);
const find = (n) => { if (!n) return null; if (String(n.className || "").includes("bk-card-state")) return n; for (const c of n.children || []) { const f = find(c); if (f) return f; } return null; };
const cur = { id: "a", name: "a", kind: "registry", baseline_dir: "/var/lib/bintrail/baselines/a" };
const card = vm.runInContext("backupScheduleCard", ctx)(cur, { configured: true, snapshots: [], schedule: ` + string(sched) + ` });
const line = find(card);
console.log(JSON.stringify(line ? { found: true, cls: line.className, text: line.textContent } : { found: false }));
`
			path := filepath.Join(t.TempDir(), "state.js")
			if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
				t.Fatal(err)
			}
			raw, err := exec.Command(node, path, appJS).CombinedOutput()
			if err != nil {
				t.Fatalf("node: %v\n%s", err, raw)
			}
			var got struct {
				Found     bool
				Cls, Text string
			}
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("decode %q: %v", raw, err)
			}
			// find() selects BY the class, so this covers both a state line
			// that stopped being rendered and one whose class was rebuilt by
			// hand (a className rewrite drops the token carrying its gutter).
			// An assertion on the class after this one could never fail.
			if !got.Found {
				t.Fatal("no element carries bk-card-state: the schedule card renders no state line, or its class was rewritten instead of added to")
			}
			if alarm := strings.Contains(got.Cls, "alarm"); alarm != tc.wantAlarm {
				t.Errorf("state line alarm = %v, want %v (class %q, text %q)", alarm, tc.wantAlarm, got.Cls, got.Text)
			}
			if tc.wantWords != "" && !strings.Contains(got.Text, tc.wantWords) {
				t.Errorf("the state line is red but its words do not say why: want %q in %q", tc.wantWords, got.Text)
			}
			if tc.rejectWords != "" && strings.Contains(got.Text, tc.rejectWords) {
				t.Errorf("the state line claims %q, which the body contradicts: %q", tc.rejectWords, got.Text)
			}
			// The line keeps the UI font: .form-msg would jump a one-sentence
			// line to monospace, which style.css already had to undo once.
			if strings.Contains(got.Cls, "form-msg") {
				t.Errorf("the state line borrows .form-msg, which changes the typeface, not only the colour (class %q)", got.Cls)
			}
		})
	}
}

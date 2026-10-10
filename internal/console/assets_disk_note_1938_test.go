package console

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// TestFullReadDiskNote_wireNamesMatchTheFrontend (#1938): the full read's
// disk check reaches the page under the names the Go structs send. The words
// themselves are made in Go and rendered as text; this guards only that the
// two places that show them still read the fields that exist.
func TestFullReadDiskNote_wireNamesMatchTheFrontend(t *testing.T) {
	tag := func(v any, field string) string {
		f, ok := reflect.TypeOf(v).FieldByName(field)
		if !ok {
			t.Fatalf("%T has no field %s", v, field)
		}
		return strings.Split(f.Tag.Get("json"), ",")[0]
	}
	js := readAsset(t, "app.js")
	toast := jsFunctionBody(t, js, "createBaseline")
	for _, name := range []string{tag(BaselineStatus{}, "DiskCheck"), tag(BaselineStatus{}, "DiskNote")} {
		if !strings.Contains(toast, "done."+name) {
			t.Errorf("createBaseline does not read done.%s, so a low disk never reaches the toast", name)
		}
	}
	card := jsFunctionBody(t, js, "backupScheduleCard")
	for _, name := range []string{tag(backupScheduleRunDTO{}, "DiskCheck"), tag(backupScheduleRunDTO{}, "DiskNote")} {
		if !strings.Contains(card, "run."+name) {
			t.Errorf("backupScheduleCard does not read run.%s, so a scheduled read on a low disk says nothing", name)
		}
	}
	detail := jsFunctionBody(t, js, "loadBackupDetail")
	for _, name := range []string{tag(baselineRunDTO{}, "DiskCheck"), tag(baselineRunDTO{}, "DiskNote")} {
		if !strings.Contains(detail, "d.run."+name) {
			t.Errorf("loadBackupDetail does not read d.run.%s, so the snapshot detail loses the disk check", name)
		}
	}
}

// The schedule card's last run carries the check from both of its sources:
// the history record and the loop's own copy of the status.
func TestScheduleRun_carriesTheDiskCheck(t *testing.T) {
	rec := scheduleRunFromRecord(&BaselineRunRecord{Kind: BaselineRunDump, DiskCheck: "low", DiskNote: "Low disk: x"})
	if rec.DiskCheck != "low" || rec.DiskNote != "Low disk: x" {
		t.Fatalf("from the record: %+v", rec)
	}
	st := scheduleRunFromStatus(BackupScheduleState{LastMethod: BackupMethodFull,
		Last: &BaselineStatus{State: "failed", DiskCheck: "unchecked", DiskNote: "Disk check did not run"}})
	if st.DiskCheck != "unchecked" || st.DiskNote != "Disk check did not run" {
		t.Fatalf("from the status: %+v", st)
	}
}

// A read that FIT on a low disk is not a failure, so nothing about it is
// drawn in the error style or put in the toast that never fades. These run
// the real diskNoteLine and diskNoteAfterRead from app.js in node.
func TestFullReadDiskNote_styleFollowsTheVerdict(t *testing.T) {
	raw := runSnapshotFailureJS(t, `
const line = fn("diskNoteLine"), after = fn("diskNoteAfterRead");
const draw = (check, note) => { const n = line(check, note); return n ? { tag: n.tag, cls: n.className, text: n.textContent } : null; };
console.log(JSON.stringify({
  lines: {
    low: draw("low", "Low disk: x. This read may fail with a full disk."),
    tight: draw("tight", "Low disk: x. This read fit. The next one may not."),
    unchecked: draw("unchecked", "Disk check did not run."),
    ok: draw("ok", "Disk check: room."),
    newer: draw("something-newer", "A note from a newer daemon."),
    none: draw("low", ""),
    missing: draw("tight", undefined),
  },
  after: {
    low: after({ disk_check: "low", disk_note: "L" }),
    tight: after({ disk_check: "tight", disk_note: "T" }),
    unchecked: after({ disk_check: "unchecked", disk_note: "U" }),
    ok: after({ disk_check: "ok", disk_note: "O" }),
    newer: after({ disk_check: "something-newer", disk_note: "N" }),
    noNote: after({ disk_check: "low" }),
    nothing: after(null),
  },
  hold: vm.runInContext("TOAST_HOLD_NOTE", ctx),
}));
`)
	type drawnLine struct{ Tag, Cls, Text string }
	type split struct{ Alarm, Said string }
	var got struct {
		Lines map[string]*drawnLine
		After map[string]split
		Hold  int
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, raw)
	}
	for name, want := range map[string]string{
		"low": "form-msg err", "tight": "form-msg warn", "unchecked": "form-hint", "ok": "form-hint",
		// A verdict this page does not know is shown, plainly: never dropped,
		// never guessed to be an alarm.
		"newer": "form-hint",
	} {
		l := got.Lines[name]
		if l == nil || l.Tag != "p" || l.Cls != want || l.Text == "" {
			t.Errorf("%s: drawn as %+v, want a line in %q", name, l, want)
		}
	}
	for _, name := range []string{"none", "missing"} {
		if got.Lines[name] != nil {
			t.Errorf("%s: a line was drawn with no note: %+v", name, got.Lines[name])
		}
	}
	for name, want := range map[string]split{
		"low":       {Alarm: "L"},  // the read may not have fit: with the failure, in the toast that stays
		"tight":     {Said: ". T"}, // it fit: after the outcome's own sentence
		"unchecked": {Said: ". U"},
		"ok":        {}, // nothing to add to "Snapshot complete"
		"newer":     {},
		"noNote":    {},
		"nothing":   {},
	} {
		if got.After[name] != want {
			t.Errorf("%s: %+v, want %+v", name, got.After[name], want)
		}
	}
	// Long enough to read a note of a few hundred characters; the old 2.2 s
	// was not, and "unchecked" notes went by in it.
	if got.Hold < 10000 {
		t.Errorf("a toast with a disk note stays %d ms", got.Hold)
	}
}

// The outcome toasts of a manual read, read from the source: the place of a
// disk note is decided by diskNoteAfterRead alone, the two toasts that can
// carry a "said" note hold long enough to read it, and the warning style has
// its own colour in the stylesheet. (A snapshot that was written and then
// failed to copy keeps its note on the snapshot's own page: that toast is
// about the copy.)
func TestFullReadDiskNote_aReadThatFitIsNotAnErrorToast(t *testing.T) {
	js := readAsset(t, "app.js")
	body := jsFunctionBody(t, js, "createBaseline")
	if strings.Contains(body, "diskNote.said.") || strings.Contains(body, `done.disk_check === "low"`) || strings.Contains(body, `done.disk_check === "unchecked"`) {
		t.Error("createBaseline decides the disk note's place itself instead of through diskNoteAfterRead")
	}
	if n := strings.Count(body, "diskNote.said ? TOAST_HOLD_NOTE : 0"); n != 2 {
		t.Errorf("%d outcome toasts hold for a disk note, want the two that can carry one", n)
	}
	for _, fnName := range []string{"loadBackupDetail", "backupScheduleCard"} {
		b := jsFunctionBody(t, js, fnName)
		if !strings.Contains(b, "diskNoteLine(") || strings.Contains(b, `=== "low" ? "form-msg err"`) {
			t.Errorf("%s styles the disk note itself instead of through diskNoteLine", fnName)
		}
	}
	css := readAsset(t, "style.css")
	if !strings.Contains(css, ".form-msg.warn { color: var(--warn); }") {
		t.Error("the warning style has no rule: a tight disk note would be drawn like plain mono text")
	}
}

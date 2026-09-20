package console

import (
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
func TestBackupsPageDoesNotFoldItsOwnSubject(t *testing.T) {
	js := readAsset(t, "app.js")
	for _, fn := range []string{"backupScheduleCard", "backupRestoreCard"} {
		// Comments stripped: both functions explain in prose why they are no
		// longer folds, and the words "details" and "summary" appear there.
		body := stripJSLineComments(jsFunctionBody(t, js, fn))
		if strings.Contains(body, `el("details"`) || strings.Contains(body, `el("summary"`) {
			t.Errorf("%s builds a fold again; the Backups page's own subject is a card (#1528)", fn)
		}
		if !strings.Contains(body, `el("section", { class: "ov-panel`) {
			t.Errorf("%s no longer builds a panel section, so it does not look like the cards around it", fn)
		}
		if strings.Contains(body, ".open = true") {
			t.Errorf("%s still opens a fold, so one was reintroduced somewhere this guard cannot see", fn)
		}
	}
}

// The alarm survived the unfolding. A failed, skipped or not-runnable
// schedule used to force the fold OPEN; with the card always visible that
// signal has to land somewhere, or a schedule whose last word is a refusal
// reads in the same grey as a healthy one.
func TestBackupScheduleAlarmMarksTheStateLine(t *testing.T) {
	body := stripJSLineComments(jsFunctionBody(t, readAsset(t, "app.js"), "backupScheduleCard"))
	if !strings.Contains(body, `state.className = "form-msg err`) {
		t.Error("a schedule in alarm no longer marks the card's state line, so the alarm is invisible above the fold that used to open (#1528)")
	}
	if !strings.Contains(body, "alarm = true") {
		t.Error("backupScheduleCard no longer raises an alarm at all")
	}
}

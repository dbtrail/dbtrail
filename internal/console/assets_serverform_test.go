package console

import (
	"strings"
	"testing"
)

// TestServerFormAnswersAboveTheButtons pins the add/edit server form's layout
// (#1605, #1608): the message and the startup-check cards are appended BEFORE
// the button row, and the Test button writes its result to its own slot in
// that row. Appended after the row, both landed below the eyeline of the
// button that caused them, at the bottom of a modal the scrim scrolls, and a
// working button read as dead on a fresh install.
func TestServerFormAnswersAboveTheButtons(t *testing.T) {
	js := readAsset(t, "app.js")
	form := jsFunctionBody(t, js, "buildServerForm")
	// LastIndex, and exactly one of each: a second copy appended after the
	// row would pass a first-occurrence check and render below the buttons.
	msg := strings.LastIndex(form, `id: "server-form-msg"`)
	cards := strings.LastIndex(form, `id: "doctor-cards"`)
	foot := strings.Index(form, `class: "modal-foot filter-actions"`)
	if msg < 0 || cards < 0 || foot < 0 {
		t.Fatalf("buildServerForm lost a node: msg=%d cards=%d foot=%d", msg, cards, foot)
	}
	if strings.Count(form, `id: "server-form-msg"`) != 1 || strings.Count(form, `id: "doctor-cards"`) != 1 {
		t.Error("buildServerForm creates the message or the check cards more than once")
	}
	if msg > foot || cards > foot {
		t.Errorf("the form message (%d) and the check cards (%d) are appended after the button row (%d); they render below the buttons again", msg, cards, foot)
	}
	if !strings.Contains(form, `id: "server-test-result"`) {
		t.Error("the button row has no slot for the Test result; the answer goes back below the buttons")
	}
	test := jsFunctionBody(t, js, "testServerForm")
	if !strings.Contains(test, `"server-test-result"`) {
		t.Error("testServerForm does not write to the button-row slot")
	}
	if !strings.Contains(test, "btn.disabled = true") || !strings.Contains(test, "btn.disabled = false") {
		t.Error("testServerForm does not show a busy state on the Test button and restore it")
	}
	// Save stays the retry: nothing in saveServer disables the submit button
	// on a failed check, and the failure scrolls the first failing card into
	// view instead of trusting the operator to find it.
	save := jsFunctionBody(t, js, "saveServer")
	if strings.Contains(save, "disabled = true") {
		t.Error("saveServer disables a control after a failed check; Save is how the checks are run again")
	}
	if !strings.Contains(save, "scrollDoctorIntoView()") {
		t.Error("a failed startup check no longer scrolls its card into view")
	}
	if strings.Contains(save, "items below") {
		t.Error("saveServer still says the checks are below the buttons")
	}
}

// TestServerThatWillNotStreamIsMarked (#1607): the two silent cases (a serve
// console, an entry with no source) put their reason and remedy on the row
// after save, and a source-less entry under a capturing console carries a
// mark on its row. A toast alone lasts seconds and says the opposite.
func TestServerThatWillNotStreamIsMarked(t *testing.T) {
	js := readAsset(t, "app.js")
	why := jsFunctionBody(t, js, "noCaptureReason")
	for _, want := range []string{"!capsCache.monitor", "!s.has_source", "bintrail-console watch", "add one"} {
		if !strings.Contains(why, want) {
			t.Errorf("noCaptureReason lacks %q: one of the two silent cases lost its reason or its remedy", want)
		}
	}
	if !strings.Contains(why, "isLiveMonitorState(s.monitor_state)") {
		t.Error("noCaptureReason does not exempt a server that already streams; an edit of a running server would be marked as never capturing")
	}
	save := jsFunctionBody(t, js, "saveServer")
	if !strings.Contains(save, "noteServerRow(saved.id, why)") {
		t.Error("saveServer no longer writes the never-streams reason on the row")
	}
	row := jsFunctionBody(t, js, "serverRow")
	if !strings.Contains(row, `chip-nosrc`) || !strings.Contains(row, "!s.has_source") {
		t.Error("serverRow carries no mark for a source-less entry under a capturing console")
	}
	css := readAsset(t, "style.css")
	if !strings.Contains(css, ".chip.chip-nosrc") {
		t.Error("style.css has no rule for the no-source chip")
	}
}

// TestIcebergStageCardsSitOnADifferentGround (#1573): the .ice-stage cards
// were --surface-2 on a --surface-2 panel, a measured 1.000 contrast the
// stylesheet's own comment admitted.
func TestIcebergStageCardsSitOnADifferentGround(t *testing.T) {
	css := readAsset(t, "style.css")
	i := strings.Index(css, ".ice-stage {")
	if i < 0 {
		t.Fatal("no .ice-stage rule")
	}
	rule := css[i : strings.Index(css[i:], "}")+i]
	if strings.Contains(rule, "var(--surface-2)") {
		t.Errorf("the .ice-stage card is still --surface-2 on the panel's --surface-2 ground: %s", rule)
	}
}

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
	if !strings.Contains(test, "btn.disabled = true") {
		t.Error("testServerForm does not show a busy state on the Test button")
	}
	// Restored in a finally, or a thrown request leaves Test disabled for the
	// life of the modal; and the error path writes to the slot, not back
	// below the buttons.
	if f := strings.Index(test, "finally {"); f < 0 || strings.Index(test, "btn.disabled = false") < f {
		t.Error("Test's busy state is not restored in a finally; a thrown request leaves the button disabled")
	}
	if !strings.Contains(test, "catch (err) { show(") {
		t.Error("testServerForm's error path does not write to the button-row slot")
	}
	// Save stays the retry: nothing in saveServer disables a control on a
	// failed check; the failed first save re-shows the form FROM THE SAVED
	// ENTRY so the next Save is a PUT of that id, not a second POST the
	// registry refuses as a duplicate name; and the failure scrolls the first
	// failing card into view. Negative checks run over the span, the view
	// that does not fail open on a // inside a string.
	saveSpan := jsFunctionSpan(t, js, "saveServer")
	save := jsFunctionBody(t, js, "saveServer")
	if strings.Contains(saveSpan, "disabled = true") {
		t.Error("saveServer disables a control after a failed check; Save is how the checks are run again")
	}
	if !strings.Contains(save, "scrollDoctorIntoView()") {
		t.Error("a failed startup check no longer scrolls its card into view")
	}
	if strings.Contains(saveSpan, "below") {
		t.Error("saveServer still says the checks are below the buttons")
	}
	sf, rd := strings.Index(save, "showServerForm(saved)"), strings.Index(save, "renderDoctor(res.doctor)")
	if sf < 0 || rd < 0 || sf > rd {
		t.Errorf("a failed first save does not re-show the form from the saved entry before rendering the checks (showServerForm at %d, renderDoctor at %d); Save would POST a duplicate", sf, rd)
	}
	// The row's own Start button path got the same treatment.
	start := jsFunctionSpan(t, js, "startMonitorRow")
	if strings.Contains(start, "below") || !strings.Contains(start, "scrollDoctorIntoView()") {
		t.Error("startMonitorRow still points below the buttons or does not scroll the failing check into view")
	}
}

// TestServerThatWillNotStreamIsMarked (#1607): the two silent cases (a serve
// console, an entry with no source) put their reason and remedy on the row
// after save, and a source-less entry under a capturing console carries a
// mark on its row. A toast alone lasts seconds and says the opposite.
func TestServerThatWillNotStreamIsMarked(t *testing.T) {
	js := readAsset(t, "app.js")
	why := jsFunctionBody(t, js, "noCaptureReason")
	// Each condition and its remedy on the SAME line, so the two cannot be
	// swapped, and in this order: a failed capability read is not serve mode
	// (its remedy is a reload), serve is broader than no-source.
	pairs := []struct{ cond, remedy string }{
		{"!capsKnown", "Reload the page"},
		{"!capsCache.monitor", "bintrail-console watch"},
		{"!s.has_source", "add one"},
	}
	prev := -1
	for _, p := range pairs {
		at := strings.Index(why, p.cond)
		if at < 0 {
			t.Errorf("noCaptureReason lost the %s case", p.cond)
			continue
		}
		line := why[at : at+strings.Index(why[at:], "\n")]
		if !strings.Contains(line, p.remedy) {
			t.Errorf("the %s case lost its remedy %q on its own line: %s", p.cond, p.remedy, line)
		}
		if at < prev {
			t.Errorf("the %s case is tested before the broader one; a serve console with no source would be told to add one", p.cond)
		}
		prev = at
	}
	if !strings.Contains(why, "isLiveMonitorState(s.monitor_state)") {
		t.Error("noCaptureReason does not exempt a server that already streams; an edit of a running server would be marked as never capturing")
	}
	save := jsFunctionBody(t, js, "saveServer")
	if !strings.Contains(save, "noCaptureNotes[saved.id] = why") {
		t.Error("saveServer no longer remembers the never-streams reason for the row")
	}
	if !strings.Contains(save, "toastError(why)") {
		t.Error("saveServer drops the reason when the row cannot be found; a 2-second toast without it is all that is left")
	}
	row := jsFunctionBody(t, js, "serverRow")
	if !strings.Contains(row, "noCaptureNotes[s.id] && noCaptureReason(s)") {
		t.Error("serverRow does not carry the remembered reason on the row's status slot, so a list rebuild erases it")
	}
	chip := strings.Index(row, "chip-nosrc")
	if chip < 0 {
		t.Fatal("serverRow carries no mark for a source-less entry")
	}
	chipLine := row[strings.LastIndex(row[:chip], "\n")+1 : chip]
	for _, operand := range []string{`s.kind !== "ephemeral"`, "capsKnown", "capsCache.monitor", "!s.has_source"} {
		if !strings.Contains(chipLine, operand) {
			t.Errorf("the NO SOURCE mark is not gated on %s: it would show where its remedy is impossible or false", operand)
		}
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
	// The ground the card sits on is --panel-bg, which the studio direction
	// (the one index.html hardcodes) sets to --surface-2. If that ground ever
	// moves to --surface, the card's --surface fill collides again.
	if !strings.Contains(rule, "var(--surface)") {
		t.Errorf("the .ice-stage card does not use the --surface fill: %s", rule)
	}
	if !strings.Contains(css, "--panel-bg: var(--surface-2);") {
		t.Error("no direction sets --panel-bg to --surface-2 any more; re-check the .ice-stage card against its ground")
	}
}

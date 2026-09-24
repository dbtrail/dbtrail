package console

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestServerFormAnswersInACenteredNotice pins where the add/edit server form
// answers (#1769, superseding the in-form placement of #1605/#1608): Save and
// Test open a notice centered on the screen, in its own mount ABOVE #modal,
// so the form under it keeps every typed value. In-form answers landed below
// the eyeline of the button that caused them, and a first save's 18 check
// cards, 14 of them green, hid the one that failed.
func TestServerFormAnswersInACenteredNotice(t *testing.T) {
	js := readAsset(t, "app.js")
	form := jsFunctionBody(t, js, "buildServerForm")
	// The old in-form slots are gone; one summary line stays, above the row.
	for _, gone := range []string{`id: "doctor-cards"`, `id: "server-test-result"`} {
		if strings.Contains(form, gone) {
			t.Errorf("buildServerForm still renders %s: the answer goes back inside the form", gone)
		}
	}
	msg := strings.LastIndex(form, `id: "server-form-msg"`)
	foot := strings.Index(form, `class: "modal-foot filter-actions"`)
	if msg < 0 || foot < 0 || strings.Count(form, `id: "server-form-msg"`) != 1 || msg > foot {
		t.Errorf("the form's summary line must exist once, above the button row (msg=%d foot=%d)", msg, foot)
	}

	test := jsFunctionBody(t, js, "testServerForm")
	if !strings.Contains(test, "openNotice(") {
		t.Error("testServerForm does not answer in the notice")
	}
	// The busy state lives on the button, restored in a finally, or a thrown
	// request leaves Test disabled and reading "Testing" for the life of the
	// dialog.
	if !strings.Contains(test, `btn.disabled = true; btn.textContent = "Testing…";`) {
		t.Error("testServerForm does not show a busy state on the Test button")
	}
	if f := strings.Index(test, "finally {"); f < 0 || !strings.Contains(test[f:], `btn.disabled = false; btn.textContent = "Test connection";`) {
		t.Error("Test's busy state is not restored in a finally")
	}
	if !strings.Contains(test, "catch (err) { show(") {
		t.Error("testServerForm's error path does not reach the notice")
	}

	// Save stays the retry: nothing in saveServer disables a control; a
	// refused save says why in the notice; the failed first save re-shows the
	// form FROM THE SAVED ENTRY (a PUT on the next Save, not a second POST the
	// registry refuses as a duplicate name) BEFORE the notice, which the
	// rebuild would otherwise leave pointing at a detached form.
	saveSpan := jsFunctionSpan(t, js, "saveServer")
	save := jsFunctionBody(t, js, "saveServer")
	if strings.Contains(saveSpan, "disabled = true") {
		t.Error("saveServer disables a control after a failed check; Save is how the checks are run again")
	}
	if !strings.Contains(save, `openNotice({ tone: "err", title: "Could not save"`) {
		t.Error("a refused save no longer says why in the notice")
	}
	sf, out := strings.Index(save, "if (!showServerForm(saved))"), strings.Index(save, "showStartupOutcome(res);")
	if sf < 0 || out < 0 || out < sf || strings.Count(save, "showStartupOutcome(res);") != 1 {
		t.Errorf("the startup outcome must open once, after the form is re-shown from the saved entry (re-show at %d, outcome at %d)", sf, out)
	}
	show := jsFunctionBody(t, js, "showServerForm")
	if !strings.Contains(show, "if (!addWrap || !mountEl) return false;") || !strings.Contains(show, "return true;") {
		t.Error("showServerForm does not report a missing mount")
	}
	// The row's own Start reaches the same notice over the server's form, and
	// a failed request still says so in a lasting toast.
	row := jsFunctionBody(t, js, "startMonitorRow")
	if !strings.Contains(row, "if (opened) showStartupOutcome(res);") || !strings.Contains(row, `toastError("Could not start: " + ((res && res.requestError) || "no answer"))`) {
		t.Error("startMonitorRow lost the notice or the toast for a failed request")
	}

	// Only failures, or only warnings, are on top; every check stays one
	// click away. A warning is not dressed as a failure.
	notice := jsFunctionBody(t, js, "startupNotice")
	if !strings.Contains(notice, `res.started ? warningChecks(checks) : checks.filter((c) => c.status === "fail")`) {
		t.Error("startupNotice no longer puts only the failures (or only the warnings) on top")
	}
	if !strings.Contains(notice, `el("summary", { text: "All "`) {
		t.Error("startupNotice dropped the full list of checks")
	}
	if !strings.Contains(notice, `tone: "warn", title: "Capture started"`) || !strings.Contains(notice, `tone: "err", title: "Capture did not start"`) {
		t.Error("startupNotice lost the warn/fail split in its tone")
	}

	// Escape closes the notice and only it: globalKeydown must take it
	// before the branch that empties #modal, and the error toast yields.
	keys := jsFunctionBody(t, js, "globalKeydown")
	n, modal := strings.Index(keys, "if (noticeOpen())"), strings.Index(keys, `getElementById("modal")`)
	if n < 0 || modal < 0 || n > modal || !strings.Contains(keys, "closeNotice();") {
		t.Errorf("globalKeydown does not close the notice before it can empty #modal (notice at %d, #modal at %d); one Escape would close the form too", n, modal)
	}
	if !strings.Contains(jsFunctionBody(t, js, "toastEscape"), "if (noticeOpen()) return;") {
		t.Error("toastEscape does not yield to the notice")
	}
	// The rest of the page goes inert under the notice and comes back.
	open, closeFn := jsFunctionBody(t, js, "openNotice"), jsFunctionBody(t, js, "closeNotice")
	if !strings.Contains(open, "n.inert = true;") || !strings.Contains(closeFn, "n.inert = false;") {
		t.Error("the notice no longer makes the page under it inert, or never gives it back")
	}

	// The sign-in gate and the notice never share the screen: under a notice
	// the gate would be inert, and Escape stops at the gate's own guard. A
	// 401 on Save opens "Could not save" first and raises the gate after.
	if !strings.Contains(jsFunctionBody(t, js, "showLoginOverlay"), "closeNotice();") {
		t.Error("showLoginOverlay does not close a notice; the sign-in gate would sit inert under it")
	}
	if !strings.Contains(open, "if (!mount || loginGateRaised) return;") {
		t.Error("openNotice opens over a raised sign-in gate")
	}
	// A start request that failed is said as such, with what the server said,
	// on the closed-dialog path too (the old toast from startMonitor is gone).
	if !strings.Contains(save, `if (!res || res.requestError) toastError("Could not start capture for "`) {
		t.Error("with the dialog closed, a failed start request is reported as failed checks")
	}
	if !strings.Contains(notice, "The request to start capture failed: ") || strings.Contains(notice, "did not answer") {
		t.Error("startupNotice says DBTrail did not answer, also when it answered with an error")
	}
	if !strings.Contains(row, "if (!res || res.requestError)") {
		t.Error("startMonitorRow lost its guard for an empty answer")
	}
	// A notice goes over a form only when one is on screen, and a Test answer
	// whose form is gone is dropped rather than shown over another server.
	if !strings.Contains(jsFunctionBody(t, js, "editServer"), "return showServerForm(s);") {
		t.Error("editServer reports a form that may not be on screen")
	}
	if !strings.Contains(test, "if (btn && btn.isConnected) openNotice(") {
		t.Error("a Test answer whose form is gone still opens, over whatever form is there now")
	}
	// Other capture-phase Escape handlers yield to the notice, so one Escape
	// cannot cancel a hidden request or close a calendar behind it.
	for _, fn := range []string{"openBusyModal", "toggleDatePicker"} {
		if !strings.Contains(jsFunctionBody(t, js, fn), "if (noticeOpen()) return;") {
			t.Errorf("%s's Escape handler does not yield to the notice", fn)
		}
	}

	html := readAsset(t, "index.html")
	if !strings.Contains(html, `<div id="notice-mount"></div>`) {
		t.Fatal("index.html has no notice mount")
	}
	css := readAsset(t, "style.css")
	scrim := cssRule(t, css, ".notice-scrim")
	if !strings.Contains(scrim, "align-items: center; justify-content: center;") {
		t.Errorf("the notice is not centered on the screen: %s", scrim)
	}
	zOf := func(rule string) int {
		m := regexp.MustCompile(`z-index:\s*(\d+)`).FindStringSubmatch(rule)
		if m == nil {
			t.Fatalf("no z-index in %s", rule)
		}
		z, _ := strconv.Atoi(m[1])
		return z
	}
	if z := zOf(scrim); z <= zOf(cssRule(t, css, ".modal-scrim")) || z <= zOf(cssRule(t, css, ".cmdk-scrim")) {
		t.Errorf("the notice (z-index %d) does not stack above the servers dialog and the palette", z)
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
	// Logout clears the capability set AND forgets it was read: otherwise a
	// stale capsKnown with an empty cache yields the confident serve reason.
	logout := jsFunctionBody(t, js, "clearAuthState")
	if !strings.Contains(logout, "capsKnown = false;") {
		t.Error("clearAuthState resets capsCache but not capsKnown")
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
	// The gate is one named condition (noSource), shared since #1856 with the
	// Test result's note on the status slot, so the mark and the note cannot
	// disagree about which rows have no source.
	chipLine := row[strings.LastIndex(row[:chip], "\n")+1 : chip]
	if !strings.Contains(chipLine, "if (noSource)") {
		t.Errorf("the NO SOURCE mark is not gated on noSource: %q", chipLine)
	}
	gate := strings.Index(row, "const noSource = ")
	if gate < 0 {
		t.Fatal("serverRow no longer names the no-source condition")
	}
	gateLine := row[gate : strings.Index(row[gate:], "\n")+gate]
	for _, operand := range []string{`s.kind !== "ephemeral"`, "capsKnown", "capsCache.monitor", "!s.has_source"} {
		if !strings.Contains(gateLine, operand) {
			t.Errorf("the NO SOURCE mark is not gated on %s: it would show where its remedy is impossible or false", operand)
		}
	}
	css := readAsset(t, "style.css")
	if !strings.Contains(css, ".chip.chip-nosrc") {
		t.Error("style.css has no rule for the no-source chip")
	}
}

// TestServerFormSectionsCannotOutgrowTheDialog (#1765): the source fieldset
// grew to the width of the grants box's longest line, because a <fieldset>
// defaults to min-inline-size: min-content, and the dialog's overflow-x:
// hidden cut off Source port, Schemas and two S3 fields. This pins the reset
// that stops it; the proof that nothing is cut off is geometric and lives in
// the console e2e ("form: every field fits inside the dialog"), because what
// triggers it is the length of a string in app.js, which no CSS check sees.
func TestServerFormSectionsCannotOutgrowTheDialog(t *testing.T) {
	css := readAsset(t, "style.css")
	if rule := cssRule(t, css, ".form-section"); !strings.Contains(rule, "min-inline-size: 0;") {
		t.Errorf(".form-section lost min-inline-size: 0; a long line in the grants box widens the form past the dialog again: %s", rule)
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

// TestServerFormTestShowsTheStartupChecksForANewServer (#1767): a new server's
// Test comes back with the source half of the startup checks, and the notice
// shows them the way Save's does: failures on top, else warnings, else one
// line, with every check one click away.
func TestServerFormTestShowsTheStartupChecksForANewServer(t *testing.T) {
	js := readAsset(t, "app.js")
	test := jsFunctionBody(t, js, "testServerForm")
	if !strings.Contains(test, "res.doctor ? unsavedTestNotice(res)") {
		t.Error("testServerForm does not show the startup checks a new server's Test returns")
	}
	n := jsFunctionBody(t, js, "unsavedTestNotice")
	f, w := strings.Index(n, "if (fails.length)"), strings.Index(n, "if (warns.length)")
	if f < 0 || w < 0 || f > w {
		t.Errorf("unsavedTestNotice must put failures before warnings (fails at %d, warns at %d)", f, w)
	}
	if !strings.Contains(n, `"Capture cannot start from this database yet.`) || !strings.Contains(n, `el("summary", { text: "All "`) {
		t.Error("unsavedTestNotice lost its failure line or the full list of checks")
	}
	// A failed S3 store keeps the answer red even over a clean database.
	if strings.Count(n, `s3Bad ? "err"`) != 2 {
		t.Error("a failed S3 store no longer turns a clean Test answer red")
	}
}

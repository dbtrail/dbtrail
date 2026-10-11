package console

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// #1527: the Index disk card explained an unmeasurable volume with "The index
// runs on another host or container", which the check never knew and which is
// plainly false on a single-machine install whose index data directory is not
// mounted into the console. The card now reads the branch the doctor landed on
// (free_reason) and says what would make free space measurable. Scoped to the
// two note functions and the card body, so a mention anywhere else in app.js
// cannot satisfy these guards.
func TestCapacityCardNamesWhyFreeSpaceIsUnmeasurable(t *testing.T) {
	js, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	src := string(js)
	note := functionBody(t, src, "function capacityNote(")
	free := functionBody(t, src, "function capacityFreeNote(")
	card := functionBody(t, src, "function diskPanel(")
	// capacityBox is in the sweep too: it is the other place this card's copy
	// is written, and the claim could come back there just as easily.
	box := functionBody(t, src, "function capacityBox(")

	for _, guess := range []string{"another host or container", "runs on another host"} {
		for where, js := range map[string]string{"capacityNote": note, "capacityFreeNote": free, "capacityBox": box} {
			if strings.Contains(js, guess) {
				t.Errorf("%s still asserts a topology the check cannot know: %q", where, guess)
			}
		}
	}

	// One branch per fallback the doctor reports, and the two that name a fix
	// name the same variable and file the bundled stack wires.
	for _, want := range []string{`case "mount_unset"`, `case "mount_unusable"`, `case "index_not_local"`, `case "host_unconfirmed"`, "BINTRAIL_INDEX_DATADIR_RO", "docker-compose.yml"} {
		if !strings.Contains(free, want) {
			t.Errorf("capacityFreeNote is missing %q, so that state stays a dead end", want)
		}
	}

	// The remote branch must offer NO mount fix: a mount that is not the
	// index's would report the wrong volume's free space, which is worse than
	// reporting nothing.
	if strings.Contains(caseBody(free, `case "index_not_local"`), "BINTRAIL_INDEX_DATADIR_RO") {
		t.Error("the index_not_local branch offers a mount fix that would measure the wrong filesystem")
	}
	// The tunnel-shaped state (a local address whose server is not confirmed
	// to be this machine) DOES offer the mount, so it must carry the
	// precondition: a local mysqld's datadir may be sitting right there, and
	// pointing at it would show a measured number for the wrong volume.
	unconfirmed := caseBody(free, `case "host_unconfirmed"`)
	if !strings.Contains(unconfirmed, "BINTRAIL_INDEX_DATADIR_RO") {
		t.Error("the host_unconfirmed branch does not name the mount, so that state stays a dead end")
	}
	for _, want := range []string{"cannot confirm", "and nothing else"} {
		if !strings.Contains(unconfirmed, want) {
			t.Errorf("the host_unconfirmed branch is missing %q: the mount advice must carry its precondition", want)
		}
	}

	// Keyed on free_known, not on the free_unknown grade: the "free on disk"
	// row also reads "not measurable from here" under a fresh index and a
	// short history, and those states need the explanation too.
	if !strings.Contains(free, "cap.free_known") || !strings.Contains(free, "cap.free_reason") {
		t.Error("capacityFreeNote must key on free_known and free_reason")
	}
	if !strings.Contains(card, "capacityFreeNote(") {
		t.Error("diskPanel never renders the free-space reason, so it reaches nobody")
	}
	if strings.Contains(free, "—") {
		t.Error("em dash in card copy")
	}
}

// caseBody returns one switch case's text, from its label to the next case or
// the default. A slice that ran to "default:" would swallow the branches in
// between and let a sibling's wording satisfy a guard about this one.
func caseBody(js, label string) string {
	i := strings.Index(js, label)
	if i < 0 {
		return ""
	}
	rest := js[i+len(label):]
	for _, stop := range []string{"\n    case ", "\n    default:"} {
		if j := strings.Index(rest, stop); j > 0 {
			rest = rest[:j]
		}
	}
	return rest
}

// goFunctionBody returns the text of one top-level Go function, from its
// signature to the start of the next one.
//
// Scope, not a whole-file search: capacity.go holds a SECOND switch over
// capacity constants (capacityCheckResult), so `strings.Contains(src, "case
// X:")` would be satisfied by a case in a function that renders something else
// entirely — a guard passing for an unrelated reason. It Fatals on a rename
// rather than returning "", because an empty haystack would turn every
// assertion below into a failure whose message blames the wrong thing.
func goFunctionBody(t *testing.T, src, signature string) string {
	t.Helper()
	i := strings.Index(src, signature)
	if i < 0 {
		t.Fatalf("%s is gone from the doctor package; this guard covers nothing", signature)
	}
	rest := src[i+len(signature):]
	if j := strings.Index(rest, "\nfunc "); j > 0 {
		rest = rest[:j]
	}
	return rest
}

// A reason value has now been added twice and both times an enumeration of
// them lagged behind (the wire DTO's comment, the card's switch). Derive the
// set from where it is defined instead of restating it: every value in the
// doctor's const block must appear in the DTO comment, and every value that
// means "not measured" must have its own arm in BOTH renderings — the
// console's capacityFreeNote and the CLI's freeUnmeasurableDetail — or that
// state silently falls to a default and loses its fix.
//
// There are three consumers and the CLI one was missed for a full review pass,
// which is the same lag this guard exists to stop. `doctor` prints its own
// text; a reason that reaches it with no arm renders the advice-free fallback
// on a terminal where nobody sees the console card that would have explained it.
//
// The declaration pattern is deliberately loose about spacing. The first draft
// required exactly one space, and a constant added WITHOUT a doc comment above
// it was invisible: gofmt pads such a line to align with its neighbour, and the
// guard skipped it while `len(found) < 4` stayed satisfied by the constants
// that were already there. Every existing constant in that block carries a
// comment, so the conventional addition was caught and only the unconventional
// one slipped — the worst shape for a guard, since it is silent exactly when
// the author was not following the pattern this file teaches. `const X ... = `
// on its own line is matched for the same reason: capacity.go uses that form
// elsewhere in the file.
func TestEveryFreeReasonIsAccountedFor(t *testing.T) {
	src, err := os.ReadFile("../doctor/capacity.go")
	if err != nil {
		t.Fatal(err)
	}
	decl := regexp.MustCompile(`(?m)^(?:\t|const )(Capacity\w+)\s+CapacityFreeReason\s*=\s*"(\w+)"`)
	found := decl.FindAllStringSubmatch(string(src), -1)
	if len(found) < 4 {
		t.Fatalf("found %d CapacityFreeReason constants in the doctor package; this guard covers nothing", len(found))
	}
	dto, err := os.ReadFile("capacity_api.go")
	if err != nil {
		t.Fatal(err)
	}
	js, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	note := functionBody(t, string(js), "function capacityFreeNote(")
	cliArms := goFunctionBody(t, string(src), "func freeUnmeasurableDetail(")

	for _, m := range found {
		name, value := m[1], m[2]
		if !strings.Contains(string(dto), value) {
			t.Errorf("%s (%q) is missing from capacity_api.go: the wire contract's own list of values is stale", name, value)
		}
		// CapacityFreeFrom* are the MEASURED paths; the card shows the number
		// and has nothing to explain, so they need no arm.
		if strings.HasPrefix(name, "CapacityFreeFrom") {
			continue
		}
		// "unknown" is the one value the default arm is FOR: it means the
		// check could not say which state applies, which is also what a
		// missing field from an older backend and an unrecognised value from
		// a newer one mean. Everything else must name itself.
		if value == defaultArmValue {
			continue
		}
		if !strings.Contains(note, `case "`+value+`"`) {
			t.Errorf("%s (%q) has no arm in capacityFreeNote: that state falls to the default and loses its fix", name, value)
		}
		// The CLI renders the same states from the same constants. Matched by
		// NAME, not value: that switch is Go and cases the constant.
		if !strings.Contains(cliArms, "case "+name+":") {
			t.Errorf("%s (%q) has no arm in freeUnmeasurableDetail: `dbtrail doctor` prints the advice-free "+
				"fallback for a state the console explains", name, value)
		}
	}
	if !strings.Contains(note, "default:") {
		t.Fatal("capacityFreeNote has no default arm, so an unrecognised reason renders nothing at all")
	}
	// And the default must stay advice-free: it covers states the console
	// cannot identify, where a mount suggestion could point anywhere.
	if strings.Contains(caseBody(note, "default:"), "BINTRAIL_INDEX_DATADIR_RO") {
		t.Error("the default arm offers a mount fix for a state the console could not identify")
	}
}

// defaultArmValue is the reason whose meaning IS capacityFreeNote's default
// arm; every other unmeasured reason must name itself.
const defaultArmValue = "unknown"

// #2323: the card for an index the rotation loop refuses to drop on. The copy
// has to say nothing is being deleted, where the choice is made, and that
// choosing deletes: the last one is the sentence an operator must read BEFORE
// saving a retention over months of history.
func TestCapacityCardSaysWhenRotationIsWaitingForAChoice(t *testing.T) {
	js, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	src := string(js)
	paused := caseBody(functionBody(t, src, "function capacityBox("), `case "rotation_paused"`)
	for _, want := range []string{"not deleting old history", "nothing is deleted", "Edit rotation", "--rotate-retain", "is deleted on the next rotation", "archive it"} {
		if !strings.Contains(paused, want) {
			t.Errorf("the rotation_paused box lacks %q", want)
		}
	}
	if strings.Contains(paused, "Turn rotation on") {
		t.Error("the rotation_paused box gives the no_retention advice: rotation IS on")
	}
	if !strings.Contains(functionBody(t, src, "function capacityStateText("), `case "rotation_paused"`) {
		t.Error("capacityStateText has no rotation_paused chip, so the state reads as the raw grade")
	}
	card := functionBody(t, src, "function diskPanel(")
	if !strings.Contains(card, "ret.drops_paused") || !strings.Contains(card, "everything, until you choose") {
		t.Error("the keeps-for row shows the window as in force while nothing is dropped on it")
	}
	rot := functionBody(t, src, "function rotationCard(")
	if !strings.Contains(rot, "rot.index_drops_paused") || !strings.Contains(rot, "everything, until you choose") {
		t.Error("the rotation card says this server keeps a window nothing is dropped on")
	}
}

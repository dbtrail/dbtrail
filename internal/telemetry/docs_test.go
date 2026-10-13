package telemetry

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

const telemetryDocPath = "../../docs/TELEMETRY.md"

// section returns the body of a "## <title>" section of the document.
func section(t *testing.T, doc, title string) string {
	t.Helper()
	start := strings.Index(doc, "## "+title+"\n")
	if start < 0 {
		t.Fatalf("TELEMETRY.md has no %q section", title)
	}
	rest := doc[start+len("## "+title+"\n"):]
	if end := strings.Index(rest, "\n## "); end >= 0 {
		return rest[:end]
	}
	return rest
}

func readDoc(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(telemetryDocPath)
	if err != nil {
		t.Fatalf("read TELEMETRY.md: %v", err)
	}
	return string(data)
}

var docFieldRow = regexp.MustCompile("(?m)^\\| `([a-z_]+)` \\|")

// TestDocumentedFieldsMatchTheWireFormat is the anti-drift guard.
//
// TELEMETRY.md is the public promise about what leaves a user's machine. A
// document that quietly falls behind the code is worse than no document,
// because people act on it: someone reads the field table, sees nothing
// alarming, and enables telemetry on a fleet. This test means the table cannot
// fall behind — adding a field to the wire format without documenting it fails
// here.
func TestDocumentedFieldsMatchTheWireFormat(t *testing.T) {
	body := section(t, readDoc(t), "What is sent")

	var documented []string
	for _, m := range docFieldRow.FindAllStringSubmatch(body, -1) {
		documented = append(documented, m[1])
	}
	if len(documented) == 0 {
		t.Fatal("found no field rows in the 'What is sent' table; the table's shape changed and this guard stopped guarding")
	}

	want := slices.Clone(AllowedFields)
	slices.Sort(want)
	got := slices.Clone(documented)
	slices.Sort(got)

	if !slices.Equal(got, want) {
		for _, f := range want {
			if !slices.Contains(got, f) {
				t.Errorf("field %q is on the wire but not documented in TELEMETRY.md", f)
			}
		}
		for _, f := range got {
			if !slices.Contains(want, f) {
				t.Errorf("TELEMETRY.md documents field %q, which is not on the wire", f)
			}
		}
	}
}

// TestDocumentedErrorClassesMatchTheTaxonomy: the document lists the complete
// set of error classes and says it is complete. Keep that true.
func TestDocumentedErrorClassesMatchTheTaxonomy(t *testing.T) {
	body := section(t, readDoc(t), "What is sent")

	// Both directions are checked against the FENCED LIST, as exact tokens:
	// the prose around it also names classes, so a substring search over the
	// whole section would stay green with a class missing from the list that
	// claims to be "exactly these, and nothing else" (#1503 removed three
	// classes nothing emitted; the reverse direction keeps the next one out).
	const marker = "`error_class` is one of exactly these, and nothing else:"
	_, afterMarker, ok := strings.Cut(body, marker)
	if !ok {
		t.Fatal("TELEMETRY.md no longer carries the error_class list marker")
	}
	_, afterFence, ok := strings.Cut(afterMarker, "```")
	if !ok {
		t.Fatal("TELEMETRY.md error_class list is not a fenced block")
	}
	listed, _, ok := strings.Cut(afterFence, "```")
	if !ok {
		t.Fatal("TELEMETRY.md error_class fenced block is not closed")
	}
	documented := map[string]bool{}
	for _, token := range strings.Fields(listed) {
		documented[token] = true
		if !classes[token] {
			t.Errorf("TELEMETRY.md lists error class %q, which nothing emits", token)
		}
	}
	for class := range classes {
		if !documented[class] {
			t.Errorf("error class %q is emitted but not in TELEMETRY.md's list", class)
		}
	}
}

// TestDocumentedDurationBucketsMatch: same, for the duration buckets, which are
// the other closed enumeration a reader is invited to rely on.
func TestDocumentedDurationBucketsMatch(t *testing.T) {
	body := section(t, readDoc(t), "What is sent")

	for _, bucket := range []string{"<100ms", "100ms-1s", "1s-10s", "10s-1m", "1m-10m", ">10m"} {
		if !strings.Contains(body, bucket) {
			t.Errorf("duration bucket %q is emitted but not listed in TELEMETRY.md", bucket)
		}
	}
}

// TestReadmeDisclosesTelemetry guards a specific past mistake: the README used
// to say bintrail "collects nothing — no telemetry, no analytics, no
// phone-home", which stopped being true the day this feature shipped. For a
// tool sold on data sovereignty, a front-page claim that contradicts the
// binary's behaviour is worse than the behaviour itself.
//
// The README must therefore link the telemetry document and state the opt-out
// where a reader looking for it would look.
func TestReadmeDisclosesTelemetry(t *testing.T) {
	data, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	readme := string(data)

	for _, want := range []string{"TELEMETRY.md", "bintrail telemetry off"} {
		if !strings.Contains(readme, want) {
			t.Errorf("README.md does not mention %q; telemetry must be disclosed and its opt-out reachable from the front page", want)
		}
	}
	// The exact phrasing that became false. Catches a revert or a copy-paste
	// from an older revision.
	for _, stale := range []string{"no telemetry, no analytics", "no phone-home"} {
		if strings.Contains(readme, stale) {
			t.Errorf("README.md still claims %q, which is not true of release builds", stale)
		}
	}
}

// TestDocumentedControlsExist keeps the opt-out instructions honest: every
// control the document tells a user to reach for must be one the code actually
// honours. A stale instruction here is the worst kind of documentation bug —
// someone believes they have opted out and has not.
func TestDocumentedControlsExist(t *testing.T) {
	doc := readDoc(t)
	for _, control := range []string{
		"DO_NOT_TRACK=1",
		EnvVar + "=off",
		"bintrail telemetry off",
		"--telemetry=off",
		DebugEnvVar,
		// Not a consent control, but the one setting that decides whether a
		// recorded opt-out survives a container upgrade.
		DirEnvVar,
	} {
		if !strings.Contains(doc, control) {
			t.Errorf("TELEMETRY.md does not mention the %q control", control)
		}
	}
}

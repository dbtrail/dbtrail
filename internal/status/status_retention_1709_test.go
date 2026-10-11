package status

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// The report says which window is in effect AND where it came from: a number
// alone reads as the running default, which is exactly what an index created
// before the default changed does not use (#1709).
func TestWriteStatus_namesTheWindowAndItsSource(t *testing.T) {
	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, nil, nil, nil, nil,
		&RetentionInfo{Raw: "30d", Source: "the retention indexes kept before it was recorded", Basis: "legacy"})
	out := buf.String()
	for _, want := range []string{"Rotation window: 30d", "before it was recorded", "--rotate-retain"} {
		if !strings.Contains(out, want) {
			t.Errorf("report does not carry %q:\n%s", want, out)
		}
	}
}

// Nothing to report is silence, not a made-up window: status reads an index it
// may not be able to ask.
func TestWriteStatus_withoutARetentionSaysNothing(t *testing.T) {
	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, nil, nil, nil, nil)
	if strings.Contains(buf.String(), "Rotation window") {
		t.Errorf("a report with no window reported one:\n%s", buf.String())
	}
}

// JSON consumers grade on the basis, never on the sentence.
func TestWriteStatusJSON_carriesTheBasis(t *testing.T) {
	d := &StatusData{Retention: &RetentionInfo{Raw: "48h", Source: "the retention this index was created under", Basis: "recorded"}}
	var buf bytes.Buffer
	if err := d.WriteJSON(&buf); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	var got struct {
		Retention *RetentionInfo `json:"retention"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Retention == nil || got.Retention.Basis != "recorded" || got.Retention.Raw != "48h" {
		t.Fatalf("retention = %+v, want 48h/recorded", got.Retention)
	}
}

// #2323: the window alone reads as in force. When the rotation loop's upgrade
// guard would refuse to drop on it, the report says so, as a condition on the
// daemon (this command cannot see its flags) and with the way out.
func TestWriteStatus_saysWhenTheWindowIsNotBeingDroppedOn(t *testing.T) {
	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, nil, nil, nil, nil,
		&RetentionInfo{Raw: "30d", Source: "the retention indexes kept before it was recorded", Basis: "legacy",
			NeedsExplicitRetain: true, OldestHistory: "2026-01-15 03:00 UTC"})
	out := buf.String()
	for _, want := range []string{"NOT DROPPED WITHOUT A RETENTION OF YOUR OWN", "2026-01-15 03:00 UTC", "A daemon started with no", "grows", "--rotate-retain", "Edit rotation", "is not affected"} {
		if !strings.Contains(out, want) {
			t.Errorf("report does not carry %q:\n%s", want, out)
		}
	}
	t.Logf("status text:\n%s", out)

	buf.Reset()
	WriteStatus(&buf, nil, nil, nil, nil, nil, nil, &RetentionInfo{Raw: "30d", Source: "x", Basis: "legacy"})
	if strings.Contains(buf.String(), "NOT DROPPED") {
		t.Errorf("an index the loop drops on was reported as paused:\n%s", buf.String())
	}

	d := &StatusData{Retention: &RetentionInfo{Raw: "30d", Source: "x", Basis: "legacy", NeedsExplicitRetain: true, OldestHistory: "2026-01-15 03:00 UTC"}}
	buf.Reset()
	if err := d.WriteJSON(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"needs_explicit_retain": true`) && !strings.Contains(buf.String(), `"needs_explicit_retain":true`) {
		t.Errorf("JSON lacks needs_explicit_retain:\n%s", buf.String())
	}
}

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

package status

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// #1639: when the baseline directory cannot be read in full, the JSON says
// baseline_staleness "unknown"; the text report must say so too instead of
// printing no Baselines section at all.
func TestWrite_baselinesUnavailableIsVisible(t *testing.T) {
	var buf bytes.Buffer
	d := &StatusData{BaselinesUnavailable: true, BaselinesErr: errors.New("a baseline folder from 2026-09-02T06:00:00Z could not be read")}
	d.Write(&buf)
	out := buf.String()
	if !strings.Contains(out, "=== Baselines ===") || !strings.Contains(out, "2026-09-02T06:00:00Z could not be read") || !strings.Contains(out, "NOT evaluated") {
		t.Fatalf("text report:\n%s", out)
	}
}

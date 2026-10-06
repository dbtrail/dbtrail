package reconstruct

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
)

func TestBinlogBaseName_2160(t *testing.T) {
	for in, want := range map[string]string{
		"binlog.000007":       "binlog",
		"binlog.1000000":      "binlog", // the #840 rollover keeps its base name
		"mysql-bin.000001":    "mysql-bin",
		"host.example-bin.03": "host.example-bin",
		"mysqld-bin.000031":   "mysqld-bin",
		"binlog":              "binlog",
		"binlog.":             "binlog.",
		"binlog.0001a":        "binlog.0001a",
		".000001":             "",
		"":                    "",
	} {
		if got := BinlogBaseName(in); got != want {
			t.Errorf("BinlogBaseName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEventMark_roundTrip_2160(t *testing.T) {
	m := EventMark{ID: 42, File: "binlog.000007", End: 200}
	got := ParseEventMark(m.Encode())
	if got == nil || *got != m {
		t.Fatalf("ParseEventMark(Encode(%+v)) = %+v", m, got)
	}
	for _, raw := range []string{"", "not json", `{"id":0,"binlog_file":"binlog.000001","end_pos":4}`, `{"id":3,"end_pos":4}`} {
		if got := ParseEventMark(raw); got != nil {
			t.Errorf("ParseEventMark(%q) = %+v, want no mark", raw, got)
		}
	}
}

func TestStartedOverAfter_2160(t *testing.T) {
	m := &EventMark{ID: 10, File: "binlog.000007", End: 200}
	for _, tc := range []struct {
		name string
		e    indexedEvent
		want bool
	}{
		{"starts where the mark ends", indexedEvent{11, "binlog.000007", 200}, false},
		{"later in the same file", indexedEvent{11, "binlog.000007", 900}, false},
		{"a later file", indexedEvent{11, "binlog.000008", 4}, false},
		{"past the rollover", indexedEvent{11, "binlog.1000000", 4}, false},
		{"the same file, lower: RESET MASTER", indexedEvent{11, "binlog.000007", 150}, true},
		{"binlog.000001 again", indexedEvent{11, "binlog.000001", 300}, true},
		{"a shorter base name", indexedEvent{11, "bin.000001", 4}, true},
		{"a longer base name sorts after, still another numbering", indexedEvent{11, "mysql-bin.000001", 4}, true},
	} {
		if got := startedOverAfter(tc.e, m); got != tc.want {
			t.Errorf("%s: startedOverAfter(%v) = %v, want %v", tc.name, tc.e, got, tc.want)
		}
	}
}

func TestCapturedBackBelow_2160(t *testing.T) {
	gap := &CaptureGap{At: time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC), Detail: "x"}
	anchor := &query.BinlogPos{File: "binlog.000007", Pos: 200}
	below := &query.BinlogPos{File: "binlog.000001", Pos: 500}
	for _, tc := range []struct {
		name   string
		gap    *CaptureGap
		cut    *query.BinlogPos
		anchor *query.BinlogPos
		want   bool
	}{
		{"a loss and capture back below the anchor", gap, below, anchor, true},
		{"no loss", nil, below, anchor, false},
		{"a loss that cannot be evaluated is not a stamped one", &CaptureGap{Unevaluable: true}, below, anchor, false},
		{"capture at the anchor", gap, anchor, anchor, false},
		{"capture past the anchor", gap, &query.BinlogPos{File: "binlog.000008", Pos: 4}, anchor, false},
		{"no cut", gap, nil, anchor, false},
		{"no anchor", gap, below, nil, false},
	} {
		err := capturedBackBelow(tc.gap, tc.anchor, tc.cut)
		if got := errors.Is(err, ErrBinlogRenumbered); got != tc.want {
			t.Errorf("%s: capturedBackBelow = %v, want renumbered %v", tc.name, err, tc.want)
		}
	}
	err := capturedBackBelow(gap, anchor, below)
	for _, want := range []string{"binlog.000001:500", "binlog.000007:200", "2026-10-05T10:00:00Z", "a new full snapshot is needed"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
}

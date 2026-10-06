package reconstruct

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
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
		{"ends where the mark ends: a row of the same rows event", indexedEvent{11, "binlog.000007", 200}, false},
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
	for _, want := range []string{"binlog.000001:500", "binlog.000007:200", "2026-10-05T10:00:00Z", "new full snapshot is needed"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
}

// The refusal reaches the run's summary as a refusal of its own cause, with
// the remedy, never as a capture gap (whose remedy is a flag) nor as a
// schema change.
func TestRefreshOutcomes_aNumberingThatStartedOver_2160(t *testing.T) {
	gap := &CaptureGap{At: time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)}
	err := fmt.Errorf("shop.orders: %w", capturedBackBelow(gap, &query.BinlogPos{File: "binlog.000007", Pos: 200}, &query.BinlogPos{File: "binlog.000001", Pos: 500}))
	out := RefreshOutcomes([]string{"shop.orders"}, nil, []TableFailure{{Schema: "shop", Table: "orders", Err: err}})
	if len(out) != 1 || out[0].Verdict != RefreshVerdictRefused || !out[0].Refused() {
		t.Fatalf("outcome = %+v, want refused", out)
	}
	t.Logf("what the run summary says: %s: %s", out[0].Verdict, out[0].Detail)
	for _, want := range []string{"shop.orders", "started again from another numbering", "new full snapshot is needed"} {
		if !strings.Contains(out[0].Detail, want) {
			t.Errorf("the summary does not say %q: %s", want, out[0].Detail)
		}
	}
	if errors.Is(err, ErrCaptureGap) || errors.Is(err, ErrSchemaChanged) {
		t.Errorf("the refusal reads as another cause: %v", err)
	}
}

// The mark is read from the footer that holds the anchor: a delta chain's
// last pair, not its base.
func TestFetchFloor_theEventMarkGoesWithThePosition_2160(t *testing.T) {
	base := baseline.DumpMetadata{BinlogFile: "binlog.000007", BinlogPos: 200, EventMark: "base-mark"}
	pair := &tableDelta{Meta: baseline.DumpMetadata{BinlogFile: "binlog.000009", BinlogPos: 900, EventMark: "pair-mark"}}
	if _, a := fetchFloor(time.Now(), base, pair); a.BinlogFile != "binlog.000009" || a.EventMark != "pair-mark" {
		t.Fatalf("anchor %s:%d with mark %q, want the pair's position and mark", a.BinlogFile, a.BinlogPos, a.EventMark)
	}
	if _, a := fetchFloor(time.Now(), base, nil); a.EventMark != "base-mark" {
		t.Fatalf("no chain: mark %q, want the base's", a.EventMark)
	}
}

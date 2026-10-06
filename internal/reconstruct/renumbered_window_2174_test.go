package reconstruct

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// #2174: the shim runs the renumbering check on every statement. A lasting
// condition is logged once per process and line, not once per statement.
func TestNoticeOnce_logsEachLineOnce_2174(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	var once NoticeOnce
	// Two handlers (two connections) share the process-wide set.
	a, b := once.To(logger), once.To(logger)
	for range 3 {
		a(slog.LevelWarn, "backfilled", "mark", "m1")
		b(slog.LevelWarn, "backfilled", "mark", "m1")
	}
	a(slog.LevelWarn, "backfilled", "mark", "m2")
	if got := strings.Count(buf.String(), "msg=backfilled"); got != 2 {
		t.Fatalf("logged %d lines, want 2 (one per mark):\n%s", got, buf.String())
	}
}

func TestReadWindow_boundedOnlyWithTableAndUntil_2174(t *testing.T) {
	at := time.Date(2026, 10, 5, 12, 1, 0, 0, time.UTC)
	for _, tc := range []struct {
		w    *ReadWindow
		want bool
	}{
		{nil, false},
		{&ReadWindow{}, false},
		{&ReadWindow{Until: at}, false},
		{&ReadWindow{Schema: "s", Table: "t"}, false},
		{&ReadWindow{Schema: "s", Table: "t", Until: at}, true},
	} {
		if got := tc.w.bounded(); got != tc.want {
			t.Errorf("%+v: bounded = %v, want %v", tc.w, got, tc.want)
		}
	}
	w := &ReadWindow{Schema: "s", Table: "t", Until: at}
	qs, args := w.probes(&EventMark{ID: 7, File: "b.1", End: 2}, at.Add(-time.Hour))
	if len(qs) != 2 || !strings.Contains(qs[0], "ORDER BY event_timestamp ASC, event_id ASC") || !strings.Contains(qs[1], "ORDER BY event_timestamp DESC, event_id DESC") {
		t.Fatalf("probes = %q", qs)
	}
	// The hour after Until's, as TO_SECONDS, so later partitions are pruned.
	if want := "TO_SECONDS(event_timestamp) < 63958424400"; !strings.Contains(qs[0], want) {
		t.Errorf("probe does not prune at %q: %s", want, qs[0])
	}
	if len(args) != 5 || args[0] != "s" || args[1] != "t" || args[2] != uint64(7) || !args[3].(time.Time).Equal(at) || !args[4].(time.Time).Equal(at.Add(-time.Hour-markSlack)) {
		t.Errorf("args = %v", args)
	}
}

func TestParseUnixSeconds_2174(t *testing.T) {
	for _, s := range []string{"1759658400", "1759658400.000000"} {
		got, err := parseUnixSeconds(s)
		if err != nil || !got.Equal(time.Unix(1759658400, 0)) {
			t.Errorf("%q = %v, %v", s, got, err)
		}
	}
	if _, err := parseUnixSeconds("x"); err == nil {
		t.Error(`"x" parsed`)
	}
}

package shim

import (
	"context"
	"errors"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2112: an expensive read that finds the copy busy WAITS for it. It reaches
// MySQL in two cases only, and each is counted under its own reason, apart
// from the copy's faults (copy_refused): the line for the copy was full when
// the statement arrived (16 already waiting), or the statement waited the
// whole 30 seconds and no slot came free. In both MySQL answers, as before;
// only the reason in the tally is new.
func TestRouter_busyCopyHasItsOwnReasons(t *testing.T) {
	cases := []struct {
		name    string
		copyErr error
		want    string
		warnKey string
	}{
		{"the line was full on arrival", sqlsandbox.ErrBusy, "mysql/copy_queue_full", "busy"},
		{"waited the whole 30 s", &sqlsandbox.BusyError{Waited: 30 * time.Second, MaxInFlight: 2}, "mysql/copy_wait_timeout", "busy"},
		// Not a busy copy, and not to be absorbed by the two reasons above:
		// the client left, or the connection's own time cap ran out, while
		// the statement waited. Counted as before.
		{"cancelled while waiting", context.Canceled, "mysql/copy_refused", "copy"},
		{"the connection's cap ran out while waiting", context.DeadlineExceeded, "mysql/copy_refused", "copy"},
		{"a fault of the copy", errors.New("Binder Error"), "mysql/copy_refused", "copy"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &fakeRouter{toCopy: true, reason: "expensive"}
			f := &fakeFreeSQL{err: tc.copyErr, updatedAt: time.Now()}
			var got []string
			h := observedHandler(t, r, f, time.Minute, &got)
			const stmt = "SELECT a, count(*) FROM t GROUP BY a"
			if _, err := h.HandleQuery(stmt); err != nil {
				t.Fatalf("the busy copy reached the client as an error: %v", err)
			}
			if len(got) != 1 || got[0] != tc.want {
				t.Errorf("observed %v, want exactly [%s]", got, tc.want)
			}
			if len(r.forwarded) != 1 || r.forwarded[0] != stmt {
				t.Errorf("forwarded to MySQL = %v, want the statement once", r.forwarded)
			}
			if len(h.routeWarned) != 1 || !h.routeWarned[tc.warnKey] {
				t.Errorf("warned keys = %v, want only %q", h.routeWarned, tc.warnKey)
			}
		})
	}
}

// The same ladder serves a prepared statement's executions.
func TestPreparedRouted_busyCopyHasItsOwnReason(t *testing.T) {
	r := &fakeRouter{toCopy: true}
	f := &fakeFreeSQL{err: sqlsandbox.ErrBusy, updatedAt: time.Now()}
	var got []string
	h := observedHandler(t, r, f, time.Minute, &got)
	const stmt = "SELECT a, count(*) FROM t WHERE b > ? GROUP BY a"
	_, _, ctx, err := h.HandleStmtPrepare(stmt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{int64(1)}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(got) == 0 || got[len(got)-1] != "mysql/copy_queue_full" {
		t.Errorf("observed %v, want the execution under mysql/copy_queue_full", got)
	}
}

// Every reason of the closed vocabulary is listed where a reader looks it
// up: the metric's table in docs/observability.md, the routing section of
// docs/time-travel-sql.md, and the phrases of the "Who answered" block on the
// web interface (ROUTE_REASON_TEXT in app.js). The reasons are read from the
// constants' own source, so one added there and not in those three fails
// here.
func TestRouteReasons_listedEverywhere(t *testing.T) {
	src, err := os.ReadFile("freesql.go")
	if err != nil {
		t.Fatal(err)
	}
	var reasons []string
	for _, m := range regexp.MustCompile(`RouteReason\s*=\s*"([a-z_]+)"`).FindAllStringSubmatch(string(src), -1) {
		reasons = append(reasons, m[1])
	}
	if len(reasons) < 23 || !slices.Contains(reasons, string(RouteReasonCopyQueueFull)) || !slices.Contains(reasons, string(RouteReasonCopyWaitTimeout)) {
		t.Fatalf("read %d reasons from freesql.go (%v): the pattern no longer finds the constants", len(reasons), reasons)
	}
	read := func(path string) string {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	appJS := read("../console/assets/app.js")
	start := strings.Index(appJS, "const ROUTE_REASON_TEXT = {")
	if start < 0 {
		t.Fatal("app.js: ROUTE_REASON_TEXT not found")
	}
	phrases := appJS[start : start+strings.Index(appJS[start:], "\n};")]
	observability, timeTravel := read("../../docs/observability.md"), read("../../docs/time-travel-sql.md")
	for _, r := range reasons {
		if !strings.Contains(phrases, "\n  "+r+": \"") {
			t.Errorf("app.js: ROUTE_REASON_TEXT has no phrase for %q", r)
		}
		if !strings.Contains(observability, "`"+r+"`") {
			t.Errorf("docs/observability.md does not name `%s`", r)
		}
		if !strings.Contains(timeTravel, "`"+r+"`") {
			t.Errorf("docs/time-travel-sql.md does not name `%s`", r)
		}
	}
}

package consoleapp

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/readrouter"
)

// TestValidateRoutePolicy: routing on with both thresholds at 0 can never
// route a statement, so the daemon refuses to start that way instead of
// filling the tally with cheap_plan; routing off accepts any thresholds.
func TestValidateRoutePolicy(t *testing.T) {
	cases := []struct {
		name    string
		maxAge  time.Duration
		cost    float64
		rows    int64
		wantErr bool
	}{
		{"off, thresholds zero", 0, 0, 0, false},
		{"on, defaults", time.Hour, 10000, 100000, false},
		{"on, cost rule only", time.Hour, 10000, 0, false},
		{"on, scan rule only", time.Hour, 0, 2, false},
		{"on, nothing could route", time.Hour, 0, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRoutePolicy(tc.maxAge, tc.cost, tc.rows)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestValidateRouteReadOnly: read-only mode guards what read routing sends
// to the source, so asking for it without routing (or without the port) is a
// misconfiguration the daemon names at startup instead of ignoring.
func TestValidateRouteReadOnly(t *testing.T) {
	cases := []struct {
		name     string
		readOnly bool
		listen   string
		maxAge   time.Duration
		wantErr  string
	}{
		{"off, nothing set", false, "", 0, ""},
		{"off, routing on", false, "127.0.0.1:3308", time.Hour, ""},
		{"on, routing on", true, "127.0.0.1:3308", time.Hour, ""},
		{"on, port on, routing off", true, "127.0.0.1:3308", 0, "--route-max-copy-age"},
		{"on, no port", true, "", time.Hour, "--flashback-listen"},
		{"on, nothing else", true, "", 0, "--flashback-listen"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRouteReadOnly(tc.readOnly, tc.listen, tc.maxAge)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("err = %v, want none", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), "--route-read-only") {
				t.Fatalf("err = %v, want one naming --route-read-only and %s", err, tc.wantErr)
			}
			t.Log(err)
		})
	}
}

// TestRouteStartupLine: the line printed at startup says which mode the
// port is in, and only the read-write one says the token can write.
func TestRouteStartupLine(t *testing.T) {
	cfg := flashbackConfig{RouteMaxCopyAge: 15 * time.Minute, RoutePolicy: readrouter.DefaultPolicy()}
	rw := routeStartupLine(cfg)
	cfg.RouteReadOnly = true
	ro := routeStartupLine(cfg)
	t.Log(rw)
	t.Log(ro)
	for _, want := range []string{"read-write", "writes included", "--route-read-only", "plan cost >= 10000", "15m0s"} {
		if !strings.Contains(rw, want) {
			t.Errorf("read-write line misses %q: %s", want, rw)
		}
	}
	for _, want := range []string{"read-only (--route-read-only)", "refused and never sent to the source", "stored function", "plan cost >= 10000"} {
		if !strings.Contains(ro, want) {
			t.Errorf("read-only line misses %q: %s", want, ro)
		}
	}
	if strings.Contains(ro, "writes included") || strings.Contains(ro, "read-write") {
		t.Errorf("read-only line still promises writes: %s", ro)
	}
	for _, line := range []string{rw, ro} {
		if strings.Contains(line, "—") || !strings.HasSuffix(line, "\n") {
			t.Errorf("line holds an em dash or lacks its newline: %q", line)
		}
	}
}

package consoleapp

import (
	"testing"
	"time"
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

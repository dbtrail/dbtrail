package cli

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
)

// --fail-on-gap is an alarm: an acknowledged loss passes it (as a cleared
// record did), an unacknowledged one fails it, and everything that could not
// be confirmed still fails closed.
func TestContinuityFailure(t *testing.T) {
	lost := sql.NullTime{Time: time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC), Valid: true}
	at := func(d time.Duration) sql.NullTime { return sql.NullTime{Time: lost.Time.Add(d), Valid: true} }
	cases := []struct {
		name    string
		stream  *status.StreamStateInfo
		err     error
		wantErr string // "" = passes
	}{
		{"clean", &status.StreamStateInfo{GapColumnsPresent: true}, nil, ""},
		{"unacknowledged loss", &status.StreamStateInfo{GapColumnsPresent: true, GapLostAt: lost}, nil, "events permanently lost (gap detected at 2026-11-03 10:00:00)"},
		{"acknowledged loss", &status.StreamStateInfo{GapColumnsPresent: true, GapLostAt: lost, GapAckedAt: at(time.Hour)}, nil, ""},
		{"acknowledged in the same second", &status.StreamStateInfo{GapColumnsPresent: true, GapLostAt: lost, GapAckedAt: lost}, nil, ""},
		{"acknowledgement older than the loss", &status.StreamStateInfo{GapColumnsPresent: true, GapLostAt: lost, GapAckedAt: at(-time.Hour)}, nil, "events permanently lost"},
		{"acknowledgement with no loss", &status.StreamStateInfo{GapColumnsPresent: true, GapAckedAt: lost}, nil, ""},
		{"legacy index", &status.StreamStateInfo{}, nil, "legacy index missing gap-detection columns"},
		{"no stream row", nil, nil, "no stream state loaded"},
		{"unreadable", nil, errors.New("conn reset"), "could not read stream state"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := continuityFailure(c.stream, c.err)
			if err != nil {
				t.Logf("--fail-on-gap says: %v", err)
			}
			switch {
			case c.wantErr == "" && err != nil:
				t.Errorf("want pass, got %v", err)
			case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
				t.Errorf("want an error containing %q, got %v", c.wantErr, err)
			}
		})
	}
}

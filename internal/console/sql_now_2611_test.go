package console

import "testing"

// #2611: under a named zone the now-family is MySQL's; UTC and fixed offsets
// stay on the copy, and a statement without a call is not refused for it.
func TestSQLNowRefusalFor(t *testing.T) {
	for _, c := range []struct {
		stmt, zone string
		refused    bool
	}{
		{"SELECT * FROM t WHERE ts >= NOW() - INTERVAL 1 DAY", "", false},
		{"SELECT * FROM t WHERE ts >= NOW() - INTERVAL 1 DAY", "Etc/GMT+5", false},
		{"SELECT * FROM t WHERE ts >= NOW() - INTERVAL 1 DAY", "America/New_York", true},
		{"SELECT curdate ( )", "America/Bogota", true},
		{"SELECT UTC_DATE()", "Europe/Madrid", true},
		{"SELECT * FROM t", "America/New_York", false},
		{"SELECT `now` FROM t", "America/New_York", false},
	} {
		if got := sqlNowRefusalFor(c.stmt, c.zone); (got != "") != c.refused {
			t.Errorf("sqlNowRefusalFor(%q, %q) = %q, want refused=%v", c.stmt, c.zone, got, c.refused)
		}
	}
}

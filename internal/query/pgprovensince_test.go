package query

import (
	"testing"
	"time"
)

// The time bound a fetch under a position sends to the index: the coarse
// floor (the start of the hour, less one more), except for a PostgreSQL
// anchor whose Since is proven, which sends the exact instant.
func TestBuildQuery_sinceUnderAPosition(t *testing.T) {
	since := time.Date(2026, 11, 18, 15, 25, 31, 0, time.UTC)
	coarse := CoarseSinceFloor(since)
	if !coarse.Before(since) || since.Sub(coarse) < time.Hour {
		t.Fatalf("the coarse floor of %v is %v", since, coarse)
	}
	lsn := &BinlogPos{File: "7/B49BCD00", Pos: 0x7B49BCD00}
	binlog := &BinlogPos{File: "binlog.000123", Pos: 4096}
	for name, c := range map[string]struct {
		opts Options
		want time.Time
	}{
		"PostgreSQL, proven":               {Options{Schema: "public", Table: "t", Since: &since, SincePos: lsn}.SinceProven(), since},
		"PostgreSQL, not proven":           {Options{Schema: "public", Table: "t", Since: &since, SincePos: lsn}, coarse},
		"MySQL":                            {Options{Schema: "s", Table: "t", Since: &since, SincePos: binlog}, coarse},
		"MySQL, settled by its caller":     {Options{Schema: "s", Table: "t", Since: &since, SincePos: binlog}.SearchesBelowItsOwnFloor(), coarse},
		"MySQL, marked proven all the same":{Options{Schema: "s", Table: "t", Since: &since, SincePos: binlog}.SinceProven(), coarse},
	} {
		_, args := buildQuery(c.opts)
		var got []time.Time
		for _, a := range args {
			if ts, ok := a.(time.Time); ok {
				got = append(got, ts)
			}
		}
		if len(got) != 1 || !got[0].Equal(c.want) {
			t.Errorf("%s: the times sent are %v, want %v alone", name, got, c.want)
		}
	}
}

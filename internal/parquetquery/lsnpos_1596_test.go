package parquetquery

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
)

// The archive leg bounds a PostgreSQL fetch by the LSN number, like the live
// one (#1596): the file-name rule would order "1/0" before "0/16B3748".
func TestBuildFilters_LSNBounds(t *testing.T) {
	since := time.Date(2026, 10, 17, 10, 30, 0, 0, time.UTC)
	where, args := buildFilters(query.Options{Schema: "s", Table: "t", Since: &since,
		SincePos: &query.BinlogPos{File: "0/16B3748", Pos: 0x16B3748},
		UntilPos: &query.BinlogPos{File: "1/0", Pos: 1 << 32}}, nil)
	q := strings.Join(where, " AND ")
	if strings.Contains(q, "length(binlog_file)") {
		t.Errorf("an LSN bound compares binlog_file text: %s", q)
	}
	for _, want := range []string{"start_pos >= ?", "end_pos <= ?", "event_timestamp >= ?"} {
		if !strings.Contains(q, want) {
			t.Errorf("filters lack %q: %s", want, q)
		}
	}
	found := map[any]bool{}
	for _, a := range args {
		found[a] = true
	}
	if !found[posArg(0x16B3748)] || !found[posArg(1<<32)] {
		t.Errorf("args %v lack the two LSN numbers", args)
	}
}

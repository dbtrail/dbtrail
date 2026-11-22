package reconstruct

import (
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
)

// A PostgreSQL table capture dropped changes of since its rows were read
// from the source is not covered, so nothing built from the index presents
// its old rows as current. Positions decide where both are known; the clock,
// with a margin, only where they are not.
func TestPGSkippedSince(t *testing.T) {
	const readLSN = 1000
	read := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	ledger := func(raw string) *status.StreamStateInfo {
		return &status.StreamStateInfo{CaptureSkips: sql.NullString{String: raw, Valid: raw != "NULL"}}
	}
	entry := func(pos int, at string, tables string) string {
		return `{"replica_identity_not_full":{"count":3,"last_at":"` + at + `","last_pos":` + strconv.Itoa(pos) + tables + `,"last_detail":"replica identity 'd'"}}`
	}
	const onlyT = `,"tables":["public.t"]`
	const later, earlier, withinMargin = "2026-11-21T12:00:05Z", "2026-11-21T11:00:00Z", "2026-11-21T11:57:00Z"
	for _, tc := range []struct {
		name   string
		st     *status.StreamStateInfo
		lsn    uint64
		read   time.Time
		refuse string
	}{
		{name: "no capture state", st: nil, lsn: readLSN, read: read},
		{name: "a NULL ledger", st: ledger("NULL"), lsn: readLSN, read: read},
		{name: "an empty ledger", st: ledger(" "), lsn: readLSN, read: read},
		{name: "a ledger that does not parse", st: ledger("{"), lsn: readLSN, read: read, refuse: "not readable"},
		{name: "a drop after the rows, by position", st: ledger(entry(1500, earlier, onlyT)), lsn: readLSN, read: read,
			refuse: "capture dropped changes of public.t (replica_identity_not_full) since its rows were read from the source: replica identity 'd'"},
		{name: "a drop at the rows' floor", st: ledger(entry(1000, earlier, onlyT)), lsn: readLSN, read: read, refuse: "public.t"},
		// A daemon whose clock is behind the server's dates a new drop
		// before the read: the position still says it is after.
		{name: "a new drop dated before the read by a clock behind", st: ledger(entry(1500, earlier, onlyT)), lsn: readLSN, read: read, refuse: "public.t"},
		// A capture that was behind records an old drop after the read: the
		// position says the rows already hold it.
		{name: "an old drop recorded late by a capture behind", st: ledger(entry(900, later, onlyT)), lsn: readLSN, read: read},
		{name: "no position for the rows: the clock, after", st: ledger(entry(900, later, onlyT)), read: read, refuse: "public.t"},
		{name: "no position for the rows: the clock, within the margin", st: ledger(entry(900, withinMargin, onlyT)), read: read, refuse: "public.t"},
		{name: "no position for the rows: the clock, well before", st: ledger(entry(900, earlier, onlyT)), read: read},
		{name: "no position for the drop: the clock", st: ledger(`{"r":{"count":1,"last_at":"` + later + `"` + onlyT + `}}`), lsn: readLSN, read: read, refuse: "public.t"},
		{name: "neither can be compared", st: ledger(`{"r":{"count":1` + onlyT + `}}`), refuse: "public.t"},
		{name: "a drop of another table", st: ledger(entry(1500, later, `,"tables":["public.other"]`)), lsn: readLSN, read: read},
		{name: "names that only look like the table", st: ledger(entry(1500, later, `,"tables":["public.t2","public.T"]`)), lsn: readLSN, read: read},
		{name: "a list cut short", st: ledger(entry(1500, later, `,"tables":["public.other"],"tables_truncated":true`)), lsn: readLSN, read: read, refuse: "public.t"},
		{name: "an entry that names no table", st: ledger(entry(1500, later, "")), lsn: readLSN, read: read, refuse: "public.t"},
		{name: "a count of zero", st: ledger(`{"r":{"count":0,"last_pos":1500` + onlyT + `}}`), lsn: readLSN, read: read},
		{name: "another table's detail is not told", st: ledger(entry(1500, later, `,"tables":["public.t","public.u"]`)), lsn: readLSN, read: read,
			refuse: "capture dropped changes of public.t (replica_identity_not_full) since its rows were read from the source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := PGSkippedSince(tc.st, tc.lsn, tc.read, "public", "t")
			if tc.refuse == "" && got != "" || tc.refuse != "" && !strings.Contains(got, tc.refuse) {
				t.Errorf("got %q, want %q", got, tc.refuse)
			}
			if strings.HasSuffix(tc.name, "is not told") && strings.Contains(got, "replica identity 'd'") {
				t.Errorf("another table's detail must not be told as this one's: %q", got)
			}
		})
	}
}

// The refusal is a capture gap: the refresh reports it as such and the
// schedule reads the source in its place.
func TestPGSkippedError_IsACaptureGap(t *testing.T) {
	err := pgSkippedError("capture dropped changes of public.t")
	if !errors.Is(err, ErrCaptureGap) || !strings.Contains(err.Error(), "needs a full read") {
		t.Errorf("got %v", err)
	}
}

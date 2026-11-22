package reconstruct

import (
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
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
		// position says the rows already hold it. Valid for a full read's
		// file alone, the only one PGReadPoint gives a position for.
		{name: "an old drop recorded late by a capture behind, over a full read", st: ledger(entry(900, later, onlyT)), lsn: readLSN, read: read},
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

// A position is compared only for a full read's file. A fold's floor is the
// newest change the index held when it ran, and capture saves the ledger
// after the rows, with its checkpoint: a fold that read a ledger without a
// drop still to be saved has a floor past it. The same shape is a copy
// folded by 0.137.0 or 0.138.0 over dropped changes. Against the last real
// read, which the fold keeps, the drop refuses.
func TestPGReadPoint(t *testing.T) {
	dumped := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	full := baseline.DumpMetadata{LSN: 1000, LastDumpAt: dumped, FoldGeneration: 0}
	folded := baseline.DumpMetadata{LSN: 3000, LastDumpAt: dumped, FoldGeneration: 1}
	unknown := baseline.DumpMetadata{LSN: 3000, FoldGeneration: -1}
	if lsn, at := PGReadPoint(full); lsn != 1000 || !at.Equal(dumped) {
		t.Errorf("a full read: %d %v", lsn, at)
	}
	if lsn, at := PGReadPoint(folded); lsn != 0 || !at.Equal(dumped) {
		t.Errorf("a fold: %d %v, want no position and the last read", lsn, at)
	}
	if lsn, at := PGReadPoint(unknown); lsn != 0 || !at.IsZero() {
		t.Errorf("nothing on record: %d %v", lsn, at)
	}

	// The drop at LSN 2000, saved ten minutes after the read; the fold's
	// floor 3000 is past it.
	st := &status.StreamStateInfo{CaptureSkips: sql.NullString{Valid: true,
		String: `{"replica_identity_not_full":{"count":1,"last_at":"2026-11-21T12:10:00Z","last_pos":2000,"tables":["public.t"]}}`}}
	for name, md := range map[string]baseline.DumpMetadata{
		"a fold that ran before the drop was saved": folded,
		"a copy folded by 0.138.0 over the drop":    {LSN: 5000, LastDumpAt: dumped, FoldGeneration: 3},
	} {
		lsn, at := PGReadPoint(md)
		if why := PGSkippedSince(st, lsn, at, "public", "t"); why == "" {
			t.Errorf("%s: read as covered", name)
		}
	}
	// A full read taken after the drop is covered.
	lsn, at := PGReadPoint(baseline.DumpMetadata{LSN: 2500, LastDumpAt: dumped.Add(time.Hour), FoldGeneration: 0})
	if why := PGSkippedSince(st, lsn, at, "public", "t"); why != "" {
		t.Errorf("a full read after the drop: %q", why)
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

// A table published as it was because capture dropped its changes reads as
// not covered in the refresh's outcomes, never as unchanged, and does not
// stop the run.
func TestRefreshOutcomes_notCovered(t *testing.T) {
	out := RefreshOutcomes([]string{"public.t", "public.u"}, []*TableReport{
		{Schema: "public", Table: "t", CarriedForward: true, NotCovered: "capture dropped changes of public.t"},
		{Schema: "public", Table: "u", CarriedForward: true},
	}, nil)
	if out[0].Verdict != RefreshVerdictNotCovered || out[0].Refused() || !strings.Contains(out[0].Detail, "not brought up to date: capture dropped changes of public.t") {
		t.Errorf("t: %+v", out[0])
	}
	if out[1].Verdict != RefreshVerdictUnchanged {
		t.Errorf("u: %+v", out[1])
	}
}

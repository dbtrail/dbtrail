package reconstruct

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
)

// A PostgreSQL table capture dropped changes of since its rows were read
// from the source is refused, so no snapshot built from the index presents
// its old rows as current.
func TestPGSkippedSince(t *testing.T) {
	read := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	ledger := func(raw string) *status.StreamStateInfo {
		return &status.StreamStateInfo{CaptureSkips: sql.NullString{String: raw, Valid: raw != "NULL"}}
	}
	const after = `"last_at":"2026-11-21T12:00:05Z"`
	const before = `"last_at":"2026-11-21T11:59:00Z"`
	for _, tc := range []struct {
		name   string
		st     *status.StreamStateInfo
		read   time.Time
		refuse string
	}{
		{name: "no capture state", st: nil, read: read},
		{name: "a NULL ledger", st: ledger("NULL"), read: read},
		{name: "an empty ledger", st: ledger(" "), read: read},
		{name: "a ledger that does not parse", st: ledger("{"), read: read, refuse: "not readable"},
		{name: "a drop of the table after the read", st: ledger(`{"replica_identity_not_full":{"count":3,` + after + `,"tables":["public.t"],"last_detail":"replica identity 'd'"}}`), read: read,
			refuse: "capture dropped changes of public.t (replica_identity_not_full) since its rows were read from the source: replica identity 'd'"},
		{name: "a drop at the very instant of the read", st: ledger(`{"replica_identity_not_full":{"count":1,"last_at":"2026-11-21T12:00:00Z","tables":["public.t"]}}`), read: read, refuse: "public.t"},
		{name: "a drop of the table before the read", st: ledger(`{"replica_identity_not_full":{"count":3,` + before + `,"tables":["public.t"]}}`), read: read},
		{name: "a drop of another table", st: ledger(`{"replica_identity_not_full":{"count":3,` + after + `,"tables":["public.other"]}}`), read: read},
		{name: "a name that only starts like the table", st: ledger(`{"replica_identity_not_full":{"count":3,` + after + `,"tables":["public.t2","public.T"]}}`), read: read},
		{name: "a list cut short", st: ledger(`{"primary_key_deferrable":{"count":3,` + after + `,"tables":["public.other"],"tables_truncated":true}}`), read: read, refuse: "primary_key_deferrable"},
		{name: "an entry that names no table", st: ledger(`{"replica_identity_not_full":{"count":3,` + after + `}}`), read: read, refuse: "public.t"},
		{name: "a count of zero", st: ledger(`{"replica_identity_not_full":{"count":0,` + after + `,"tables":["public.t"]}}`), read: read},
		{name: "a read not known", st: ledger(`{"replica_identity_not_full":{"count":1,` + before + `,"tables":["public.t"]}}`), refuse: "public.t"},
		{name: "a detail not given when the entry names other tables too", st: ledger(`{"replica_identity_not_full":{"count":3,` + after + `,"tables":["public.t","public.u"],"last_detail":"of public.u"}}`), read: read,
			refuse: "capture dropped changes of public.t (replica_identity_not_full) since its rows were read from the source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := pgSkippedSince(tc.st, tc.read, "public", "t")
			if tc.refuse == "" && got != "" || tc.refuse != "" && !strings.Contains(got, tc.refuse) {
				t.Errorf("got %q, want %q", got, tc.refuse)
			}
			if strings.Contains(got, "of public.u") {
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

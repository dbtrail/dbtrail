package console

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

func TestPGNeedTails(t *testing.T) {
	if pgNeedTails(context.Background()) {
		t.Error("an unmarked context needs its tails")
	}
	ctx := WithPGNeedTails(context.Background())
	if !pgNeedTails(ctx) {
		t.Error("a marked context does not")
	}
	child, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if !pgNeedTails(child) {
		t.Error("a context derived from a marked one does not")
	}
	if pgNoOlderTails(ctx) {
		t.Error("marked for the snapshot alone, it is marked for older changes too")
	}
	older := WithPGNoOlderTails(context.Background())
	if !pgNoOlderTails(older) || pgNeedTails(older) || !PGNoOlderTails(older) {
		t.Error("WithPGNoOlderTails does not mark older changes alone")
	}
}

// #3116: what an earlier answer of the copy read is kept per connection
// (SQLOnCopy.seen), and each connection of a port gets its own SQLOnCopy, so
// a new connection is held to nothing another one was shown.
func TestSQLOnCopyForStartsEachConnectionClean_3116(t *testing.T) {
	s := &Server{sqlRunner: &fakeSQLRunner{}}
	b := &bundle{}
	first, why := s.sqlOnCopyFor(b, "x")
	if first == nil {
		t.Fatalf("no SQL on the copy: %s", why)
	}
	first.seen.Store(42)
	second, _ := s.sqlOnCopyFor(b, "x")
	if second == first {
		t.Fatal("two connections share one SQLOnCopy")
	}
	if got := second.seen.Load(); got != 0 {
		t.Errorf("a new connection starts with the floor %d, want 0", got)
	}
}

// #3116: the three ways a PostgreSQL statement in fresh mode was read
// without its recent changes with nothing said now give a reason, and only
// those.
func TestPGTailsUnsaid_3116(t *testing.T) {
	a := views.BaselineTable{Schema: "public", Table: "a"}
	b := views.BaselineTable{Schema: "public", Table: "b"}
	two := []views.BaselineTable{a, b}
	for _, tc := range []struct {
		name       string
		known      bool
		tail, read []views.BaselineTable
		tailed     bool
		stmt       string
		want       string // a part of the reason; "" for none
	}{
		{"every table with its changes", true, two, two, true, "SELECT 1 FROM a", ""},
		{"a table with no tail now, or tails stopped over their budget", true, two, two, false, "SELECT 1 FROM a, b", "(public.a, public.b) cannot all be laid over"},
		{"tables not told from the text", false, nil, two, false, "SELECT 1 FROM café", "could not be told from its text"},
		{"no table of the copy", true, nil, nil, false, "SELECT 1", ""},
		{"not told, and no table read", false, nil, nil, false, "SELECT 1", ""},
		{"the change log", true, two, two, false, "SELECT * FROM events", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := pgTailsUnsaid(tc.known, tc.tail, tc.read, tc.tailed, tc.stmt)
			if (tc.want == "") != (got == "") || !strings.Contains(got, tc.want) {
				t.Errorf("%q, want %q", got, tc.want)
			}
		})
	}
}

// The kinds a heavy statement's older answer is counted under are read from
// the copy's own notes.
func TestPGOlderKind_3116(t *testing.T) {
	at := time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct{ note, want string }{
		{freshSnapshotNote("the newest captured changes of public.a are not ready yet", at), PGOlderTailsNotReady},
		{freshSnapshotNote("x", time.Time{}), PGOlderTailsNotReady},
		{"Answered from captured changes older than an earlier answer of the copy on this connection: the copy's newest version is behind it.", PGOlderThanEarlier},
		{snapshotAloneNote, ""},
		{"", ""},
	} {
		if got := PGOlderKind(tc.note); got != tc.want {
			t.Errorf("%q: %q, want %q", tc.note, got, tc.want)
		}
	}
}

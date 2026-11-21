package consoleapp

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// The rows SHOW dbtrail.router_status answers on the PostgreSQL port (#3021).

func statusText(rows [][2]string) string {
	var b strings.Builder
	for _, r := range rows {
		b.WriteString(r[0] + "=" + r[1] + "\n")
	}
	return b.String()
}

func TestPGPortStatusRows(t *testing.T) {
	now := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	base := pgPortStatusInput{Server: "7f4d577430b48821", Mode: "fresh", Snapshot: now.Add(-90 * time.Minute), Now: now, Aged: true}
	counts := func(in pgPortStatusInput) pgPortStatusInput {
		in.Counted, in.Since = true, now.Add(-2*time.Hour)
		in.Copy, in.Source, in.Refused = 110, 12, 2
		in.Reasons = map[string]uint64{pgshim.ReasonVeto: 12, pgshim.ReasonAnswersTheSame: 110, pgshim.ReasonReadOnly: 2}
		return in
	}
	for _, tc := range []struct {
		what string
		in   pgPortStatusInput
		want string
	}{
		{"the first statement of a connection", counts(base), `server=7f4d577430b48821
forwarding=on
sql_mode=fresh
copy_snapshot=2026-11-20T13:30:00Z
copy_age_seconds=5400
last_answer=none
counting_since=2026-11-20T13:00:00Z
statements_copy=110
statements_source=12
statements_refused=2
reason_answers_the_same=110
reason_read_only=2
reason_veto=12
`},
		{"the copy answered, with its captured changes", func() pgPortStatusInput {
			in := base
			in.Last = pgshim.LastAnswer{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame}
			in.Age = console.CopyRunAge{Ran: now, Through: now.Add(-1200 * time.Millisecond), Tailed: true}
			return in
		}(), `server=7f4d577430b48821
forwarding=on
sql_mode=fresh
copy_snapshot=2026-11-20T13:30:00Z
copy_age_seconds=5400
last_answer=copy
last_answer_reason=answers_the_same
last_answer_reason_means=everything in the statement answers on the copy as on PostgreSQL
last_answer_error=no
last_answer_indexed_through=2026-11-20T14:59:58Z
last_answer_age_seconds=2
`},
		{"the copy answered from its snapshot alone", func() pgPortStatusInput {
			in := base
			in.Mode = "fast"
			in.Last = pgshim.LastAnswer{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame}
			in.Age = console.CopyRunAge{Ran: now, Snapshot: true}
			return in
		}(), `server=7f4d577430b48821
forwarding=on
sql_mode=fast
copy_snapshot=2026-11-20T13:30:00Z
copy_age_seconds=5400
last_answer=copy
last_answer_reason=answers_the_same
last_answer_reason_means=everything in the statement answers on the copy as on PostgreSQL
last_answer_error=no
last_answer_indexed_through=unknown
last_answer_age_seconds=unknown
last_answer_age_unknown_reason=` + pgAgeSnapshotAlone + "\n"},
		{"the copy answered, and which files it read cannot be told", func() pgPortStatusInput {
			in := base
			in.Last = pgshim.LastAnswer{Side: pgshim.RouteCopy, Reason: pgshim.ReasonCopyCommand}
			in.Age = console.CopyRunAge{Ran: now, Unknown: true}
			return in
		}(), `server=7f4d577430b48821
forwarding=on
sql_mode=fresh
copy_snapshot=2026-11-20T13:30:00Z
copy_age_seconds=5400
last_answer=copy
last_answer_reason=copy_command
last_answer_reason_means=a command of the copy's own (SHOW TABLES, DESCRIBE)
last_answer_error=no
last_answer_indexed_through=unknown
last_answer_age_seconds=unknown
last_answer_age_unknown_reason=` + pgAgeUnplaced + "\n"},
		{"the copy answered a statement that reads no table", func() pgPortStatusInput {
			in := base
			in.Last = pgshim.LastAnswer{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame}
			return in
		}(), `server=7f4d577430b48821
forwarding=on
sql_mode=fresh
copy_snapshot=2026-11-20T13:30:00Z
copy_age_seconds=5400
last_answer=copy
last_answer_reason=answers_the_same
last_answer_reason_means=everything in the statement answers on the copy as on PostgreSQL
last_answer_error=no
last_answer_indexed_through=unknown
last_answer_age_seconds=unknown
last_answer_age_unknown_reason=the last answer read no table of the copy
`},
		{"the source answered", func() pgPortStatusInput {
			in := base
			in.Last = pgshim.LastAnswer{Side: pgshim.RouteSource, Reason: pgshim.ReasonVeto}
			// An age left from an earlier answer is not this one's.
			in.Age = console.CopyRunAge{Ran: now, Through: now, Tailed: true}
			return in
		}(), `server=7f4d577430b48821
forwarding=on
sql_mode=fresh
copy_snapshot=2026-11-20T13:30:00Z
copy_age_seconds=5400
last_answer=source
last_answer_reason=veto
last_answer_reason_means=something in the statement is not known to answer on the copy as on PostgreSQL
last_answer_error=no
`},
		{"a refusal", func() pgPortStatusInput {
			in := base
			in.Last = pgshim.LastAnswer{Side: pgshim.RouteRefused, Reason: pgshim.ReasonReadOnly, Failed: true}
			return in
		}(), `server=7f4d577430b48821
forwarding=on
sql_mode=fresh
copy_snapshot=2026-11-20T13:30:00Z
copy_age_seconds=5400
last_answer=refused
last_answer_reason=read_only
last_answer_reason_means=not a read, and the port sends nothing that writes to the source
last_answer_error=yes
`},
		{"the copy refused, and nobody else could answer", func() pgPortStatusInput {
			in := base
			in.ForwardingOff = pgNoForwardAccount
			in.Last = pgshim.LastAnswer{Side: pgshim.RouteCopy, Reason: pgshim.ReasonNoForwardAccount, Failed: true}
			in.Age = console.CopyRunAge{Ran: now, Through: now, Tailed: true}
			return in
		}(), `server=7f4d577430b48821
forwarding=off
forwarding_off_reason=the server has no forwarding account
forwarding_account=none
sql_mode=fresh
copy_snapshot=2026-11-20T13:30:00Z
copy_age_seconds=5400
last_answer=copy
last_answer_reason=no_forward_account
last_answer_reason_means=the server has no forwarding account, so the copy answers alone
last_answer_error=yes
`},
		{"before any snapshot", func() pgPortStatusInput {
			in := base
			in.Snapshot = time.Time{}
			return in
		}(), `server=7f4d577430b48821
forwarding=on
sql_mode=fresh
copy_snapshot=unknown
copy_age_seconds=unknown
last_answer=none
`},
		{"a server whose copy cannot be read", func() pgPortStatusInput {
			in := base
			in.Snapshot, in.CopyUnavailable, in.Mode = time.Time{}, "it has no local copy yet", ""
			return in
		}(), `server=7f4d577430b48821
forwarding=on
copy_snapshot=unknown
copy_age_seconds=unknown
copy_unavailable_reason=it has no local copy yet
last_answer=none
`},
		{"a snapshot newer than this machine's clock", func() pgPortStatusInput {
			in := base
			in.Snapshot = now.Add(3 * time.Second)
			return in
		}(), `server=7f4d577430b48821
forwarding=on
sql_mode=fresh
copy_snapshot=2026-11-20T15:00:03Z
copy_age_seconds=0
last_answer=none
`},
		{"a port that keeps no ages", func() pgPortStatusInput {
			in := base
			in.Aged = false
			in.Last = pgshim.LastAnswer{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame}
			return in
		}(), `server=7f4d577430b48821
forwarding=on
sql_mode=fresh
copy_snapshot=2026-11-20T13:30:00Z
copy_age_seconds=5400
last_answer=copy
last_answer_reason=answers_the_same
last_answer_reason_means=everything in the statement answers on the copy as on PostgreSQL
last_answer_error=no
`},
	} {
		if got := statusText(pgPortStatusRows(tc.in)); got != tc.want {
			t.Errorf("%s:\n got:\n%s\nwant:\n%s", tc.what, got, tc.want)
		}
	}
}

// A connection's age is the age of its last answer, and only when the copy
// gave that answer.
func TestPGConnAgesLast(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	c := r.agesFor("p")
	routed := c.routed()
	if a, aged := c.lastAge(); !aged || a.Read() {
		t.Fatalf("before any statement: %+v %v", a, aged)
	}
	tailed := fresh(0, -time.Second)
	c.stash(tailed)
	routed(pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame})
	if a, _ := c.lastAge(); a != tailed {
		t.Fatalf("after the copy answered: %+v", a)
	}
	// The copy ran it, and the source answered after all.
	c.stash(fresh(time.Second, 0))
	routed(pgshim.RouteNote{Side: pgshim.RouteSource, Reason: pgshim.ReasonCopyColumnsDiffer})
	if a, _ := c.lastAge(); a.Read() {
		t.Fatalf("after the source answered: %+v", a)
	}
	// The copy ran it, and the client got an error.
	c.stash(tailed)
	routed(pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame})
	c.stash(tailed)
	routed(pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame, Failed: true})
	if a, _ := c.lastAge(); a.Read() {
		t.Fatalf("after an answer that failed: %+v", a)
	}
	// A port with no tally keeps no ages, and says so.
	var none *pgConnAges
	if _, aged := none.lastAge(); aged {
		t.Fatal("a nil pgConnAges says it keeps ages")
	}
}

func TestPGForwardingOff(t *testing.T) {
	hides := func() string { return "rows are hidden" }
	shows := func() string { return "" }
	for _, tc := range []struct {
		what  string
		f     *pgshim.Forward
		hides func() string
		want  string
	}{
		{"no account", nil, nil, pgNoForwardAccount},
		{"an account the port may not use", &pgshim.Forward{Refused: console.PGRouteRefusedIsCapture}, shows, console.PGRouteRefusedIsCapture},
		{"an empty forward", &pgshim.Forward{}, shows, pgNoForwardAccount},
		{"an account", &pgshim.Forward{DSN: "postgres://fwd@h/db"}, shows, ""},
		{"an account, no gate", &pgshim.Forward{DSN: "postgres://fwd@h/db"}, nil, ""},
		{"an account, and a server that hides", &pgshim.Forward{DSN: "postgres://fwd@h/db"}, hides, pgHiddenBarred},
	} {
		if got := pgForwardingOff(tc.f, tc.hides); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.what, got, tc.want)
		}
	}
}

// The counts in the statement's answer are the server's own.
func TestPGRouteStatsCounted(t *testing.T) {
	var none *pgRouteStats
	in := pgPortStatusInput{}
	none.counted("p", &in)
	if in.Counted {
		t.Fatal("a port with no tally says it counts")
	}
	since := time.Date(2026, 11, 20, 13, 0, 0, 0, time.UTC)
	r := newPGRouteStats(since, false)
	r.counted("p", &in)
	if !in.Counted || !in.Since.Equal(since) || in.Copy+in.Source+in.Refused != 0 || in.Reasons == nil {
		t.Fatalf("a server nothing reached yet: %+v", in)
	}
	r.note("p", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame})
	r.note("p", pgshim.RouteNote{Side: pgshim.RouteSource, Reason: pgshim.ReasonVeto})
	r.note("q", pgshim.RouteNote{Side: pgshim.RouteRefused, Reason: pgshim.ReasonReadOnly})
	r.counted("p", &in)
	if in.Copy != 1 || in.Source != 1 || in.Refused != 0 || len(in.Reasons) != 2 {
		t.Fatalf("server p: %+v", in)
	}
	// A copy of the tally's map, not the map.
	in.Reasons[pgshim.ReasonVeto] = 99
	var again pgPortStatusInput
	r.counted("p", &again)
	if again.Reasons[pgshim.ReasonVeto] != 1 {
		t.Fatal("the answer's reasons are the tally's own map")
	}
}

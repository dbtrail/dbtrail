package consoleapp

import (
	"context"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// What the PostgreSQL port answers to SHOW dbtrail.router_status (#3021),
// the counterpart of the MySQL port's SHOW ROUTER STATUS: name and value
// rows about the server of the connection that asks and about that
// connection's last statement. The statement itself, and what keeps it from
// ever reaching the copy or the source, is pgshim's (portstatus.go); this
// file is the rows.
//
// The row names are the MySQL port's where they mean the same (server,
// copy_snapshot, copy_age_seconds, last_answer, last_answer_indexed_through,
// last_answer_age_seconds, counting_since, statements_copy,
// statements_refused, reason_<name>), so a script reads either port.

// pgPortStatusInput is everything the rows are worded from, read when the
// statement is asked.
type pgPortStatusInput struct {
	// Server is the registry id of the connection's server.
	Server string
	// ForwardingOff is why nothing of this connection is sent to the
	// source, "" when statements may go there.
	ForwardingOff string
	// ForwardingAccount names the account statements are sent with
	// (#3054), "" for none.
	ForwardingAccount string
	// Mode is "fresh" or "fast", "" when not known.
	Mode string
	// CopyUnavailable is why SQL on the copy is not available for the
	// server, "" when it is; Snapshot is then the newest snapshot's time,
	// zero when there is none or it cannot be read.
	CopyUnavailable string
	Snapshot        time.Time
	Now             time.Time
	// Last is the connection's last answer, and Age how old the data of it
	// was when the copy gave it. Aged is false on a port that keeps no
	// ages (no tally).
	Last pgshim.LastAnswer
	Age  console.CopyRunAge
	Aged bool
	// Counted says the port keeps a tally; Since is when it started, and
	// the counts are the server's.
	Counted               bool
	Since                 time.Time
	Copy, Source, Refused uint64
	Reasons               map[string]uint64
}

// pgAgeNoTable is why the last answer has no age when it read no table.
const pgAgeNoTable = "the last answer read no table of the copy"

// pgNoForwardAccount is why nothing is sent to the source of a server the
// port was given no account for.
const pgNoForwardAccount = "the server has no forwarding account"

// pgPortStatusRows words in as the statement's rows, in the order shown.
func pgPortStatusRows(in pgPortStatusInput) [][2]string {
	out := [][2]string{{"server", in.Server}}
	if in.ForwardingOff == "" {
		out = append(out, [2]string{"forwarding", "on"})
	} else {
		out = append(out, [2]string{"forwarding", "off"}, [2]string{"forwarding_off_reason", in.ForwardingOff})
	}
	// The MySQL port's row of that name: which account, or none.
	switch {
	case in.ForwardingAccount != "":
		out = append(out, [2]string{"forwarding_account", in.ForwardingAccount})
	case in.ForwardingOff != "":
		out = append(out, [2]string{"forwarding_account", "none"})
	}
	if in.Mode != "" {
		out = append(out, [2]string{"sql_mode", in.Mode})
	}
	// The newest snapshot, as on the MySQL port. In fresh mode an answer
	// reads the changes captured since on top of it, so the last answer's
	// own age, below, is the one that says how old what it read was.
	switch {
	case in.CopyUnavailable != "":
		out = append(out, [2]string{"copy_snapshot", "unknown"}, [2]string{"copy_age_seconds", "unknown"},
			[2]string{"copy_unavailable_reason", in.CopyUnavailable})
	case in.Snapshot.IsZero():
		out = append(out, [2]string{"copy_snapshot", "unknown"}, [2]string{"copy_age_seconds", "unknown"})
	default:
		out = append(out,
			[2]string{"copy_snapshot", in.Snapshot.UTC().Format(time.RFC3339)},
			[2]string{"copy_age_seconds", strconv.FormatInt(int64(max(in.Now.Sub(in.Snapshot), 0).Seconds()), 10)})
	}
	out = append(out, in.Last.Rows()...)
	if in.Aged && in.Last.Side == pgshim.RouteCopy && !in.Last.Failed {
		out = append(out, pgLastAgeRows(in.Age)...)
	}
	if !in.Counted {
		return out
	}
	out = append(out,
		[2]string{"counting_since", in.Since.UTC().Format(time.RFC3339)},
		[2]string{"statements_copy", strconv.FormatUint(in.Copy, 10)},
		[2]string{"statements_source", strconv.FormatUint(in.Source, 10)},
		[2]string{"statements_refused", strconv.FormatUint(in.Refused, 10)})
	reasons := make([]string, 0, len(in.Reasons))
	for r := range in.Reasons {
		reasons = append(reasons, r)
	}
	slices.Sort(reasons)
	for _, r := range reasons {
		out = append(out, [2]string{"reason_" + r, strconv.FormatUint(in.Reasons[r], 10)})
	}
	return out
}

// pgLastAgeRows says how old the data of the copy's answer was: the instant
// it held every captured change up to and how long before the answer that
// was, rounded up as in /status, or "unknown" for both with the reason.
func pgLastAgeRows(a console.CopyRunAge) [][2]string {
	if a.Tailed {
		return [][2]string{
			{"last_answer_indexed_through", a.Through.UTC().Format(time.RFC3339)},
			{"last_answer_age_seconds", strconv.FormatInt(*pgAgeSeconds(max(a.Ran.Sub(a.Through), 0)), 10)},
		}
	}
	why := pgAgeNoTable
	switch {
	case a.Snapshot:
		why = pgAgeSnapshotAlone
	case a.Unknown:
		why = pgAgeUnplaced
	}
	return [][2]string{
		{"last_answer_indexed_through", "unknown"},
		{"last_answer_age_seconds", "unknown"},
		{"last_answer_age_unknown_reason", why},
	}
}

// lastAge is the age of the connection's last answer, zero when the copy
// did not give it; aged is false when c is nil.
func (c *pgConnAges) lastAge() (a console.CopyRunAge, aged bool) {
	if c == nil {
		return console.CopyRunAge{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last, true
}

// counted fills in's counts with server id's; nothing when r is nil.
func (r *pgRouteStats) counted(id string, in *pgPortStatusInput) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	in.Counted, in.Since, in.Reasons = true, r.since, map[string]uint64{}
	if t := r.perServer[id]; t != nil {
		in.Copy, in.Source, in.Refused = t.copy, t.source, t.refused
		for reason, n := range t.reasons {
			in.Reasons[reason] = n
		}
	}
}

// pgForwardingOff is why nothing of a connection is sent to its server's
// source, "" when statements may go there: no account the port may use
// (with what to do about it, when the server says), or a server that hides
// tables or columns (pgForwardBarred's rule, asked here without its log
// line).
func pgForwardingOff(f *pgshim.Forward, hides func() string) string {
	switch {
	case f == nil:
		return pgNoForwardAccount
	case f.DSN == "" && f.Refused != "":
		return f.Refused
	case f.DSN == "":
		return pgNoForwardAccount
	case hides != nil && hides() != "":
		return pgHiddenBarred
	}
	return ""
}

// pgPortStatusFor is pgshim.Binding.Status for a connection of server tgt.
// Everything is read when the statement is asked: bound is still being
// filled when this is called, and a server's mode, its rules and its
// snapshot change while a connection is open.
func pgPortStatusFor(srv *console.Server, tgt console.FlashbackTarget, bound *pgshim.Binding, routes *pgRouteStats, ages *pgConnAges) func(context.Context, pgshim.LastAnswer) [][2]string {
	return func(ctx context.Context, last pgshim.LastAnswer) [][2]string {
		in := pgPortStatusInput{Server: tgt.ID, Now: time.Now(), Last: last}
		in.ForwardingOff = pgForwardingOff(bound.Forward, tgt.TimeTravelRefusal)
		in.ForwardingAccount = pgForwardingAccount(bound.Forward)
		in.Mode, _ = srv.SQLModeOf(tgt.ID)
		if tgt.SQL == nil {
			in.CopyUnavailable = tgt.SQLUnavailable
			if in.CopyUnavailable == "" {
				in.CopyUnavailable = "SQL on the copy is not available for this server"
			}
		} else {
			in.Snapshot = tgt.SQL.CopyUpdatedAt(ctx)
		}
		in.Age, in.Aged = ages.lastAge()
		routes.counted(tgt.ID, &in)
		return pgPortStatusRows(in)
	}
}

// pgForwardingAccount names the account f sends statements with, "" when it
// has none: the capture account (console.PGForwardsWithCapture) or the
// server's forwarding user by name.
func pgForwardingAccount(f *pgshim.Forward) string {
	switch {
	case f == nil || f.DSN == "":
		return ""
	case f.CaptureAccount:
		return console.PGForwardsWithCapture
	}
	u, err := url.Parse(f.DSN)
	if err != nil || u.User == nil {
		return "forwarding user"
	}
	return "forwarding user " + u.User.Username()
}

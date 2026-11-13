package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// `port max-behind <server> [<seconds>|off]` (#2783): the terminal's way to
// say or set a server's lag limit for the copy (#2777), which only the API
// set before. The daemon's route is the one `port mode` uses,
// PUT /api/servers/{id}/sql-mode, sent with this one field only, so the
// server's fresh/fast mode and LIKE switch are left as they are.

const (
	// maxBehindCap is the largest limit the daemon saves: a year
	// (console.maxBehindSourceCap). Checked here too, so the refusal names
	// the forms this command takes, not the API's field.
	maxBehindCap = 365 * 24 * 60 * 60

	// maxBehindExact is the limit under which it is not kept to the second:
	// capture's state is read at most every 30 s and counts as caught up for
	// a minute after its last stamp (console.captureReadEvery and
	// caughtUpWindow, docs/time-travel-sql.md#lag-limit).
	maxBehindExact = 90
)

// maxBehindForms is what every refusal of a value says is accepted.
const maxBehindForms = "give whole seconds (300), a duration (90s, 5m, 1h30m), or off (the same as 0) to remove it"

// parseMaxBehind reads the limit as given on the command line: a whole
// number of seconds, a Go duration that is a whole number of seconds, or
// off. 0 is off.
func parseMaxBehind(arg string) (int64, error) {
	v := strings.TrimSpace(arg)
	switch {
	case v == "":
		return 0, errors.New("the limit is empty: " + maxBehindForms)
	case strings.EqualFold(v, "off"):
		return 0, nil
	case strings.HasPrefix(v, "-"):
		return 0, fmt.Errorf("the limit cannot be negative (%q): %s", arg, maxBehindForms)
	}
	var secs int64
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		secs = n
	} else if errors.Is(err, strconv.ErrRange) {
		return 0, fmt.Errorf("%q is more than a year, the largest limit (%d s)", arg, maxBehindCap)
	} else if _, ferr := strconv.ParseFloat(v, 64); ferr == nil {
		return 0, fmt.Errorf("%q is not written as whole seconds: %s", arg, maxBehindForms)
	} else {
		d, derr := time.ParseDuration(v)
		if derr != nil {
			// A duration Go cannot hold (past about 292 years) is refused
			// for its size, not its form.
			if durationRe.MatchString(v) {
				return 0, fmt.Errorf("%q is more than a year, the largest limit (%d s)", arg, maxBehindCap)
			}
			return 0, fmt.Errorf("%q is not a limit this command reads: %s", arg, maxBehindForms)
		}
		if d%time.Second != 0 {
			return 0, fmt.Errorf("%q has a fraction of a second: %s", arg, maxBehindForms)
		}
		secs = int64(d / time.Second)
	}
	if secs > maxBehindCap {
		return 0, fmt.Errorf("%q is more than a year, the largest limit (%d s)", arg, maxBehindCap)
	}
	return secs, nil
}

// durationRe is the shape of a Go duration (digits, an optional fraction,
// and a unit, repeated), whatever its size.
var durationRe = regexp.MustCompile(`^\+?([0-9]+(\.[0-9]*)?|\.[0-9]+)(ns|us|µs|μs|ms|s|m|h)(([0-9]+(\.[0-9]*)?|\.[0-9]+)(ns|us|µs|μs|ms|s|m|h))*$`)

// maxBehindText is a limit as people read it: "off", "45 s", or "300 s (5m)".
func maxBehindText(secs int64) string {
	if secs <= 0 {
		return "off"
	}
	if secs < 60 {
		return fmt.Sprintf("%d s", secs)
	}
	d := (time.Duration(secs) * time.Second).String() // 5m0s, 1h30m0s, 1m30s
	if strings.HasSuffix(d, "m0s") {
		d = strings.TrimSuffix(d, "0s")
	}
	if strings.HasSuffix(d, "h0m") {
		d = strings.TrimSuffix(d, "0m")
	}
	return fmt.Sprintf("%d s (%s)", secs, d)
}

func newPortMaxBehindCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "max-behind <server> [<seconds>|off]",
		Short: "Say or set how far behind the source a server's copy may answer on the MySQL port",
		Long: `Say or set how far behind the source a server's copy may answer on the MySQL port.

  bintrail-console port max-behind shop        the limit saved, or off
  bintrail-console port max-behind shop 5m     past 5 minutes behind, MySQL answers
  bintrail-console port max-behind shop 300    the same, in seconds
  bintrail-console port max-behind shop off    no limit (the same as 0)

With a limit, a statement that reads the copy's tables is answered there only
while its data is at most that far behind the source, read like MySQL's
Seconds_Behind_Source. Past it, a routed statement goes to MySQL, and one on a
connection that does not route (<server>@copy, or Engine Routing off) gets
error 1105. Off by default. It does not change the server's fresh or fast
mode, nor its LIKE switch, and open connections follow it within a few
seconds.

Under about a minute and a half it is not kept to the second: capture's state
is read every 30 seconds and counts as caught up for a minute after its last
stamp.`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 || len(args) > 2 {
				return errors.New("name one server, and a limit to set it: port max-behind <server> [<seconds>|off]")
			}
			if len(args) == 2 {
				_, err := parseMaxBehind(args[1])
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var want *int64
			if len(args) == 2 {
				v, err := parseMaxBehind(args[1])
				if err != nil {
					return err
				}
				want = &v
			}
			c, err := resolveCtl(srvURL, srvAccessFile)
			if err != nil {
				return err
			}
			return runPortMaxBehind(cmd.Context(), c, args[0], want, cmdOut())
		},
	}
}

// errMaxBehindNotSaid is a daemon that answers about the server without the
// limit: one older than it, which neither has nor applies one.
func errMaxBehindNotSaid(c *ctlClient) error {
	return fmt.Errorf("%s does not say a lag limit for its servers: it is older than the limit (#2777). Update DBTrail to use it", c.base)
}

// runPortMaxBehind says, or saves and then says, server name's lag limit.
func runPortMaxBehind(ctx context.Context, c *ctlClient, name string, want *int64, out io.Writer) error {
	s, err := serverByName(ctx, c, name)
	if err != nil {
		return err
	}
	switch {
	case s.Kind == "ephemeral":
		return fmt.Errorf("%q was given where DBTrail starts and has no choices of its own, so it cannot have a lag limit. Add it as a server (dbtrail server add) to set one", s.Name)
	case !s.HasSource:
		return fmt.Errorf("%q has no source database, so the MySQL port does not answer for it", s.Name)
	}
	if want == nil {
		if s.SQLMaxBehindSource == nil {
			return errMaxBehindNotSaid(c)
		}
		if *s.SQLMaxBehindSource <= 0 {
			fmt.Fprintf(out, "No lag limit for %q: the copy answers whatever its lag. Set one:  bintrail-console port max-behind %s 5m\n", s.Name, mysqlLineUser(s.Name))
			return nil
		}
		fmt.Fprintf(out, "Lag limit for %q: %s. %s\n", s.Name, maxBehindText(*s.SQLMaxBehindSource), maxBehindPastIt)
		maxBehindMargin(out, *s.SQLMaxBehindSource)
		return nil
	}
	// A daemon older than the limit refuses the field as unknown JSON: say
	// what that means before asking, from the list that already told.
	if s.SQLMaxBehindSource == nil {
		return errMaxBehindNotSaid(c)
	}
	var saved struct {
		MaxBehindSource *int64 `json:"max_behind_source_seconds"`
	}
	if err := c.do(ctx, http.MethodPut, "/api/servers/"+s.ID+"/sql-mode", map[string]int64{"max_behind_source_seconds": *want}, &saved); err != nil {
		return err
	}
	if saved.MaxBehindSource == nil {
		return errMaxBehindNotSaid(c)
	}
	if *saved.MaxBehindSource != *want {
		return fmt.Errorf("DBTrail took the request and the lag limit of %q is %s, not %s", s.Name, maxBehindText(*saved.MaxBehindSource), maxBehindText(*want))
	}
	if *want == 0 {
		fmt.Fprintf(out, "Saved for %q: no lag limit. The copy answers whatever its lag.\n", s.Name)
		return nil
	}
	fmt.Fprintf(out, "Saved for %q: a lag limit of %s. %s Open connections follow it within a few seconds.\n", s.Name, maxBehindText(*want), maxBehindPastIt)
	maxBehindMargin(out, *want)
	return nil
}

const maxBehindPastIt = "Past it, a routed statement goes to MySQL, and one that does not route gets error 1105."

// maxBehindMargin says, for a limit under maxBehindExact, why it is not kept
// to the second. The limit is saved all the same.
func maxBehindMargin(out io.Writer, secs int64) {
	if secs <= 0 || secs >= maxBehindExact {
		return
	}
	fmt.Fprintf(out, "Under %d s it is not kept to the second: capture's state is read every 30 s and counts as caught up for a minute after its last stamp, so for about a minute and a half after capture stops the copy can still answer. A limit of a few minutes is kept.\n", maxBehindExact)
}

// printMaxBehindList is `port status`'s lag limit per server the port
// answers, like SHOW ROUTER STATUS's max_behind_source_seconds. A list that
// did not load is said, not left out: an absent section would read as no
// limits.
func printMaxBehindList(out io.Writer, servers []ctlServer, listErr error) {
	if listErr != nil {
		fmt.Fprintf(out, "\nThe servers' lag limits could not be read: %v\n", listErr)
		return
	}
	var rows []ctlServer
	for _, s := range servers {
		if s.HasSource {
			rows = append(rows, s)
		}
	}
	if len(rows) == 0 {
		return
	}
	width := 0
	for _, s := range rows {
		width = max(width, len(s.Name))
	}
	fmt.Fprintln(out, "\nLag limit per server (bintrail-console port max-behind <server>):")
	for _, s := range rows {
		var limit string
		switch {
		case s.Kind == "ephemeral":
			limit = "off (set at startup: it cannot have one)"
		case s.SQLMaxBehindSource == nil:
			limit = "not said (this DBTrail is older than the lag limit)"
		default:
			limit = maxBehindText(*s.SQLMaxBehindSource)
		}
		fmt.Fprintf(out, "  %-*s  %s\n", width, s.Name, limit)
	}
}

package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/console"
)

// `snapshot` and `port`: the two steps after `server add`, so that going
// from an installed DBTrail to a query answered on port 3309 never needs the
// web interface. Like `server`, they ask the running daemon (ctl.go).

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Take the copy of a database's tables, and keep it current, from the terminal",
	Long: `Take the copy of a database's tables, and keep it current.

  bintrail-console snapshot run shop                 read every table once
  bintrail-console snapshot schedule shop --every 1h keep the copy current
  bintrail-console snapshot status shop

The first copy reads every table of the database, with a short lock so they
are all read at one moment: pick a quiet time. Keeping it current reads
nothing from the database again: it uses the changes DBTrail recorded.`,
}

var portCmd = &cobra.Command{
	Use:   "port",
	Short: "Turn on the MySQL port (3309) and get its password, from the terminal",
	Long: `Turn the MySQL port on and off, and get its password.

  bintrail-console port on            turn it on; prints the password, once
  bintrail-console port status
  bintrail-console port new-password  replace the password
  bintrail-console port routing on    send what the copy cannot answer to MySQL
  bintrail-console port mode shop fast  answer shop from its last snapshot alone
  bintrail-console port off

Connect with the mysql client. The user is the server's name:

  mysql -h 127.0.0.1 -P 3309 -u shop -p`,
}

var (
	snapNoWait bool
	snapEvery  string
	snapOff    bool
	portFormat string

	// snapshotPoll is how often `snapshot run` asks how the read is going.
	snapshotPoll = 2 * time.Second
)

func init() {
	run := &cobra.Command{
		Use:   "run <server>",
		Short: "Read every table of a database now, and wait until it is done",
		Long: `Reads every table of the database once and waits until the copy is ready,
or says why it failed. Ctrl-C stops the waiting, not the read: DBTrail goes
on, and "snapshot status" says how it ended. --no-wait starts it and returns.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveCtl(srvURL, srvAccessFile)
			if err != nil {
				return err
			}
			return runSnapshotRun(cmd.Context(), c, args[0], !snapNoWait, cmdOut())
		},
	}
	run.Flags().BoolVar(&snapNoWait, "no-wait", false, "Start the read and return without waiting for it")

	status := &cobra.Command{
		Use:   "status <server>",
		Short: "Say how the last read of a database ended, or that one is running",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveCtl(srvURL, srvAccessFile)
			if err != nil {
				return err
			}
			return runSnapshotStatus(cmd.Context(), c, args[0], cmdOut())
		},
	}

	schedule := &cobra.Command{
		Use:   "schedule <server>",
		Short: "Keep the copy current: update it every so often",
		Long: `Updates the copy every --every (30m, 1h, 6h, 1d...) from the changes DBTrail
recorded, without reading the database again. --off stops the updates.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if (snapEvery == "") == !snapOff {
				return errors.New("say how often with --every (for example --every 1h), or --off to stop the updates")
			}
			c, err := resolveCtl(srvURL, srvAccessFile)
			if err != nil {
				return err
			}
			return runSnapshotSchedule(cmd.Context(), c, args[0], snapEvery, cmdOut())
		},
	}
	schedule.Flags().StringVar(&snapEvery, "every", "", "How often to update the copy: 30m, 1h, 6h, 1d...")
	schedule.Flags().BoolVar(&snapOff, "off", false, "Stop updating the copy")

	addCtlFlags(snapshotCmd, &srvURL, &srvAccessFile)
	snapshotCmd.AddCommand(run, status, schedule)
	rootCmd.AddCommand(snapshotCmd)

	portVerb := func(use, short string, run func(context.Context, *ctlClient, io.Writer) error) *cobra.Command {
		return &cobra.Command{
			Use: use, Short: short, Args: cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				c, err := resolveCtl(srvURL, srvAccessFile)
				if err != nil {
					return err
				}
				return run(cmd.Context(), c, cmdOut())
			},
		}
	}
	routing := &cobra.Command{
		Use:       "routing on|off",
		Short:     "Send the statements the copy cannot answer to MySQL (Engine Routing)",
		Args:      cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		ValidArgs: []string{"on", "off"},
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveCtl(srvURL, srvAccessFile)
			if err != nil {
				return err
			}
			return runPortRouting(cmd.Context(), c, args[0] == "on", cmdOut())
		},
	}
	mode := &cobra.Command{
		Use:   "mode <server> [fresh|fast]",
		Short: "Say or choose how current a server's answers on the MySQL port are: fresh or fast",
		Long: `Say or choose how current a server's answers on the MySQL port are.

  bintrail-console port mode shop          what is saved, and what the router applies
  bintrail-console port mode shop fast     answer from the last snapshot alone
  bintrail-console port mode shop fresh    lay the newest captured changes over it

Fresh, the default, answers about as far behind your MySQL as capture,
usually seconds, and keeps those changes in the router's memory. Fast answers
as of the copy's last update, and leaves that memory to the statements.

BINTRAIL_SQL_TAILS (SQL_FRESH in the compose .env), when it says on or off,
decides for every server: a choice saved here then applies once it is removed.`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 || len(args) > 2 {
				return errors.New("name one server, and fresh or fast to choose: port mode <server> [fresh|fast]")
			}
			if len(args) == 2 && args[1] != "fresh" && args[1] != "fast" {
				return fmt.Errorf("the mode is fresh or fast, not %q", args[1])
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveCtl(srvURL, srvAccessFile)
			if err != nil {
				return err
			}
			want := ""
			if len(args) == 2 {
				want = args[1]
			}
			return runPortMode(cmd.Context(), c, args[0], want, portModeWait, cmdOut())
		},
	}
	addCtlFlags(portCmd, &srvURL, &srvAccessFile)
	portCmd.AddCommand(mode,
		portVerb("on", "Turn the MySQL port on. Prints its password when one is made", func(ctx context.Context, c *ctlClient, w io.Writer) error {
			return runPortSet(ctx, c, true, w)
		}),
		portVerb("off", "Turn the MySQL port off", func(ctx context.Context, c *ctlClient, w io.Writer) error {
			return runPortSet(ctx, c, false, w)
		}),
		portVerb("status", "Say whether the MySQL port is on, and where it answers", runPortStatus),
		portVerb("new-password", "Replace the MySQL port's password. The old one stops working", runPortNewPassword),
		routing,
	)
	rootCmd.AddCommand(portCmd)
}

// serverByName finds the server a command was given, among those the daemon
// lists.
func serverByName(ctx context.Context, c *ctlClient, name string) (ctlServer, error) {
	servers, err := listServers(ctx, c)
	if err != nil {
		return ctlServer{}, err
	}
	return findServer(servers, name)
}

// The daemon's routes for a full read and for the schedule keep the names
// they had before the product said "snapshot" (#1573). Spelled once each.
const (
	snapshotRouteSuffix = "/baseline"
	scheduleRouteSuffix = "/backup-schedule"
)

func snapshotRoute(id string) string { return "/api/servers/" + id + snapshotRouteSuffix }

// ctlBaseline is the daemon's console.BaselineStatus, as these commands read
// it.
type ctlBaseline struct {
	Baseline console.BaselineStatus `json:"baseline"`
}

// errSnapshotFailed ends `snapshot run` with a non-zero exit when the read
// ended without a copy.
var errSnapshotFailed = errors.New("the copy was not taken")

func runSnapshotRun(ctx context.Context, c *ctlClient, name string, wait bool, out io.Writer) error {
	s, err := serverByName(ctx, c, name)
	if err != nil {
		return err
	}
	var st ctlBaseline
	if err := c.do(ctx, http.MethodPost, snapshotRoute(s.ID), nil, &st); err != nil {
		return err
	}
	if !wait {
		fmt.Fprintf(out, "Reading every table of %q. See how it ends:  bintrail-console snapshot status %s\n", s.Name, s.Name)
		return nil
	}
	fmt.Fprintf(out, "Reading every table of %q ... (Ctrl-C stops the waiting, not the read)\n", s.Name)
	started := time.Now()
	lastNote := started
	for {
		// An answer with no state is not "done": it is not an answer.
		switch st.Baseline.State {
		case "running":
		case "succeeded":
			fmt.Fprintln(out, snapshotDoneText(s.Name, st.Baseline))
			return nil
		case "failed":
			return snapshotFailure(st.Baseline)
		default:
			return fmt.Errorf("DBTrail answered the state %q for the read, which says neither that it runs nor how it ended. See `bintrail-console snapshot status %s`", st.Baseline.State, s.Name)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("stopped waiting. The read goes on: see `bintrail-console snapshot status %s`", s.Name)
		case <-time.After(snapshotPoll):
		}
		if time.Since(lastNote) >= time.Minute {
			lastNote = time.Now()
			fmt.Fprintf(out, "  still reading (%s)\n", time.Since(started).Round(time.Minute))
		}
		st = ctlBaseline{}
		if err := c.do(ctx, http.MethodGet, snapshotRoute(s.ID), nil, &st); err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("stopped waiting. The read goes on: see `bintrail-console snapshot status %s`", s.Name)
			}
			return fmt.Errorf("the read was started, and asking how it goes failed: %w\nSee `bintrail-console snapshot status %s`", err, s.Name)
		}
	}
}

func snapshotDoneText(name string, b console.BaselineStatus) string {
	text := fmt.Sprintf("The copy of %q is ready: %s.", name, plural(b.Tables, "table"))
	if n := len(b.LeftOutTables) + b.LeftOutTablesOmitted; n > 0 {
		text += fmt.Sprintf(" %s left out: the web interface names each and says why"+console.OnPage(console.PageSnapshots)+".", plural(n, "table"))
	}
	return text + fmt.Sprintf("\nKeep it current:  bintrail-console snapshot schedule %s --every 1h\nThe MySQL port:  bintrail-console port status", name)
}

// snapshotFailure says why a read failed. A read can fail AFTER leaving a
// complete copy on this machine (only its upload failed): that is not "no
// copy", and saying so would send someone to read the whole database again.
func snapshotFailure(b console.BaselineStatus) error {
	why := b.LastError
	if why == "" {
		why = "DBTrail gave no reason; its log has it"
	}
	if b.Published {
		return fmt.Errorf("the copy is ready on this machine, and sending it to its S3 location failed: %s", why)
	}
	return fmt.Errorf("%w: %s", errSnapshotFailed, why)
}

func runSnapshotStatus(ctx context.Context, c *ctlClient, name string, out io.Writer) error {
	s, err := serverByName(ctx, c, name)
	if err != nil {
		return err
	}
	var st ctlBaseline
	if err := c.do(ctx, http.MethodGet, snapshotRoute(s.ID), nil, &st); err != nil {
		return err
	}
	b := st.Baseline
	switch b.State {
	case "running":
		fmt.Fprintf(out, "%s: a read is running%s.\n", s.Name, sinceText(" since ", b.Since))
	case "succeeded":
		fmt.Fprintf(out, "%s: the last read ended well%s: %s.\n", s.Name, sinceText(" at ", b.FinishedAt), plural(b.Tables, "table"))
	case "failed":
		fmt.Fprintf(out, "%s: the last read failed%s.\n  %v\n", s.Name, sinceText(" at ", b.FinishedAt), snapshotFailure(b))
	case "idle", "":
		fmt.Fprintf(out, "%s: no read has run since DBTrail started. Take one:  bintrail-console snapshot run %s\n", s.Name, s.Name)
	default:
		fmt.Fprintf(out, "%s: %s\n", s.Name, b.State)
	}
	return nil
}

func sinceText(prefix, at string) string {
	if at == "" {
		return ""
	}
	return prefix + at
}

func runSnapshotSchedule(ctx context.Context, c *ctlClient, name, every string, out io.Writer) error {
	s, err := serverByName(ctx, c, name)
	if err != nil {
		return err
	}
	path := "/api/servers/" + s.ID + scheduleRouteSuffix
	if every == "" {
		if err := c.do(ctx, http.MethodDelete, path, nil, nil); err != nil {
			return err
		}
		fmt.Fprintf(out, "The copy of %q is no longer updated by itself.\n", s.Name)
		return nil
	}
	// The answer carries the schedule as saved. A new schedule on a server
	// with an S3 destination sends there once a day unless told otherwise
	// (#2658), and this command does not name the interval, so it says what
	// the daemon chose. An older daemon answers without it: nothing is said.
	var saved struct {
		Schedule struct {
			UploadEvery string `json:"upload_every"`
		} `json:"schedule"`
	}
	if err := c.do(ctx, http.MethodPut, path, map[string]string{"every": strings.TrimSpace(every)}, &saved); err != nil {
		return err
	}
	fmt.Fprintf(out, "The copy of %q is updated every %s.\n", s.Name, strings.TrimSpace(every))
	if up := strings.TrimSpace(saved.Schedule.UploadEvery); up != "" {
		fmt.Fprintf(out, "A snapshot is sent to S3 every %s. The updates in between are not sent, and DBTrail's machine keeps only the newest of them. Change it on the %s page.\n", up, console.PageSnapshots)
	}
	return nil
}

// ctlPort is the daemon's answer about the MySQL port, as far as these
// commands read it. Password is present only in the answer that made one.
type ctlPort struct {
	Enabled           *bool  `json:"enabled"`
	Listen            string `json:"listen"`
	Host              string `json:"host"`
	Port              string `json:"port"`
	HasPassword       bool   `json:"has_password"`
	PasswordCreatedAt string `json:"password_created_at"`
	RoutingOn         bool   `json:"routing_on"`
	Error             string `json:"error"`
	Notice            string `json:"notice"`
	Password          string `json:"password"`
}

// portAnswer asks about the port. An answer that does not say whether the
// port is on is not DBTrail's.
func portAnswer(ctx context.Context, c *ctlClient, method, path string, body any) (ctlPort, error) {
	var p ctlPort
	if err := c.do(ctx, method, path, body, &p); err != nil {
		return ctlPort{}, err
	}
	if p.Enabled == nil {
		return ctlPort{}, fmt.Errorf("%s answered, and not about the MySQL port. Is that address DBTrail's?", c.base)
	}
	return p, nil
}

func (p ctlPort) where() string {
	host, port := p.Host, p.Port
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "3309"
	}
	return host + " -P " + port
}

// portUser is the user name to show in the mysql line: the server's own name
// when there is exactly one to connect as, and a placeholder otherwise. It
// never fails: the line is a convenience, and the list not loading must not
// hide whether the port is on.
func portUser(ctx context.Context, c *ctlClient) string {
	servers, err := listServers(ctx, c)
	if err != nil {
		return "<server name>"
	}
	name := ""
	for _, s := range servers {
		if !s.HasSource {
			continue
		}
		if name != "" {
			return "<server name>"
		}
		name = s.Name
	}
	return mysqlLineUser(name)
}

// mysqlLineUser is name as the user of a printed mysql line, which people
// copy into a shell. Only a name made of letters, digits and . _ @ : - is
// printed as it is: anything else (a space, a quote, a character the shell
// expands) is left for the person to type and quote.
func mysqlLineUser(name string) string {
	if name == "" {
		return "<server name>"
	}
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '@', c == ':', c == '-':
		default:
			return "<server name>"
		}
	}
	return name
}

func printPort(out io.Writer, p ctlPort, user string) {
	if !*p.Enabled {
		fmt.Fprintln(out, "The MySQL port is off. Turn it on:  bintrail-console port on")
		return
	}
	who := "The user is a server's name (bintrail-console server list)"
	if user != "<server name>" {
		who = "The user is the server's name"
	}
	fmt.Fprintf(out, "The MySQL port is on. %s:\n\n  mysql -h %s -u %s -p\n\n", who, p.where(), user)
	if p.RoutingOn {
		fmt.Fprintln(out, "Engine Routing is on: what the copy cannot answer goes to MySQL.")
	} else {
		fmt.Fprintln(out, "Engine Routing is off: every statement runs on the copy. Turn it on:  bintrail-console port routing on")
	}
	// The port being saved as on is not the port answering: the daemon
	// reports what stopped it, and that must not be left out.
	if p.Error != "" {
		fmt.Fprintf(out, "\nIt is not answering: %s\n", p.Error)
	}
	if p.Notice != "" {
		fmt.Fprintf(out, "\n%s\n", p.Notice)
	}
}

func runPortSet(ctx context.Context, c *ctlClient, on bool, out io.Writer) error {
	p, err := portAnswer(ctx, c, http.MethodPut, "/api/flashback", map[string]bool{"enabled": on})
	if err != nil {
		return err
	}
	if *p.Enabled != on {
		return fmt.Errorf("DBTrail took the request and the MySQL port is still %s", map[bool]string{true: "on", false: "off"}[*p.Enabled])
	}
	user := ""
	if on {
		user = portUser(ctx, c)
	}
	printPort(out, p, user)
	if !on {
		return nil
	}
	switch {
	case p.Password != "":
		fmt.Fprintf(out, "\nPassword (shown only now, keep it): %s\n", p.Password)
	case p.HasPassword:
		fmt.Fprintf(out, "\nThe password is the one made%s. Lost it?  bintrail-console port new-password\n", sinceText(" on ", p.PasswordCreatedAt))
	default:
		return errors.New("the port is on and DBTrail reports no password for it. Make one:  bintrail-console port new-password")
	}
	if p.Error != "" {
		return errors.New("the MySQL port is saved as on and is not answering: " + p.Error)
	}
	return nil
}

func runPortStatus(ctx context.Context, c *ctlClient, out io.Writer) error {
	p, err := portAnswer(ctx, c, http.MethodGet, "/api/flashback", nil)
	if err != nil {
		return err
	}
	user := ""
	if *p.Enabled {
		user = portUser(ctx, c)
	}
	printPort(out, p, user)
	return nil
}

func runPortNewPassword(ctx context.Context, c *ctlClient, out io.Writer) error {
	p, err := portAnswer(ctx, c, http.MethodPost, "/api/flashback/password", nil)
	if err != nil {
		return err
	}
	if p.Password == "" {
		return errors.New("DBTrail took the request and gave no new password. The old one may still be the one in force: see `bintrail-console port status`")
	}
	fmt.Fprintf(out, "New password (shown only now, keep it): %s\nThe port takes it within a few seconds; the old one stops working.\n", p.Password)
	return nil
}

func runPortRouting(ctx context.Context, c *ctlClient, on bool, out io.Writer) error {
	p, err := portAnswer(ctx, c, http.MethodPut, "/api/flashback", map[string]bool{"routing": on})
	if err != nil {
		return err
	}
	if p.RoutingOn != on {
		return fmt.Errorf("DBTrail took the request and Engine Routing is still %s", map[bool]string{true: "on", false: "off"}[p.RoutingOn])
	}
	if on {
		fmt.Fprintln(out, "Engine Routing is on: what the copy cannot answer goes to MySQL.")
		// On is not enough: each server forwards as a user of its own, and
		// one without it refuses those statements. The list not loading must
		// not turn a change that was made into a failure.
		if servers, err := listServers(ctx, c); err == nil {
			for _, s := range servers {
				if s.HasSource && (!s.HasRoute || s.RouteIsCapture) {
					fmt.Fprintf(out, "%q has no user to forward as yet, so it refuses those statements:  bintrail-console server forward-user %s -u <user> -p\n", s.Name, s.Name)
				}
			}
		}
		if !*p.Enabled {
			fmt.Fprintln(out, "The MySQL port is off, so it applies from the moment it is turned on:  bintrail-console port on")
		}
	} else {
		fmt.Fprintln(out, "Engine Routing is off: every statement runs on the copy.")
	}
	return nil
}

// portModeWait is how long `port mode <server> <mode>` waits for the router
// to say it applies the choice. It reads the server list every two seconds.
var (
	portModeWait = 8 * time.Second
	portModePoll = 2 * time.Second
)

// ctlRouterView is GET /api/router, as `port mode` reads it: whether this
// DBTrail knows its router, and the router's own status document.
type ctlRouterView struct {
	Configured bool   `json:"configured"`
	Error      string `json:"error"`
	Status     *struct {
		Fresh          *bool  `json:"fresh"`
		SQLModeForced  string `json:"sql_mode_forced"`
		PerServerMode  bool   `json:"per_server_mode"`
		FreshByDefault bool   `json:"fresh_by_default"`
		Servers        map[string]struct {
			SQLMode     string `json:"sql_mode"`
			SQLModeFrom string `json:"sql_mode_from"`
		} `json:"servers"`
	} `json:"status"`
}

// routerApplies is the mode the router answers server s in, and why it is
// that one: "" when the router cannot be read or says nothing about modes,
// with what to say instead.
func (v ctlRouterView) routerApplies(s ctlServer) (mode, note string) {
	switch {
	case !v.Configured:
		return "", "This DBTrail does not know where its router runs (ROUTER_URL in the compose .env), so what it applies cannot be said here. A router reads the saved choice within a few seconds."
	case v.Status == nil:
		why := v.Error
		if why == "" {
			why = "it gave no status"
		}
		return "", "The router could not be read (" + strings.TrimRight(why, ".") + "), so what it applies cannot be said here."
	case v.Status.SQLModeForced == "fresh" || v.Status.Fresh != nil && *v.Status.Fresh:
		return "fresh", "The router answers every server in fresh mode: SQL_FRESH=on in the compose .env (BINTRAIL_SQL_TAILS=on elsewhere) decides. The saved choice applies once that is removed (then docker compose up -d router)."
	case v.Status.SQLModeForced == "fast":
		return "fast", "The router answers every server in fast mode: SQL_FRESH=off in the compose .env (BINTRAIL_SQL_TAILS=off elsewhere) decides. The saved choice applies once that is removed (then docker compose up -d router)."
	case !v.Status.PerServerMode:
		return "fast", "The router is older than the per-server choice and answers every server in fast mode. Update the router to choose here."
	case !v.Status.FreshByDefault && !s.SQLFreshSaved:
		return "fast", "The router is older than fresh being the default, and answers a server with nothing saved in fast mode. Save fresh here, or update the router."
	}
	if n, ok := v.Status.Servers[s.ID]; ok && n.SQLMode != "" {
		return n.SQLMode, ""
	}
	// A router that reads the choice lists every server that is fresh: one
	// it does not list answers in fast mode.
	return "fast", ""
}

// runPortMode says, or sets and then says, server name's mode on the port.
func runPortMode(ctx context.Context, c *ctlClient, name, want string, wait time.Duration, out io.Writer) error {
	s, err := serverByName(ctx, c, name)
	if err != nil {
		return err
	}
	switch {
	case s.Kind == "ephemeral":
		return fmt.Errorf("%q was given where DBTrail starts and has no choice of its own: it answers in fast mode unless SQL_FRESH=on (BINTRAIL_SQL_TAILS=on) says fresh for every server. Add it as a server (dbtrail server add) to choose", s.Name)
	case !s.HasSource:
		return fmt.Errorf("%q has no source database, so the MySQL port does not answer for it", s.Name)
	}
	if want != "" {
		var saved struct {
			Fresh      bool `json:"fresh"`
			FreshSaved bool `json:"fresh_saved"`
		}
		if err := c.do(ctx, http.MethodPut, "/api/servers/"+s.ID+"/sql-mode", map[string]bool{"fresh": want == "fresh"}, &saved); err != nil {
			return err
		}
		if saved.Fresh != (want == "fresh") {
			return fmt.Errorf("DBTrail took the request and %q is still saved as %s", s.Name, map[bool]string{true: "fresh", false: "fast"}[saved.Fresh])
		}
		s.SQLFresh, s.SQLFreshSaved = saved.Fresh, true
	}
	savedMode := map[bool]string{true: "fresh", false: "fast"}[s.SQLFresh]
	if s.SQLFreshSaved {
		fmt.Fprintf(out, "Saved for %q: %s.\n", s.Name, savedMode)
	} else {
		fmt.Fprintf(out, "Nothing is saved for %q: it is %s, the default.\n", s.Name, savedMode)
	}
	// What the router applies: read until it agrees with what was just
	// saved, for a little while, since it reads the choice every few
	// seconds. A plain look reads once.
	deadline := time.Now().Add(wait)
	for {
		var v ctlRouterView
		if err := c.do(ctx, http.MethodGet, "/api/router", nil, &v); err != nil {
			fmt.Fprintf(out, "The router could not be read (%v), so what it applies cannot be said here.\n", err)
			return nil
		}
		applied, note := v.routerApplies(s)
		forced := v.Status != nil && (v.Status.SQLModeForced != "" || v.Status.Fresh != nil && *v.Status.Fresh)
		if applied == savedMode || applied == "" || forced || want == "" || !time.Now().Before(deadline) {
			switch {
			case applied == "":
				fmt.Fprintln(out, note)
			case applied == savedMode:
				fmt.Fprintf(out, "The router answers it in %s mode.\n", applied)
				if note != "" {
					fmt.Fprintln(out, note)
				}
			case note != "":
				fmt.Fprintf(out, "The router answers it in %s mode. %s\n", applied, note)
			default:
				fmt.Fprintf(out, "The router still answers it in %s mode. It reads the choice every few seconds:  bintrail-console port mode %s\n", applied, mysqlLineUser(s.Name))
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(portModePoll):
		}
	}
}

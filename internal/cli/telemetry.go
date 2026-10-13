package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/telemetry"
)

var telFormat string

// AddTelemetryCommand registers the telemetry control surface on root. Every
// binary that can report registers it, so `telemetry off` works from whichever
// one the operator happens to have on PATH.
func AddTelemetryCommand(root *cobra.Command) {
	root.AddCommand(telemetryCmd)
}

// TelemetryHook wires usage telemetry into one binary's root command: a binary
// declares one, starts a span from its root PersistentPreRunE, and runs its
// command tree through Execute.
//
// This is deliberately the ONLY instrumentation point. Cobra funnels every
// subcommand through the root's PersistentPreRunE, so a new command needs no
// telemetry code at all. The flip side: cobra runs only the CLOSEST hook, so a
// subcommand that defined its own PersistentPreRunE would silently
// un-instrument its whole subtree. A CI guard for that is tracked in #1061 —
// until it lands, nothing but review catches it.
type TelemetryHook struct {
	client *telemetry.Client
	span   *telemetry.Span
}

// processClient is the telemetry client Start resolved for THIS process.
// Published at package level because long-running commands implemented in this
// package (shim) need the daemon beacon loop (telemetry.Client.RunDaemon) but
// cannot reach the binary's hook variable — cliapp, consoleapp and
// cmd/bintrail-pg each hold their own. One process, one Start, one client, so
// a package-level publication is exact. Nil (an exempt command, or Start never
// ran) is safe: every telemetry.Client method tolerates a nil receiver.
var processClient *telemetry.Client

// Start resolves consent and begins recording the command about to run, unless
// the command is one of the exempt trees (see uninstrumented).
func (h *TelemetryHook) Start(cmd *cobra.Command) {
	if uninstrumented(cmd) {
		return
	}
	h.client = telemetry.Init(telemetry.Config{Flag: telemetryFlagValue(cmd)})
	processClient = h.client
	h.span = h.client.RecordCommand(commandPath(cmd))
}

// SetClientForTest injects a resolved client into the hook and the package
// seam, returning a restore func. Test support ONLY (the ext.ResetForTest
// pattern): the per-daemon wiring tests (#1362) drive a REAL daemon run
// function — runStream, runServe, runShim, … — and need an observable client
// in place of the one Start would resolve from the live environment.
// Production code resolves the client exclusively through Start.
func (h *TelemetryHook) SetClientForTest(c *telemetry.Client) (restore func()) {
	prevHook, prevProcess := h.client, processClient
	h.client, processClient = c, c
	return func() { h.client, processClient = prevHook, prevProcess }
}

// uninstrumented reports whether cmd belongs to a tree that must not be
// recorded. Two kinds:
//
// The `telemetry` subtree — initialising there would drain and deliver the
// spooled backlog moments before `telemetry off` discards it, so an operator
// opting out would watch the tool phone home on its way out.
//
// Cobra's completion machinery, which is not user work. `__complete` is an
// ordinary child of root, so the root hook fires for it: every TAB press in a
// shell with completion installed would spool an event, attempt a POST, and pay
// up to shutdownGrace of latency on a keystroke. It would also poison the
// aggregates — sanitizeCommand rejects underscores, so `__complete` lands in
// "other", which would then be the largest bucket in the dataset on any
// developer's machine with nothing to distinguish it from a real unknown
// command.
func uninstrumented(cmd *cobra.Command) bool {
	// Walk to the top-level command (the direct child of root).
	top := cmd
	for top.HasParent() && top.Parent().HasParent() {
		top = top.Parent()
	}
	if !top.HasParent() {
		return false // the root itself
	}
	name := top.Name()
	return name == telemetryCmd.Name() ||
		name == "help" ||
		name == "completion" ||
		strings.HasPrefix(name, "__") // __complete, __completeNoDesc
}

// Client exposes the resolved client, for long-running commands that need the
// daemon loop (telemetry.Client.RunDaemon). Nil when the command is exempt or
// Start was never called; every Client method tolerates that.
func (h *TelemetryHook) Client() *telemetry.Client { return h.client }

// Execute runs the command tree and records how it ended.
//
// A panic is recorded as an internal error and then re-raised: the panic value
// and the exit status are unchanged and the original frames are preserved,
// though Go marks the trace "[recovered, repanicked]" and prepends this
// function's own two frames. The panic VALUE is never recorded — panic messages
// routinely carry DSNs, table names and file paths.
func (h *TelemetryHook) Execute(root *cobra.Command) (err error) {
	defer func() {
		if r := recover(); r != nil {
			h.span.SetError(telemetry.ClassInternal)
			h.span.Finish()
			h.client.Shutdown()
			panic(r)
		}
	}()
	err = root.Execute()
	if err != nil {
		h.span.SetError(telemetry.ClassifyError(err))
	}
	h.span.Finish()
	// Give any in-flight delivery of EARLIER runs' events a bounded moment to
	// land. Without it a short command's process exits before its own detached
	// drain can finish, and telemetry never delivers at all for anyone whose
	// commands are quick. Costs nothing when there is no backlog.
	h.client.Shutdown()
	return err
}

// commandPath renders the invoked command as a hyphenated path — "archive
// reconcile" becomes "archive-reconcile" — so sibling subcommands stay
// distinguishable in the aggregates. It is built from cobra command NAMES,
// which are compile-time constants, and so can never carry an argument or a
// flag value.
func commandPath(cmd *cobra.Command) string {
	var parts []string
	for c := cmd; c != nil && c.HasParent(); c = c.Parent() {
		parts = append([]string{c.Name()}, parts...)
	}
	if len(parts) == 0 {
		return "root"
	}
	return strings.Join(parts, "-")
}

var telemetryCmd = &cobra.Command{
	Use:   "telemetry",
	Short: "Inspect and control usage telemetry",
	Long: `Inspect and control bintrail's usage telemetry.

Telemetry is metadata-only: command names, version, OS/arch and a bounded
error class. No identifier is stored or sent, and never your data, schemas,
tables, DSNs, hostnames, IPs or file paths. Run "telemetry show" to see the
exact bytes that would be sent.

Precedence, highest first: DO_NOT_TRACK, --telemetry=on|off,
BINTRAIL_TELEMETRY, the config file, then the default (on).`,
}

var telemetryStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show whether telemetry is on, and what decided it",
	RunE: func(cmd *cobra.Command, args []string) error {
		if !cliutil.IsValidOutputFormat(telFormat) {
			return fmt.Errorf("invalid --format %q: must be text or json", telFormat)
		}
		dir, dirErr := telemetry.ConfigDir()
		decision := telemetry.Resolve(telemetryFlagValue(cmd), dir)
		isCI := telemetry.IsCI()
		ep := telemetry.Endpoint()

		// Reporting requires consent AND a build that can send AND not being in
		// CI. Keep the reasons separate so "off" is never mysterious.
		reporting := decision.Enabled && !isCI && ep != "" && dirErr == nil

		if telFormat == "json" {
			// With no home directory there is no spool; emit empty rather than
			// a bare relative path that looks like a real location.
			spool, dirProblem := "", ""
			if dirErr == nil {
				spool = telemetry.SpoolDir(dir)
			} else {
				dirProblem = dirErr.Error()
			}
			return cliutil.OutputJSON(map[string]any{
				"reporting":        reporting,
				"consent":          decision.Enabled,
				"decided_by":       string(decision.Source),
				"endpoint_set":     ep != "",
				"ci_detected":      isCI,
				"config_dir":       dir,
				"config_dir_error": dirProblem,
				"spool_dir":        spool,
				"schema_version":   telemetry.SchemaVersion,
			})
		}

		state := "OFF"
		if reporting {
			state = "ON"
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Telemetry:    %s\n", state)
		if dirErr != nil && decision.Source == telemetry.SourceDefault {
			// "on, by default" would be a guess: the file that may hold an
			// explicit choice could not be looked for.
			fmt.Fprintln(cmd.OutOrStdout(), "Consent:      not readable (the consent file's folder is unavailable, see below)")
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "Consent:      %s (decided by: %s)\n",
				onOff(decision.Enabled), decision.Source)
		}
		if ep == "" {
			fmt.Fprintln(cmd.OutOrStdout(), "Endpoint:     not compiled in; this build cannot send anything")
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "Endpoint:     %s\n", ep)
		}
		if isCI {
			fmt.Fprintln(cmd.OutOrStdout(), "CI detected:  yes; reporting suppressed regardless of consent")
		}
		if dirErr != nil {
			// The cause is printed as it is: "no home directory" and "the
			// override is not an absolute path" are fixed in different places.
			fmt.Fprintf(cmd.OutOrStdout(), "Config dir:   unavailable (%v); telemetry disabled\n", dirErr)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "Spool:        %s\n", telemetry.SpoolDir(dir))
		}
		return nil
	},
}

var telemetryShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Print the exact JSON that would be sent (sends nothing)",
	Long: `Print a representative event and the daily beacon of a long-running
command, byte for byte as they would be transmitted.

This command performs no network access. It exists so the payload can be
inspected without trusting the documentation: what you see here is the
complete set of fields the wire format permits.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		out, err := telemetry.SampleEventJSON()
		if err != nil {
			return fmt.Errorf("render sample event: %w", err)
		}
		// A missing config directory is not an error here: the beacon is then
		// rendered as a first one, which is what such a machine would send if
		// it could send at all.
		dir, _ := telemetry.ConfigDir()
		beacon, err := telemetry.SampleBeaconJSON(dir)
		if err != nil {
			return fmt.Errorf("render sample beacon: %w", err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "%s\n\nA long-running command (watch, stream, ...) also sends this once a day:\n%s\n\nFields permitted on the wire: %v\n",
			out, beacon, telemetry.AllowedFields)
		fmt.Fprintf(cmd.OutOrStdout(), "Sent to: %s\n", endpointDescription())
		return nil
	},
}

var telemetryOnCmd = &cobra.Command{
	Use:   "on",
	Short: "Enable usage telemetry",
	RunE:  func(cmd *cobra.Command, args []string) error { return setTelemetry(cmd, true) },
}

var telemetryOffCmd = &cobra.Command{
	Use:   "off",
	Short: "Disable usage telemetry",
	RunE:  func(cmd *cobra.Command, args []string) error { return setTelemetry(cmd, false) },
}

func setTelemetry(cmd *cobra.Command, enabled bool) error {
	dir, err := telemetry.ConfigDir()
	if err != nil {
		return fmt.Errorf("locate config directory: %w", err)
	}
	if err := telemetry.SetEnabled(dir, enabled); err != nil {
		return err
	}
	if !enabled {
		// Events spooled before this decision would otherwise sit on disk
		// forever: the drain only ever runs while telemetry is ENABLED, so
		// nothing would ever send them or age them out. Reported as an error
		// rather than swallowed — telling an operator "nothing will be sent"
		// while their events are still on disk is the kind of half-truth this
		// whole feature cannot afford.
		if err := telemetry.PurgeSpool(dir); err != nil {
			return fmt.Errorf("telemetry is now off, but discarding already-spooled events failed: %w", err)
		}
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Telemetry %s (recorded in %s).\n",
		onOff(enabled), telemetry.StatePath(dir))
	if !enabled {
		fmt.Fprintln(cmd.OutOrStdout(),
			"Nothing further will be recorded or sent, and anything already\n"+
				"spooled locally has been discarded.\n"+
				"To disable telemetry for every tool on this machine, set DO_NOT_TRACK=1.")
	}
	return nil
}

// telemetryFlagValue reads the root --telemetry flag if the binary defines one.
// Absent flag means "unset", which falls through to the next control.
func telemetryFlagValue(cmd *cobra.Command) string {
	if f := cmd.Root().PersistentFlags().Lookup("telemetry"); f != nil {
		return f.Value.String()
	}
	return ""
}

func endpointDescription() string {
	if ep := telemetry.Endpoint(); ep != "" {
		return ep
	}
	return "nowhere; this build has no endpoint compiled in"
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func init() {
	telemetryStatusCmd.Flags().StringVar(&telFormat, "format", "text", "Output format: text or json")
	telemetryCmd.AddCommand(telemetryStatusCmd, telemetryShowCmd, telemetryOnCmd, telemetryOffCmd)
}

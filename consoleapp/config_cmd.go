package consoleapp

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"text/tabwriter"
	"unicode"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/rotation"
)

// `config check` reads /etc/dbtrail/dbtrail.cnf the way DBTrail reads it when
// it starts, and says what every line that does not do what it looks like it
// does is (#2671). A program that starts with such a line starts anyway and
// prints a warning: an option file left over from an older version must not
// stop capture. This is where the same lines are an error, so a deploy can
// fail on them.

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Check DBTrail's option file, /etc/dbtrail/dbtrail.cnf, and list the settings in effect",
}

var configCheckCmd = &cobra.Command{
	Use:   "check",
	Short: "Say which lines of /etc/dbtrail/dbtrail.cnf are not used, and why",
	Long: `Reads /etc/dbtrail/dbtrail.cnf as DBTrail does when it starts, and lists every
line that is not used: an option DBTrail does not have (with the nearest one),
an option with no value, an option the environment already sets, an option
for a setting a value saved in the web interface overrides (index rotation,
the backup settings), an include.
It never prints a value.

Exits 1 when it finds one, so a deploy can stop on it. A program that starts
with such a line starts anyway and prints the same warning.

Whether a line is hidden by the environment depends on the environment, so
run it where DBTrail runs: with the Docker install, the dbtrail command already
runs it inside the container. Under systemd, the service's environment is not
the one of your shell.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		cli.LoadEnvFile()
		r := cli.CheckOptionFile()
		path := os.Getenv("BINTRAIL_CONSOLE_SERVERS")
		if path == "" {
			path = console.DefaultRegistryPath()
		}
		problems, note := savedOverrides(r, path)
		r.Problems = append(r.Problems, problems...)
		if note != "" {
			r.Notes = append(r.Notes, note)
		}
		return reportOptionFile(cmd.OutOrStdout(), r)
	},
}

var configShowAll bool

var configShowCmd = &cobra.Command{
	Use:   "show",
	Short: "List the settings in effect and where each one comes from",
	Long: `Lists every DBTrail setting that has a value, with the value and where it
comes from: the environment, an env file (.bintrail.env or config.env), or a
line of /etc/dbtrail/dbtrail.cnf. A setting that holds a password, a token or a
key is listed without its value, and so is any value that looks like it
carries a login or a token. --all also lists the settings with no value,
which each program then decides by itself.

It shows what a program started now, with this environment, would read. A
running program may have been started with another environment, or with
command-line flags, which come before all of these: with the Docker install,
the dbtrail command already runs this inside the container. Under systemd, the
service's environment is not the one of your shell.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		cli.LoadEnvFile()
		reportSettings(cmd.OutOrStdout(), cli.SettingsInEffect(os.Environ(), os.LookupEnv), configShowAll)
		return nil
	},
}

func init() {
	configShowCmd.Flags().BoolVar(&configShowAll, "all", false, "also list the settings with no value")
	configCmd.AddCommand(configCheckCmd, configShowCmd)
	rootCmd.AddCommand(configCmd)
}

// hiddenValue is what config show prints for a secret's value.
const hiddenValue = "(not shown: a secret)"

// hiddenLoginValue is what it prints for a value of another setting that
// looks like it carries a login or a token (see carriesLogin).
const hiddenLoginValue = "(not shown: it looks like it carries a login or a token)"

// carriesLogin matches what puts a login or a token in a value, anywhere in
// it: user:password@ or :password@ (a URL's user information, a DSN such as
// u:p@tcp(host:3306)/db), a URL whose user part is a token
// (https://TOKEN@host), and a query parameter whose name says token,
// password, secret, key, signature, credential or auth (?token=, the
// console's own URL with its token; &X-Amz-Signature=, a presigned URL).
var carriesLogin = regexp.MustCompile(`(?i)[^\s/:@]*:[^\s@]*@|://[^\s/@]+@|[?&;][^=&\s]*(token|passw|pwd|secret|key|sig|credential|auth)[^=&\s]*=`)

// showValue is a value as config show prints it: as it is, unless it has a
// character that would break the line or read as something else, which is
// then quoted.
func showValue(v string) string {
	for _, r := range v {
		if !unicode.IsPrint(r) || r == '\t' {
			return strconv.Quote(v)
		}
	}
	if strings.TrimSpace(v) != v {
		return strconv.Quote(v)
	}
	return v
}

func reportSettings(w io.Writer, r cli.SettingsReport, all bool) {
	if len(r.Set) == 0 {
		fmt.Fprintln(w, "No setting has a value: every program decides each one by itself (its default).")
	} else {
		fmt.Fprintf(w, "%s with a value:\n", plural(len(r.Set), "setting"))
		tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', 0)
		for _, s := range r.Set {
			v := hiddenValue
			switch {
			case s.Secret:
			case carriesLogin.MatchString(s.Value):
				// A setting not listed as a secret can still be given a URL
				// or a DSN with a login in it: not printed either.
				v = hiddenLoginValue
			default:
				v = showValue(s.Value)
			}
			fmt.Fprintf(tw, "  %s\t%s\t%s\n", s.Option, v, s.Where())
		}
		tw.Flush()
	}
	for _, s := range r.Set {
		if s.CommandsUse != "" {
			fmt.Fprintf(w, "Note: the dbtrail commands that talk to a running DBTrail (server, port, snapshot) ignore env files for %s, so they use %s.\n",
				s.Option, s.CommandsUse)
		}
	}
	if len(r.Empty) > 0 {
		fmt.Fprintf(w, "Set to nothing in the environment, which counts as no value. A line of %s still applies to them; a line of an env file does not: %s.\n",
			cli.OptionFilePath, strings.Join(r.Empty, ", "))
	}
	if len(r.EmptyInEnvFile) > 0 {
		where := cli.EnvFileUsed()
		if where == "" {
			where = "an env file"
		}
		fmt.Fprintf(w, "Set to nothing in %s, which counts as no value. A line of %s still applies to them: %s.\n",
			where, cli.OptionFilePath, strings.Join(r.EmptyInEnvFile, ", "))
	}
	if all {
		if len(r.Unset) > 0 {
			fmt.Fprintf(w, "%s with no value, which each program decides by itself:\n", plural(len(r.Unset), "setting"))
			for _, o := range r.Unset {
				fmt.Fprintf(w, "  %s\n", o)
			}
		}
	} else if n := len(r.Unset) + len(r.Empty) + len(r.EmptyInEnvFile); n > 0 {
		word := "more setting"
		if len(r.Set) == 0 {
			word = "setting"
		}
		fmt.Fprintf(w, "%s with no value. dbtrail config show --all lists them.\n", plural(n, word))
	}
	for _, k := range r.Unknown {
		in := ""
		if p := r.UnknownIn[k]; p != "" {
			in = " (in " + p + ")"
		}
		fmt.Fprintf(w, "Note: %s is set%s, and no DBTrail program reads it. Is the name right?\n", k, in)
	}
	for _, k := range r.Retired {
		fmt.Fprintf(w, "Note: %s is set, and no longer does anything.\n", k)
	}
	fmt.Fprintln(w, "Command-line flags given to a running program come before all of these and are not seen here.")
}

func reportOptionFile(w io.Writer, r cli.OptionReport) error {
	if !r.Found {
		fmt.Fprintf(w, "There is no %s, so there is nothing to check. DBTrail uses its defaults and the environment.\n", r.Path)
		return nil
	}
	if r.Ignored != "" {
		fmt.Fprintf(w, "%s %s\n", r.Path, r.Ignored)
		return fmt.Errorf("%s is not used", r.Path)
	}
	fmt.Fprintf(w, "%s sets %s.\n", r.Path, plural(len(r.Applied), "setting"))
	for _, a := range r.Applied {
		fmt.Fprintf(w, "  line %d: %s\n", a.Line, a.Option)
	}
	for _, n := range r.Notes {
		fmt.Fprintf(w, "Note: %s\n", n)
	}
	if len(r.Problems) == 0 {
		fmt.Fprintln(w, "Every line is used.")
		return nil
	}
	fmt.Fprintf(w, "%s:\n", plural(len(r.Problems), "problem"))
	for _, p := range r.Problems {
		fmt.Fprintf(w, "  %s\n", p)
	}
	return fmt.Errorf("%s has %s", r.Path, plural(len(r.Problems), "problem"))
}

// savedInTheWeb are the settings the web interface can also save, where the
// saved value comes before the environment, and so before the option file,
// which reaches the programs through it (#2671): index rotation and the
// daemon's backup settings. A line of the file for one of them is not used
// while a value is saved there. key is the backup setting's name; empty for
// rotation, whose policy is saved as one.
var savedInTheWeb = []struct {
	env, key, where string
}{
	{"BINTRAIL_ROTATE_RETAIN", "", "a rotation policy saved in the web interface (Settings, Retention, Edit rotation)"},
	{"BINTRAIL_ROTATE_INTERVAL", "", "a rotation policy saved in the web interface (Settings, Retention, Edit rotation)"},
	{"BINTRAIL_ROTATE_ADD_FUTURE", "", "a rotation policy saved in the web interface (Settings, Retention, Edit rotation)"},
	{"BINTRAIL_CONSOLE_BASELINE_RETAIN", console.BackupSettingBaselineRetain, "a value saved in the web interface (Snapshots, settings)"},
	{"BINTRAIL_CONSOLE_BASELINE_LOCK_MODE", console.BackupSettingLockMode, "a value saved in the web interface"},
	{"BINTRAIL_CONSOLE_BASELINE_STAGING", console.BackupSettingStagingDir, "a value saved in the web interface"},
	{"BINTRAIL_CONSOLE_VERIFY_TABLES", console.BackupSettingVerifyTables, "a value saved in the web interface"},
}

// savedOverrides says which lines the option file applies are not used
// because the web interface saved a value for their setting, reading the
// server list at serversPath, where those values are kept. note, when not
// empty, says the list could not be read, so nothing was compared. No value
// is printed.
func savedOverrides(r cli.OptionReport, serversPath string) (problems []string, note string) {
	if len(r.Applied) == 0 {
		return nil, ""
	}
	var reg *console.Registry
	load := func() bool {
		if reg != nil {
			return true
		}
		var err error
		if reg, err = console.LoadRegistry(serversPath); err != nil {
			note = fmt.Sprintf("the server list (%s) could not be read, so the lines were not compared with what the web interface saved: %v", serversPath, err)
			return false
		}
		return true
	}
	for _, a := range r.Applied {
		for _, w := range savedInTheWeb {
			if w.env != a.Env {
				continue
			}
			if !load() {
				return problems, note
			}
			saved := false
			if w.key == "" {
				// The policy the rotation loop runs: one it would refuse is
				// not in force, and the startup values are (effectiveRotation).
				if rc, ok := reg.Rotation(); ok {
					st, err := rotation.ParseSettings(rc.Retain, rc.Interval, rc.AddFuture, true)
					saved = err == nil && st.Enabled
				}
			} else if v, ok := reg.BackupSettings().Get(w.key); ok {
				// A working folder saved empty hands it back to the variable.
				saved = v != "" || w.key != console.BackupSettingStagingDir
			}
			if saved {
				problems = append(problems, fmt.Sprintf("line %d: %s is not used by the capture and web service (watch), because %s comes first. Change it there, or remove this line.", a.Line, a.Option, w.where))
			}
		}
	}
	return problems, note
}

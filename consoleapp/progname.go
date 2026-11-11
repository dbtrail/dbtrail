package consoleapp

import (
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
)

// The program is installed under two names: bintrail-console, the one the
// service runs as, and dbtrail, a link to it that people type. Help, messages
// and errors that tell someone what to run next must name the one they just
// typed, or the next command they are handed is one they have never seen.
//
// The texts are written once, with "bintrail-console <verb>", and rewritten
// where they leave the program (useProgName): the help, standard output of
// the commands that talk to the daemon, and the error Main prints.

// canonicalProg is the name the texts are written with.
const canonicalProg = "bintrail-console"

// prog is the name this process was started as.
var prog = programName(os.Args)

// progNameEnv lets a caller that runs this program on someone's behalf say
// what that someone typed. The dbtrail command of a Docker install runs
// bintrail-console inside the container and sets it to dbtrail.
const progNameEnv = "DBTRAIL_PROG"

var progNameShape = regexp.MustCompile(`^[a-z][a-z0-9-]{0,30}$`)

// programName is the name in progNameEnv when it is a plain word, else the
// base name of the command line's first word. A test binary, or an empty
// command line, is the canonical name.
func programName(args []string) string {
	if name := os.Getenv(progNameEnv); progNameShape.MatchString(name) {
		return name
	}
	if len(args) == 0 {
		return canonicalProg
	}
	name := filepath.Base(args[0])
	name = strings.TrimSuffix(name, ".exe")
	if name == "" || name == "." || name == string(filepath.Separator) || strings.HasSuffix(name, ".test") {
		return canonicalProg
	}
	return name
}

// progVerbs matches the program's name where it is a command to run: right
// before one of its verbs. The name alone is left as it is, because it also
// names the service (`systemctl status bintrail-console`) and the package.
var progVerbs = regexp.MustCompile(`(^|[^A-Za-z0-9_-])bintrail-console( +)(server|snapshot|port|user|watch|serve|router|telemetry|sql-compare|completion|help|--help|--version)\b`)

// useProgName rewrites the commands a text names to the name this process
// was started as.
func useProgName(text string) string {
	if prog == canonicalProg {
		return text
	}
	return progVerbs.ReplaceAllString(text, "${1}"+prog+"${2}${3}")
}

// progWriter applies useProgName to what a command prints. Each message is
// one Write, so a name is never split across two.
type progWriter struct{ w io.Writer }

func (p progWriter) Write(b []byte) (int, error) {
	if prog == canonicalProg {
		return p.w.Write(b)
	}
	if _, err := io.WriteString(p.w, useProgName(string(b))); err != nil {
		return 0, err
	}
	return len(b), nil
}

// cmdOut is where the commands that talk to the daemon print.
func cmdOut() io.Writer { return progWriter{os.Stdout} }

// applyProgName gives the command tree the name this process was started
// as: the usage line, and every help text that names a command to run.
func applyProgName(root *cobra.Command) {
	if prog == canonicalProg {
		return
	}
	root.Use = prog
	var walk func(*cobra.Command)
	walk = func(c *cobra.Command) {
		c.Short, c.Long, c.Example = useProgName(c.Short), useProgName(c.Long), useProgName(c.Example)
		for _, sub := range c.Commands() {
			walk(sub)
		}
	}
	walk(root)
}

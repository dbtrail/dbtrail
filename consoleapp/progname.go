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
// what that someone can type. The dbtrail command of a Docker install runs
// bintrail-console inside the container and sets it to dbtrail when that
// word runs it, or else to its own absolute path on the host: until the
// person links it into their PATH, a suggested "dbtrail snapshot run" would
// be a command not found.
const progNameEnv = "DBTRAIL_PROG"

var progNameShape = regexp.MustCompile(`^[a-z][a-z0-9-]{0,30}$`)

// progPathShape is an absolute path: no control characters, quotes,
// backslashes, dollar signs or backticks, which a shell would read as
// something else, and a last element of its own.
var progPathShape = regexp.MustCompile("^/[^\\x00-\\x1f\\x7f'\"\\\\$`]{0,1000}[^/\\x00-\\x1f\\x7f'\"\\\\$`]$")

// shellWordUnsafe is what a shell would split or expand in a path, escaped
// with a backslash so the path prints as one word that can be pasted.
var shellWordUnsafe = regexp.MustCompile(`[^A-Za-z0-9_./+:@%,=-]`)

// programName is the name in progNameEnv when it is a plain word or an
// absolute path (printed as one shell word), else the base name of the
// command line's first word. A test binary, or an empty command line, is
// the canonical name.
func programName(args []string) string {
	name := os.Getenv(progNameEnv)
	if progNameShape.MatchString(name) {
		return name
	}
	if progPathShape.MatchString(name) {
		return shellWordUnsafe.ReplaceAllStringFunc(name, func(c string) string { return `\` + c })
	}
	if len(args) == 0 {
		return canonicalProg
	}
	name = filepath.Base(args[0])
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
	// A path may hold a $ no more, but the replacement reads $ as a group.
	return progVerbs.ReplaceAllString(text, "${1}"+strings.ReplaceAll(prog, "$", "$$")+"${2}${3}")
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
	// cobra takes the root's name up to its first space, so a path that
	// holds one (escaped or not) would be cut: the usage line then names
	// the file alone. The texts below still name the whole path.
	root.Use = prog
	if strings.Contains(prog, " ") {
		root.Use = filepath.Base(strings.ReplaceAll(prog, `\`, ""))
	}
	var walk func(*cobra.Command)
	walk = func(c *cobra.Command) {
		c.Short, c.Long, c.Example = useProgName(c.Short), useProgName(c.Long), useProgName(c.Example)
		for _, sub := range c.Commands() {
			walk(sub)
		}
	}
	walk(root)
}

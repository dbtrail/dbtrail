package cli

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/dbtrail/dbtrail/internal/mycnf"
	"github.com/dbtrail/dbtrail/internal/settings"
)

// OptionFilePath is the option file every DBTrail program reads when it
// starts (#2671). It is the only place one is looked for: never the current
// folder, so what decides where a command sends a password cannot be chosen
// by whoever prepared the folder it is run in. The compose file mounts the
// operator's dbtrail.cnf here.
const OptionFilePath = "/etc/dbtrail/dbtrail.cnf"

// OptionGroup is the group of the file DBTrail reads. Other groups are left
// alone, so one file can hold sections for other programs.
const OptionGroup = "dbtrail"

// optionFilePath is OptionFilePath, changed only by tests.
var optionFilePath = OptionFilePath

// OptionReport is what reading the option file found. It never holds a
// value: a value may be a password.
type OptionReport struct {
	Path string
	// Found: there is something at Path.
	Found bool
	// Ignored, when not empty, says why nothing in the file was used.
	Ignored string
	// Applied are the options that set a setting, in file order.
	Applied []AppliedOption
	// Problems are lines that do not do what they look like they do, one
	// sentence each, with the line number. `config check` fails on any.
	Problems []string
	// Notes are true and harmless: a line a later line overrides.
	Notes []string

	values map[string]string
}

// AppliedOption is one setting the file set.
type AppliedOption struct {
	Env    string
	Option string
	Line   int
}

// ReadOptionFile reads the option file at path and decides what each line
// does, without changing the environment. lookupEnv says what is already set;
// a setting already set to something is not taken from the file (one set to
// the empty string is). fromEnvFile says which of
// those came from an env file, so a problem can name where the value is.
func ReadOptionFile(path string, lookupEnv func(string) (string, bool), fromEnvFile func(string) bool) OptionReport {
	r := OptionReport{Path: path}
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return r
	}
	r.Found = true
	if err != nil {
		r.Ignored = fmt.Sprintf("cannot be read: %v", err)
		return r
	}
	if info.IsDir() {
		r.Ignored = "is a directory, not a file"
		return r
	}
	// As MySQL does with a my.cnf: a file anyone on the machine can change
	// could point capture at another server, so it is not used at all.
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o002 != 0 {
		r.Ignored = fmt.Sprintf("can be changed by every user on this machine, so it is not used. Fix it with:  chmod o-w %s", path)
		return r
	}
	data, err := os.ReadFile(path)
	if err != nil {
		r.Ignored = fmt.Sprintf("cannot be read: %v", err)
		return r
	}
	lines, err := mycnf.Options(data, OptionGroup)
	if err != nil {
		r.Ignored = fmt.Sprintf("is not a valid option file (%v), so none of it is used", err)
		return r
	}

	for _, n := range lines.Ungrouped {
		r.Problems = append(r.Problems, fmt.Sprintf("line %d is an option before any [group], so it is ignored. Put it under [%s].", n, OptionGroup))
	}
	for _, n := range lines.Includes {
		r.Problems = append(r.Problems, fmt.Sprintf("line %d is an include, and DBTrail does not follow includes, so the file it names is not read.", n))
	}

	// The last line of an option wins, as in MySQL; an earlier one is a note.
	last := map[string]int{}
	for i, o := range lines.Options {
		last[o.Name] = i
	}
	secretsRead := []string{}
	r.values = map[string]string{}
	for i, o := range lines.Options {
		if o.Name == "" {
			r.Problems = append(r.Problems, fmt.Sprintf("line %d has no option name before the = sign, so it is ignored.", o.Line))
			continue
		}
		e, ok := settings.ByOption(o.Name)
		if !ok {
			if o.Loose {
				continue
			}
			// A line with no = sign is not quoted: it may be a password
			// pasted on a line of its own.
			what := fmt.Sprintf("line %d: %q", o.Line, o.Name)
			if !o.HasValue {
				what = fmt.Sprintf("line %d, which has no = sign,", o.Line)
			}
			msg := fmt.Sprintf("%s is not a DBTrail option, so it is ignored.", what)
			if near := settings.Nearest(o.Name); near != "" {
				msg = fmt.Sprintf("%s is not a DBTrail option, so it is ignored. Did you mean %s?", what, near)
			}
			r.Problems = append(r.Problems, msg)
			continue
		}
		if e.Kind != settings.Setting {
			r.Problems = append(r.Problems, fmt.Sprintf("line %d: %s %s, so it is ignored.", o.Line, o.Name, kindRefusal(e.Kind)))
			continue
		}
		if !o.HasValue {
			r.Problems = append(r.Problems, fmt.Sprintf("line %d: %s has no value, so it is ignored. Write it as %s = <value>.", o.Line, o.Name, o.Name))
			continue
		}
		if last[o.Name] != i {
			r.Notes = append(r.Notes, fmt.Sprintf("line %d: %s is set again on line %d, which is the one used.", o.Line, o.Name, lines.Options[last[o.Name]].Line))
			continue
		}
		// An empty variable does not count as set: the compose file passes
		// many as ${NAME:-}, empty when the operator chose nothing.
		if v, set := lookupEnv(e.Env); set && v != "" {
			where := "the environment DBTrail was started with"
			if fromEnvFile(e.Env) {
				where = "an env file (.bintrail.env or config.env)"
			}
			r.Problems = append(r.Problems, fmt.Sprintf("line %d: %s is not used, because %s sets %s, and that comes first.", o.Line, o.Name, where, e.Env))
			continue
		}
		r.Applied = append(r.Applied, AppliedOption{Env: e.Env, Option: o.Name, Line: o.Line})
		r.values[e.Env] = o.Value
		if e.Secret {
			secretsRead = append(secretsRead, fmt.Sprintf("%s (line %d)", o.Name, o.Line))
		}
	}
	if len(secretsRead) > 0 && runtime.GOOS != "windows" && info.Mode().Perm()&0o004 != 0 {
		r.Problems = append(r.Problems, fmt.Sprintf("every user on this machine can read the file, and it holds %s, which can carry a password. Fix it with:  chmod o-r %s", strings.Join(secretsRead, ", "), path))
	}
	return r
}

func kindRefusal(k settings.Kind) string {
	switch k {
	case settings.Internal:
		return "is set by DBTrail itself for its own processes, never by a file"
	case settings.Retired:
		return "no longer does anything"
	case settings.Test:
		return "is read by DBTrail's tests only"
	}
	return "cannot be set from a file"
}

// Lines is every problem and the reason the file was ignored, prefixed with
// the file's path, for a program to print when it starts.
func (r OptionReport) Lines() []string {
	var out []string
	if r.Ignored != "" {
		out = append(out, fmt.Sprintf("%s %s", r.Path, r.Ignored))
	}
	for _, p := range r.Problems {
		out = append(out, fmt.Sprintf("%s %s", r.Path, p))
	}
	return out
}

var (
	optionMu     sync.Mutex
	optionLoaded OptionReport
)

// loadOptionFile applies the option file to the environment, below every
// variable already set, and prints its problems. It runs once, in the same
// sync.Once as the env file and after it, so the order is: environment, then
// .bintrail.env or config.env, then dbtrail.cnf.
func loadOptionFile() {
	r := ReadOptionFile(optionFilePath, os.LookupEnv, FromEnvFile)
	for _, a := range r.Applied {
		os.Setenv(a.Env, r.values[a.Env])
	}
	for _, l := range r.Lines() {
		fmt.Fprintf(os.Stderr, "warning: %s\n", l)
	}
	optionMu.Lock()
	optionLoaded = r
	optionMu.Unlock()
}

// FromOptionFile reports the line of the option file that set key, when the
// option file is where its value came from.
func FromOptionFile(key string) (line int, ok bool) {
	optionMu.Lock()
	defer optionMu.Unlock()
	i := slices.IndexFunc(optionLoaded.Applied, func(a AppliedOption) bool { return a.Env == key })
	if i < 0 {
		return 0, false
	}
	return optionLoaded.Applied[i].Line, true
}

// CheckOptionFile reads the option file again, as the program would if it
// started now, for `config check`. A setting the file itself set when this
// process started is not counted as set by the environment.
func CheckOptionFile() OptionReport {
	lookup := func(k string) (string, bool) {
		if _, ok := FromOptionFile(k); ok {
			return "", false
		}
		return os.LookupEnv(k)
	}
	return ReadOptionFile(optionFilePath, lookup, FromEnvFile)
}

// UseOptionFileForTest points the option file at path, and returns the
// function that puts it back.
func UseOptionFileForTest(path string) (undo func()) {
	before := optionFilePath
	optionFilePath = path
	return func() { optionFilePath = before }
}

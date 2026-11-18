package cli

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/dbtrail/dbtrail/internal/settings"
)

// Where a setting's value came from, for `dbtrail config show` (#2671).
const (
	// FromEnvironment: the environment the program was started with.
	FromEnvironment = "environment"
	// FromEnvFileSource: .bintrail.env or config.env.
	FromEnvFileSource = "env file"
	// FromOptionFileSource: /etc/dbtrail/dbtrail.cnf.
	FromOptionFileSource = "option file"
)

// SettingInEffect is one operator setting that has a value, and where the
// value came from.
type SettingInEffect struct {
	Env    string
	Option string
	Secret bool
	// Value is the value. Never print it when Secret is set.
	Value string
	// Source is FromEnvironment, FromEnvFileSource or FromOptionFileSource.
	Source string
	// Path is the env file's or the option file's path; Line, the option
	// file's line.
	Path string
	Line int
	// CommandsUse is set for where the running DBTrail is (console-listen,
	// console-token, console-auth) when the value came from an env file:
	// the dbtrail commands that talk to it (server, port, snapshot) ignore
	// env files for these, so what they use is said here: the option file's
	// line that the env file hid, or nothing.
	CommandsUse string
}

// Where says where the value came from, in a few words.
func (s SettingInEffect) Where() string {
	switch s.Source {
	case FromOptionFileSource:
		return fmt.Sprintf("%s, line %d", s.Path, s.Line)
	case FromEnvFileSource:
		if s.Path == "" {
			return "an env file, as " + s.Env
		}
		return s.Path + ", as " + s.Env
	}
	return "the environment, as " + s.Env
}

// SettingsReport is what `dbtrail config show` prints.
type SettingsReport struct {
	// Set is every operator setting with a value, by option name.
	Set []SettingInEffect
	// Unset is every operator setting with no value, which its program then
	// decides by itself (its default), by option name.
	Unset []string
	// Empty are the operator settings whose variable is set to nothing. The
	// compose file passes many as ${NAME:-}, empty when nothing was chosen;
	// most programs read that as not set, and so does the option file, which
	// still applies a line for them.
	Empty []string
	// EmptyInEnvFile are the same, set to nothing by a line of the env file.
	EmptyInEnvFile []string
	// Unknown are BINTRAIL_ variables of the environment that no DBTrail
	// program reads: most likely a typo.
	Unknown []string
	// UnknownIn is, for an unknown name an env file set, that file.
	UnknownIn map[string]string
	// Retired are BINTRAIL_ variables set that no longer do anything.
	Retired []string
}

// SettingsInEffect reads, for every operator setting, its value and where it
// came from, for a program started now with this environment. environ is
// os.Environ(); lookup is os.LookupEnv. The env file and the option file must
// have been loaded (LoadEnvFile) for their values to be told apart from the
// environment's.
func SettingsInEffect(environ []string, lookup func(string) (string, bool)) SettingsReport {
	var r SettingsReport
	for _, e := range settings.All() {
		v, ok := lookup(e.Env)
		switch e.Kind {
		case settings.Setting:
		case settings.Retired:
			if ok && v != "" {
				r.Retired = append(r.Retired, e.Env)
			}
			continue
		default:
			// Internal and test names are not an operator's to set: they are
			// never listed.
			continue
		}
		if !ok {
			r.Unset = append(r.Unset, e.Option())
			continue
		}
		if v == "" {
			if FromEnvFile(e.Env) {
				r.EmptyInEnvFile = append(r.EmptyInEnvFile, e.Option())
			} else {
				r.Empty = append(r.Empty, e.Option())
			}
			continue
		}
		s := SettingInEffect{Env: e.Env, Option: e.Option(), Secret: e.Secret, Value: v, Source: FromEnvironment}
		if line, ok := FromOptionFile(e.Env); ok {
			s.Source, s.Path, s.Line = FromOptionFileSource, optionFilePath, line
		} else if FromEnvFile(e.Env) {
			s.Source, s.Path = FromEnvFileSource, EnvFileUsed()
			if slices.Contains(daemonAddressKeys, e.Env) {
				s.CommandsUse = "nothing: they use their default"
				if line, ok := optionFileShadowedLine(e.Env); ok {
					s.CommandsUse = fmt.Sprintf("line %d of %s", line, optionFilePath)
				}
			}
		}
		r.Set = append(r.Set, s)
	}
	for _, kv := range environ {
		k, _, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(k, "BINTRAIL_") {
			continue
		}
		if _, known := settings.Lookup(k); !known && !slices.Contains(r.Unknown, k) {
			r.Unknown = append(r.Unknown, k)
			if FromEnvFile(k) {
				if r.UnknownIn == nil {
					r.UnknownIn = map[string]string{}
				}
				r.UnknownIn[k] = EnvFileUsed()
				if r.UnknownIn[k] == "" {
					r.UnknownIn[k] = "an env file"
				}
			}
		}
	}
	slices.SortFunc(r.Set, func(a, b SettingInEffect) int { return strings.Compare(a.Option, b.Option) })
	slices.Sort(r.Unset)
	slices.Sort(r.Empty)
	slices.Sort(r.EmptyInEnvFile)
	slices.Sort(r.Unknown)
	slices.Sort(r.Retired)
	return r
}

// Origin says where key's value came from in this process, in words for a
// sentence: "line 4 of /etc/dbtrail/dbtrail.cnf (console-sql-memory)", "the
// env file /srv/dbtrail/.bintrail.env (BINTRAIL_CONSOLE_SQL_MEMORY)", or "the
// environment variable BINTRAIL_CONSOLE_SQL_MEMORY". "" when key is not set
// to anything. The env file and the option file must have been loaded
// (LoadEnvFile). A web interface that shows a setting as locked by where
// DBTrail starts names the place with it (#2671).
func Origin(key string) string {
	if v, ok := os.LookupEnv(key); !ok || v == "" {
		return ""
	}
	option := key
	if e, ok := settings.Lookup(key); ok {
		option = e.Option()
	}
	if line, ok := FromOptionFile(key); ok {
		return fmt.Sprintf("line %d of %s (%s)", line, optionFilePath, option)
	}
	if FromEnvFile(key) {
		if p := EnvFileUsed(); p != "" {
			return fmt.Sprintf("the env file %s (%s)", p, key)
		}
		return fmt.Sprintf("an env file (%s)", key)
	}
	return "the environment variable " + key
}

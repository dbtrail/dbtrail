package consoleapp

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/dbtrail/dbtrail/internal/telemetry"
	"github.com/go-sql-driver/mysql"
)

// indexDSNExample is the shape every index-DSN refusal shows, so a first run
// that got the format wrong sees one that works.
const indexDSNExample = "user:password@tcp(host:3306)/binlog_index"

// configError is a startup refusal caused by what the operator configured.
// It reaches telemetry as config_invalid instead of unknown, which is what a
// plain fmt.Errorf becomes.
type configError struct{ msg string }

func (e configError) Error() string          { return e.msg }
func (e configError) TelemetryClass() string { return telemetry.ClassConfigInvalid }

// settingError is a startup refusal of a setting the operator gave (a flag or
// its variable). Like configError it is config_invalid, which gets watch the
// startup page instead of a restart loop. Its message repeats the value given,
// which may be something pasted into the wrong variable (a DSN with its
// password), so a page anyone can reach shows public, the same words with
// that value taken out; the log and a trusted page show msg.
type settingError struct {
	msg    string
	public string
}

func (e settingError) Error() string          { return e.msg }
func (e settingError) TelemetryClass() string { return telemetry.ClassConfigInvalid }
func (e settingError) ShownToAnyone() bool    { return true }
func (e settingError) PublicMessage() string  { return e.public }

// quoted is a Go-quoted string as %q writes it, the way every one of these
// messages repeats a value.
var quoted = regexp.MustCompile(`"(?:[^"\\]|\\.)*"`)

// valueGiven replaces a value in a message a stranger can read.
const valueGiven = "(the value given)"

// configRefusal marks err, a refusal of a setting the operator gave, as a
// settingError, keeping its words. typed are the values the operator gave for
// the settings the message is about: every quoted piece of the message that
// is one of them, or part of one (a unit cut out of "7x"), is replaced for
// the public page. A number a message repeats unquoted comes from a numeric
// flag, which cannot hold anything else. An error that already carries its
// own class, or that comes from the network or the filesystem, is returned
// unchanged: this is for setting refusals only.
func configRefusal(err error, typed ...string) error {
	if err == nil {
		return nil
	}
	var classed telemetry.Classed
	var netErr net.Error
	var pathErr *fs.PathError
	if errors.As(err, &classed) || errors.As(err, &netErr) || errors.As(err, &pathErr) {
		return err
	}
	msg := err.Error()
	public := quoted.ReplaceAllStringFunc(msg, func(q string) string {
		inner, uerr := strconv.Unquote(q)
		if uerr != nil {
			return valueGiven
		}
		for _, v := range typed {
			if inner != "" && v != "" && strings.Contains(v, inner) {
				return valueGiven
			}
		}
		return q
	})
	return settingError{msg: msg, public: public}
}

// checkIndexDSN refuses a missing or malformed --index-dsn before anything
// waits on the network, with a message that says how to write it. The
// messages never include the DSN itself: it carries a password.
func checkIndexDSN(dsn string) error {
	trimmed := strings.TrimSpace(dsn)
	if trimmed == "" {
		return configError{"--index-dsn is required: pass --index-dsn or set BINTRAIL_INDEX_DSN to the index MySQL, e.g. " + indexDSNExample}
	}
	if strings.HasPrefix(strings.ToLower(trimmed), "mysql://") {
		return configError{"invalid --index-dsn: it is written as a mysql:// URL; drop the mysql:// prefix and wrap host:port in tcp(...), e.g. " + indexDSNExample}
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return configError{fmt.Sprintf("invalid --index-dsn: %v; the host goes inside tcp(...) and the database name after the slash, e.g. %s", err, indexDSNExample)}
	}
	if cfg.DBName == "" {
		return configError{"--index-dsn must include a database name, e.g. " + indexDSNExample}
	}
	return nil
}

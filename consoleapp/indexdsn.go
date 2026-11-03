package consoleapp

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"regexp"
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
// startup page instead of a restart loop, but its message repeats the value
// given, so the page shows it to anyone only when that value is plainly a
// setting (public); otherwise the page points at the log.
type settingError struct {
	msg    string
	public bool
}

func (e settingError) Error() string          { return e.msg }
func (e settingError) TelemetryClass() string { return telemetry.ClassConfigInvalid }
func (e settingError) ShownToAnyone() bool    { return e.public }

// plainSetting is what a format, a flavor, a size, a count or a duration looks
// like. A value outside it may be something pasted into the wrong variable (a
// DSN with its password, a host, a path), which a page anyone can reach must
// not repeat.
var plainSetting = regexp.MustCompile(`^[A-Za-z0-9 ._+-]{1,16}$`)

// configRefusal marks err, a refusal of a setting the operator gave, as a
// settingError, keeping its words. typed are the values the message repeats:
// the page shows the message only when every one is a plainSetting. An error
// that already carries its own class, or that comes from the network or the
// filesystem, is returned unchanged: this is for setting refusals only.
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
	public := true
	for _, v := range typed {
		// An unset source of the setting (the flag when the variable gave
		// it, or the reverse) repeats nothing.
		if v != "" && !plainSetting.MatchString(v) {
			public = false
		}
	}
	return settingError{msg: err.Error(), public: public}
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

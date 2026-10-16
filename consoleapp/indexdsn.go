package consoleapp

import (
	"fmt"
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

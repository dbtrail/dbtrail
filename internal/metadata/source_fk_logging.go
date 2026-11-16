package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
)

// DDLSourceFKLogging records, per start of the source server, whether that
// server wrote the changes its foreign keys cascade to into the binary log.
//
// MySQL 9.6 moved foreign-key enforcement from InnoDB to the SQL layer
// (WL#11249), and since then the rows a cascade deletes or rewrites are
// logged like any other change, unless the server runs with
// innodb_native_foreign_keys = ON. Before 9.6, and on MariaDB, InnoDB applies
// the cascade below the binary log and only the parent change is logged.
// recover-cascade reconstructs the unlogged case (internal/cascade) and must
// instead reverse the logged rows in the other: synthesizing rows the index
// already holds as deletes finds nothing, and a recovery that restores the
// parent without its children is the failure this table exists to prevent.
//
// The setting is read off the server (the variable, never the version
// number), and it can only change when the server restarts. A row is one
// start of one server (server_uuid, server_started_at) and the span over
// which the index has PROOF that the same start was still running: every
// reading that finds the same start (capture connects, snapshots, and a
// periodic check while capture runs) moves last_seen_at forward, and two
// readings of one start mean no restart happened between them. An event is
// judged by the start whose span holds its time; an event outside every span
// (written before the first reading, or while nobody was watching across a
// restart) is unknown, never credited to the nearest start.
//
// native_foreign_keys holds @@innodb_native_foreign_keys as SHOW VARIABLES
// spells it ("ON", "OFF") and is NULL when the server has no such variable
// (MySQL before 9.6, MariaDB): InnoDB cascades, nothing logged.
const DDLSourceFKLogging = `CREATE TABLE IF NOT EXISTS source_fk_logging (
    server_uuid         VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '@@server_uuid; empty when the server has none (MariaDB)',
    server_started_at   BIGINT       NOT NULL COMMENT 'unix seconds: UNIX_TIMESTAMP() minus Uptime, as the source answered',
    last_seen_at        BIGINT       NOT NULL COMMENT 'unix seconds: the latest reading that found this same start running',
    native_foreign_keys VARCHAR(16)  NULL     COMMENT '@@innodb_native_foreign_keys; NULL when the server has no such variable',
    PRIMARY KEY (server_uuid, server_started_at)
) ENGINE=InnoDB COMMENT='whether each start of the source logged its FK cascades; see metadata.DDLSourceFKLogging'`

// serverStartSlack is how far apart two readings of one server start may
// land: the clock and Uptime are read in one statement, but both have
// one-second resolution, so the same start can read one second apart.
const serverStartSlack = 2

// SourceFKLogging is one row of source_fk_logging.
type SourceFKLogging struct {
	ServerUUID string
	// ServerStarted is when the server that wrote the events started.
	ServerStarted time.Time
	// LastSeen is the latest reading that found this start still running.
	LastSeen time.Time
	// NativeForeignKeys is @@innodb_native_foreign_keys; "" when the server
	// has no such variable.
	NativeForeignKeys string
}

// LogsCascades reports whether that server wrote its cascaded child changes
// to the binary log: it has the variable and it is OFF.
func (s SourceFKLogging) LogsCascades() bool {
	return strings.EqualFold(s.NativeForeignKeys, "OFF") || s.NativeForeignKeys == "0"
}

// ReadSourceFKLogging asks the source when it started and whether it logs
// its foreign-key cascades.
func ReadSourceFKLogging(ctx context.Context, sourceDB *sql.DB) (SourceFKLogging, error) {
	var now int64
	var uptime string
	// One statement, so the clock and the uptime are read together. Uptime
	// from performance_schema when it is on, else SHOW GLOBAL STATUS below.
	err := sourceDB.QueryRowContext(ctx,
		`SELECT UNIX_TIMESTAMP(), VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Uptime'`).Scan(&now, &uptime)
	if err != nil {
		var name string
		if serr := sourceDB.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE 'Uptime'").Scan(&name, &uptime); serr != nil {
			return SourceFKLogging{}, fmt.Errorf("read the source's Uptime: %w", serr)
		}
		if cerr := sourceDB.QueryRowContext(ctx, "SELECT UNIX_TIMESTAMP()").Scan(&now); cerr != nil {
			return SourceFKLogging{}, fmt.Errorf("read the source's clock: %w", cerr)
		}
	}
	up, err := strconv.ParseInt(strings.TrimSpace(uptime), 10, 64)
	if err != nil {
		return SourceFKLogging{}, fmt.Errorf("read the source's Uptime: %q is not a number", uptime)
	}
	out := SourceFKLogging{ServerStarted: time.Unix(now-up, 0).UTC(), LastSeen: time.Unix(now, 0).UTC()}
	var native sql.NullString
	err = sourceDB.QueryRowContext(ctx, "SELECT @@GLOBAL.innodb_native_foreign_keys").Scan(&native)
	switch {
	case err == nil:
		// A boolean variable reads back as 0 or 1 through @@ (SHOW VARIABLES
		// says OFF or ON): keep the SHOW VARIABLES spelling.
		switch v := strings.ToUpper(strings.TrimSpace(native.String)); v {
		case "0":
			out.NativeForeignKeys = "OFF"
		case "1":
			out.NativeForeignKeys = "ON"
		default:
			out.NativeForeignKeys = v
		}
	case unknownVariable(err):
		// No such variable: InnoDB runs the cascades, nothing is logged.
	default:
		return SourceFKLogging{}, fmt.Errorf("read @@innodb_native_foreign_keys: %w", err)
	}
	var uuid sql.NullString
	if err := sourceDB.QueryRowContext(ctx, "SELECT @@GLOBAL.server_uuid").Scan(&uuid); err != nil && !unknownVariable(err) {
		return SourceFKLogging{}, fmt.Errorf("read @@server_uuid: %w", err)
	}
	out.ServerUUID = uuid.String
	return out, nil
}

func unknownVariable(err error) bool {
	var me *drivermysql.MySQLError
	return errors.As(err, &me) && me.Number == 1193 // ER_UNKNOWN_SYSTEM_VARIABLE
}

// RecordSourceFKLogging reads the source's setting and keeps it with the
// index: a reading that finds a recorded start (within serverStartSlack
// seconds) moves that start's last_seen_at forward, any other is a new start.
func RecordSourceFKLogging(ctx context.Context, sourceDB, indexDB *sql.DB) (SourceFKLogging, error) {
	s, err := ReadSourceFKLogging(ctx, sourceDB)
	if err != nil {
		return s, err
	}
	if _, err := indexDB.ExecContext(ctx, DDLSourceFKLogging); err != nil {
		return s, fmt.Errorf("create source_fk_logging: %w", err)
	}
	start, seen := s.ServerStarted.Unix(), s.LastSeen.Unix()
	var native any
	if s.NativeForeignKeys != "" {
		native = s.NativeForeignKeys
	}
	// The same start, read a second apart, is one row: extend it. A changed
	// setting under the same start cannot happen (the variable is read-only),
	// so it is not compared.
	res, err := indexDB.ExecContext(ctx,
		`UPDATE source_fk_logging SET last_seen_at = GREATEST(last_seen_at, ?)
		 WHERE server_uuid = ? AND server_started_at BETWEEN ? AND ?`,
		seen, s.ServerUUID, start-serverStartSlack, start+serverStartSlack)
	if err != nil {
		return s, fmt.Errorf("write source_fk_logging: %w", err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		return s, nil
	}
	var existing int
	if err := indexDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM source_fk_logging WHERE server_uuid = ? AND server_started_at BETWEEN ? AND ?`,
		s.ServerUUID, start-serverStartSlack, start+serverStartSlack).Scan(&existing); err != nil {
		return s, fmt.Errorf("read source_fk_logging: %w", err)
	}
	if existing > 0 { // matched, last_seen_at already later
		return s, nil
	}
	if _, err := indexDB.ExecContext(ctx,
		`INSERT IGNORE INTO source_fk_logging (server_uuid, server_started_at, last_seen_at, native_foreign_keys) VALUES (?, ?, ?, ?)`,
		s.ServerUUID, start, seen, native); err != nil {
		return s, fmt.Errorf("write source_fk_logging: %w", err)
	}
	return s, nil
}

// recordSourceFKLoggingBestEffort is the snapshot's call: a snapshot that
// could not record the setting is still a good snapshot. A missing reading
// leaves the events around it outside every recorded span, which
// recover-cascade judges unknown and flags (internal/cascade.Logging).
func recordSourceFKLoggingBestEffort(sourceDB, indexDB *sql.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := RecordSourceFKLogging(ctx, sourceDB, indexDB); err != nil {
		slog.Warn("could not record whether the source logs its foreign-key cascades; "+
			"recover-cascade flags the cascades of changes it cannot attribute", "error", err)
	}
}

// LoadSourceFKLogging returns every recorded start, oldest first. An index
// without the table (older than it, or never connected to a MySQL source)
// returns none and no error.
func LoadSourceFKLogging(ctx context.Context, indexDB *sql.DB) ([]SourceFKLogging, error) {
	rows, err := indexDB.QueryContext(ctx,
		`SELECT server_uuid, server_started_at, last_seen_at, native_foreign_keys FROM source_fk_logging ORDER BY server_started_at`)
	if err != nil {
		var me *drivermysql.MySQLError
		if errors.As(err, &me) && me.Number == 1146 { // ER_NO_SUCH_TABLE
			return nil, nil
		}
		return nil, fmt.Errorf("read source_fk_logging: %w", err)
	}
	defer rows.Close()
	var out []SourceFKLogging
	for rows.Next() {
		var uuid string
		var start, seen int64
		var native sql.NullString
		if err := rows.Scan(&uuid, &start, &seen, &native); err != nil {
			return nil, fmt.Errorf("read source_fk_logging: %w", err)
		}
		out = append(out, SourceFKLogging{ServerUUID: uuid, ServerStarted: time.Unix(start, 0).UTC(),
			LastSeen: time.Unix(seen, 0).UTC(), NativeForeignKeys: native.String})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read source_fk_logging: %w", err)
	}
	return out, nil
}

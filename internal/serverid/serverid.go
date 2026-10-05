// Package serverid manages persistent server identities for bintrail.
// Each source server is assigned a unique bintrail_id (UUID) on first contact —
// keyed on the server's @@server_uuid for MySQL, or on a synthesized
// address-derived anchor for MariaDB (which has no @@server_uuid; see
// SyntheticServerUUID). Identity resolution uses five rules to handle UUID
// regeneration, host migration, and cloned-server conflicts while preserving the
// bintrail_id.
package serverid

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"

	"github.com/dbtrail/dbtrail/internal/config"
)

// DDLBintrailServers is the canonical CREATE TABLE statement for bintrail_servers.
// Used by `bintrail init` and by testutil.InitIndexTables to ensure a single
// source of truth for the schema.
const DDLBintrailServers = `CREATE TABLE IF NOT EXISTS bintrail_servers (
    bintrail_id       CHAR(36)        NOT NULL,
    server_uuid       CHAR(36)        NOT NULL,
    host              VARCHAR(255)    NOT NULL,
    port              SMALLINT UNSIGNED NOT NULL DEFAULT 3306,
    username          VARCHAR(255)    NOT NULL,
    created_at        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    decommissioned_at TIMESTAMP       NULL DEFAULT NULL,
    PRIMARY KEY (bintrail_id),
    INDEX idx_server_uuid    (server_uuid),
    INDEX idx_host_port_user (host, port, username)
) ENGINE=InnoDB`

// DDLBintrailServerChanges is the canonical CREATE TABLE statement for
// bintrail_server_changes. Append-only audit trail — never update or delete rows.
const DDLBintrailServerChanges = `CREATE TABLE IF NOT EXISTS bintrail_server_changes (
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    bintrail_id   CHAR(36)        NOT NULL,
    field_changed ENUM('server_uuid','host','port','username') NOT NULL,
    old_value     VARCHAR(255)    NOT NULL,
    new_value     VARCHAR(255)    NOT NULL,
    detected_at   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_bintrail_id (bintrail_id),
    INDEX idx_detected_at (detected_at)
) ENGINE=InnoDB`

// ErrConflict is returned by ResolveServer when the observed server_uuid matches
// one active record while the observed host+port+username matches a different
// active record — a cloned-server situation that requires manual resolution.
//
// It is a comparable value, not an errors.New sentinel, so it can declare its
// usage-telemetry class (a cloned server is a setup the operator has to fix,
// hence config_invalid) while errors.Is(err, ErrConflict) keeps working.
var ErrConflict error = conflictError{}

type conflictError struct{}

func (conflictError) Error() string {
	return "server identity conflict: server_uuid and host:port:username match different records — resolve manually"
}

// TelemetryClass implements telemetry.Classed.
func (conflictError) TelemetryClass() string { return "config_invalid" }

// Server represents an active row in bintrail_servers.
type Server struct {
	BintrailID string
	ServerUUID string
	Host       string
	Port       uint16
	Username   string
}

// resolution is the outcome of resolveIdentity.
type resolution int

const (
	resNoChange  resolution = iota // Rule 1: exact match — no update needed
	resMigration                   // Rule 2: UUID matches, host/port/user changed
	resUUIDRegen                   // Rule 3: host+port+user matches, UUID changed
	resNew                         // Rule 4: no match → register new server
	resConflict                    // Rule 5: ambiguous → manual resolution required
)

// resolveIdentity applies the five-rule identity resolution algorithm against
// a set of active servers. It is a pure function with no DB side-effects,
// making it straightforward to unit-test.
//
// Rules:
//  1. UUID + host + port + user all match same record → no change
//  2. UUID matches one record, host/port/user differ → migration (update fields)
//  3. host+port+user match one record, UUID differs → UUID regeneration (update UUID)
//  4. No record matches either criterion → new server
//  5. UUID matches one record AND host+port+user match a DIFFERENT record → conflict
func resolveIdentity(servers []Server, serverUUID, host string, port uint16, username string) (*Server, resolution) {
	var uuidMatch *Server
	var hpuMatch *Server
	for i := range servers {
		s := &servers[i]
		if s.ServerUUID == serverUUID {
			uuidMatch = s
		}
		if s.Host == host && s.Port == port && s.Username == username {
			hpuMatch = s
		}
	}

	switch {
	case uuidMatch != nil && hpuMatch != nil && uuidMatch.BintrailID == hpuMatch.BintrailID:
		// Rule 1: exact match on all components.
		return uuidMatch, resNoChange
	case uuidMatch != nil && hpuMatch != nil && uuidMatch.BintrailID != hpuMatch.BintrailID:
		// Rule 5: UUID matches one record, host+port+user match a different one.
		return nil, resConflict
	case uuidMatch != nil:
		// Rule 2: UUID matches, host/port/user changed (migration or reconfiguration).
		return uuidMatch, resMigration
	case hpuMatch != nil:
		// Rule 3: host+port+user match, UUID changed (UUID regeneration).
		return hpuMatch, resUUIDRegen
	default:
		// Rule 4: no match at all → genuinely new server.
		return nil, resNew
	}
}

// loadCandidatesForUpdate fetches active server records that match either the
// server_uuid or the host+port+username, locking them for update to prevent
// concurrent registrations from producing duplicate active entries.
//
// The SELECT ... FOR UPDATE + InnoDB gap locks ensure that no other transaction
// can insert a matching row between the SELECT and the subsequent INSERT/UPDATE
// in this transaction.
func loadCandidatesForUpdate(ctx context.Context, tx *sql.Tx, serverUUID, host string, port uint16, username string) ([]Server, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT bintrail_id, server_uuid, host, port, username
		 FROM bintrail_servers
		 WHERE (server_uuid = ? OR (host = ? AND port = ? AND username = ?))
		   AND decommissioned_at IS NULL
		 FOR UPDATE`,
		serverUUID, host, port, username)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var servers []Server
	for rows.Next() {
		var s Server
		if err := rows.Scan(&s.BintrailID, &s.ServerUUID, &s.Host, &s.Port, &s.Username); err != nil {
			return nil, err
		}
		servers = append(servers, s)
	}
	return servers, rows.Err()
}

// logChange inserts a row into bintrail_server_changes to audit an identity component change.
func logChange(ctx context.Context, tx *sql.Tx, bintrailID, field, oldVal, newVal string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO bintrail_server_changes (bintrail_id, field_changed, old_value, new_value)
		 VALUES (?, ?, ?, ?)`,
		bintrailID, field, oldVal, newVal)
	return err
}

// ResolveServer looks up or registers a server in bintrail_servers, returning its
// stable bintrail_id. It runs inside a transaction with SELECT ... FOR UPDATE to
// prevent concurrent callers from creating duplicate active entries for the same
// server. Any detected component changes are recorded in bintrail_server_changes.
//
// Returns ErrConflict if server_uuid and host+port+username match two different
// active records (cloned-server situation). The caller should log a warning and
// refuse to operate until the conflict is resolved manually.
func ResolveServer(ctx context.Context, db *sql.DB, serverUUID, host string, port uint16, username string) (string, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	id, err := resolveInTx(ctx, tx, serverUUID, host, port, username)
	if err != nil {
		return "", err
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit: %w", err)
	}
	return id, nil
}

func resolveInTx(ctx context.Context, tx *sql.Tx, serverUUID, host string, port uint16, username string) (string, error) {
	servers, err := loadCandidatesForUpdate(ctx, tx, serverUUID, host, port, username)
	if err != nil {
		return "", fmt.Errorf("load candidates: %w", err)
	}

	matched, rule := resolveIdentity(servers, serverUUID, host, port, username)

	switch rule {
	case resNoChange:
		return matched.BintrailID, nil

	case resConflict:
		var uuidMatch, hpuMatch *Server
		for i := range servers {
			s := &servers[i]
			if s.ServerUUID == serverUUID {
				uuidMatch = s
			}
			if s.Host == host && s.Port == port && s.Username == username {
				hpuMatch = s
			}
		}
		return "", fmt.Errorf("%w: server_uuid %q belongs to bintrail_id %q but %s:%d/%s belongs to bintrail_id %q",
			ErrConflict, serverUUID, uuidMatch.BintrailID, host, port, username, hpuMatch.BintrailID)

	case resMigration:
		id := matched.BintrailID
		if matched.Host != host {
			if err := logChange(ctx, tx, id, "host", matched.Host, host); err != nil {
				return "", fmt.Errorf("log host change: %w", err)
			}
		}
		if matched.Port != port {
			if err := logChange(ctx, tx, id, "port", fmt.Sprint(matched.Port), fmt.Sprint(port)); err != nil {
				return "", fmt.Errorf("log port change: %w", err)
			}
		}
		if matched.Username != username {
			if err := logChange(ctx, tx, id, "username", matched.Username, username); err != nil {
				return "", fmt.Errorf("log username change: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE bintrail_servers SET host = ?, port = ?, username = ?, updated_at = UTC_TIMESTAMP()
			 WHERE bintrail_id = ?`,
			host, port, username, id); err != nil {
			return "", fmt.Errorf("update server record: %w", err)
		}
		return id, nil

	case resUUIDRegen:
		id := matched.BintrailID
		if err := logChange(ctx, tx, id, "server_uuid", matched.ServerUUID, serverUUID); err != nil {
			return "", fmt.Errorf("log uuid change: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE bintrail_servers SET server_uuid = ?, updated_at = UTC_TIMESTAMP()
			 WHERE bintrail_id = ?`,
			serverUUID, id); err != nil {
			return "", fmt.Errorf("update server uuid: %w", err)
		}
		return id, nil

	default: // resNew
		id := uuid.NewString()
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO bintrail_servers (bintrail_id, server_uuid, host, port, username)
			 VALUES (?, ?, ?, ?, ?)`,
			id, serverUUID, host, port, username); err != nil {
			return "", fmt.Errorf("register server: %w", err)
		}
		return id, nil
	}
}

// DecommissionServer sets decommissioned_at on the given bintrail_id, excluding
// it from future identity resolution. The record and its archived data are preserved.
func DecommissionServer(ctx context.Context, db *sql.DB, bintrailID string) error {
	res, err := db.ExecContext(ctx,
		`UPDATE bintrail_servers SET decommissioned_at = UTC_TIMESTAMP()
		 WHERE bintrail_id = ? AND decommissioned_at IS NULL`,
		bintrailID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("bintrail_id %q not found or already decommissioned", bintrailID)
	}
	return nil
}

// mariadbIdentityNamespace is the fixed UUIDv5 namespace under which MariaDB
// server-identity anchors are synthesized. MariaDB has no @@server_uuid, so
// SyntheticServerUUID derives a stable, collision-resistant anchor from the
// source address instead (see that function). The namespace is an arbitrary
// constant — its only job is to keep these synthesized UUIDs in their own space,
// disjoint from any real MySQL @@server_uuid.
var mariadbIdentityNamespace = uuid.MustParse("d87207bb-1448-4136-b360-d029bbce63d4")

// SyntheticServerUUID returns a deterministic UUIDv5 identity anchor for a
// source that exposes no @@server_uuid (i.e. MariaDB). It is derived from the
// source's host:port so that:
//
//   - the same server resolves to the same anchor across restarts (stable
//     bintrail_id, stable archive prefix), and
//   - two different MariaDB servers — necessarily reachable at distinct
//     addresses — resolve to distinct anchors, so they auto-separate into
//     distinct bintrail_id=<uuid>/ Parquet prefixes instead of colliding.
//
// The anchor feeds ResolveServer exactly like a real @@server_uuid: it is the
// strong re-identification key, while the bintrail_id itself stays a fresh
// random UUID assigned on first contact. Deriving from the address means a
// server moved to a new host gets a new identity — an accepted MariaDB
// limitation (MySQL's @@server_uuid survives migration; MariaDB has no
// equivalent), not a correctness bug.
func SyntheticServerUUID(host string, port uint16) string {
	seed := fmt.Sprintf("mariadb|%s:%d", host, port)
	return uuid.NewSHA1(mariadbIdentityNamespace, []byte(seed)).String()
}

// DeriveServerID returns a deterministic uint32 server-id by hashing the
// source DSN's host:user:dbname triple. The same DSN always produces the same
// ID on every machine, which is its flaw as a replication identity: two
// installations capturing one source through the same connection derive the
// same id and displace each other in a loop. Capture uses DeriveForInstall;
// this remains for callers with no index to tell installations apart, and as
// the value DeriveForInstall falls back to.
//
// Returns an error when the DSN cannot be parsed — callers must handle this
// rather than silently substituting a non-deterministic value.
func DeriveServerID(dsn string) (uint32, error) {
	return deriveServerID(dsn, "")
}

// deriveServerID hashes the source triple and, when salt is not empty, the
// salt with it. An empty salt gives DeriveServerID's value exactly.
func deriveServerID(dsn, salt string) (uint32, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return 0, fmt.Errorf("parse DSN: %w", err)
	}
	seed := fmt.Sprintf("%s|%s|%s", cfg.Addr, cfg.User, cfg.DBName)
	if salt != "" {
		seed += "|" + salt
	}
	sum := sha256.Sum256([]byte(seed))
	raw := binary.BigEndian.Uint32(sum[:4])
	// Map into [100000000, 4294967294]: subtract floor from uint32 range, mod
	// into the resulting width, then add the floor back. Keeps the value high
	// enough that collisions with typical hand-picked replica server-ids are
	// unlikely.
	const floor = uint32(100000000)
	const width = uint32(4294967295 - floor) // 4194967295
	return (raw % width) + floor, nil
}

// readIndexServerUUID asks the index's MySQL server for its @@server_uuid.
// It connects to the server, not to the index database: the id is needed
// before that database is sure to exist. A variable for tests.
var readIndexServerUUID = func(ctx context.Context, indexDSN string) (string, error) {
	cfg, err := mysql.ParseDSN(indexDSN)
	if err != nil {
		return "", fmt.Errorf("parse index DSN: %w", err)
	}
	cfg.DBName = ""
	db, err := config.Connect(cfg.FormatDSN())
	if err != nil {
		return "", err
	}
	defer db.Close()
	var id sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&id); err != nil {
		return "", err
	}
	return id.String, nil
}

// InstallSalt is what tells one installation apart from another capturing the
// same source: the @@server_uuid of the MySQL server its index lives on, which
// MySQL generates once and keeps with the data, and the index database's
// name. Two installations have two index servers, or two databases on one.
// It is stable across restarts, so the id derived with it is too.
//
// Not told apart: an installation cloned WITH its index's data directory (a
// machine image, a volume snapshot) carries the same @@server_uuid, and the
// two then derive the same id, as every installation did before.
//
// The read is tried a few times: the id chosen at startup is kept for the
// life of the process, and one dropped connection (a MySQL still finishing
// its first start) must not decide it.
func InstallSalt(ctx context.Context, indexDSN string) (string, error) {
	if strings.TrimSpace(indexDSN) == "" {
		return "", errors.New("no index DSN")
	}
	cfg, err := mysql.ParseDSN(indexDSN)
	if err != nil {
		return "", fmt.Errorf("parse index DSN: %w", err)
	}
	var id string
	for attempt := 1; ; attempt++ {
		if id, err = readIndexServerUUID(ctx, indexDSN); err == nil || attempt == installSaltAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return "", err
		case <-time.After(installSaltRetryWait):
		}
	}
	if err != nil {
		return "", err
	}
	id = strings.ToLower(strings.TrimSpace(id))
	if id == "" {
		return "", errors.New("the index server reports no @@server_uuid")
	}
	return id + "|" + cfg.DBName, nil
}

const installSaltAttempts = 3

// installSaltRetryWait is a variable so tests do not wait.
var installSaltRetryWait = time.Second

// DeriveForInstall returns the replication server-id for capturing sourceDSN
// from the installation whose index is indexDSN: stable across restarts, and
// different from the one another installation derives for the same source.
//
// sourceOnly is nil when the index said who it is. Otherwise it is why not
// (no index DSN, an index server that does not answer or has no
// @@server_uuid), and the id is DeriveServerID's: it works, and every
// installation capturing this source through the same connection shares it.
// That is never an error, because capture must not wait on this; a caller
// that is about to capture logs it (WarnSourceOnly), and one that only
// reports (doctor) says it.
func DeriveForInstall(ctx context.Context, sourceDSN, indexDSN string) (id uint32, sourceOnly error, err error) {
	salt, saltErr := InstallSalt(ctx, indexDSN)
	if saltErr != nil {
		id, err = deriveServerID(sourceDSN, "")
		return id, saltErr, err
	}
	id, err = deriveServerID(sourceDSN, salt)
	return id, nil, err
}

// WarnSourceOnly logs that capture is about to use the id every installation
// shares, and why. server names the source when the caller has a name for it.
func WarnSourceOnly(id uint32, server string, why error) {
	slog.Warn("replication server-id derived from the source connection alone; another DBTrail capturing this source through the same connection would use the same id and the two would interrupt each other. It is chosen again at the next start",
		"server_id", id, "server", server, "reason", why.Error())
}

// AutoDerive is the derivation for a command that was given no --server-id:
// it derives, says on w which id it chose and from what, and warns when the
// id is the shared one.
func AutoDerive(ctx context.Context, w io.Writer, sourceDSN, indexDSN string) (uint32, error) {
	id, sourceOnly, err := DeriveForInstall(ctx, sourceDSN, indexDSN)
	if err != nil {
		return 0, fmt.Errorf("cannot auto-derive --server-id from --source-dsn: %w (pass --server-id explicitly to bypass)", err)
	}
	if sourceOnly != nil {
		WarnSourceOnly(id, "", sourceOnly)
		fmt.Fprintf(w, "Auto-derived server-id from source DSN: %d\n", id)
		return id, nil
	}
	fmt.Fprintf(w, "Auto-derived server-id from the source connection and this installation's index: %d\n", id)
	return id, nil
}

// SetIndexServerUUIDForTest replaces the query that reads the index server's
// @@server_uuid, so a test can derive ids without a MySQL to ask.
func SetIndexServerUUIDForTest(read func(ctx context.Context, indexDSN string) (string, error)) (restore func()) {
	prev := readIndexServerUUID
	readIndexServerUUID = read
	return func() { readIndexServerUUID = prev }
}

// SetInstallSaltRetryWaitForTest shortens the wait between reads of the index
// server's identity.
func SetInstallSaltRetryWaitForTest(d time.Duration) (restore func()) {
	prev := installSaltRetryWait
	installSaltRetryWait = d
	return func() { installSaltRetryWait = prev }
}

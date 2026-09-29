package console

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// Source-family flavors. The registry records which capture engine a source
// needs: MySQL and MariaDB both index through the go-mysql binlog path (MariaDB
// differs only by GTID dialect at stream time), while
// PostgreSQL indexes through pgstreamrun. The string values are deliberately the
// SAME literals stream_state.flavor stores (see internal/query.SourceFlavor and
// recovery.DialectForFlavor). For MySQL and MariaDB the saved value is a hint:
// capture asks the server and follows it, and the console warns when the two
// disagree, so the saved label and stream_state.flavor CAN differ. Empty is treated as MySQL so every pre-#1019 registry
// entry keeps working — the same additive discipline as an empty SSLMode.
const (
	FlavorMySQL    = "mysql"
	FlavorMariaDB  = "mariadb"
	FlavorPostgres = "postgres"
)

// NormalizeFlavor maps a request/registry flavor string to a canonical value.
// Empty → mysql; "postgresql" is accepted as an alias for "postgres". An
// unrecognized value is an error so a create/update rejects a typo rather than
// silently mis-routing capture.
func NormalizeFlavor(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", FlavorMySQL:
		return FlavorMySQL, nil
	case FlavorMariaDB:
		return FlavorMariaDB, nil
	case FlavorPostgres, "postgresql":
		return FlavorPostgres, nil
	default:
		return "", fmt.Errorf("unknown source flavor %q (want mysql, mariadb, or postgres)", s)
	}
}

// SourceFlavor returns the entry's normalized source family, defaulting a blank
// (pre-#1019) entry to mysql. Consumed by the monitor supervisor (#1020), the
// baseline trigger (#1021), and the console UI gating. A stored value that no
// longer normalizes (only reachable via a hand-edit — create/update validate on
// write) degrades to mysql, which fails loudly at doctor time rather than
// silently corrupting anything.
func (e ServerEntry) SourceFlavor() string {
	f, err := NormalizeFlavor(e.Flavor)
	if err != nil {
		return FlavorMySQL
	}
	return f
}

// IsPostgres reports whether the entry's source is PostgreSQL.
func (e ServerEntry) IsPostgres() bool { return e.SourceFlavor() == FlavorPostgres }

// validatePGSourceMonitorConfig enforces that a monitorable PostgreSQL source
// (flavor postgres with a source DSN configured) also names a replication slot
// and a publication — capture cannot start without both, and the operator must
// create the publication itself (validate-don't-create). An index-only PG entry
// (no source DSN) needs neither. A no-op for MySQL/MariaDB.
func validatePGSourceMonitorConfig(flavor, sourceDSN, slot, publication string) error {
	if flavor != FlavorPostgres || sourceDSN == "" {
		return nil
	}
	if strings.TrimSpace(slot) == "" || strings.TrimSpace(publication) == "" {
		return errors.New("a monitored PostgreSQL source requires both a replication slot and a publication")
	}
	return nil
}

// PGReplDSN derives the replication (walsender) connection string from a stored
// PostgreSQL query DSN by adding replication=database. pgcapture/pgstreamrun and
// pgbaseline require a distinct replication connection (a CopyBoth conn that
// cannot run ordinary SQL); the console stores only the query DSN and derives
// this one, so the replication-param logic lives in exactly one place. Consumed
// by the monitor supervisor (#1020) and the baseline trigger (#1021).
//
// Pure net/url — no pgx: internal/console is on the read side of the
// dependency-guard (internal/event/depguard_test.go) and must never link the
// PostgreSQL capture libraries. A postgres:// URL round-trips through net/url
// and re-parses cleanly in pgconn.ParseConfig downstream.
func PGReplDSN(queryDSN string) (string, error) {
	u, err := url.Parse(queryDSN)
	if err != nil {
		return "", errors.New("invalid PostgreSQL source DSN")
	}
	q := u.Query()
	if q.Get("replication") != "" {
		return "", errors.New("source DSN already carries a replication parameter; store the ordinary query DSN and let DBTrail derive the replication connection")
	}
	q.Set("replication", "database")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// pgURL assembles a canonical postgres:// DSN from structured parts. rawQuery
// (already URL-encoded, e.g. carried forward from a stored DSN) is appended
// verbatim; pass "" for a fresh build. Port defaults to 5432.
func pgURL(host, port, user, password, dbname, rawQuery string) (string, error) {
	if host == "" {
		return "", errors.New("source host is required")
	}
	if user == "" {
		return "", errors.New("source user is required")
	}
	if dbname == "" {
		return "", errors.New("source database is required for a PostgreSQL source (a logical-replication connection is per-database)")
	}
	if port == "" {
		port = "5432"
	}
	u := &url.URL{
		Scheme:   "postgres",
		Host:     net.JoinHostPort(host, port),
		Path:     "/" + dbname,
		RawQuery: rawQuery,
	}
	if password == "" {
		u.User = url.User(user)
	} else {
		u.User = url.UserPassword(user, password)
	}
	return u.String(), nil
}

// validatePGQueryDSN checks a raw pasted PostgreSQL source DSN: it must be a
// postgres:// URL (libpq key=value strings aren't accepted here), name a
// database, and NOT carry a replication parameter (the console derives the
// replication connection itself). The parse error is never echoed — it could
// embed the password.
func validatePGQueryDSN(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return errors.New("invalid PostgreSQL source_dsn (expected postgres://user:pass@host:5432/dbname)")
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return errors.New("PostgreSQL source_dsn must be a postgres:// URL (paste a postgres:// URL, not a libpq key=value string)")
	}
	if strings.TrimPrefix(u.Path, "/") == "" {
		return errors.New("PostgreSQL source_dsn must include a database name (postgres://user:pass@host:5432/dbname)")
	}
	if u.Query().Get("replication") != "" {
		return errors.New("PostgreSQL source_dsn must be an ordinary (query) connection; drop replication=database; DBTrail derives the replication connection automatically")
	}
	return nil
}

// buildPGSourceDSN assembles the stored PostgreSQL SOURCE DSN (the query DSN)
// for a create/update request, mirroring buildMySQLSourceDSN's tri-state on
// req.SourceDSN (buildSourceDSN itself is now just the flavor dispatcher).
// Unlike a MySQL source DSN (server-level, no database), a PG
// query DSN MUST name a database because logical replication is per-database.
// TLS beyond the libpq default is configured by pasting a raw source_dsn (the
// #879 hand-edit precedent), not a dedicated form field.
func buildPGSourceDSN(req serverRequest, stored string) (string, error) {
	if req.SourceDSN != nil {
		raw := strings.TrimSpace(*req.SourceDSN)
		if raw == "" {
			return "", nil // explicit clear
		}
		if req.SourcePassword != nil {
			return "", errors.New("specify either source_dsn or the structured source_password field, not both (a dsn carries its own password)")
		}
		if err := validatePGQueryDSN(raw); err != nil {
			return "", err
		}
		return raw, nil
	}

	// No raw DSN and no structured source fields → keep the stored config as-is.
	if req.SourceHost == "" && req.SourcePort == "" && req.SourceUser == "" &&
		req.SourceDatabase == "" && req.SourcePassword == nil {
		return stored, nil
	}

	// Structured build/merge: decompose the stored DSN (if any) with net/url and
	// overlay the request fields. Stored query params (e.g. sslmode from a raw
	// paste) are carried forward so a later host-only edit doesn't silently drop
	// TLS settings.
	host, port, user, dbname := req.SourceHost, req.SourcePort, req.SourceUser, req.SourceDatabase
	password, rawQuery := "", ""
	if stored != "" {
		if su, err := url.Parse(stored); err == nil {
			if host == "" {
				host = su.Hostname()
			}
			if port == "" {
				port = su.Port()
			}
			if su.User != nil {
				if user == "" {
					user = su.User.Username()
				}
				if pw, ok := su.User.Password(); ok {
					password = pw
				}
			}
			if dbname == "" {
				dbname = strings.TrimPrefix(su.Path, "/")
			}
			rawQuery = su.RawQuery
		}
	}
	if req.SourcePassword != nil {
		password = *req.SourcePassword
	}
	return pgURL(host, port, user, password, dbname, rawQuery)
}

// fillPGSourceDSNParts decomposes a PostgreSQL query DSN into the masked DTO
// fields — the credentials themselves never leave the process. Parse failures
// leave the parts blank rather than leaking the raw DSN.
func fillPGSourceDSNParts(dto *serverDTO, dsn string) {
	if dsn == "" {
		return
	}
	dto.HasSource = true
	u, err := url.Parse(dsn)
	if err != nil {
		return
	}
	dto.SourceHost = u.Hostname()
	dto.SourcePort = u.Port()
	if u.User != nil {
		dto.SourceUser = u.User.Username()
		_, hasPw := u.User.Password()
		dto.HasSourcePassword = hasPw
	}
	dto.SourceDatabase = strings.TrimPrefix(u.Path, "/")
}

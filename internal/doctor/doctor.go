package doctor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/serverid"
)

// CheckStatus is the outcome of a single preflight check. Constrained to the
// four constants below; unknown values are rejected by (*Report).add.
type CheckStatus string

const (
	StatusPass CheckStatus = "pass"
	StatusFail CheckStatus = "fail"
	StatusWarn CheckStatus = "warn"
	StatusSkip CheckStatus = "skip"
)

type CheckResult struct {
	Name   string      `json:"name"`
	Status CheckStatus `json:"status"`
	// Detail and Remediation are typically empty for StatusPass/StatusSkip and
	// populated for StatusFail/StatusWarn.
	Detail      string `json:"detail,omitempty"`
	Remediation string `json:"remediation,omitempty"`
}

type Report struct {
	Checks   []CheckResult `json:"checks"`
	Passed   int           `json:"passed"`
	Failed   int           `json:"failed"`
	Warnings int           `json:"warnings"`
	Skipped  int           `json:"skipped"`

	// ReadyFooter / FixFooter customize the trailing one-line guidance in the TEXT
	// output (the all-passed line and the has-failures line respectively). They are
	// excluded from JSON. Empty values fall back to the default MySQL-oriented
	// guidance, so existing callers are unaffected; a non-MySQL caller (e.g.
	// bintrail-pg doctor) sets them to its own command names.
	ReadyFooter string `json:"-"`
	FixFooter   string `json:"-"`
}

// Add appends a check result and updates the counters. It is the exported entry
// point for callers building a Report outside this package — e.g. the PostgreSQL
// doctor, which cannot live in this package because internal/doctor must stay free
// of the pgx/pglogrepl dependency (cmd/bintrail's pgfree ban). MySQL's own Build
// uses the unexported add.
//
// Add (and the internal add) are the ONLY sanctioned way to grow a Report: appending
// to the exported Checks slice directly would leave Passed/Failed/Warnings/Skipped
// stale. Callers must always go through Add.
func (r *Report) Add(c CheckResult) { r.add(c) }

// binlogRetentionMinSeconds is the minimum binlog retention bintrail asks for
// (docs/streaming.md:279). Hoisted so the check function and any test referring
// to the threshold share a single source of truth.
const binlogRetentionMinSeconds = 172800

// queryErrorRemediation returns the generic remediation block for "the SELECT
// itself failed" FAIL paths. Connection drops outnumber permission gaps in
// practice, so retry is bullet 1 and grants are bullet 2.
func queryErrorRemediation(query string) string {
	return "The check could not query " + query + ". Common causes (most likely first):\n" +
		"  - Connection dropped or timed out: retry once before investigating further\n" +
		"  - User lacks required SELECT privilege on the relevant system table or variable\n" +
		"  - Server overloaded — raise the per-check timeout or check server load"
}

// schemaGrantsRemediation is the "bintrail sees no tables at all" advice,
// shared by the two outcomes checkSchemaVisibility reaches with it: a schema
// that is verifiably invisible, and one where the probe that would have told
// "empty" from "invisible" failed — still the likeliest cause, just not a
// verified one.
const schemaGrantsRemediation = "Bintrail needs at least SELECT on information_schema to read column metadata.\n" +
	"Grant minimum read access:\n\n" +
	"  GRANT SELECT ON *.* TO <bintrail-user>;\n\n" +
	"Or scope to the schemas you want indexed:\n\n" +
	"  GRANT SELECT ON <schema>.* TO <bintrail-user>;\n\n" +
	"Also double-check the schema names for typos."

// isUnknownDatabaseErr reports whether err (possibly wrapped) is MySQL error
// 1049 (ER_BAD_DB, "Unknown database ..."): the DSN names a database that
// doesn't exist yet. `bintrail init` (and therefore `up`) creates the index
// database itself, so the index checks must treat 1049 as "probe the server
// instead", not as a hard failure — the remediation text already promised as
// much before the behavior matched it (#384).
func isUnknownDatabaseErr(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1049
}

// connectWithoutDB reconnects to the DSN's server with the database name
// stripped (the same DBName="" + FormatDSN pattern as
// indexer.EnsureDatabase, but routed through config.Connect to keep parseTime and
// the connect timeout), for checks that must run before the index database
// exists.
func connectWithoutDB(dsn string) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.DBName = ""
	return config.Connect(cfg.FormatDSN())
}

// Build runs every preflight check and returns the structured
// report without rendering it — the seam the control-plane supervisor uses to
// surface doctor results as cards in the console UI (runDoctorTo keeps the
// CLI's write-and-exit behavior on top of it).
func Build(parent context.Context, sourceDSN, indexDSN, schemasCSV string, indexRetain time.Duration) *Report {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()

	report := &Report{}
	schemas := cliutil.ParseSchemaList(schemasCSV)

	// ── Source MySQL checks ──────────────────────────────────────────────────
	sourceDB, err := config.Connect(sourceDSN)
	if err != nil {
		report.add(CheckResult{
			Name:   SourceConnectionCheckName,
			Status: StatusFail,
			Detail: err.Error(),
			Remediation: "Verify --source-dsn is reachable: try `mysql -h<host> -P<port> -u<user> -p<pass>`.\n" +
				"For RDS/Aurora: ensure the security group allows ingress from bintrail's IP on port 3306.",
		})
		return report
	}
	defer sourceDB.Close()

	report.add(checkSourceConnection(ctx, sourceDB))
	report.add(checkLogBin(ctx, sourceDB))
	report.add(checkBinlogFormat(ctx, sourceDB))
	report.add(checkBinlogRowImage(ctx, sourceDB))
	report.add(checkBinlogRetention(ctx, sourceDB))
	report.add(checkSyncBinlog(ctx, sourceDB))
	report.add(checkStatementCapture(ctx, sourceDB))
	report.add(checkRowMetadata(ctx, sourceDB))
	report.add(checkBinlogRowValueOptions(ctx, sourceDB))
	report.add(checkReplicationGrants(ctx, sourceDB))
	report.add(checkServerIDCollision(ctx, sourceDB, sourceDSN))
	report.add(checkFKCascades(sourceDB, schemas))
	report.add(checkSchemaVisibility(ctx, sourceDB, schemas))
	report.add(checkPrimaryKeys(sourceDB, schemas))

	// ── Index MySQL checks (optional) ─────────────────────────────────────────
	if indexDSN != "" {
		indexCfg, parseErr := mysql.ParseDSN(indexDSN)
		if parseErr != nil {
			report.add(CheckResult{
				Name:        "Index DSN parse",
				Status:      StatusFail,
				Detail:      parseErr.Error(),
				Remediation: `Expected DSN format: user:pass@tcp(host:port)/binlog_index`,
			})
		} else if indexCfg.DBName == "" {
			report.add(CheckResult{
				Name:        "Index DSN database name",
				Status:      StatusFail,
				Detail:      "DSN does not include a database name",
				Remediation: "Add a database name to the DSN, e.g. user:pass@tcp(host:3306)/binlog_index",
			})
		} else {
			report.add(checkSourceIndexColocation(sourceDSN, indexDSN))
			report.add(checkIndexConnection(ctx, indexDSN, indexCfg.DBName))
			report.add(checkIndexWriteAccess(ctx, indexDSN, indexCfg.DBName))
			report.add(checkIndexCapacity(ctx, indexDSN, indexCfg.DBName, indexRetain))
		}
	} else {
		report.add(CheckResult{
			Name:   "Index database",
			Status: StatusSkip,
			Detail: "--index-dsn not provided",
		})
	}

	return report
}

func checkSourceConnection(ctx context.Context, db *sql.DB) CheckResult {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return CheckResult{
			Name:   SourceConnectionCheckName,
			Status: StatusFail,
			Detail: err.Error(),
			Remediation: "The connection opened but SELECT VERSION() failed. Common causes:\n" +
				"  - Permission denied: ensure the user has at least SELECT on *.*\n" +
				"  - Transient network issue: retry once before investigating further\n" +
				"  - Server restarted mid-handshake: wait and retry",
		}
	}
	return CheckResult{
		Name:   SourceConnectionCheckName,
		Status: StatusPass,
		Detail: "MySQL " + version,
	}
}

func checkLogBin(ctx context.Context, db *sql.DB) CheckResult {
	var val string
	err := db.QueryRowContext(ctx, "SELECT @@log_bin").Scan(&val)
	if err != nil {
		return CheckResult{
			Name:        "log_bin enabled",
			Status:      StatusFail,
			Detail:      err.Error(),
			Remediation: queryErrorRemediation("@@log_bin"),
		}
	}
	if val != "1" && !strings.EqualFold(val, "ON") {
		return CheckResult{
			Name:   "log_bin enabled",
			Status: StatusFail,
			Detail: fmt.Sprintf("log_bin=%q (binary logging is OFF)", val),
			Remediation: "Binary logging is disabled. Set in my.cnf and restart MySQL:\n\n" +
				"  [mysqld]\n" +
				"  log_bin = mysql-bin\n" +
				"  server_id = 1\n\n" +
				"Then restart MySQL. log_bin cannot be enabled at runtime.",
		}
	}
	return CheckResult{Name: "log_bin enabled", Status: StatusPass, Detail: "ON"}
}

func checkBinlogFormat(ctx context.Context, db *sql.DB) CheckResult {
	err := metadata.ValidateBinlogFormatContext(ctx, db)
	if err != nil {
		return CheckResult{
			Name:   "binlog_format=ROW",
			Status: StatusFail,
			Detail: err.Error(),
			Remediation: "Set on the source MySQL (MySQL 8.0+ — survives restart without editing my.cnf):\n\n" +
				"  SET PERSIST binlog_format = 'ROW';\n\n" +
				"On MySQL 5.7 use SET GLOBAL and also add to my.cnf:\n\n" +
				"  [mysqld]\n" +
				"  binlog_format = ROW",
		}
	}
	return CheckResult{Name: "binlog_format=ROW", Status: StatusPass, Detail: "ROW"}
}

func checkBinlogRowImage(ctx context.Context, db *sql.DB) CheckResult {
	err := metadata.ValidateBinlogRowImageContext(ctx, db)
	if err != nil {
		return CheckResult{
			Name:   "binlog_row_image=FULL",
			Status: StatusFail,
			Detail: err.Error(),
			Remediation: "Set on the source MySQL (MySQL 8.0+ — survives restart):\n\n" +
				"  SET PERSIST binlog_row_image = 'FULL';\n\n" +
				"On MySQL 5.7 use SET GLOBAL and also add to my.cnf:\n\n" +
				"  [mysqld]\n" +
				"  binlog_row_image = FULL\n\n" +
				"Without FULL, UPDATE/DELETE binlog events omit unchanged columns, " +
				"so bintrail cannot reconstruct full before/after images for recovery.",
		}
	}
	return CheckResult{Name: "binlog_row_image=FULL", Status: StatusPass, Detail: "FULL"}
}

// checkBinlogRowValueOptions warns when the source sets
// binlog_row_value_options=PARTIAL_JSON. With that option MySQL logs partial
// JSON updates (JSON_SET/JSON_REPLACE/JSON_REMOVE) as compact diff fragments in
// the row image rather than the full column value; bintrail's parser cannot
// apply those partial diffs and skips the affected JSON updates at runtime with
// only a log warning — a silent capture gap for JSON columns (#777). Advisory
// only (WARN, never FAIL): the option is harmless on sources with no
// partially-updated JSON columns and is the operator's binlog-size tradeoff, but
// they must know it can drop JSON updates. Absent variable (MySQL <8.0, MariaDB)
// → SKIP; empty value (the default) → PASS.
func checkBinlogRowValueOptions(ctx context.Context, db *sql.DB) CheckResult {
	const name = "binlog_row_value_options (JSON capture)"
	var val string
	if err := db.QueryRowContext(ctx, "SELECT @@binlog_row_value_options").Scan(&val); err != nil {
		// MySQL error 1193 (unknown system variable) means the server predates
		// the option or is a flavor that lacks it — nothing to check, not a fault.
		var myErr *mysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1193 {
			return CheckResult{Name: name, Status: StatusSkip, Detail: "binlog_row_value_options is not available on this server"}
		}
		return CheckResult{Name: name, Status: StatusWarn, Detail: "could not read binlog_row_value_options: " + err.Error()}
	}
	up := strings.ToUpper(strings.TrimSpace(val))
	if up == "" {
		return CheckResult{Name: name, Status: StatusPass, Detail: "full JSON row images"}
	}
	if strings.Contains(up, "PARTIAL_JSON") {
		return CheckResult{
			Name:   name,
			Status: StatusWarn,
			Detail: fmt.Sprintf("binlog_row_value_options=%q — partial JSON updates are logged as diffs bintrail cannot apply, so those JSON updates are skipped at capture (silent for JSON columns)", val),
			Remediation: "Disable partial-JSON binlog logging on the source so JSON updates are captured in full:\n\n" +
				"  SET PERSIST binlog_row_value_options = '';\n\n" +
				"This makes JSON UPDATEs log the whole column value (larger binlog) instead of an unappliable partial diff.",
		}
	}
	// Any other non-empty value is unrecognized. A doctor check whose job is to
	// surface capture gaps must not paint an unknown setting green — WARN rather
	// than fall through to a reassuring PASS.
	return CheckResult{
		Name:   name,
		Status: StatusWarn,
		Detail: fmt.Sprintf("binlog_row_value_options=%q is unrecognized — verify it does not enable partial JSON row logging, or JSON updates may be skipped at capture", val),
		Remediation: "If you did not set this deliberately, clear it for full JSON row images:\n\n" +
			"  SET PERSIST binlog_row_value_options = '';",
	}
}

// checkBinlogRetention emits a WARN (not FAIL) when binlog retention is below
// the 2-day recommendation in docs/streaming.md — short windows can leave
// bintrail unable to fill gaps after a restart.
func checkBinlogRetention(ctx context.Context, db *sql.DB) CheckResult {
	// On RDS/Aurora the effective retention is governed by the
	// mysql.rds_configuration 'binlog retention hours' setting, NOT by
	// @@binlog_expire_logs_seconds (which reports the engine default and can
	// paint a green PASS while RDS purges binlogs as soon as replication no
	// longer needs them — #812). When that row is queryable, base the verdict
	// on it. Absent table/row (non-RDS) → fall through to the standard probe.
	if raw, isRDS, probeErr := rdsBinlogRetentionHours(ctx, db); probeErr != nil {
		// A permission/transient failure reading mysql.rds_configuration must not
		// be laundered into "not RDS" (which would fall through to the engine
		// variable and paint a misleading PASS). Surface it.
		return CheckResult{
			Name:   "Binlog retention >= 2 days",
			Status: StatusWarn,
			Detail: "could not read mysql.rds_configuration ('binlog retention hours'): " + probeErr.Error() +
				" — if this is RDS/Aurora, the engine variable below can overstate the real retention",
			Remediation: "On RDS/Aurora, grant the check user read access so bintrail can verify the managed retention:\n\n" +
				"  GRANT SELECT ON mysql.rds_configuration TO '<user>'@'%';",
		}
	} else if isRDS {
		return rdsBinlogRetentionVerdict("Binlog retention >= 2 days", raw)
	}

	var raw string
	// binlog_expire_logs_seconds is MySQL 8.0+; older servers use expire_logs_days.
	err := db.QueryRowContext(ctx, "SELECT @@binlog_expire_logs_seconds").Scan(&raw)
	if err != nil {
		// Try the legacy variable.
		var days string
		if dErr := db.QueryRowContext(ctx, "SELECT @@expire_logs_days").Scan(&days); dErr == nil {
			d, parseErr := strconv.Atoi(days)
			if parseErr != nil {
				return CheckResult{
					Name:   "Binlog retention >= 2 days",
					Status: StatusWarn,
					Detail: fmt.Sprintf("could not parse expire_logs_days=%q: %v", days, parseErr),
				}
			}
			if d < 2 {
				return CheckResult{
					Name:   "Binlog retention >= 2 days",
					Status: StatusWarn,
					Detail: fmt.Sprintf("expire_logs_days=%d (legacy variable)", d),
					Remediation: "Set retention to at least 2 days so bintrail can fill gaps after a restart:\n\n" +
						"  SET PERSIST expire_logs_days = 2;",
				}
			}
			return CheckResult{Name: "Binlog retention >= 2 days", Status: StatusPass, Detail: days + " days"}
		}
		return CheckResult{
			Name:   "Binlog retention >= 2 days",
			Status: StatusWarn,
			Detail: "could not read binlog retention setting: " + err.Error(),
		}
	}
	seconds, parseErr := strconv.Atoi(raw)
	if parseErr != nil {
		return CheckResult{
			Name:   "Binlog retention >= 2 days",
			Status: StatusWarn,
			Detail: fmt.Sprintf("could not parse binlog_expire_logs_seconds=%q: %v", raw, parseErr),
		}
	}
	if seconds == 0 {
		// 0 = never expire; harmless for bintrail (retention is effectively infinite).
		return CheckResult{
			Name:   "Binlog retention >= 2 days",
			Status: StatusWarn,
			Detail: "binlog_expire_logs_seconds=0 (no automatic expiration)",
		}
	}
	if seconds < binlogRetentionMinSeconds {
		return CheckResult{
			Name:   "Binlog retention >= 2 days",
			Status: StatusWarn,
			Detail: fmt.Sprintf("binlog_expire_logs_seconds=%d (%dh)", seconds, seconds/3600),
			Remediation: fmt.Sprintf("Set retention to at least 2 days (%ds) so bintrail can fill gaps after a restart:\n\n"+
				"  SET PERSIST binlog_expire_logs_seconds = %d;", binlogRetentionMinSeconds, binlogRetentionMinSeconds),
		}
	}
	return CheckResult{
		Name:   "Binlog retention >= 2 days",
		Status: StatusPass,
		Detail: fmt.Sprintf("%dh", seconds/3600),
	}
}

// rdsBinlogRetentionHours reads the RDS/Aurora-managed binlog retention from
// mysql.rds_configuration, distinguishing three outcomes so a permission failure
// is never mistaken for "not RDS" (#812):
//   - row read (value may be NULL, the RDS default) → isRDS=true, probeErr=nil.
//   - sql.ErrNoRows (table exists, row unset) → still RDS → isRDS=true so the
//     caller WARNs rather than trusting the engine variable.
//   - 1146 no-such-table → a self-managed server, genuinely not RDS → isRDS=false.
//   - any other error (1142 permission, transient) → probeErr set, so the caller
//     surfaces it instead of falling back to the misleading engine-variable PASS.
func rdsBinlogRetentionHours(ctx context.Context, db *sql.DB) (raw sql.NullString, isRDS bool, probeErr error) {
	var v sql.NullString
	err := db.QueryRowContext(ctx,
		"SELECT value FROM mysql.rds_configuration WHERE name = 'binlog retention hours'").Scan(&v)
	switch {
	case err == nil:
		return v, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return sql.NullString{}, true, nil
	default:
		var myErr *mysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1146 {
			return sql.NullString{}, false, nil
		}
		return sql.NullString{}, false, err
	}
}

// rdsBinlogRetentionVerdict turns the RDS-managed 'binlog retention hours'
// value into a check result. NULL (the RDS default) or a sub-2-day value →
// WARN with the CALL mysql.rds_set_configuration remediation; >= 2 days → PASS.
func rdsBinlogRetentionVerdict(name string, raw sql.NullString) CheckResult {
	const minHours = binlogRetentionMinSeconds / 3600 // 48
	setRemediation := fmt.Sprintf(
		"Set RDS/Aurora binlog retention to at least 2 days so bintrail can fill gaps after a restart:\n\n"+
			"  CALL mysql.rds_set_configuration('binlog retention hours', %d);", minHours)

	if !raw.Valid {
		return CheckResult{
			Name:        name,
			Status:      StatusWarn,
			Detail:      "RDS/Aurora 'binlog retention hours' is NULL (the default) — RDS purges binlogs as soon as replication no longer needs them, regardless of binlog_expire_logs_seconds",
			Remediation: setRemediation,
		}
	}
	hours, parseErr := strconv.Atoi(strings.TrimSpace(raw.String))
	if parseErr != nil {
		return CheckResult{
			Name:   name,
			Status: StatusWarn,
			Detail: fmt.Sprintf("could not parse RDS 'binlog retention hours'=%q: %v", raw.String, parseErr),
		}
	}
	if hours*3600 < binlogRetentionMinSeconds {
		return CheckResult{
			Name:        name,
			Status:      StatusWarn,
			Detail:      fmt.Sprintf("RDS 'binlog retention hours'=%d (below 2 days) — this, not binlog_expire_logs_seconds, governs binlog retention on RDS/Aurora", hours),
			Remediation: setRemediation,
		}
	}
	return CheckResult{
		Name:   name,
		Status: StatusPass,
		Detail: fmt.Sprintf("%dh (RDS binlog retention hours)", hours),
	}
}

// checkServerIDCollision warns when the replication server-id bintrail derives
// from --source-dsn (serverid.DeriveServerID, a deterministic sha256 of the
// host:user:dbname triple) collides with the source's own @@server_id (#819).
// The derivation is deterministic on purpose (clean resume across restarts),
// but that means two bintrail instances pointed at the SAME source with the
// same user derive the SAME server-id and MySQL disconnects the duplicate in a
// reconnect loop — and a 1/2^32 hash collision with the source's own id kicks
// the source off its own replica identity. Advisory only (WARN, never FAIL):
// this is a topology property bintrail can surface but must not block boot on.
func checkServerIDCollision(ctx context.Context, db *sql.DB, sourceDSN string) CheckResult {
	const name = "Replication server-id collision"
	derived, err := serverid.DeriveServerID(sourceDSN)
	if err != nil {
		return CheckResult{
			Name:   name,
			Status: StatusWarn,
			Detail: "could not derive replication server-id from --source-dsn: " + err.Error(),
		}
	}
	var srcRaw string
	if err := db.QueryRowContext(ctx, "SELECT @@server_id").Scan(&srcRaw); err != nil {
		return CheckResult{
			Name:   name,
			Status: StatusWarn,
			Detail: "could not read @@server_id: " + err.Error(),
		}
	}
	srcID, parseErr := strconv.ParseUint(strings.TrimSpace(srcRaw), 10, 64)
	if parseErr != nil {
		return CheckResult{
			Name:   name,
			Status: StatusWarn,
			Detail: fmt.Sprintf("could not parse @@server_id=%q: %v", srcRaw, parseErr),
		}
	}
	if srcID == uint64(derived) {
		return CheckResult{
			Name:   name,
			Status: StatusWarn,
			Detail: fmt.Sprintf("derived replication server-id %d equals the source's own @@server_id — the replication connection would collide and MySQL would reject or flap the stream", derived),
			Remediation: "The server-id is derived deterministically from --source-dsn (host|user|dbname). " +
				"Vary the DSN's user or host form to shift the derived id off the source's @@server_id, " +
				"or set an explicit --server-id on `stream`.",
		}
	}
	return CheckResult{
		Name:   name,
		Status: StatusPass,
		Detail: fmt.Sprintf("derived server-id %d (source @@server_id=%d) — note: any bintrail instance on this same --source-dsn derives this SAME id and would collide on the replication connection", derived, srcID),
	}
}

// checkSyncBinlog warns when the source's sync_binlog is not 1: with
// sync_binlog=0 (or N>1), an OS crash can drop committed transactions from the
// binlog tail before bintrail's stream ever reads them — a fundamental,
// source-side loss no amount of gap-detection on the index side can recover,
// because the data never reached the binlog at all. This is advisory only
// (never FAIL): it is the source operator's durability tradeoff, and the
// cheapest thing bintrail can do is make sure they know about it.
func checkSyncBinlog(ctx context.Context, db *sql.DB) CheckResult {
	const name = "Source sync_binlog=1 (crash-safety)"
	var val string
	if err := db.QueryRowContext(ctx, "SELECT @@sync_binlog").Scan(&val); err != nil {
		return CheckResult{
			Name:   name,
			Status: StatusWarn,
			Detail: "could not read @@sync_binlog: " + err.Error(),
		}
	}
	if val == "1" {
		return CheckResult{Name: name, Status: StatusPass, Detail: "sync_binlog=1"}
	}
	return CheckResult{
		Name:   name,
		Status: StatusWarn,
		Detail: fmt.Sprintf("sync_binlog=%s — an OS crash can drop the binlog's tail of already-committed transactions before bintrail ever streams them; this loss happens upstream of bintrail and cannot be detected until a later `bintrail verify` reports a MISMATCH", val),
		Remediation: "Set sync_binlog=1 on the source for crash-safe binary logging (fsyncs the binlog on every " +
			"commit — a throughput/durability tradeoff you may already be making deliberately):\n\n" +
			"  SET PERSIST sync_binlog = 1;\n\n" +
			"If you keep sync_binlog at a weaker setting for throughput reasons, be aware that a source crash can " +
			"silently lose committed data bintrail never had a chance to capture.",
	}
}

// checkStatementCapture reports whether the source logs the original SQL
// statement alongside each row event (#699): MySQL's
// binlog_rows_query_log_events or MariaDB's binlog_annotate_row_events. The
// capture is OPTIONAL — it feeds the query_text/query_hash forensics columns —
// so this check never FAILs: ON → PASS, OFF → WARN with an enable suggestion
// (validate, never set), variable absent on both probes → SKIP. Both probes use
// SELECT @@var, which errors (MySQL 1193) rather than returning rows for a
// variable the flavor doesn't have — the checkBinlogRetention fallback pattern.
func checkStatementCapture(ctx context.Context, db *sql.DB) CheckResult {
	const name = "Statement capture (query_text)"
	isOn := func(val string) bool { return val == "1" || strings.EqualFold(val, "ON") }
	// Only MySQL error 1193 (unknown system variable) means the variable is
	// genuinely absent; any other failure is a read problem and must surface
	// the real error rather than a fabricated flavor diagnosis.
	isUnknownVar := func(err error) bool {
		var myErr *mysql.MySQLError
		return errors.As(err, &myErr) && myErr.Number == 1193
	}

	var val string
	err := db.QueryRowContext(ctx, "SELECT @@binlog_rows_query_log_events").Scan(&val)
	if err == nil {
		if isOn(val) {
			return CheckResult{Name: name, Status: StatusPass, Detail: "binlog_rows_query_log_events=ON"}
		}
		return CheckResult{
			Name:   name,
			Status: StatusWarn,
			Detail: "binlog_rows_query_log_events=OFF — events index without the originating SQL statement (query_text stays NULL)",
			Remediation: "Optional: log the original statement with each row event so `bintrail query` can show it (dynamic, no restart; costs binlog bytes per statement):\n\n" +
				"  SET PERSIST binlog_rows_query_log_events = ON;\n\n" +
				"Not retroactive: only events written AFTER the change carry text, so `query --query-hash` still matches nothing in the window before it (#1437).",
		}
	}
	if !isUnknownVar(err) {
		return CheckResult{
			Name:   name,
			Status: StatusWarn,
			Detail: "could not read binlog_rows_query_log_events: " + err.Error(),
		}
	}

	// MariaDB names the same capability binlog_annotate_row_events
	// (default ON since 10.2.4). Note stream capture additionally requires
	// `--source-flavor mariadb` so the syncer requests ANNOTATE events.
	err = db.QueryRowContext(ctx, "SELECT @@binlog_annotate_row_events").Scan(&val)
	if err == nil {
		if isOn(val) {
			return CheckResult{
				Name:   name,
				Status: StatusPass,
				Detail: "binlog_annotate_row_events=ON (MariaDB; stream capture also needs --source-flavor mariadb)",
			}
		}
		return CheckResult{
			Name:   name,
			Status: StatusWarn,
			Detail: "binlog_annotate_row_events=OFF — events index without the originating SQL statement (query_text stays NULL)",
			Remediation: "Optional: log the original statement with each row event so `bintrail query` can show it (stream capture also needs `--source-flavor mariadb`):\n\n" +
				"  SET GLOBAL binlog_annotate_row_events = ON;\n\n" +
				"Persist it in my.cnf ([mysqld] binlog_annotate_row_events=ON) to survive restarts. " +
				"Not retroactive: only events written AFTER the change carry text (#1437).",
		}
	}
	if !isUnknownVar(err) {
		return CheckResult{
			Name:   name,
			Status: StatusWarn,
			Detail: "could not read binlog_annotate_row_events: " + err.Error(),
		}
	}

	return CheckResult{
		Name:   name,
		Status: StatusSkip,
		Detail: "neither binlog_rows_query_log_events nor binlog_annotate_row_events is available on this server",
	}
}

// checkRowMetadata reports whether the source embeds column names in every
// binlog TABLE_MAP event (#700): binlog_row_metadata=FULL (MySQL 8.0+,
// MariaDB 10.5+) lets bintrail cross-check the schema snapshot against
// per-event ground truth and fail loud on a stale snapshot — including the
// same-column-count drift (a rename, or a DROP+ADD in one ALTER) that the
// count guard cannot see and that would otherwise index values under the
// wrong column names. The setting is OPTIONAL, so this check never FAILs:
// FULL → PASS, MINIMAL → WARN with an enable suggestion (validate, never
// set), variable absent → SKIP.
func checkRowMetadata(ctx context.Context, db *sql.DB) CheckResult {
	const name = "Schema-drift detection (binlog_row_metadata)"
	var val string
	if err := db.QueryRowContext(ctx, "SELECT @@binlog_row_metadata").Scan(&val); err != nil {
		// Only MySQL error 1193 (unknown system variable) means the server
		// genuinely lacks the variable (MySQL 5.7, MariaDB <10.5). Any other
		// failure is a read problem — surface the real error instead of a
		// fabricated version diagnosis (unlike checkBinlogRetention, which
		// treats any error on the modern variable as absent and falls back).
		var myErr *mysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1193 {
			return CheckResult{
				Name:   name,
				Status: StatusSkip,
				Detail: "binlog_row_metadata is not available on this server (needs MySQL 8.0+ or MariaDB 10.5+)",
			}
		}
		return CheckResult{
			Name:   name,
			Status: StatusWarn,
			Detail: "could not read binlog_row_metadata: " + err.Error(),
		}
	}
	if strings.EqualFold(val, "FULL") {
		return CheckResult{Name: name, Status: StatusPass, Detail: "binlog_row_metadata=FULL"}
	}
	return CheckResult{
		Name:   name,
		Status: StatusWarn,
		Detail: "binlog_row_metadata=" + val + " — a stale schema snapshot cannot be detected at capture time (a same-column-count change like a rename would index values under the wrong column names)",
		Remediation: "Optional: embed column names in row-event metadata so bintrail can verify the snapshot against every event (dynamic, no restart; adds a handful of bytes per column to each TABLE_MAP event):\n\n" +
			"  -- MySQL 8.0+:\n" +
			"  SET PERSIST binlog_row_metadata = 'FULL';\n\n" +
			"  -- MariaDB 10.5+ (no SET PERSIST; persist it in my.cnf under [mysqld]):\n" +
			"  SET GLOBAL binlog_row_metadata = 'FULL';",
	}
}

func checkReplicationGrants(ctx context.Context, db *sql.DB) CheckResult {
	rows, err := db.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return CheckResult{
			Name:        ReplicationGrantsCheckName,
			Status:      StatusFail,
			Detail:      err.Error(),
			Remediation: queryErrorRemediation("SHOW GRANTS"),
		}
	}
	defer rows.Close()

	var grants []string
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err != nil {
			return CheckResult{
				Name:        ReplicationGrantsCheckName,
				Status:      StatusFail,
				Detail:      err.Error(),
				Remediation: queryErrorRemediation("SHOW GRANTS"),
			}
		}
		grants = append(grants, g)
	}

	slave, client := metadata.HasReplPrivileges(grants)
	if slave && client {
		return CheckResult{
			Name:   ReplicationGrantsCheckName,
			Status: StatusPass,
			Detail: "REPLICATION SLAVE, REPLICATION CLIENT",
		}
	}

	var missing []string
	if !slave {
		missing = append(missing, "REPLICATION SLAVE")
	}
	if !client {
		missing = append(missing, "REPLICATION CLIENT")
	}

	// Try to extract the user from the first grant line to make remediation concrete.
	user := "<bintrail-user>"
	if len(grants) > 0 {
		if u := extractGrantUser(grants[0]); u != "" {
			user = u
		}
	}

	return CheckResult{
		Name:   ReplicationGrantsCheckName,
		Status: StatusFail,
		Detail: "missing: " + strings.Join(missing, ", "),
		Remediation: fmt.Sprintf("Run on the source MySQL as a privileged user (e.g. root):\n\n"+
			"  GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO %s;\n"+
			"  FLUSH PRIVILEGES;\n\n"+
			"REPLICATION SLAVE lets bintrail stream binlog events.\n"+
			"REPLICATION CLIENT lets it run SHOW BINARY LOGS / SHOW MASTER STATUS for gap detection.", user),
	}
}

// extractGrantUser pulls the "user@host" out of a SHOW GRANTS line like
// `GRANT USAGE ON *.* TO 'bintrail'@'%'` so we can render the GRANT remediation
// with the actual user instead of a placeholder. Returns empty when the format
// is unfamiliar — the caller falls back to a placeholder.
func extractGrantUser(grant string) string {
	upper := strings.ToUpper(grant)
	i := strings.Index(upper, " TO ")
	if i < 0 {
		return ""
	}
	rest := strings.TrimSpace(grant[i+4:])
	// Strip trailing clauses like " IDENTIFIED BY ..." (older MySQL).
	if j := strings.IndexAny(rest, " "); j > 0 {
		// Keep only the user@host token, unless it contains spaces inside quotes.
		// Safe heuristic: if the first char is a quote, take until matching close + @host.
		if rest[0] == '\'' || rest[0] == '`' {
			// Find the next quote after position 1, then the @, then take until end-of-token.
			closing := rest[0]
			k := strings.IndexByte(rest[1:], closing)
			if k > 0 {
				end := 1 + k + 1
				// Append @host segment if present.
				if end < len(rest) && rest[end] == '@' {
					rest2 := rest[end+1:]
					if len(rest2) > 0 && (rest2[0] == '\'' || rest2[0] == '`') {
						closing2 := rest2[0]
						m := strings.IndexByte(rest2[1:], closing2)
						if m > 0 {
							return rest[:end+1+m+2]
						}
					}
				}
				return rest[:end]
			}
		}
		return rest[:j]
	}
	return rest
}

func checkFKCascades(db *sql.DB, schemas []string) CheckResult {
	err := metadata.ValidateNoFKCascades(db, schemas)
	if err == nil {
		return CheckResult{Name: "No FK CASCADE constraints", Status: StatusPass}
	}
	return CheckResult{
		Name:   "No FK CASCADE constraints",
		Status: StatusWarn,
		Detail: err.Error(),
		Remediation: "Foreign keys with ON DELETE CASCADE or ON UPDATE CASCADE produce side-effect row\n" +
			"changes that InnoDB executes below the binary log (MySQL Bug #32506), so plain\n" +
			"`recover` cannot reconstruct cascade-deleted child rows. Options:\n\n" +
			"  1. Drop or change the cascade rules:\n" +
			"     ALTER TABLE <child> DROP FOREIGN KEY <fk_name>;\n" +
			"     ALTER TABLE <child> ADD CONSTRAINT <fk_name> FOREIGN KEY (...) REFERENCES <parent>(...)\n" +
			"         ON DELETE RESTRICT ON UPDATE RESTRICT;\n\n" +
			"  2. Keep the cascades and reconstruct cascade-deleted child rows with\n" +
			"     `bintrail recover-cascade` (Phase-1: binlog-window; baseline fallback #552).\n\n" +
			"Ingestion (`stream`/`watch`/`up`/`index --source-dsn`) no longer refuses cascade\n" +
			"schemas — it WARNS and proceeds — so the FK graph is captured for cascade recovery.\n" +
			"Plain `recover` still produces incomplete SQL for cascade-affected tables; use\n" +
			"`recover-cascade` instead for those.",
	}
}

// pkNameLimit caps how many table names a missing-primary-key finding prints
// before it falls back to counting. A schema converted from MyISAM can have
// hundreds, and a wall of names is read as noise where a count plus a query
// is read as a task.
const pkNameLimit = 10

// PrimaryKeyCheckName is the check that reports tables with no primary key.
// Exported so the daemons can hold it advisory the way they hold the capacity
// check: nothing here gates the snapshot, so a failure to ASK the question
// must not refuse boot on a source that is capturing fine.
const PrimaryKeyCheckName = "Every table has a PRIMARY KEY"

// checkPrimaryKeys reports tables the snapshot will not capture for lack of a
// primary key.
//
// It calls metadata.TablesWithoutPrimaryKey, which is the snapshot's OWN
// classifier, rather than asking information_schema a similar question. That
// is the whole design: a preflight that predicts the pipeline with a second
// implementation drifts from it, and both directions of drift were measured on
// live servers before this landed. A TABLE_CONSTRAINTS-shaped question warns
// about a UNIQUE NOT NULL table that MySQL marks COLUMN_KEY = 'PRI' and the
// product keys perfectly well, and a TABLE_TYPE = 'BASE TABLE' filter misses
// MariaDB's SYSTEM VERSIONED tables, which is the one shape where the data
// loss is total (#1272).
//
// WARN, never FAIL, on every path including its own errors. Nothing downstream
// consumes this answer: the snapshot re-derives it and refuses or excludes on
// its own. Failing here would let a transient information_schema error stop
// capture on a source that is otherwise healthy, which is the trade
// checkIndexCapacity already refused to make.
func checkPrimaryKeys(db *sql.DB, schemas []string) CheckResult {
	tables, err := metadata.TablesWithoutPrimaryKey(db, schemas)
	switch {
	case errors.Is(err, metadata.ErrNoColumnsVisible):
		// Not a pass. Schema visibility has its own check and has already
		// spoken; this one simply has no evidence, and saying "every table
		// has a primary key" on no evidence is the failure it exists to break.
		return CheckResult{
			Name:   PrimaryKeyCheckName,
			Status: StatusSkip,
			Detail: "no tables are visible in the requested scope, so this was not checked",
		}
	case err != nil:
		return CheckResult{
			Name:   PrimaryKeyCheckName,
			Status: StatusWarn,
			Detail: "could not be checked: " + err.Error(),
			Remediation: "This check is advisory and does not block capture. The snapshot applies the\n" +
				"same rule itself and will refuse or exclude a table with no primary key when\n" +
				"it runs, so the gap is reported either way, later and less conveniently.\n\n" +
				"The account needs to read information_schema.COLUMNS and\n" +
				"information_schema.TABLES for the monitored schemas.",
		}
	case len(tables) == 0:
		return CheckResult{Name: PrimaryKeyCheckName, Status: StatusPass}
	}

	names := tables
	if len(names) > pkNameLimit {
		names = names[:pkNameLimit]
	}
	detail := fmt.Sprintf("%d table(s) without a primary key: %s", len(tables), strings.Join(names, ", "))
	if len(tables) > len(names) {
		detail += fmt.Sprintf(", and %d more", len(tables)-len(names))
	}
	scope := ""
	if len(schemas) > 0 {
		scope = " --schemas " + strings.Join(schemas, ",")
	}
	return CheckResult{
		Name:   PrimaryKeyCheckName,
		Status: StatusWarn,
		Detail: detail,
		// States what HAPPENS, not what degrades. An earlier draft of this said
		// the tables were captured but could not be addressed by row, which is
		// wrong in the reassuring direction: the operator reads it as degraded
		// recovery over data they still hold, and they hold none.
		Remediation: "These tables are NOT captured. bintrail identifies a row by its primary key,\n" +
			"and a table without one is refused rather than captured without identity:\n\n" +
			"  - taking the first snapshot REFUSES outright, and the stream does not start\n" +
			"  - a later snapshot, after a schema change, EXCLUDES them and skips their\n" +
			"    row events, capturing every other table\n\n" +
			"So this is not degraded recovery for those tables. There is no history for\n" +
			"them at all, and none of it can be recovered later by adding the key: only\n" +
			"changes made AFTER the key exists are captured.\n\n" +
			"Give each table a primary key. Any existing column that is UNIQUE and\n" +
			"NOT NULL will do:\n\n" +
			"  ALTER TABLE <schema>.<table> ADD PRIMARY KEY (<unique, NOT NULL column>);\n\n" +
			"When no column qualifies, add a surrogate key, which is what InnoDB is\n" +
			"already keeping internally and invisibly:\n\n" +
			"  ALTER TABLE <schema>.<table> ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;\n\n" +
			"To list them all yourself, in the same scope this check used:\n\n" +
			"  bintrail doctor --source-dsn <dsn>" + scope + "\n\n" +
			"This check is advisory: it does not block anything. The snapshot is what\n" +
			"refuses, when it runs.",
	}
}

// checkSchemaVisibility queries information_schema to ensure bintrail can see
// at least one table in the target schemas. A pass here means the snapshot
// step will succeed.
func checkSchemaVisibility(ctx context.Context, db *sql.DB, schemas []string) CheckResult {
	var query string
	var args []any
	if len(schemas) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		query = fmt.Sprintf(`SELECT COUNT(*), COUNT(DISTINCT TABLE_SCHEMA)
			FROM information_schema.TABLES
			WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN (%s)`, placeholders)
		for _, s := range schemas {
			args = append(args, s)
		}
	} else {
		query = `SELECT COUNT(*), COUNT(DISTINCT TABLE_SCHEMA)
			FROM information_schema.TABLES
			WHERE TABLE_TYPE = 'BASE TABLE'
			  AND TABLE_SCHEMA NOT IN ('information_schema','performance_schema','mysql','sys')`
	}

	var tableCount, schemaCount int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&tableCount, &schemaCount); err != nil {
		return CheckResult{
			Name:        "Schema visibility",
			Status:      StatusFail,
			Detail:      err.Error(),
			Remediation: queryErrorRemediation("information_schema.TABLES"),
		}
	}

	if tableCount == 0 {
		filter := "user schemas"
		if len(schemas) > 0 {
			filter = strings.Join(schemas, ", ")
		}
		// Zero tables has two very different causes: the schema genuinely
		// has no tables yet (fix: create one) vs. bintrail cannot see it at
		// all (fix: grants / schema-name typo). Telling the operator to
		// GRANT when the schema is simply empty sends them down the wrong
		// path (#402).
		n, probeErr := countVisibleSchemas(ctx, db, schemas)
		if probeErr == nil && n > 0 {
			return CheckResult{
				Name:   "Schema visibility",
				Status: StatusFail,
				Detail: "schema visible but contains no tables yet: " + filter,
				Remediation: "Bintrail snapshots table schemas when monitoring starts and cannot monitor\n" +
					"an empty schema. Create at least one table first:\n\n" +
					"  CREATE TABLE <schema>.<table> (id INT PRIMARY KEY, ...);\n\n" +
					"Then start monitoring again.",
			}
		}
		if probeErr != nil {
			// The probe that tells "empty" from "invisible" failed, so this
			// is NOT a verified grants problem: keep the visibility name (it
			// grades config_invalid, not db_permission, for usage telemetry)
			// and show the probe's error instead of swallowing it. The grants
			// advice stays, since that is still the likeliest cause.
			return CheckResult{
				Name:        "Schema visibility",
				Status:      StatusFail,
				Detail:      "no tables visible in " + filter + " (schema probe failed: " + probeErr.Error() + ")",
				Remediation: schemaGrantsRemediation,
			}
		}
		return CheckResult{
			Name:        SchemaAccessCheckName,
			Status:      StatusFail,
			Detail:      "no tables visible in " + filter,
			Remediation: schemaGrantsRemediation,
		}
	}
	return CheckResult{
		Name:   "Schema visibility",
		Status: StatusPass,
		Detail: fmt.Sprintf("%d tables across %d schemas", tableCount, schemaCount),
	}
}

// countVisibleSchemas reports how many of the requested schemas exist and are
// visible to the doctor's connection (all non-system schemas when the filter
// is empty). It distinguishes "schema is empty" from "schema is invisible"
// in checkSchemaVisibility.
func countVisibleSchemas(ctx context.Context, db *sql.DB, schemas []string) (int, error) {
	var query string
	var args []any
	if len(schemas) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		query = fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.SCHEMATA
			WHERE SCHEMA_NAME IN (%s)`, placeholders)
		for _, s := range schemas {
			args = append(args, s)
		}
	} else {
		query = `SELECT COUNT(*) FROM information_schema.SCHEMATA
			WHERE SCHEMA_NAME NOT IN ('information_schema','performance_schema','mysql','sys')`
	}
	var n int
	err := db.QueryRowContext(ctx, query, args...).Scan(&n)
	return n, err
}

func checkIndexConnection(ctx context.Context, dsn, dbName string) CheckResult {
	db, err := config.Connect(dsn)
	if err != nil {
		// Error 1049 means only the DATABASE is absent — init creates it,
		// so verify the SERVER is reachable instead of failing (#384).
		if isUnknownDatabaseErr(err) {
			serverDB, serverErr := connectWithoutDB(dsn)
			if serverErr == nil {
				defer serverDB.Close()
				var version string
				vErr := serverDB.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
				if vErr == nil {
					return CheckResult{
						Name:   IndexConnectionCheckName,
						Status: StatusPass,
						Detail: fmt.Sprintf("MySQL %s, database=%s (does not exist yet — `bintrail init` will create it)", version, dbName),
					}
				}
				serverErr = vErr
			}
			// The database is absent AND the server-level probe failed too.
			// Surface BOTH errors: 1049 alone would mislead — its remediation
			// says "init will create it" — when the real, newer problem is
			// e.g. max_user_connections, a timeout, or the server going away
			// between the two connects. A diagnostic must not swallow the
			// diagnostic.
			err = fmt.Errorf("%w (server-level probe also failed: %v)", err, serverErr)
		}
		return CheckResult{
			Name:   IndexConnectionCheckName,
			Status: StatusFail,
			Detail: err.Error(),
			Remediation: "Verify --index-dsn is reachable. The database does not need to exist yet — " +
				"`bintrail init` will create it. But the user needs CREATE DATABASE if so.",
		}
	}
	defer db.Close()
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return CheckResult{
			Name:        IndexConnectionCheckName,
			Status:      StatusFail,
			Detail:      err.Error(),
			Remediation: queryErrorRemediation("VERSION()"),
		}
	}
	return CheckResult{
		Name:   IndexConnectionCheckName,
		Status: StatusPass,
		Detail: fmt.Sprintf("MySQL %s, database=%s", version, dbName),
	}
}

// checkIndexWriteAccess verifies the user can CREATE TABLE in the target index
// database. Opens its own connection; delegates to checkIndexWriteAccessOn for
// the actual probing so the IO and the logic can be tested independently.
func checkIndexWriteAccess(ctx context.Context, dsn, dbName string) CheckResult {
	db, err := config.Connect(dsn)
	if err != nil {
		// Error 1049: the database is absent. Probe at the server level —
		// checkIndexWriteAccessOn's SCHEMATA lookup then exercises the
		// CREATE DATABASE privilege path this check exists to verify (#384).
		if isUnknownDatabaseErr(err) {
			serverDB, serverErr := connectWithoutDB(dsn)
			if serverErr == nil {
				defer serverDB.Close()
				return checkIndexWriteAccessOn(ctx, serverDB, dbName)
			}
			// Surface both errors — see checkIndexConnection for why 1049
			// alone would mislead here.
			err = fmt.Errorf("%w (server-level probe also failed: %v)", err, serverErr)
		}
		return CheckResult{
			Name:   IndexWriteAccessCheckName,
			Status: StatusFail,
			Detail: err.Error(),
			Remediation: "Could not connect to --index-dsn. Verify the host/port/user are correct " +
				"and that the user has connect privileges. The database itself does not need to " +
				"exist yet — `bintrail init` (or `bintrail up`) will create it given CREATE DATABASE.",
		}
	}
	defer db.Close()
	return checkIndexWriteAccessOn(ctx, db, dbName)
}

// checkIndexWriteAccessOn runs the actual SCHEMATA/CREATE/DROP probe sequence
// against an already-open *sql.DB.
func checkIndexWriteAccessOn(ctx context.Context, db *sql.DB, dbName string) CheckResult {
	// First ensure the database exists. If it doesn't, we need CREATE DATABASE privilege.
	createdByProbe := false
	var dbExists string
	dbErr := db.QueryRowContext(ctx,
		"SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
		dbName).Scan(&dbExists)
	if errors.Is(dbErr, sql.ErrNoRows) {
		// Attempt CREATE DATABASE to see if we have the privilege.
		_, createErr := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName))
		if createErr != nil {
			return CheckResult{
				Name:   IndexWriteAccessCheckName,
				Status: StatusFail,
				Detail: fmt.Sprintf("database %q does not exist and user cannot CREATE DATABASE: %v", dbName, createErr),
				Remediation: fmt.Sprintf("Either create the database manually as a privileged user:\n\n"+
					"  CREATE DATABASE `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;\n\n"+
					"Or grant CREATE on it to the bintrail user:\n\n"+
					"  GRANT ALL PRIVILEGES ON `%s`.* TO <bintrail-user>;", dbName, dbName),
			}
		}
		createdByProbe = true
		// A diagnostic must not leave server state behind: best-effort drop
		// of the probe-created database on exit (#384). A drop failure is
		// logged, not surfaced — it can only happen when the user also
		// lacks DROP, which the table probe below already reports as FAIL —
		// and init re-creates the database for real (CREATE DATABASE IF NOT
		// EXISTS), so a leftover costs nothing.
		defer func() {
			if _, dropErr := db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName)); dropErr != nil {
				slog.Warn("doctor: could not drop probe-created database", "database", dbName, "error", dropErr)
			}
		}()
	} else if dbErr != nil {
		return CheckResult{
			Name:        IndexWriteAccessCheckName,
			Status:      StatusFail,
			Detail:      dbErr.Error(),
			Remediation: queryErrorRemediation("information_schema.SCHEMATA"),
		}
	}

	// Test CREATE TABLE / DROP TABLE in the target database.
	probeTable := fmt.Sprintf("`%s`.`_bintrail_doctor_probe`", dbName)
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+probeTable+" (id INT)"); err != nil {
		return CheckResult{
			Name:   IndexWriteAccessCheckName,
			Status: StatusFail,
			Detail: "cannot CREATE TABLE: " + err.Error(),
			Remediation: fmt.Sprintf("Grant table-creation privileges on the index database:\n\n"+
				"  GRANT ALL PRIVILEGES ON `%s`.* TO <bintrail-user>;\n"+
				"  FLUSH PRIVILEGES;", dbName),
		}
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE "+probeTable); err != nil {
		// FAIL, not WARN: bintrail's rotate/partition-management code needs DROP
		// (drop p_future, drop old partitions). A user with CREATE but no DROP
		// passes here and then fails at runtime during `rotate` — worse than
		// failing now.
		return CheckResult{
			Name:   IndexWriteAccessCheckName,
			Status: StatusFail,
			Detail: "user has CREATE but not DROP TABLE: " + err.Error(),
			Remediation: fmt.Sprintf("Grant DROP — bintrail rotates partitions and needs it at runtime:\n\n"+
				"  GRANT ALL PRIVILEGES ON `%s`.* TO <bintrail-user>;\n"+
				"  FLUSH PRIVILEGES;\n\n"+
				"Also clean up the probe table left behind:\n\n"+
				"  DROP TABLE %s;", dbName, probeTable),
		}
	}
	// Deliberately no "(probe database dropped)" claim here: the deferred
	// drop runs AFTER this return value is built, so the Detail cannot
	// honestly assert the drop happened.
	detail := "CREATE/DROP TABLE OK"
	if createdByProbe {
		detail = fmt.Sprintf("database %q does not exist yet — CREATE DATABASE privilege verified; CREATE/DROP TABLE OK", dbName)
	}
	return CheckResult{
		Name:   IndexWriteAccessCheckName,
		Status: StatusPass,
		Detail: detail,
	}
}

// checkSourceIndexColocation warns when the index resolves to the same
// host:port as the source being captured (#978). bintrail's index is
// self-contained by design — recovery never needs the original binlog files —
// but that guarantee only holds if the index's disk can outlive the source's.
// Co-locating them puts both on the same failure domain: if that disk dies,
// the index dies with the source it exists to protect. Pure DSN parsing, no
// live query needed — advisory only (never FAIL, and a garbage DSN degrades
// to SKIP rather than panicking): a single-host demo or dev box is a
// legitimate, deliberate choice bintrail can only flag, not block.
func checkSourceIndexColocation(sourceDSN, indexDSN string) CheckResult {
	const name = "Index co-located with source"
	srcCfg, err := mysql.ParseDSN(sourceDSN)
	if err != nil {
		return CheckResult{Name: name, Status: StatusSkip, Detail: "could not parse --source-dsn: " + err.Error()}
	}
	idxCfg, err := mysql.ParseDSN(indexDSN)
	if err != nil {
		return CheckResult{Name: name, Status: StatusSkip, Detail: "could not parse --index-dsn: " + err.Error()}
	}
	if !strings.EqualFold(srcCfg.Addr, idxCfg.Addr) {
		return CheckResult{
			Name:   name,
			Status: StatusPass,
			Detail: fmt.Sprintf("source %s, index %s — separate hosts", srcCfg.Addr, idxCfg.Addr),
		}
	}
	return CheckResult{
		Name:   name,
		Status: StatusWarn,
		Detail: fmt.Sprintf("source and index both resolve to %s — they share a disk, so if it dies the index dies along with the source it was meant to protect, defeating the recovery safety net", srcCfg.Addr),
		Remediation: "Run the index database on a separate MySQL instance from the source: " +
			"docs/deployment.md#separate-server-recommended.",
	}
}

// ─── Report tabulation and output ────────────────────────────────────────────

func (r *Report) add(c CheckResult) {
	r.Checks = append(r.Checks, c)
	switch c.Status {
	case StatusPass:
		r.Passed++
	case StatusFail:
		r.Failed++
	case StatusWarn:
		r.Warnings++
	case StatusSkip:
		r.Skipped++
	default:
		// Unknown status — log so a regression in a caller is visible instead of
		// silently miscounting. We still append to Checks so the JSON output is
		// complete and operators can see the malformed entry.
		slog.Warn("doctor: check produced unknown status", "name", c.Name, "status", c.Status)
	}
}

// Write renders the report to w. format is "text" (default) or "json".
func (r *Report) Write(w io.Writer, format string) error {
	if format == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(r)
	}

	for _, c := range r.Checks {
		var mark string
		switch c.Status {
		case StatusPass:
			mark = "✓"
		case StatusFail:
			mark = "✗"
		case StatusWarn:
			mark = "!"
		case StatusSkip:
			mark = "-"
		default:
			mark = "?"
		}
		if c.Detail != "" {
			fmt.Fprintf(w, "%s %s (%s)\n", mark, c.Name, c.Detail)
		} else {
			fmt.Fprintf(w, "%s %s\n", mark, c.Name)
		}
		if c.Remediation != "" {
			for _, line := range strings.Split(c.Remediation, "\n") {
				fmt.Fprintf(w, "    %s\n", line)
			}
		}
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "Passed: %d  Failed: %d  Warnings: %d  Skipped: %d\n",
		r.Passed, r.Failed, r.Warnings, r.Skipped)
	ready := r.ReadyFooter
	if ready == "" {
		ready = "Ready to stream. Run `bintrail up --source-dsn ... --index-dsn ...` to start."
	}
	fix := r.FixFooter
	if fix == "" {
		fix = "Fix the failures above, then re-run `bintrail doctor` to verify."
	}
	if r.Failed == 0 {
		fmt.Fprintln(w, "\n"+ready)
	} else {
		fmt.Fprintln(w, "\n"+fix)
	}
	return nil
}

// Err returns a non-nil error when any required check failed, so the CLI exits
// non-zero for CI/scripting use cases. Warnings do not cause failure.
func (r *Report) Err() error {
	return r.ErrExcluding()
}

// Names of the checks whose failure means something other than "a setting on
// the server is wrong". PreflightError.TelemetryClass keys on them, so they
// are constants shared with the producers (pgstreamrun's doctor included)
// rather than literals that could drift apart.
const (
	SourceConnectionCheckName   = "Source MySQL connection"
	IndexConnectionCheckName    = "Index MySQL connection"
	PGSourceConnectionCheckName = "Source PostgreSQL connection"
	ReplicationGrantsCheckName  = "REPLICATION SLAVE + CLIENT grants"
	IndexWriteAccessCheckName   = "Index write access"
	// SchemaAccessCheckName is the schema-visibility probe's grants branch:
	// no tables visible at all, whose remediation is GRANT SELECT. The
	// empty-schema branch keeps the "Schema visibility" name — it is not a
	// permission problem, and telling the operator to GRANT there sends
	// them down the wrong path (#402) — and so do the probe's query-error
	// branches, which grade config_invalid; an unreachable source is caught
	// by the connection check ahead of them. An extension check that reused
	// one of these names would be classified as if it were the built-in.
	SchemaAccessCheckName = "Schema access"
	// ExtensionPanicCheckName is the FAIL cliapp records when a registered
	// extension check panics: a bug, not a setting, so it classifies as
	// internal.
	ExtensionPanicCheckName = "extension doctor checks"
)

// PreflightError is the typed form of "N preflight check(s) failed", so a
// consumer can recognise a preflight refusal with errors.As instead of by its
// text. It carries the failing checks' NAMES only (the fixed check names such
// as "binlog_format=ROW"; extensions registered through cliapp add their own)
// — never a Detail, which quotes server variables, grants and hostnames. The
// message bytes are the ones the untyped error used to print.
type PreflightError struct {
	// Checks lists the failing checks' names in report order, after the
	// caller's exclusions (Err: all of them; ErrExcluding: minus the named
	// advisory checks). The count in the message is derived from it, so the
	// two cannot disagree.
	Checks []string
}

func (e *PreflightError) Error() string {
	return fmt.Sprintf("%d preflight check(s) failed", len(e.Checks))
}

// TelemetryClass implements telemetry.Classed, deriving the class from WHICH
// checks failed rather than reporting every refusal as one bucket:
// db_connection when the source or the index could not be reached,
// db_permission for grants, index write access or schema access, internal
// when a registered extension check panicked, and config_invalid for
// everything else — binlog_format, row image, log_bin, a malformed index DSN,
// an extension's own check, and on standalone `doctor` (which excludes
// nothing) the disk-capacity check: a setting the operator has to change.
// Keyed on the check NAME, so a check whose probe query itself failed grades
// as that check (config_invalid), not as the query's cause; an unreachable
// source is caught by the connection check ahead of it. Precedence when
// several fail together: connection, then internal,
// then permission, then configuration — a connection failure is the root of
// whatever failed after it (the index checks run in that order, so a
// connect failure fails both the connection and the write-access check).
// One imprecision is deliberate: the connection checks record a wrong
// password as a connection failure (the cause is stringified into Detail),
// so db_connection here reads as "could not connect as configured".
func (e *PreflightError) TelemetryClass() string {
	best, bestRank := "config_invalid", 0
	for _, name := range e.Checks {
		class, rank := checkClass(name)
		if rank > bestRank {
			best, bestRank = class, rank
		}
	}
	return best
}

// checkClass maps a failing check's name to its telemetry class and the
// class's precedence (higher wins when several checks fail together).
func checkClass(name string) (string, int) {
	switch name {
	case SourceConnectionCheckName, IndexConnectionCheckName, PGSourceConnectionCheckName:
		return "db_connection", 3
	case ExtensionPanicCheckName:
		return "internal", 2
	case ReplicationGrantsCheckName, IndexWriteAccessCheckName, SchemaAccessCheckName:
		return "db_permission", 1
	}
	return "config_invalid", 0
}

// BootRefusal is the error a daemon exits with when the preflight refuses
// boot; `up` and `bintrail-console watch` share it. The %w is load-bearing:
// it keeps the *PreflightError reachable, so the refusal keeps its
// usage-telemetry class (config_invalid, db_connection, ...) instead of
// collapsing to unknown.
func BootRefusal(fatal error) error {
	return fmt.Errorf("preflight failed (use --skip-doctor to bypass at your own risk): %w", fatal)
}

// ErrExcluding is Err but ignoring failures of the named advisory checks.
// `up`'s preflight uses it for the capacity projection: refusing to boot the
// stream over a disk forecast would manufacture the very forensic gap the
// check warns about — an unattended host reboot would crash-loop instead of
// capturing while there is still room. The standalone `doctor` command keeps
// full FAIL semantics (CI smoke tests SHOULD go red on a capacity overrun).
func (r *Report) ErrExcluding(advisory ...string) error {
	var failing []string
	for _, c := range r.Checks {
		if c.Status != StatusFail || slices.Contains(advisory, c.Name) {
			continue
		}
		failing = append(failing, c.Name)
	}
	if len(failing) == 0 {
		return nil
	}
	return &PreflightError{Checks: failing}
}
